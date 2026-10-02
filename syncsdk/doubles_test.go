package syncsdk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// fakeTransport 本地可复现的分页服务替身。
// 按页脚本响应，可注入临时/永久失败，并统计并发与请求时间。
type fakeTransport struct {
	mu        sync.Mutex
	pages     map[string]string // cursor -> body；cursor "" 为首页
	failures  map[string][]error // cursor -> 按次序注入的失败
	calls     map[string]int
	curConc   int
	maxConc   int
	callTimes []time.Time
	delay     time.Duration // 每次调用的模拟耗时
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{pages: map[string]string{}, failures: map[string][]error{}, calls: map[string]int{}}
}

// addPage 注册一页：items 为 id 列表，next 为下一页游标（"" 表示尾页）。
func (f *fakeTransport) addPage(cursor string, version int, next string, ids ...string) {
	items := make([]string, len(ids))
	for i, id := range ids {
		items[i] = fmt.Sprintf(`{"id":%q,"data":{"n":%d}}`, id, i)
	}
	f.pages[cursor] = fmt.Sprintf(`{"version":%d,"items":[%s],"next_cursor":%q}`,
		version, strings.Join(items, ","), next)
}

func (f *fakeTransport) injectFailure(cursor string, errs ...error) {
	f.failures[cursor] = append(f.failures[cursor], errs...)
}

func (f *fakeTransport) Fetch(ctx context.Context, cursor string) ([]byte, error) {
	f.mu.Lock()
	f.curConc++
	if f.curConc > f.maxConc {
		f.maxConc = f.curConc
	}
	f.calls[cursor]++
	f.callTimes = append(f.callTimes, time.Now())
	n := f.calls[cursor]
	var fail error
	if fs := f.failures[cursor]; len(fs) >= n {
		fail = fs[n-1]
	}
	f.mu.Unlock()

	if f.delay > 0 {
		select {
		case <-ctx.Done():
			f.mu.Lock()
			f.curConc--
			f.mu.Unlock()
			return nil, ctx.Err()
		case <-time.After(f.delay):
		}
	}

	f.mu.Lock()
	f.curConc--
	body, ok := f.pages[cursor]
	f.mu.Unlock()
	if fail != nil {
		return nil, fail
	}
	if !ok {
		return nil, fmt.Errorf("no such page for cursor %q", cursor)
	}
	return []byte(body), nil
}

func (f *fakeTransport) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

// memStore 本地检查点存储替身，带乐观推进校验。
type memStore struct {
	mu sync.Mutex
	cp *Checkpoint
	// rejectRegress 为 true 时拒绝回退的 Save（模拟真实存储的约束）。
	rejectRegress bool
	saves         int32
}

func (m *memStore) Load(ctx context.Context) (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cp == nil {
		return nil, nil
	}
	cp := *m.cp
	return &cp, nil
}

func (m *memStore) Save(ctx context.Context, cp *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rejectRegress && m.cp != nil {
		// 与 FileStore 同一套单调性/终态规则：完成态（空游标）是终态，
		// 回退到中途游标或改变已完成 seq 都拒绝，防止替身放过真实存储会拦的进度。
		if err := monotonic(m.cp, cp); err != nil {
			return err
		}
	}
	c := *cp
	m.cp = &c
	atomic.AddInt32(&m.saves, 1)
	return nil
}
