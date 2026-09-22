package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeTransport 是本地可复现的传输替身：不依赖任何网络，
// 按脚本响应分页请求，支持注入限流/临时失败，并记录并发与幂等键。
type fakeTransport struct {
	mu         sync.Mutex
	pages      map[string]*Page // cursor -> page（cursor 为空串表示首页）
	rateLimitN int              // 剩余需要以 429 响应的次数
	failN      int              // 剩余需要以 500 响应的次数
	version    Version          // 响应版本（可与客户端不同，测试协商）

	delay       time.Duration // 每次请求的模拟处理延迟，用于制造并发重叠
	inFlight    int
	maxInFlight int            // 观测到的最大并发
	calls       []*Request     // 全部请求记录
	idemSeen    map[string]int // 幂等键 -> 次数
}

func newFakeTransport(pages map[string]*Page) *fakeTransport {
	return &fakeTransport{
		pages:    pages,
		version:  Version{Major: 1, Minor: 2},
		idemSeen: make(map[string]int),
	}
}

func (f *fakeTransport) Do(ctx context.Context, req *Request) (*Response, error) {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	f.calls = append(f.calls, req)
	f.idemSeen[req.IdempotencyKey]++
	rateLimited := f.rateLimitN > 0
	if rateLimited {
		f.rateLimitN--
	}
	failed := !rateLimited && f.failN > 0
	if failed {
		f.failN--
	}
	page := f.pages[req.Cursor]
	version := f.version
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.delay):
		}
	}

	if rateLimited {
		return &Response{Status: 429, RetryAfter: 1}, nil
	}
	if failed {
		return &Response{Status: 500}, nil
	}
	if page == nil {
		return &Response{Status: 404}, nil
	}
	// 以替身版本编码响应，模拟对端可能更新（携带新增字段）。
	body, err := EncodePage(&Page{
		Version: version,
		Cursor:  page.Cursor,
		Items:   page.Items,
		Extra:   page.Extra,
	})
	if err != nil {
		return nil, err
	}
	return &Response{Status: 200, Body: body}, nil
}

// retries 返回指定幂等键的重试次数（调用次数 - 1）。
func (f *fakeTransport) retries(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.idemSeen[key] - 1
}

// buildChain 构造 n 页链式数据：第 i 页含 items ["item-i-0", ...]，
// 游标序号从 1 开始单调递增，末页游标为空串。
// 返回 pages（游标 -> 页，空游标为首页）与按抓取顺序排列的全部 item id。
func buildChain(n, perPage int) (map[string]*Page, []string) {
	pages := make(map[string]*Page, n)
	var all []string
	prev := ""
	for i := 1; i <= n; i++ {
		var items []json.RawMessage
		for j := 0; j < perPage; j++ {
			id := fmt.Sprintf("item-%d-%d", i, j)
			all = append(all, id)
			items = append(items, json.RawMessage(fmt.Sprintf("%q", id)))
		}
		cur := ""
		if i < n {
			cur = EncodeCursor(Cursor{Seq: uint64(i), Token: fmt.Sprintf("tok-%d", i)})
		}
		pages[prev] = &Page{Cursor: cur, Items: items}
		prev = cur
	}
	return pages, all
}

// memSink 记录已提交的页，用于断言不漏不重。
type memSink struct {
	mu    sync.Mutex
	items []string
	seqs  []uint64
}

func (m *memSink) Commit(ctx context.Context, seq uint64, items []json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seqs = append(m.seqs, seq)
	for _, it := range items {
		var s string
		if err := json.Unmarshal(it, &s); err != nil {
			return err
		}
		m.items = append(m.items, s)
	}
	return nil
}

func (m *memSink) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.items...)
}

// logErr 按测试要求打印输入与归一化后的错误。
func logErr(t *testing.T, input string, err error) {
	t.Helper()
	if err == nil {
		t.Logf("input=%s -> ok", input)
		return
	}
	t.Logf("input=%s -> kind=%s retryable=%v err=%v", input, KindOf(err), Retryable(err), err)
}
