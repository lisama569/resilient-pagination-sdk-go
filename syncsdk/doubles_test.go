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
	pages     map[string]string  // cursor -> body；cursor "" 为首页
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
	if m.rejectRegress && m.cp != nil && m.cp.Cursor != "" {
		if err := ValidateResume(m.cp, cp); err != nil {
			return err
		}
	}
	c := *cp
	m.cp = &c
	atomic.AddInt32(&m.saves, 1)
	return nil
}

// multiMemStore 是多数据流的内存检查点替身，按 StreamID 隔离，
// 并在 Save 时强制单调推进（回退拒绝）。
type multiMemStore struct {
	mu     sync.Mutex
	cps    map[StreamID]*Checkpoint
	failOn map[StreamID]error // 注入 Save 失败
	saves  map[StreamID]int
}

func newMultiMemStore() *multiMemStore {
	return &multiMemStore{cps: map[StreamID]*Checkpoint{}, failOn: map[StreamID]error{}, saves: map[StreamID]int{}}
}

func (m *multiMemStore) LoadStream(ctx context.Context, id StreamID) (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.cps[id]
	if !ok {
		return nil, nil
	}
	out := *cp
	return &out, nil
}

func (m *multiMemStore) SaveStream(ctx context.Context, id StreamID, cp *Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e := m.failOn[id]; e != nil {
		return e
	}
	if prev := m.cps[id]; prev != nil {
		if err := ValidateSave(prev, cp); err != nil {
			return err
		}
	}
	c := *cp
	m.cps[id] = &c
	m.saves[id]++
	return nil
}

func (m *multiMemStore) ListStreams(ctx context.Context) ([]StreamID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]StreamID, 0, len(m.cps))
	for id := range m.cps {
		ids = append(ids, id)
	}
	return ids, nil
}

// multiCommitSink 按流记录已提交 ID，模拟各流独立、幂等的 sink。
type multiCommitSink struct {
	mu       sync.Mutex
	byStream map[StreamID][]string
	seen     map[StreamID]map[string]bool
	// failAfter 为每条流配置在第 n 批提交时返回的错误（0 表示不注入）。
	failAfter map[StreamID]int
	batch     map[StreamID]int
}

func newMultiCommitSink() *multiCommitSink {
	return &multiCommitSink{
		byStream:  map[StreamID][]string{},
		seen:      map[StreamID]map[string]bool{},
		failAfter: map[StreamID]int{},
		batch:     map[StreamID]int{},
	}
}

func (s *multiCommitSink) commit(ctx context.Context, id StreamID, items []Item) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batch[id]++
	if n := s.failAfter[id]; n > 0 && s.batch[id] == n {
		return fmt.Errorf("sink %s failure at batch %d", id, n)
	}
	if s.seen[id] == nil {
		s.seen[id] = map[string]bool{}
	}
	for _, it := range items {
		if !s.seen[id][it.ID] {
			s.seen[id][it.ID] = true
			s.byStream[id] = append(s.byStream[id], it.ID)
		}
	}
	return nil
}

func (s *multiCommitSink) ids(id StreamID) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]string(nil), s.byStream[id]...)
	return out
}

// dispatchingTransport 按游标前缀把请求路由到各自独立的 fakeTransport，
// 用于模拟同一个共享 Client 背后多条互不相干的数据流；
// 同时统计聚合后的全局并发，验证总并发不超过共享 Client 上限。
type dispatchingTransport struct {
	mu       sync.Mutex
	streams  map[string]*fakeTransport
	prefixOf map[string]string // cursor -> stream 前缀
	curConc  int
	maxConc  int
	delay    time.Duration
	fetches  atomic.Int64 // 独立原子计数，背压观测用，避免与内层 ft 锁争用
}

func newDispatchingTransport() *dispatchingTransport {
	return &dispatchingTransport{streams: map[string]*fakeTransport{}, prefixOf: map[string]string{}}
}

func (d *dispatchingTransport) add(name string) *fakeTransport {
	ft := newFakeTransport()
	d.streams[name] = ft
	return ft
}

// streamBase 给每条流分配一个不相交的 offset 区段，保证跨流游标全局唯一且合法。
func streamBase(index int) int { return index * 1_000_000 }

// setupStream 在 dispatcher 中为某条流建链：首页游标为 EncodeCursor(base,2)，
// 后续页 offset 依次为 base+pageSize、base+2*pageSize……
// 返回该流的起始（首页）游标，用作 StreamSpec.StartCursor。
// 每个游标都注册到精确路由表，游标本身始终是合法编码，可过 DecodeCursor 校验。
func (d *dispatchingTransport) setupStream(name string, base, pageSize, total int) string {
	ft := d.streams[name]
	cursors := []string{}
	for off := base; ; off += pageSize {
		cur := EncodeCursor(off, 2)
		d.prefixOf[cur] = name
		cursors = append(cursors, cur)
		if off+pageSize >= base+total {
			break
		}
	}
	for i := len(cursors) - 1; i >= 0; i-- {
		next := ""
		if i+1 < len(cursors) {
			next = cursors[i+1]
		}
		ids := []string{}
		start := base + i*pageSize
		for j := start; j < start+pageSize && j < base+total; j++ {
			ids = append(ids, fmt.Sprintf("%s-id-%06d", name, j-base))
		}
		ft.addPage(cursors[i], 2, next, ids...)
	}
	return cursors[0]
}

// injectFailureAt 直接对某条流指定页序（0 起）注入失败。
func (d *dispatchingTransport) injectFailureAt(name string, base, pageIndex, pageSize int, errs ...error) {
	cur := EncodeCursor(base+pageIndex*pageSize, 2)
	d.streams[name].injectFailure(cur, errs...)
}

func (d *dispatchingTransport) Fetch(ctx context.Context, cursor string) ([]byte, error) {
	name := d.prefixOf[cursor]
	ft := d.streams[name]
	if ft == nil {
		return nil, fmt.Errorf("dispatcher: no stream for cursor %q", cursor)
	}
	d.fetches.Add(1)
	d.mu.Lock()
	d.curConc++
	if d.curConc > d.maxConc {
		d.maxConc = d.curConc
	}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.curConc--
		d.mu.Unlock()
	}()
	if d.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.delay):
		}
	}
	return ft.Fetch(ctx, cursor)
}

// totalFetches 返回所有流累计发起的抓取次数（无锁、原子）。
func (d *dispatchingTransport) totalFetches() int { return int(d.fetches.Load()) }
