package syncsdk

import (
	"context"
	"sync"
)

// StreamID 标识一条互相独立的数据流；不同流的进度互不串用。
type StreamID string

// StreamTransport 是按数据流区分的传输层（可选）。
// 未提供时，Runner 使用 Client 共享的 Transport（忽略流标识）。
type StreamTransport interface {
	Fetch(ctx context.Context, stream StreamID, cursor string) (body []byte, err error)
}

// StreamConfig 描述一条数据流及其容量上限。
type StreamConfig struct {
	ID    StreamID
	Store CheckpointStore
	// Commit 在检查点推进前提交一页数据，须幂等；nil 表示不提交。
	Commit func(ctx context.Context, items []Item) error
	// Transport 可选：nil 时所有流共享客户端的 Transport。
	Transport StreamTransport
	// MaxInFlight 限制该流“已抓取但尚未确认”的在途页数（预取窗口），<=0 取 1。
	MaxInFlight int
	// MaxBufferedItems 限制该流已抓取未提交的条目缓冲总量；超出时抓取侧背压等待。
	// 单页自身超过该上限时允许临时超过（否则无法推进），<=0 取默认值。
	MaxBufferedItems int
}

// StopReason 是一条数据流结束的可区分原因。
type StopReason string

const (
	// StopCompleted 正常抓到尾页。
	StopCompleted StopReason = "completed"
	// StopFailed 该流自身失败（限流耗尽、传输、提交、进度异常等），不影响其他流。
	StopFailed StopReason = "failed"
	// StopCanceled 外部取消/超时：不再发起新抓取，已确认进度保留。
	StopCanceled StopReason = "canceled"
)

const defaultMaxBufferedItems = 1024

// StreamResult 是单条数据流的运行结果。
type StreamResult struct {
	Stream     StreamID
	Reason     StopReason
	Items      []Item      // 本次运行已确认（提交）的条目
	Checkpoint *Checkpoint // 已落盘的最新进度（恢复起点）
	Err        error       // Reason != StopCompleted 时的归一化错误（带流归属）
}

// Runner 在同一进程内并行同步多条互不相干的数据流。
// 各流共享一个 Client，因此整体并发与请求速率统一受 Client 配置约束；
// 每条流另有独立的在途页数与条目缓冲上限，超出即背压。
type Runner struct {
	client  *Client
	streams []StreamConfig
}

// NewRunner 校验配置并构造多流编排器。
// 每个流必须有唯一非空 ID 和非 nil Store；client 不能为 nil。
func NewRunner(client *Client, streams []StreamConfig) (*Runner, error) {
	if client == nil {
		return nil, &Error{Kind: KindTransport, Message: "runner requires a non-nil client"}
	}
	if len(streams) == 0 {
		return nil, &Error{Kind: KindTransport, Message: "runner requires at least one stream"}
	}
	ids := map[StreamID]bool{}
	for _, st := range streams {
		if st.ID == "" {
			return nil, &Error{Kind: KindTransport, Message: "stream id must not be empty"}
		}
		if ids[st.ID] {
			return nil, &Error{Kind: KindTransport, Message: "duplicate stream id: " + string(st.ID)}
		}
		ids[st.ID] = true
		if st.Store == nil {
			return nil, &Error{Kind: KindTransport, Message: "stream " + string(st.ID) + " requires a checkpoint store"}
		}
	}
	cp := make([]StreamConfig, len(streams))
	copy(cp, streams)
	return &Runner{client: client, streams: cp}, nil
}

// Run 并行运行全部数据流，返回每条流各自的结果（按配置顺序排列）。
// 外部 ctx 取消/超时会停止所有流；单条流失败只影响该流，其余流已确认的
// 进度照常落盘，调用方按 StreamResult.Reason / Err.Kind 区分结束原因。
func (r *Runner) Run(ctx context.Context) []StreamResult {
	results := make([]StreamResult, len(r.streams))
	var wg sync.WaitGroup
	for i, sc := range r.streams {
		wg.Add(1)
		go func(i int, sc StreamConfig) {
			defer wg.Done()
			results[i] = runStream(ctx, r.client, sc)
		}(i, sc)
	}
	wg.Wait()
	return results
}

// inflightToken 是一页在途预取的配额令牌。
type inflightToken struct{}

// bufferedPage 是抓取完成、等待按序提交的一页。
// 三种形态互斥：done=true（抓到尾页标记）；err!=nil（抓取终止）；否则为正常页。
type bufferedPage struct {
	page  *Page
	items []Item
	seq   uint64
	done  bool
	err   error
}

func (sc StreamConfig) maxInFlight() int {
	if sc.MaxInFlight <= 0 {
		return 1
	}
	return sc.MaxInFlight
}

func (sc StreamConfig) maxBufferedItems() int {
	if sc.MaxBufferedItems <= 0 {
		return defaultMaxBufferedItems
	}
	return sc.MaxBufferedItems
}

// tag 给归一化错误打上流归属；非 SDK 错误原样返回。
func tag(err error, id StreamID) error {
	if se, ok := err.(*Error); ok {
		se.Stream = id
		return se
	}
	return err
}

// runStream 运行单条流：抓取协程预取并受背压约束，当前协程按序提交与推进检查点。
func runStream(parent context.Context, client *Client, sc StreamConfig) StreamResult {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	res := StreamResult{Stream: sc.ID}
	load, err := sc.Store.Load(ctx)
	if err != nil {
		if parent.Err() != nil {
			res.Reason = StopCanceled
		} else {
			res.Reason = StopFailed
		}
		res.Err = tag(err, sc.ID)
		return res
	}
	if load == nil {
		load = &Checkpoint{}
	}
	if load.Cursor != "" {
		if _, _, derr := DecodeCursor(load.Cursor); derr != nil {
			res.Reason = StopFailed
			res.Err = tag(derr, sc.ID)
			return res
		}
	}

	checkpoint := &Checkpoint{Cursor: load.Cursor, Seq: load.Seq}
	res.Checkpoint = checkpoint

	maxPages := sc.maxInFlight()
	maxItems := sc.maxBufferedItems()
	pageTokens := make(chan inflightToken, maxPages)
	pending := make(chan bufferedPage, maxPages+1)
	for i := 0; i < maxPages; i++ {
		pageTokens <- inflightToken{}
	}

	var bufMu sync.Mutex
	bufItems := 0
	bufChanged := sync.NewCond(&bufMu)
	// waitRoom 阻塞直到缓冲能容纳 n 个条目或 ctx 结束（返回 false）。
	waitRoom := func(n int) bool {
		bufMu.Lock()
		for bufItems > 0 && bufItems+n > maxItems {
			bufChanged.Broadcast() // 提交侧可能正等待锁，保险唤醒
			bufChanged.Wait()
			if ctx.Err() != nil {
				bufMu.Unlock()
				return false
			}
		}
		bufItems += n
		bufMu.Unlock()
		return true
	}
	free := func(n int) {
		if n <= 0 {
			return
		}
		bufMu.Lock()
		if n > bufItems {
			n = bufItems
		}
		bufItems -= n
		bufChanged.Broadcast()
		bufMu.Unlock()
	}

	seen := map[string]bool{}

	// ctx 取消时唤醒所有阻塞在背压条件上的抓取协程，避免永久等待。
	stopWake := make(chan struct{})
	defer close(stopWake)
	go func() {
		select {
		case <-ctx.Done():
			bufMu.Lock()
			bufChanged.Broadcast()
			bufMu.Unlock()
		case <-stopWake:
		}
	}()

	// 抓取协程：持有游标、预取窗口与条目背压。
	// 所有 pending 发送都同时监听 ctx.Done：提交侧失败退出后 ctx 会被取消，
	// 抓取协程即便正阻塞在满 channel 上也必须能退出，不能泄漏。
	sendPage := func(bp bufferedPage) bool {
		select {
		case pending <- bp:
			return true
		case <-ctx.Done():
			return false
		}
	}
	go func() {
		cursor := load.Cursor
		seq := load.Seq
		for {
			select {
			case <-ctx.Done():
				sendPage(bufferedPage{err: &Error{Kind: KindCanceled, Message: "canceled before fetching next page", Cause: ctx.Err()}})
				return
			case tok := <-pageTokens:
				_ = tok
			}

			page, ferr := client.FetchPageOn(ctx, sc.ID, cursor, sc.Transport)
			if ferr != nil {
				sendPage(bufferedPage{err: ferr})
				return
			}

			// 页内按 ID 去重（抓取顺序即页顺序），与单流 Syncer 语义一致。
			fresh := make([]Item, 0, len(page.Items))
			for _, it := range page.Items {
				if seen[it.ID] {
					continue
				}
				seen[it.ID] = true
				fresh = append(fresh, it)
			}

			if !waitRoom(len(fresh)) {
				sendPage(bufferedPage{err: &Error{Kind: KindCanceled, Message: "canceled while applying backpressure", Cause: ctx.Err()}})
				return
			}
			seq += uint64(len(fresh))
			if !sendPage(bufferedPage{page: page, items: fresh, seq: seq}) {
				return
			}

			if page.NextCursor == "" {
				sendPage(bufferedPage{done: true})
				return
			}
			cursor = page.NextCursor
		}
	}()

	committed := []Item{}
	for {
		// 同时监听取消：抓取协程可能因 ctx 已取消而不再投递消息，
		// 提交协程不能只阻塞在 pending 上，否则 Run 永远等不到结束。
		select {
		case <-ctx.Done():
			res.Reason = StopCanceled
			res.Err = tag(&Error{Kind: KindCanceled, Message: "canceled; committed progress retained", Cause: parent.Err()}, sc.ID)
			res.Items = committed
			return res
		case bp := <-pending:
			switch {
			case bp.err != nil:
				// 抓取终止：错误归类决定结束原因。
				if parent.Err() != nil || IsKind(bp.err, KindCanceled) {
					res.Reason = StopCanceled
				} else {
					res.Reason = StopFailed
					cancel() // 停止本流后续一切抓取
				}
				res.Err = tag(bp.err, sc.ID)
				res.Items = committed
				return res
			case bp.done:
				res.Reason = StopCompleted
				res.Items = committed
				return res
			}

			page := bp.page
			fresh := bp.items
			// 顺序语义：先提交数据，成功后才推进检查点。
			if sc.Commit != nil && len(fresh) > 0 {
				if cerr := sc.Commit(ctx, fresh); cerr != nil {
					free(len(fresh))
					cancel()
					res.Reason = StopFailed
					res.Err = tag(&Error{Kind: KindTransport, Message: "commit failed", Cause: cerr}, sc.ID)
					res.Items = committed
					return res
				}
			}
			committed = append(committed, fresh...)
			free(len(fresh))

			next := &Checkpoint{Cursor: page.NextCursor, Seq: bp.seq}
			if page.NextCursor != "" {
				if verr := ValidateResume(checkpoint, next); verr != nil {
					cancel()
					res.Reason = StopFailed
					res.Err = tag(verr, sc.ID)
					res.Items = committed
					return res
				}
			}
			if serr := sc.Store.Save(ctx, next); serr != nil {
				cancel()
				if parent.Err() != nil || ctx.Err() != nil {
					res.Reason = StopCanceled
					res.Err = tag(&Error{Kind: KindCanceled, Message: "canceled while saving checkpoint", Cause: parent.Err()}, sc.ID)
				} else {
					res.Reason = StopFailed
					res.Err = tag(&Error{Kind: KindTransport, Message: "save checkpoint failed", Cause: serr}, sc.ID)
				}
				res.Items = committed
				return res
			}
			checkpoint = next
			res.Checkpoint = next
			// 页面已确认，释放一个在途页令牌；若 ctx 已结束则抓取协程不再需要它。
			select {
			case pageTokens <- inflightToken{}:
			case <-ctx.Done():
			}
		}
	}
}
