package syncsdk

import (
	"context"
)

// fetchedPage 是抓取协程交给提交协程的一页成果或终止信号。
type fetchedPage struct {
	page *Page // 成功抓到的页
	err  error // 终止信号；page==nil 且 err==nil 表示正常抓完
	// held 为本页新鲜条目占用的缓冲配额，提交/丢弃后归还。
	held int
}

// streamRunner 编排单条数据流。
//
// 容量与背压：
//   - MaxInFlight 限定已抓取但尚未确认（提交+存盘）的在途页数；
//   - MaxBuffered 限定已抓取但尚未确认的本地缓冲条目数；
//   - 任一配额耗尽，抓取协程阻塞，不再向服务端发新请求（背压，不无限堆积）。
//
// 停止语义：
//   - 外部取消/超时：不再发起新抓取，已确认进度保留，返回 StopCanceled；
//   - 抓取失败：先把已抓到的页确认落盘，再返回 StopFailed；
//   - 提交/存盘失败：立即收尾，返回 StopFailed；其它流不受影响。
type streamRunner struct {
	client *Client
	store  StreamCheckpointStore
	spec   StreamSpec
}

// tag 把错误归属到当前流，便于多流下区分“哪条流、哪类问题”。
func (r *streamRunner) tag(err error) error {
	if err == nil {
		return nil
	}
	if se, ok := err.(*Error); ok {
		cp := *se
		cp.Stream = r.spec.ID
		return &cp
	}
	return &Error{Kind: KindTransport, Message: "unclassified failure", Cause: err, Stream: r.spec.ID}
}

// run 执行一条流，任何情况下都会返回（部分）结果以及可区分的结束原因。
func (r *streamRunner) run(ctx context.Context) (*Result, error) {
	if r.spec.ID == "" {
		return &Result{Checkpoint: &Checkpoint{}, Stop: StopFailed},
			r.tag(&Error{Kind: KindMalformed, Message: "stream id must not be empty"})
	}

	cp, err := r.store.LoadStream(ctx, r.spec.ID)
	if err != nil {
		return &Result{Checkpoint: &Checkpoint{}, Stop: StopFailed}, r.tag(err)
	}
	if cp == nil {
		cp = &Checkpoint{Cursor: r.spec.StartCursor}
	} else if cp.Cursor == "" {
		// 存储中已有记录且游标为空：该流此前已抓到尾页完成，重启不再抓取。
		return &Result{Checkpoint: &Checkpoint{Cursor: "", Seq: cp.Seq}, Completed: true, Stop: StopCompleted}, nil
	}

	res := &Result{Checkpoint: &Checkpoint{Cursor: cp.Cursor, Seq: cp.Seq}, Stop: StopFailed}

	maxInFlight := r.spec.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	inflight := make(chan struct{}, maxInFlight)
	// MaxBuffered<=0 表示不限制条目缓冲（nil 通道实现永不阻塞）。
	var itemTokens chan struct{}
	if r.spec.MaxBuffered > 0 {
		itemTokens = make(chan struct{}, r.spec.MaxBuffered)
	}

	// abort 由提交协程在发生致命错误（提交/存盘失败）或取消时关闭，
	// 通知抓取协程不要再预取后续页。
	abort := make(chan struct{})
	pages := make(chan fetchedPage)
	seen := map[string]bool{}

	// ---------------- 抓取协程：串行游标链 + 有界预取 ----------------
	go func() {
		defer close(pages)
		cursor := cp.Cursor
		for {
			select {
			case <-abort:
				return
			default:
			}
			// 在途页配额：满则背压，暂停抓取。
			select {
			case inflight <- struct{}{}:
			case <-ctx.Done():
				pages <- fetchedPage{err: r.tag(&Error{Kind: KindCanceled, Message: "canceled while acquiring inflight slot", Cause: ctx.Err()})}
				return
			case <-abort:
				return
			}
			page, ferr := r.client.FetchPage(ctx, cursor)
			if ferr != nil {
				<-inflight // 该页未进入流水线，归还在途配额
				pages <- fetchedPage{err: r.tag(ferr)}
				return
			}

			// 页内按 ID 去重，防止服务端重发造成重复。
			fresh := make([]Item, 0, len(page.Items))
			for _, it := range page.Items {
				if seen[it.ID] {
					continue
				}
				seen[it.ID] = true
				fresh = append(fresh, it)
			}
			// 缓冲条目配额：满则背压，不再堆积；MaxBuffered<=0 表示不限制。
			held := 0
			if itemTokens == nil {
				held = len(fresh)
			} else {
				for range fresh {
					select {
					case itemTokens <- struct{}{}:
						held++
					case <-ctx.Done():
						page.Items = fresh
						pages <- fetchedPage{page: page, held: held,
							err: r.tag(&Error{Kind: KindCanceled, Message: "canceled while buffering items", Cause: ctx.Err()})}
						return
					case <-abort:
						page.Items = fresh
						pages <- fetchedPage{page: page, held: held}
						return
					}
				}
			}
			page.Items = fresh

			if page.NextCursor == "" {
				// 尾页：发送后由主循环确认并收尾，在途配额在确认/排空处归还。
				pages <- fetchedPage{page: page, held: held}
				return
			}
			pages <- fetchedPage{page: page, held: held}
			cursor = page.NextCursor
		}
	}()

	// ---------------- 提交协程（主循环）：先提交、后存盘 ----------------
	// stopping 为 true 后只排空 pages（释放配额、让抓取协程正常退出），
	// 不再提交或存盘，避免抓取协程阻塞在无缓冲发送上而泄漏。
	stopping := false
	var fatal error
	for fp := range pages {
		// 归还本页占用的配额（在途页 + 缓冲条目）。
		if fp.page != nil {
			<-inflight
			for i := 0; i < fp.held; i++ {
				if itemTokens != nil {
					<-itemTokens
				}
			}
		}
		if stopping {
			continue
		}

		// 终止信号（抓取结束/失败/取消）。此前已接收的页均已确认。
		if fp.err != nil || fp.page == nil {
			stopping = true
			switch {
			case IsKind(fp.err, KindCanceled) || ctx.Err() != nil:
				fatal = r.tag(&Error{Kind: KindCanceled, Message: "stream interrupted by cancellation or timeout", Cause: ctx.Err()})
			case fp.err != nil:
				fatal = fp.err
			}
			continue
		}

		// 外部取消一旦发生：不再发起新抓取（抓取协程随 ctx 退出），
		// 已经在途但未确认的页直接丢弃（重启会从已存盘游标重新抓，不丢不重）。
		if ctx.Err() != nil {
			stopping = true
			select {
			case <-abort:
			default:
				close(abort)
			}
			fatal = r.tag(&Error{Kind: KindCanceled, Message: "stream interrupted by cancellation or timeout", Cause: ctx.Err()})
			continue
		}

		// 顺序语义：先提交数据，成功后才能推进检查点。
		if r.spec.Commit != nil && len(fp.page.Items) > 0 {
			if cerr := r.spec.Commit(ctx, r.spec.ID, fp.page.Items); cerr != nil {
				stopping = true
				select {
				case <-abort:
				default:
					close(abort)
				}
				if ctx.Err() != nil || IsKind(cerr, KindCanceled) {
					fatal = r.tag(&Error{Kind: KindCanceled, Message: "canceled during commit", Cause: ctx.Err()})
				} else {
					fatal = r.tag(&Error{Kind: KindTransport, Message: "commit failed", Cause: cerr})
				}
				continue
			}
		}

		seq := res.Checkpoint.Seq + uint64(len(fp.page.Items))
		next := &Checkpoint{Cursor: fp.page.NextCursor, Seq: seq}
		if fp.page.NextCursor != "" {
			// 游标回退或失效必须显式拒绝，不得从中间继续。
			if verr := ValidateResume(res.Checkpoint, next); verr != nil {
				stopping = true
				select {
				case <-abort:
				default:
					close(abort)
				}
				fatal = r.tag(verr)
				continue
			}
		}
		if serr := r.store.SaveStream(ctx, r.spec.ID, next); serr != nil {
			stopping = true
			select {
			case <-abort:
			default:
				close(abort)
			}
			fatal = r.tag(serr)
			continue
		}
		res.Checkpoint = next
		res.Items = append(res.Items, fp.page.Items...)

		if fp.page.NextCursor == "" {
			res.Completed = true
			res.Stop = StopCompleted
			stopping = true
		}
	}

	if fatal != nil {
		if IsKind(fatal, KindCanceled) {
			res.Stop = StopCanceled
		} else {
			res.Stop = StopFailed
		}
		return res, fatal
	}
	return res, nil
}
