package syncsdk

import (
	"context"
)

// Result 是一次同步运行的汇总。
type Result struct {
	Items      []Item
	Checkpoint *Checkpoint
	Completed  bool // true 表示抓到了最后一页
	// Stop 是可区分的结束原因；单流 Run 默认填 StopCompleted/StopCanceled/StopFailed。
	Stop StopReason
}

// Syncer 编排分页抓取与检查点推进。
// 顺序语义：先提交数据，再推进检查点；中断时已确认进度被保留。
type Syncer struct {
	client *Client
	store  CheckpointStore
	// Commit 在检查点推进前提交一页数据；须幂等（同一批 Item 可能被重复提交）。
	Commit func(ctx context.Context, items []Item) error
}

// NewSyncer 构造同步器。
func NewSyncer(client *Client, store CheckpointStore, commit func(ctx context.Context, items []Item) error) *Syncer {
	return &Syncer{client: client, store: store, Commit: commit}
}

// Run 从检查点恢复并同步到尾页或中断。
// 中断（取消/超时）时返回已确认的进度，不丢失也不重复。
func (s *Syncer) Run(ctx context.Context) (*Result, error) {
	cp, err := s.store.Load(ctx)
	if err != nil {
		return nil, &Error{Kind: KindTransport, Message: "load checkpoint failed", Cause: err}
	}
	if cp == nil {
		cp = &Checkpoint{}
	} else if cp.Cursor == "" && cp.Seq > 0 {
		// 已持久化完成终态：重启不再抓取，直接返回完成。
		return &Result{Checkpoint: &Checkpoint{Cursor: "", Seq: cp.Seq}, Completed: true, Stop: StopCompleted}, nil
	}
	// 恢复前校验：持久化的游标本身必须合法。
	if cp.Cursor != "" {
		if _, _, err := DecodeCursor(cp.Cursor); err != nil {
			return nil, err
		}
	}

	res := &Result{Checkpoint: &Checkpoint{Cursor: cp.Cursor, Seq: cp.Seq}}
	seen := map[string]bool{}
	cursor := cp.Cursor
	seq := cp.Seq

	for {
		page, err := s.client.FetchPage(ctx, cursor)
		if err != nil {
			// 已提交页的进度已持久化，安全返回部分结果。
			if IsKind(err, KindCanceled) {
				res.Stop = StopCanceled
			} else {
				res.Stop = StopFailed
			}
			return res, err
		}

		// 页内按 ID 去重，防止服务端重发造成重复数据。
		fresh := make([]Item, 0, len(page.Items))
		for _, it := range page.Items {
			if seen[it.ID] {
				continue
			}
			seen[it.ID] = true
			fresh = append(fresh, it)
		}

		// 顺序语义：先提交数据，成功后才能推进检查点。
		if s.Commit != nil && len(fresh) > 0 {
			if err := s.Commit(ctx, fresh); err != nil {
				res.Stop = StopFailed
				return res, &Error{Kind: KindTransport, Message: "commit failed", Cause: err}
			}
		}
		seq += uint64(len(fresh))
		res.Items = append(res.Items, fresh...)

		next := &Checkpoint{Cursor: page.NextCursor, Seq: seq}
		if page.NextCursor != "" {
			// 游标回退或失效必须显式拒绝，不得从中间继续。
			if err := ValidateResume(&Checkpoint{Cursor: cursor, Seq: res.Checkpoint.Seq}, next); err != nil {
				res.Stop = StopFailed
				return res, err
			}
		}
		if err := s.store.Save(ctx, next); err != nil {
			res.Stop = StopFailed
			return res, &Error{Kind: KindTransport, Message: "save checkpoint failed", Cause: err}
		}
		res.Checkpoint = next

		if page.NextCursor == "" {
			res.Completed = true
			res.Stop = StopCompleted
			return res, nil
		}
		cursor = page.NextCursor
	}
}
