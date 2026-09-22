package sdk

import (
	"context"
	"encoding/json"
	"fmt"
)

// Sink 接收并提交一页数据。Commit 成功后才允许推进检查点，
// 保证“先提交数据、后推进游标”的顺序语义。
type Sink interface {
	Commit(ctx context.Context, seq uint64, items []json.RawMessage) error
}

// Syncer 编排抓取、提交与检查点推进，支持断点恢复。
type Syncer struct {
	client *Client
	store  CheckpointStore
	sink   Sink
}

// NewSyncer 创建同步器。
func NewSyncer(c *Client, store CheckpointStore, sink Sink) *Syncer {
	return &Syncer{client: c, store: store, sink: sink}
}

// Run 从上次检查点恢复并同步到末尾。重启或重复消费不漏不重；
// 游标回退或失效被显式拒绝。返回已确认的进度。
//
// 顺序语义：每一页先 Sink.Commit 提交数据，成功后才 CheckpointStore.Save
// 推进游标；任一步失败都会终止且进度停留在最后一个已确认位置。
func (s *Syncer) Run(ctx context.Context) (Checkpoint, error) {
	const op = "Syncer.Run"

	cp, err := s.store.Load(ctx)
	if err != nil {
		return cp, err
	}
	if cp.Done {
		return cp, nil // 已完成：重复消费直接返回，不产生重复数据
	}
	// 恢复点校验：非空游标必须可解码且序号一致，失效游标被显式拒绝。
	if cp.Cursor != "" {
		if _, err := DecodeCursor(cp.Cursor); err != nil {
			return cp, wrap(ErrKindCursorInvalid, op, "检查点游标失效，拒绝从中间继续", err)
		}
	}

	for {
		page, err := s.client.FetchPage(ctx, cp.Cursor)
		if err != nil {
			return cp, err
		}

		// 单调性校验：服务端返回的下一游标序号必须严格前进。
		next := Checkpoint{Done: page.Cursor == ""}
		if page.Cursor != "" {
			c, err := DecodeCursor(page.Cursor)
			if err != nil {
				return cp, wrap(ErrKindCursorInvalid, op, "服务端返回的游标非法", err)
			}
			if c.Seq <= cp.Seq {
				return cp, wrap(ErrKindCursorRegression, op,
					fmt.Sprintf("游标回退：当前序号 %d，服务端返回 %d", cp.Seq, c.Seq), nil)
			}
			next.Cursor = page.Cursor
			next.Seq = c.Seq
		} else {
			next.Seq = cp.Seq + 1
		}

		// 先提交数据，再推进检查点。
		if err := s.sink.Commit(ctx, cp.Seq+1, page.Items); err != nil {
			return cp, wrap(KindOf(err), op, "提交数据失败，检查点未推进", err)
		}
		if err := s.store.Save(ctx, next); err != nil {
			return cp, err
		}
		cp = next

		if cp.Done {
			return cp, nil
		}
	}
}
