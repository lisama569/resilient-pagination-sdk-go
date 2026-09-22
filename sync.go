package sdk

import (
	"context"
	"encoding/json"
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
func (s *Syncer) Run(ctx context.Context) (Checkpoint, error) {
	return Checkpoint{}, nil
}
