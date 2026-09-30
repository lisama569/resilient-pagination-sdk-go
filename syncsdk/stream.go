package syncsdk

import "context"

// StreamID 标识一条相互独立的数据流。各流的进度互相隔离，不得串用。
type StreamID string

// CommitFunc 提交一条流的一页数据；须幂等（重启后同一批可能重复提交）。
type CommitFunc func(ctx context.Context, streamID StreamID, items []Item) error

// StopReason 是一次流运行可区分的结束原因。
type StopReason string

const (
	// StopCompleted 正常抓到尾页。
	StopCompleted StopReason = "completed"
	// StopCanceled 外部取消或超时触发的收尾。
	StopCanceled StopReason = "canceled"
	// StopFailed 因错误中止；已确认进度仍然保留。
	StopFailed StopReason = "failed"
)

// StreamSpec 描述一条待同步的数据流及其容量上限。
type StreamSpec struct {
	ID     StreamID
	Commit CommitFunc
	// StartCursor 首游标；空串表示从头开始。
	StartCursor string
	// MaxInFlight 已抓取但尚未确认（提交+存盘）的在途页上限，<=0 取 1。
	MaxInFlight int
	// MaxBuffered 已抓取但尚未确认的缓冲条目上限，<=0 表示不限制。
	MaxBuffered int
}

// StreamCheckpointStore 是多数据流的检查点持久化抽象。
// 每个 StreamID 的进度命名空间互相独立。
type StreamCheckpointStore interface {
	LoadStream(ctx context.Context, id StreamID) (*Checkpoint, error)
	// SaveStream 仅做单调推进：回退或内容对不上时显式拒绝。
	SaveStream(ctx context.Context, id StreamID, cp *Checkpoint) error
	ListStreams(ctx context.Context) ([]StreamID, error)
}
