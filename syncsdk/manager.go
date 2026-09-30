package syncsdk

import (
	"context"
	"strconv"
	"sync"
)

// StreamOutcome 是一条流的最终结果：结束原因、已确认进度与分类错误。
type StreamOutcome struct {
	Stream StreamID
	Result *Result
	// Err 为 nil 表示该流正常完成；否则可用 KindOf 区分问题类别、
	// Err.(*Error).Stream 区分是哪条流。
	Err error
}

// Manager 在同一进程内并行同步多条互相独立的数据流，
// 共享一个 Client（全局并发/速率上限不被突破），各流进度与失败互相隔离。
type Manager struct {
	client *Client
	store  StreamCheckpointStore
}

// NewManager 构造多流管理器。store 必须按 StreamID 隔离进度。
func NewManager(client *Client, store StreamCheckpointStore) *Manager {
	return &Manager{client: client, store: store}
}

// RunAll 并行运行所有流，等待全部收尾后返回按 StreamID 索引的结果。
//
//   - 各流有各自的检查点与提交回调，进度互不串用；
//   - 某条流因限流耗尽/超时/取消/提交失败而中止时，不影响其他流，
//     其已确认进度已经落盘；
//   - ctx 是所有流共享的总控：取消它会同时收尾所有流。
func (m *Manager) RunAll(ctx context.Context, specs []StreamSpec) map[StreamID]*StreamOutcome {
	var wg sync.WaitGroup
	var mu sync.Mutex
	out := map[StreamID]*StreamOutcome{}
	seenID := map[StreamID]bool{}

	for i := range specs {
		spec := specs[i]
		// 参数错误也以“该流的失败结果”形式返回，不影响其他流启动。
		if spec.ID == "" {
			mu.Lock()
			out[StreamID(anonKey(i))] = &StreamOutcome{
				Stream: StreamID(anonKey(i)),
				Result: &Result{Checkpoint: &Checkpoint{}, Stop: StopFailed},
				Err:    &Error{Kind: KindMalformed, Message: "stream id must not be empty"},
			}
			mu.Unlock()
			continue
		}
		if seenID[spec.ID] {
			mu.Lock()
			out[spec.ID] = &StreamOutcome{
				Stream: spec.ID,
				Result: &Result{Checkpoint: &Checkpoint{}, Stop: StopFailed},
				Err:    &Error{Kind: KindMalformed, Message: "duplicate stream id: " + string(spec.ID), Stream: spec.ID},
			}
			mu.Unlock()
			continue
		}
		seenID[spec.ID] = true

		wg.Add(1)
		go func(spec StreamSpec) {
			defer wg.Done()
			r := &streamRunner{client: m.client, store: m.store, spec: spec}
			res, err := r.run(ctx)
			mu.Lock()
			out[spec.ID] = &StreamOutcome{Stream: spec.ID, Result: res, Err: err}
			mu.Unlock()
		}(spec)
	}
	wg.Wait()
	return out
}

// anonKey 给没有 ID 的流生成占位键（仅用于返回值索引，不应与合法 ID 冲突）。
func anonKey(i int) string { return "__empty_stream_" + strconv.Itoa(i) + "__" }
