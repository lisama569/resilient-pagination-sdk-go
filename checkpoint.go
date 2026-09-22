package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync"
)

// Checkpoint 记录已确认的同步进度。
type Checkpoint struct {
	Cursor string // 下一页待抓取的游标；空串表示从头开始（Done=false）或已完成（Done=true）
	Seq    uint64 // 已确认的页序号
	Done   bool   // 是否已同步到末尾
}

// CheckpointStore 持久化检查点。Save 必须拒绝游标回退。
type CheckpointStore interface {
	Load(ctx context.Context) (Checkpoint, error)
	Save(ctx context.Context, cp Checkpoint) error
}

// validateCheckpoint 校验待保存的检查点：
// 非空游标必须可解码且序号与 Seq 一致；回退（序号小于已确认进度）被拒绝。
func validateCheckpoint(op string, cur, next Checkpoint) error {
	if next.Cursor != "" {
		c, err := DecodeCursor(next.Cursor)
		if err != nil {
			return err // 已是 ErrKindCursorInvalid
		}
		if c.Seq != next.Seq {
			return wrap(ErrKindCursorInvalid, op,
				fmt.Sprintf("游标序号 %d 与检查点序号 %d 不一致", c.Seq, next.Seq), nil)
		}
	}
	if next.Seq < cur.Seq {
		return wrap(ErrKindCursorRegression, op,
			fmt.Sprintf("游标回退：已确认序号 %d，拒绝保存序号 %d", cur.Seq, next.Seq), nil)
	}
	return nil
}

// MemoryStore 是进程内检查点存储（用于测试与嵌入式场景），协程安全。
type MemoryStore struct {
	mu sync.Mutex
	cp Checkpoint
}

// NewMemoryStore 创建内存检查点存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (m *MemoryStore) Load(ctx context.Context) (Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, wrap(ErrKindCanceled, "MemoryStore.Load", "已取消", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cp, nil
}

func (m *MemoryStore) Save(ctx context.Context, cp Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return wrap(ErrKindCanceled, "MemoryStore.Save", "已取消", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := validateCheckpoint("MemoryStore.Save", m.cp, cp); err != nil {
		return err
	}
	m.cp = cp
	return nil
}

// FileStore 以原子写（临时文件 + rename）持久化检查点，支持重启恢复。
type FileStore struct {
	Path string
	mu   sync.Mutex
}

// NewFileStore 创建文件检查点存储。
func NewFileStore(path string) *FileStore { return &FileStore{Path: path} }

func (f *FileStore) Load(ctx context.Context) (Checkpoint, error) {
	const op = "FileStore.Load"
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, wrap(ErrKindCanceled, op, "已取消", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Checkpoint{}, nil // 首次运行：从零开始
	}
	if err != nil {
		return Checkpoint{}, wrap(ErrKindCheckpoint, op, "读取检查点失败", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, wrap(ErrKindCheckpoint, op, "检查点文件损坏", err)
	}
	return cp, nil
}

func (f *FileStore) Save(ctx context.Context, cp Checkpoint) error {
	const op = "FileStore.Save"
	if err := ctx.Err(); err != nil {
		return wrap(ErrKindCanceled, op, "已取消", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	cur, err := f.loadLocked()
	if err != nil {
		return err
	}
	if err := validateCheckpoint(op, cur, cp); err != nil {
		return err
	}

	data, err := json.Marshal(cp)
	if err != nil {
		return wrap(ErrKindCheckpoint, op, "序列化检查点失败", err)
	}
	// 原子写：先写临时文件并 fsync，再 rename。
	tmp := f.Path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return wrap(ErrKindCheckpoint, op, "写入临时检查点失败", err)
	}
	if err := os.Rename(tmp, f.Path); err != nil {
		return wrap(ErrKindCheckpoint, op, "替换检查点文件失败", err)
	}
	return nil
}

func (f *FileStore) loadLocked() (Checkpoint, error) {
	data, err := os.ReadFile(f.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return Checkpoint{}, nil
	}
	if err != nil {
		return Checkpoint{}, wrap(ErrKindCheckpoint, "FileStore.Load", "读取检查点失败", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return Checkpoint{}, wrap(ErrKindCheckpoint, "FileStore.Load", "检查点文件损坏", err)
	}
	return cp, nil
}
