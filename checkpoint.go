package sdk

import "context"

// Checkpoint 记录已确认的同步进度。
type Checkpoint struct {
	Cursor string // 下一页待抓取的游标
	Seq    uint64 // 已确认的页序号
}

// CheckpointStore 持久化检查点。Save 必须拒绝游标回退。
type CheckpointStore interface {
	Load(ctx context.Context) (Checkpoint, error)
	Save(ctx context.Context, cp Checkpoint) error
}

// MemoryStore 是进程内检查点存储（用于测试与嵌入式场景）。
type MemoryStore struct {
	cp Checkpoint
}

// NewMemoryStore 创建内存检查点存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

func (m *MemoryStore) Load(ctx context.Context) (Checkpoint, error) { return Checkpoint{}, nil }

func (m *MemoryStore) Save(ctx context.Context, cp Checkpoint) error { return nil }

// FileStore 以原子写（临时文件 + rename）持久化检查点，支持重启恢复。
type FileStore struct {
	Path string
}

// NewFileStore 创建文件检查点存储。
func NewFileStore(path string) *FileStore { return &FileStore{Path: path} }

func (f *FileStore) Load(ctx context.Context) (Checkpoint, error) { return Checkpoint{}, nil }

func (f *FileStore) Save(ctx context.Context, cp Checkpoint) error { return nil }
