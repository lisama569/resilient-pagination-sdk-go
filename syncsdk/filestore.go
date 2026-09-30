package syncsdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// DefaultStreamID 是单流场景下文件存储使用的默认命名空间。
const DefaultStreamID StreamID = "default"

const (
	fileFormatVersion = 1
	fileSuffix        = ".ckpt.json"
	filePerm          = 0o600
	dirPerm           = 0o755
)

// storedRecord 是检查点在磁盘上的布局（一条流一个文件）。
// Checksum 覆盖除自身外的全部语义字段，用于检测倒退以外的损坏/篡改。
type storedRecord struct {
	FormatVersion int    `json:"format_version"`
	Stream        string `json:"stream"`
	Seq           uint64 `json:"seq"`
	Cursor        string `json:"cursor"`
	Checksum      string `json:"checksum"`
}

// checksumBody 是参与校验和计算的规范化内容（含换行分隔，字段本身不含裸换行）。
func checksumBody(version int, stream string, seq uint64, cursor string) string {
	return fmt.Sprintf("%d\n%s\n%d\n%s", version, stream, seq, cursor)
}

func recordChecksum(rec *storedRecord) string {
	sum := sha256.Sum256([]byte(checksumBody(rec.FormatVersion, rec.Stream, rec.Seq, rec.Cursor)))
	return hex.EncodeToString(sum[:])
}

// FileStore 是跨进程的本地文件检查点存储：原子写入、读回校验，
// 每条流一个独立文件，进程重启后可恢复。
//
// 持久化规则：
//   - Save 先做单调推进校验（相对文件中既有进度，回退一律拒绝），再原子落盘；
//   - Load 重新计算校验和并核对流名/格式，损坏或对不上返回 KindCheckpointCorrupt；
//   - 游标本身必须可解码，否则 KindCursorInvalid。
type FileStore struct {
	dir string
	mu  sync.Mutex // 串行化同进程内的读改写
}

// NewFileStore 打开（必要时创建）目录 dir 下的检查点存储。
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, &Error{Kind: KindMalformed, Message: "checkpoint dir must not be empty"}
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, &Error{Kind: KindTransport, Message: "create checkpoint dir failed", Cause: err}
	}
	return &FileStore{dir: dir}, nil
}

// safeStreamName 限定流名只能由安全字符构成，避免路径穿越。
func safeStreamName(id StreamID) (string, error) {
	s := string(id)
	if s == "" || s == "." || s == ".." {
		return "", &Error{Kind: KindMalformed, Message: fmt.Sprintf("invalid stream id %q", id)}
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-' || r == '_' || r == '.':
		default:
			return "", &Error{Kind: KindMalformed, Message: "stream id allows only [A-Za-z0-9._-]: " + s}
		}
	}
	return s, nil
}

func (f *FileStore) pathFor(id StreamID) (string, error) {
	name, err := safeStreamName(id)
	if err != nil {
		return "", err
	}
	return filepath.Join(f.dir, name+fileSuffix), nil
}

// readRecord 读取并解析一条流的原始记录；文件不存在返回 (nil, nil, nil)。
func (f *FileStore) readRecord(id StreamID) (*storedRecord, error) {
	path, err := f.pathFor(id)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, &Error{Kind: KindTransport, Message: "read checkpoint file failed", Cause: err}
	}
	var rec storedRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, &Error{Kind: KindCheckpointCorrupt, Message: "checkpoint is not valid JSON", Cause: err}
	}
	return &rec, nil
}

// verifyRecord 对读回的记录做完整一致性校验。
func verifyRecord(wantID StreamID, rec *storedRecord) (*Checkpoint, error) {
	if rec.FormatVersion != fileFormatVersion {
		return nil, &Error{Kind: KindCheckpointCorrupt,
			Message: fmt.Sprintf("unknown checkpoint format_version %d", rec.FormatVersion)}
	}
	if rec.Stream != string(wantID) {
		return nil, &Error{Kind: KindCheckpointCorrupt,
			Message: fmt.Sprintf("checkpoint stream mismatch: file=%q want=%q", rec.Stream, wantID)}
	}
	if rec.Checksum != recordChecksum(rec) {
		return nil, &Error{Kind: KindCheckpointCorrupt, Message: "checkpoint checksum mismatch (tampered or corrupted)"}
	}
	cp := &Checkpoint{Cursor: rec.Cursor, Seq: rec.Seq}
	if cp.Cursor != "" {
		if _, _, err := DecodeCursor(cp.Cursor); err != nil {
			return nil, err
		}
	}
	return cp, nil
}

// LoadStream 读回一条流的进度；无记录返回 (nil, nil)。
func (f *FileStore) LoadStream(ctx context.Context, id StreamID) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &Error{Kind: KindCanceled, Message: "canceled while loading checkpoint", Cause: err}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, err := f.readRecord(id)
	if err != nil || rec == nil {
		return nil, err
	}
	return verifyRecord(id, rec)
}

// SaveStream 先做单调推进校验，再把进度原子写入磁盘。
func (f *FileStore) SaveStream(ctx context.Context, id StreamID, cp *Checkpoint) error {
	if cp == nil {
		return &Error{Kind: KindMalformed, Message: "checkpoint must not be nil"}
	}
	if err := ctx.Err(); err != nil {
		return &Error{Kind: KindCanceled, Message: "canceled while saving checkpoint", Cause: err}
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	prev, err := f.readRecord(id)
	if err != nil {
		return err
	}
	var prevCP *Checkpoint
	if prev != nil {
		prevCP, err = verifyRecord(id, prev)
		if err != nil {
			return err
		}
	}
	if cp.Cursor != "" {
		if _, _, err := DecodeCursor(cp.Cursor); err != nil {
			return err
		}
	}
	// 单调推进：序列号/游标回退一律拒绝；空游标为合法完成终态。
	if err := ValidateSave(prevCP, cp); err != nil {
		return err
	}

	rec := &storedRecord{
		FormatVersion: fileFormatVersion,
		Stream:        string(id),
		Seq:           cp.Seq,
		Cursor:        cp.Cursor,
	}
	rec.Checksum = recordChecksum(rec)
	return f.writeAtomic(id, rec)
}

// writeAtomic 写临时文件 -> fsync -> 改名，保证读回的要么是旧版要么是新版。
func (f *FileStore) writeAtomic(id StreamID, rec *storedRecord) error {
	path, err := f.pathFor(id)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return &Error{Kind: KindCheckpointCorrupt, Message: "encode checkpoint failed", Cause: err}
	}
	tmp, err := os.CreateTemp(f.dir, ".tmp-ckpt-*")
	if err != nil {
		return &Error{Kind: KindTransport, Message: "create temp checkpoint failed", Cause: err}
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return &Error{Kind: KindTransport, Message: "write checkpoint failed", Cause: err}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return &Error{Kind: KindTransport, Message: "fsync checkpoint failed", Cause: err}
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return &Error{Kind: KindTransport, Message: "close checkpoint failed", Cause: err}
	}
	if err := os.Chmod(tmpName, filePerm); err != nil {
		cleanup()
		return &Error{Kind: KindTransport, Message: "chmod checkpoint failed", Cause: err}
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return &Error{Kind: KindTransport, Message: "rename checkpoint failed", Cause: err}
	}
	return nil
}

// ListStreams 枚举磁盘上已有进度的全部流。
func (f *FileStore) ListStreams(ctx context.Context) ([]StreamID, error) {
	if err := ctx.Err(); err != nil {
		return nil, &Error{Kind: KindCanceled, Message: "canceled while listing streams", Cause: err}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, &Error{Kind: KindTransport, Message: "list checkpoint dir failed", Cause: err}
	}
	var ids []StreamID
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, fileSuffix) {
			continue
		}
		id := StreamID(strings.TrimSuffix(name, fileSuffix))
		rec, err := f.readRecord(id)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			continue
		}
		if _, err := verifyRecord(id, rec); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// Load / Save 实现单流 CheckpointStore，委托到 DefaultStreamID。
func (f *FileStore) Load(ctx context.Context) (*Checkpoint, error) {
	return f.LoadStream(ctx, DefaultStreamID)
}

func (f *FileStore) Save(ctx context.Context, cp *Checkpoint) error {
	return f.SaveStream(ctx, DefaultStreamID, cp)
}

// boundStore 把 FileStore 的某条流呈现为单流 CheckpointStore。
type boundStore struct {
	f  *FileStore
	id StreamID
}

func (b *boundStore) Load(ctx context.Context) (*Checkpoint, error) {
	return b.f.LoadStream(ctx, b.id)
}

func (b *boundStore) Save(ctx context.Context, cp *Checkpoint) error {
	return b.f.SaveStream(ctx, b.id, cp)
}

// Bind 返回绑定到指定流的单流 CheckpointStore 视图。
func (f *FileStore) Bind(id StreamID) CheckpointStore {
	return &boundStore{f: f, id: id}
}

// 编译期断言：*FileStore 同时满足多流与单流存储接口。
var (
	_ StreamCheckpointStore = (*FileStore)(nil)
	_ CheckpointStore       = (*FileStore)(nil)
)
