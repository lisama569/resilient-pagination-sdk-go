package syncsdk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// FileStore 把一条数据流的检查点原子地落成本地文件，支持跨进程恢复。
//
// 落盘格式（JSON 信封）：
//
//	{"format":1,"stream":"<流标识>","cursor":"...","seq":N,"gen":M,"checksum":"<crc32>"}
//
// checksum 覆盖除 checksum 外的全部字段（按固定字段序序列化），任何字节级
// 篡改/截断都会在 Load 时被发现；stream 字段把文件与数据流绑定，A 流的进度
// 文件不能被 B 流读走；gen 为单调递增的写入代际，配合已观测到的高水位，
// 读回比之前更旧（倒退）的进度时显式拒绝。
type FileStore struct {
	path     string
	streamID string

	mu      sync.Mutex
	last    *Checkpoint // 本进程已观测/写入的高水位（nil 表示尚未见到）
	lastGen uint64
}

const checkpointFormatV1 = 1

// checksumPayload 是参与校验和的字段集合，字段顺序即校验字节序。
type checksumPayload struct {
	Format int    `json:"format"`
	Stream string `json:"stream"`
	Cursor string `json:"cursor"`
	Seq    uint64 `json:"seq"`
	Gen    uint64 `json:"gen"`
}

type checkpointEnvelope struct {
	Format   int    `json:"format"`
	Stream   string `json:"stream"`
	Cursor   string `json:"cursor"`
	Seq      uint64 `json:"seq"`
	Gen      uint64 `json:"gen"`
	Checksum string `json:"checksum"`
}

// NewFileStore 为指定数据流构造文件检查点存储。streamID 不能为空。
func NewFileStore(path, streamID string) (*FileStore, error) {
	if streamID == "" {
		return nil, &Error{Kind: KindCheckpointCorrupt, Message: "stream id must not be empty"}
	}
	return &FileStore{path: path, streamID: streamID}, nil
}

func corrupt(format string, args ...any) error {
	return &Error{Kind: KindCheckpointCorrupt, Message: fmt.Sprintf(format, args...)}
}

func envelopeChecksum(env *checkpointEnvelope) (string, error) {
	p := checksumPayload{Format: env.Format, Stream: env.Stream, Cursor: env.Cursor, Seq: env.Seq, Gen: env.Gen}
	b, err := json.Marshal(p)
	if err != nil {
		return "", &Error{Kind: KindCheckpointCorrupt, Message: "failed to encode checkpoint", Cause: err}
	}
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(b)), nil
}

// monotonic 判定候选检查点相对已观测高水位是否为合法推进。
// 与通用的 ValidateResume 不同，空游标在这里表示“已抓到尾页”的完成态，
// 只要 seq 严格前进即为合法；任何回退都归为 checkpoint_corrupt。
// 候选游标自身格式非法（不可解码）仍保持 cursor_invalid，由调用方先行校验。
func monotonic(last, next *Checkpoint) error {
	if last == nil || last.Cursor == "" {
		return nil // 首次运行或基线已完成，无回退可言
	}
	if next == nil {
		return corrupt("checkpoint regressed to nil")
	}
	if next.Seq < last.Seq {
		return corrupt("sequence regression: %d < %d", next.Seq, last.Seq)
	}
	if next.Cursor == "" {
		// 推进到尾页（完成态）：seq 必须严格前进，防止中途进度被空游标覆盖。
		if next.Seq <= last.Seq {
			return corrupt("non-progressing completion cursor would overwrite offset checkpoint")
		}
		return nil
	}
	if err := ValidateResume(last, next); err != nil {
		return corrupt("checkpoint regression: %v", err)
	}
	return nil
}

// Load 从磁盘读回检查点并完成全部完整性校验。
// 文件不存在视为首次运行，返回 (nil, nil)。
// 任何损坏、流不匹配、游标失效、进度回退都返回 KindCheckpointCorrupt。
func (s *FileStore) Load(ctx context.Context) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &Error{Kind: KindCanceled, Message: "canceled while loading checkpoint", Cause: err}
	}
	body, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, &Error{Kind: KindTransport, Message: "read checkpoint file failed", Cause: err}
	}

	var env checkpointEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, corrupt("checkpoint file is not valid JSON: %v", err)
	}
	if env.Format != checkpointFormatV1 {
		return nil, corrupt("unsupported checkpoint format %d", env.Format)
	}
	want, err := envelopeChecksum(&env)
	if err != nil {
		return nil, err
	}
	if env.Checksum != want {
		return nil, corrupt("checkpoint checksum mismatch (tampered or truncated)")
	}
	if env.Stream != s.streamID {
		return nil, corrupt("checkpoint belongs to stream %q, refusing to resume stream %q", env.Stream, s.streamID)
	}
	cp := &Checkpoint{Cursor: env.Cursor, Seq: env.Seq}
	if cp.Cursor != "" {
		if _, _, err := DecodeCursor(cp.Cursor); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// 读回的进度不得早于本进程已观测的高水位（防止旧备份/倒退文件被接着跑）。
	if s.last != nil {
		if env.Gen < s.lastGen {
			return nil, corrupt("checkpoint generation regressed: %d < %d", env.Gen, s.lastGen)
		}
		if err := monotonic(s.last, cp); err != nil {
			return nil, err
		}
	}
	s.last = cp
	s.lastGen = env.Gen
	cp2 := *cp
	return &cp2, nil
}

// Save 原子地写入检查点：先写同目录临时文件再 rename，崩溃不会留下半截文件。
// 进度相对本存储已观测高水位回退时返回 KindCheckpointCorrupt。
func (s *FileStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return &Error{Kind: KindCanceled, Message: "canceled while saving checkpoint", Cause: err}
	}
	if cp == nil {
		return corrupt("cannot save nil checkpoint")
	}
	if cp.Cursor != "" {
		if _, _, err := DecodeCursor(cp.Cursor); err != nil {
			return err
		}
	}

	s.mu.Lock()
	if s.last != nil {
		if err := monotonic(s.last, cp); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	gen := s.lastGen + 1
	env := &checkpointEnvelope{
		Format: checkpointFormatV1,
		Stream: s.streamID,
		Cursor: cp.Cursor,
		Seq:    cp.Seq,
		Gen:    gen,
	}
	sum, err := envelopeChecksum(env)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	env.Checksum = sum
	body, err := json.Marshal(env)
	if err != nil {
		s.mu.Unlock()
		return &Error{Kind: KindCheckpointCorrupt, Message: "failed to encode checkpoint", Cause: err}
	}

	if err := atomicWriteFile(s.path, body); err != nil {
		s.mu.Unlock()
		return &Error{Kind: KindTransport, Message: "write checkpoint file failed", Cause: err}
	}
	cp2 := *cp
	s.last = &cp2
	s.lastGen = gen
	s.mu.Unlock()
	return nil
}

// atomicWriteFile 在同目录写临时文件后 rename，保证 readers 要么看到旧内容要么看到新内容。
func atomicWriteFile(path string, body []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var randBytes [8]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		return err
	}
	tmp := filepath.Join(dir, ".cp-"+hex.EncodeToString(randBytes[:])+".tmp")
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
