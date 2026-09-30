# resilient-pagination-sdk-go

带版本协商、限流退避、断点恢复，并支持**多数据流并行**与**跨进程进度持久化**的分页同步 SDK。全部测试基于本地替身，不依赖外部网络、真实凭据或付费服务。

## 架构

```
syncsdk/
  protocol.go    协议版本协商 + 响应解析校验（宽容未知字段，严格必填字段）
  client.go      共享客户端：全局并发上限、速率限制、指数退避重试
  checkpoint.go  游标编解码（带校验和）、恢复/落盘合法性校验
  syncer.go      单流编排：先提交数据、后推进检查点
  stream.go      多流类型：StreamID / StreamSpec / 容量上限 / 结束原因 / 多流存储接口
  engine.go      单条流的有界在途流水线：背压预取、先提交后存盘、可区分的收尾
  manager.go     多流并行编排：共享一个 Client，进度与失败互相隔离
  filestore.go   跨进程本地文件存储：每流一个文件、原子写、读回校验和
  errors.go      归一化错误分类（Kind，带 Stream 归属）
```

## 两种使用方式

### 单流（原有，保持兼容）

```go
client := syncsdk.NewClient(transport, syncsdk.Config{RatePerSecond: 100})
syncer := syncsdk.NewSyncer(client, store, func(ctx context.Context, items []syncsdk.Item) error {
    return sink.Upsert(ctx, items) // 须幂等
})
res, err := syncer.Run(ctx)
res.Stop // completed | canceled | failed
```

### 多数据流并行（新增）

同一个进程里同时同步多条互不相干的流，**共用一个 `Client`**（全局并发与速率上限不被突破），每条流有各自独立的检查点与提交回调，互不串用：

```go
client  := syncsdk.NewClient(transport, syncsdk.Config{MaxConcurrency: 4, RatePerSecond: 50})
store, _ := syncsdk.NewFileStore("./var/checkpoints") // 跨进程持久化
mgr     := syncsdk.NewManager(client, store)

outcomes := mgr.RunAll(ctx, []syncsdk.StreamSpec{{
    ID:          "orders",                 // 流标识；进度按 ID 隔离
    StartCursor: ordersFirstCursor,        // 仅首次生效；重启从磁盘进度继续
    MaxInFlight: 4,                        // 在途页上限（背压）
    MaxBuffered: 1000,                     // 本地缓冲条目上限（背压，<=0 不限）
    Commit: func(ctx context.Context, id syncsdk.StreamID, items []syncsdk.Item) error {
        return sinks[id].Upsert(ctx, items) // 须幂等
    },
}, {
    ID: "users", StartCursor: usersFirstCursor, MaxInFlight: 2, Commit: usersCommit,
}})

for id, o := range outcomes {
    // o.Err==nil 且 o.Result.Stop==completed 表示该流抓完
    // 失败也能分清是哪条流、哪类问题：
    kind := syncsdk.KindOf(o.Err) // retry_exhausted / canceled / transport ...
    var stream syncsdk.StreamID
    if se, ok := o.Err.(*syncsdk.Error); ok {
        stream = se.Stream // "orders" / "users"
    }
    _ = id; _ = kind; _ = stream
}
```

- 某条流因限流耗尽、超时、取消或提交失败而中止时，**只影响该流**；其它流照常完成，已确认进度已经落盘，不会回滚。
- `ctx` 是所有流共享的总控：取消/超时会同时让所有流尽快收尾。

## 多数据流的进度持久化与恢复（FileStore）

- 每条流在目录下对应一个独立文件 `<stream-id>.ckpt.json`，流名仅允许 `[A-Za-z0-9._-]`，杜绝路径穿越；`FileStore.Bind(id)` 可拿到单流视图，`FileStore` 自身也实现单流 `CheckpointStore`（命名空间 `default`）。
- 文件含 `format_version / stream / seq / cursor / checksum`，写入采用「临时文件 → fsync → 原子改名」，崩溃后读回的要么是旧版要么是新版，不会读到半截内容。
- **重启读回时严格校验**：校验和不符（被篡改/磁盘损坏）、流名对不上、格式版本未知 → `checkpoint_corrupt`；游标不可解码 → `cursor_invalid`。内容对不上一律**显式拒绝，绝不当成正常进度继续抓**。
- **单调推进**：落盘前校验序列号与游标偏移不得回退，回退返回 `cursor_invalid`。空游标（`next_cursor == ""`）表示「已抓到尾页、整条流完成」这一合法终态，只要 `seq` 不倒退即可落盘；重启读回该终态会直接判定完成，不再重抓首页。
- 顺序语义不变：每页先 `Commit` 数据、成功后才 `SaveStream` 检查点，因此持久化进度与已提交数据一致，重启后无遗漏、无重复（at-least-once 提交 + 幂等 sink + 页内按 ID 去重）。

## 并发、速率与容量上限

- **全局并发** `Config.MaxConcurrency`：所有流共享同一客户端信号量，多流并行时聚合在途请求数不超过该值。
- **全局速率** `Config.RatePerSecond`：时隙预留式限流，多流聚合请求速率不超过该值；等待可被取消中断。
- **每条流在途页上限** `StreamSpec.MaxInFlight`（默认 1）：抓取协程有界预取，配额占满即背压暂停，不再向服务端发请求。
- **每条流缓冲条目上限** `StreamSpec.MaxBuffered`（<=0 表示不限）：已抓取待确认的条目数受限，占满即背压，不会无限堆积内存。

## 取消、超时与结束原因

- `RunAll` 中每条流的结果带 `Result.Stop`：`completed`（抓完尾页）、`canceled`（外部取消/超时）、`failed`（重试耗尽/传输/提交/进度异常）。
- 取消或超时后**不再发起新抓取**，在途但未确认的页直接丢弃（重启会从已落盘游标重抓，不丢不重），把已确认的检查点与部分结果返回给调用方。
- 底层解析/传输异常一律包装进 `*syncsdk.Error.Cause`，多流下额外带 `Error.Stream`；用 `syncsdk.KindOf(err)` / `syncsdk.IsKind(err, k)` 区分问题类别。

## 重试与幂等语义

- `Transport.Fetch` 返回 `*RetryableError`（如 429/503）时按指数退避重试（`InitialBackoff` 起，上限 `MaxBackoff`，最多 `MaxRetries` 次），重试使用同一游标，天然幂等；耗尽 → `retry_exhausted`，非可重试错误 → `transport`。
- `Commit` 回调必须幂等：提交成功但检查点保存失败时，重启后同一批可能被重复提交（at-least-once）。SDK 内部另按 item ID 去重，防止服务端重发造成页内重复。

## 版本兼容规则

- 支持版本集合见 `SupportedVersions`（当前 `{2, 1}`）。
- 响应信封必填 `version`、`items`、`next_cursor`，每条 item 必填 `id`。
- 新增字段原样保留在 `Page.Extra`，不报错也不丢弃；缺必填字段 → `missing_field`；未知版本 → `unknown_version`；结构非法 → `malformed`。

## 本地验证

```bash
go build ./...
go test ./... -race -count=1 -v
```

覆盖（32 个用例）：版本不匹配/缺字段/畸形/未知字段保留；单流完整抓取、重试与耗尽、非重试不重试、并发与速率上限、退避中取消；多流并行进度隔离与全局并发/速率不超限、单流限流或提交失败不影响其它流且可定位流与类别；跨进程落盘读回、中断重启无丢无重、完成终态短路、校验和篡改/流名不符/垃圾内容拒绝、游标与序列号回退拒绝、非法流名拒绝；取消/超时停止新抓取并返回部分进度后可恢复；在途页与缓冲条目背压。
