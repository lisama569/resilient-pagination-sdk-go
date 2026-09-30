# resilient-pagination-sdk-go

带版本协商、限流退避与断点恢复的分页同步 SDK。全部测试基于本地替身，不依赖外部网络、真实凭据或付费服务。

## 架构

```
syncsdk/
  protocol.go    协议版本协商 + 响应解析校验（宽容未知字段，严格必填字段）
  client.go      共享客户端：并发上限、速率限制、指数退避重试
  checkpoint.go  游标编解码（带校验和）与恢复合法性校验
  syncer.go      单流同步编排：先提交数据、后推进检查点
  filestore.go   跨进程文件检查点：CRC 校验、流归属绑定、原子写、防回退
  runner.go      多数据流并行编排：共享客户端、每流容量背压、独立结果/原因
  errors.go      归一化错误分类（Kind，多流时带 Stream 归属）
```

## 多数据流并行（Runner）

同一进程里要同时同步多条互不相干的数据流时，用 `Runner`：

```go
client := syncsdk.NewClient(sharedTransport, syncsdk.Config{
    MaxConcurrency: 8,      // 所有流加总的在途请求上限
    RatePerSecond:  50,     // 所有流加总的请求速率上限
    MaxRetries:     3,
})

ordersStore, _ := syncsdk.NewFileStore(dir+"/orders.json", "orders")
billingStore, _ := syncsdk.NewFileStore(dir+"/billing.json", "billing")

runner, _ := syncsdk.NewRunner(client, []syncsdk.StreamConfig{
    {
        ID:               "orders",
        Store:            ordersStore,
        Commit:           sink.Commit,          // 必须幂等
        MaxInFlight:      2,                    // 该流“已抓取未确认”的在途页数
        MaxBufferedItems: 500,                  // 该流已抓取未提交的条目缓冲上限
    },
    {ID: "billing", Store: billingStore, Commit: sink.Commit},
})
results := runner.Run(ctx) // 每条流一个 StreamResult，按配置顺序返回
for _, r := range results {
    switch r.Reason {
    case syncsdk.StopCompleted: // 正常抓完
    case syncsdk.StopFailed:    // 该流自身失败，r.Err 的 Kind 区分问题类别
    case syncsdk.StopCanceled:  // 外部取消/超时，r.Items/r.Checkpoint 是已确认部分结果
    }
}
```

规则与语义：

- **共享一个客户端、不串进度**：所有流走同一个 `Client`，因此**整体**并发 ≤ `MaxConcurrency`、**整体**速率 ≤ `RatePerSecond`；每条流用各自的 `CheckpointStore` 与 `Commit`，条目与检查点互不相干。
- **传输层二选一**：默认所有流共用客户端的 `Transport`（`Fetch(ctx, cursor)`，适合不同游标天然区分数据的场景）；需要按流分流时给某条流配置 `StreamTransport`（`Fetch(ctx, streamID, cursor)`），其余流仍可走共享传输。
- **单流失败互相隔离**：一条流限流耗尽、超时、提交失败或进度异常，只结束该流（`StopFailed`），不影响其他流**已经确认的进度落盘**。错误是归一化的 `*syncsdk.Error`，`err.Stream` 标明属于哪条流，`KindOf(err)` 标明问题类别。
- **结束原因可区分**：`StreamResult.Reason` ∈ `completed` / `failed` / `canceled`；`Err` 在非 completed 时给出归一化错误，`Items`/`Checkpoint` 始终是该流本次运行**已确认**的部分结果。
- **取消/超时快速收尾**：外部 `ctx` 取消或超时后，任何流都**不再发起新的抓取**，抓取/退避/限流/背压等待都能被及时唤醒；已“先提交、后落盘”的进度保留，可直接用于下次恢复。

### 每流容量上限与背压

- `MaxInFlight`：该流“已抓取但尚未确认（提交+保存检查点）”的在途**页数**，即预取窗口；超出时抓取侧等待，不继续预取。
- `MaxBufferedItems`：该流已抓取未提交的**条目**缓冲总量；提交变慢导致缓冲逼近上限时，抓取侧阻塞背压，不会无限堆积内存。单页自身大于该上限时允许临时超过（否则无法推进该页）。
- 提交严格按页序执行（先 `Commit` 成功才 `Save` 检查点），预取只发生在抓取侧，因此在途窗口内的乱序返回不会造成乱序提交或进度错位。

## 跨进程进度持久化（FileStore）

`FileStore` 把每条流的进度原子地落成本地文件，进程重启后由新实例读回接着跑：

- **落盘信封**：`{"format":1,"stream":"<流>","cursor":"...","seq":N,"gen":M,"checksum":"<crc32>"}`，`checksum` 覆盖除自身外的全部字段。
- **原子写**：先写同目录临时文件再 `rename`，崩溃只会留下旧版或新版，不会出现半截文件。
- **损坏/篡改拒绝**：字节被改、文件截断、JSON/格式非法、格式版本未知 → `checkpoint_corrupt`，显式拒绝继续，不会当成正常进度往下抓。
- **流归属绑定**：文件内的 `stream` 必须与打开它的流 ID 一致，A 流的进度文件无法被 B 流读走（`checkpoint_corrupt`）。
- **游标合法性**：落盘/读回都会解码游标校验和，失效游标 → `cursor_invalid`。
- **防回退**：保存与读回都相对本存储已观测的高水位做单调性校验（seq、游标偏移、写入代际 `gen`）；读回比之前更旧（旧备份覆盖、seq/偏移回退）→ `checkpoint_corrupt`。空游标表示“抓到尾页”的完成态，seq 必须严格前进才合法。
- **重启一致性**：仍然遵循“先提交数据、后推进检查点”，配合幂等 `Commit`，重启后从最后一个已确认检查点继续，不丢不重（at-least-once + 按 ID 去重）。
- 单流的 `Syncer` 与多流的 `Runner` 都接受 `CheckpointStore` 接口，因此内存替身（测试）与 `FileStore`（真实跨进程）可互换。


## 版本兼容规则

- 客户端支持版本集合见 `SupportedVersions`（当前 `{2, 1}`）。
- 响应信封必填字段：`version`、`items`、`next_cursor`；每条 item 必填 `id`。
- **新增字段**：旧版本使用方不报错也不静默丢弃，未知字段原样保留在 `Page.Extra` 中。
- **缺失必填字段** → 拒绝，错误类别 `missing_field`。
- **未知版本** → 拒绝，错误类别 `unknown_version`。
- **结构非法**（JSON 解析失败、字段类型错误）→ 拒绝，错误类别 `malformed`。
- 底层解析/传输异常一律包装进 `*syncsdk.Error.Cause`，不以原始形式透出给调用方；用 `syncsdk.KindOf(err)` / `syncsdk.IsKind(err, k)` 区分失败原因。

## 重试与幂等语义

- `Transport.Fetch` 返回 `*RetryableError` 时视为临时失败（如 429/503），按指数退避重试（`InitialBackoff` 起，上限 `MaxBackoff`），最多 `MaxRetries` 次；重试用**同一游标**，天然幂等，不产生重复数据或重复副作用。
- 重试耗尽 → `retry_exhausted`；非可重试错误 → `transport`，不重试。
- 并发数不超过 `MaxConcurrency`（信号量），请求速率不超过 `RatePerSecond`（时隙预留式限流）。
- `context` 取消/超时会及时中断等待（含退避与限流等待），返回 `canceled`，已确认进度保留在检查点中。
- `Syncer.Commit` 回调必须幂等：若提交成功但检查点保存失败，重启后同一批数据可能被重复提交（at-least-once）。SDK 内部另按 item ID 去重，防止服务端重发造成页内重复。

## 游标格式与断点恢复

- 游标为不透明字符串：`v<version>.<offset>.<crc32 校验和>`，由 `EncodeCursor` 生成、`DecodeCursor` 校验。
- 校验和不匹配、格式非法、负偏移 → `cursor_invalid`，显式拒绝。
- 恢复时校验 `ValidateResume`：序列号回退、游标偏移回退、游标失效均显式拒绝，**不会**从中间继续。
- 顺序语义：每页先 `Commit` 数据，成功后才 `Save` 检查点；因此任意时刻持久化的检查点与已提交数据一致，重启或重复消费后无遗漏、无重复。
- 错误类别（`Kind`）：`unknown_version` / `missing_field` / `malformed` / `cursor_invalid` / `retry_exhausted` / `canceled` / `transport` / `checkpoint_corrupt`；多流场景下 `*syncsdk.Error.Stream` 标明失败归属的流。

## 本地验证

```bash
go build ./...
go test ./... -race -count=1 -v
```

测试覆盖（均基于本地替身，无外部网络/凭据）：

- 原单流能力不回归：版本协商、缺失/畸形字段、未知字段保留、并发抓取上限、限流、退避重试与耗尽、取消保留进度、断点恢复、游标回退/失效拒绝、多协程共享客户端。
- 多流并行：多流进度隔离与前缀校验、共享客户端下的全局并发/速率上限、单流失败隔离且错误带流归属、`retry_exhausted` 分类。
- 跨重启恢复：`FileStore` 往返一致；单流/多流在“进程重启”（全新 client/runner，仅共享文件与幂等 sink）后无遗漏无重复。
- 进度异常：校验和篡改、文件截断、流归属不匹配、seq/游标/代际回退、失效游标，全部显式拒绝且不发起抓取。
- 取消/超时：全部流以 `canceled` 收尾、返回的部分进度与已提交数一致、取消后不再发起新抓取。
- 背压：在途页上限与条目缓冲上限分别生效，提交阻塞时抓取数稳定在容量上界、释放后完整跑完；提交失败填满在途窗口时不死锁、不泄漏协程。

测试日志会打印输入、各流结果（原因/类别/进度）、观测并发与重试次数。

