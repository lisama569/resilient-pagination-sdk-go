# resilient-pagination-sdk-go

带版本协商、限流退避与断点恢复的分页同步 SDK。全部测试基于本地替身，不依赖外部网络、真实凭据或付费服务。

## 架构

```
syncsdk/
  protocol.go    协议版本协商 + 响应解析校验（宽容未知字段，严格必填字段）
  client.go      共享客户端：并发上限、速率限制、指数退避重试
  checkpoint.go  游标编解码（带校验和）与恢复合法性校验
  syncer.go      同步编排：先提交数据、后推进检查点
  errors.go      归一化错误分类（Kind）
```

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

## 本地验证

```bash
go build ./...
go test ./... -race -count=1 -v
```

测试覆盖：版本不匹配、缺失字段、畸形响应、未知字段保留、并发抓取上限、限流、退避重试与耗尽、取消保留进度、断点恢复、游标回退/失效拒绝、多协程共享客户端。测试日志会打印输入、重试次数与归一化后的错误（kind）。
