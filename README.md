# resilient-pagination-sdk-go

带版本协商、限流退避与断点恢复的分页同步 SDK。全部测试基于本地可复现的
测试替身（`fakeTransport`），不依赖外部网络、真实凭据或付费服务。

## 功能概览

- **协议版本协商**：请求携带客户端版本，响应按主版本兼容规则校验；
  旧版本使用方遇到新增字段时不报错、不丢弃（原样保留在 `Page.Extra`）。
- **并发抓取与限流退避**：客户端可在多协程间共享，并发槽 + 令牌桶保证
  聚合并发数与速率不超过配置上限；可重试失败按指数退避（带抖动）重试。
- **断点与游标持久化**：检查点原子落盘（临时文件 + rename），重启或重复
  消费不漏不重；游标回退或失效被显式拒绝。

## 版本兼容规则

- 版本格式为 `major.minor`（如 `1.2`）。
- **主版本相同即兼容**：次版本更高的一端可能携带新增字段，旧端将其完整
  保留在 `Page.Extra` 中（`map[string]json.RawMessage`），既不报错也不
  静默丢弃，且往返编码后仍然保留。
- 协商（`Negotiate`）：在 `SupportedVersions` 中选取主版本一致、次版本
  不超过对端的最高版本。
- 失败分类（`KindOf(err)` 可区分）：
  - `version_unknown`：主版本不受支持；
  - `version_mismatch`：主版本匹配但无可用次版本，或响应与客户端主版本不一致；
  - `missing_field`：响应缺失必填字段（`version` / `cursor` / `items`）；
  - `malformed`：JSON 无法解析或字段类型非法。
- 底层 `encoding/json`、`strconv` 等异常一律包装在 `*sdk.Error.Err` 中，
  不向调用方直接透出原始异常类型。

## 重试与幂等语义

- 可重试错误（`Retryable(err) == true`）：`transient`（传输异常、5xx）、
  `rate_limited`（429，尊重 `RetryAfter` 下限）。其余类别一律不重试。
- 退避策略（`Backoff`）：`Base * Factor^attempt`，封顶 `Max`，叠加
  `±Jitter` 对称抖动；最多重试 `MaxRetries` 次。
- **幂等键**：同一逻辑页（由游标唯一确定）的所有重试共用同一幂等键
  （`Request.IdempotencyKey`），传输层/服务端可据此去重，重试不会产生
  重复数据或重复副作用。
- 取消/超时（`canceled`）及时终止：等待并发槽、等待限流令牌、退避
  等待、传输期间均响应 `ctx` 取消，并保留已确认的进度。

## 游标格式

游标为不透明字符串：

```
sdk1.<base64url("<seq>:<token>")>.<crc32 校验和（8 位十六进制）>
```

- `seq`：单调递增的页序号，用于回退检测；
- `token`：服务端分页令牌；
- 校验和覆盖编码前载荷，篡改或截断会导致 `cursor_invalid`。

检查点推进规则：

- 每一页**先 `Sink.Commit` 提交数据，成功后才能 `CheckpointStore.Save`
  推进游标**（顺序语义）；任一步失败，进度停留在最后已确认位置。
- `Save` 显式拒绝：序号回退（`cursor_regression`）、游标不可解码或序号
  与检查点不一致（`cursor_invalid`），不会从中间继续。
- 同步完成后检查点标记 `Done`，重复消费为 no-op。

## 本地验证方法

```sh
go build ./...          # 编译
go vet ./...            # 静态检查
go test ./...           # 全部测试（本地替身，无网络）
go test -v ./...        # 查看输入、重试次数与归一化错误日志
go test -race ./...     # 并发竞态检测
go test -count=20 ./... # 重复运行验证稳定性
```

测试覆盖：版本不匹配（`version_test.go`）、畸形响应与新增字段保留
（`codec_test.go`）、并发抓取上限与限流重试、取消保留进度
（`client_test.go`）、游标回退/失效与持久化恢复（`checkpoint_test.go`）、
端到端重启不漏不重与顺序语义（`sync_test.go`）。
