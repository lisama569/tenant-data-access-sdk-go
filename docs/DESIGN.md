# tenantdata：可本地嵌入的多租户数据访问 SDK

`tenantdata` 是一个可直接嵌入调用方进程的 Go SDK（包路径
`github.com/lisama569/tenant-data-access-sdk-go/tenantdata`），
用本地内存数据模拟租户数据集，把**身份校验 → 策略判定 → 数据读取 → 审计留痕**
串成一条确定、可解释、可审计的执行链路。

## 1. 快速嵌入

```go
policies := tenantdata.NewMemoryPolicyStore()   // 版本化策略存储
data     := tenantdata.NewMemoryDataStore()     // 多版本数据集
audit    := tenantdata.NewMemoryAuditSink()     // 审计留痕（可替换为自有 AuditSink）

client := tenantdata.NewClient(tenantdata.DefaultConfig(), policies, data, audit)
client.SetLogger(myLogger) // 可选：实现 tenantdata.Logger（Printf）

_ = policies.Put(tenantdata.Policy{Version: 1, TenantID: "acme",
    AllowedScopes: []string{tenantdata.ScopeRead}, EffectiveAt: now})
_ = data.CreateDataset(ctx, "orders")
_ = data.Upsert(ctx, "orders", tenantdata.Record{ID: "o-1", TenantID: "acme"})

id := tenantdata.Identity{
    RequestID: "req-1", TenantID: "acme", CallerID: "billing-svc",
    Scopes: []string{tenantdata.ScopeRead}, ExpiresAt: now.Add(time.Hour),
}
page, err := client.Read(ctx, id, tenantdata.ReadOptions{
    Dataset: "orders", TenantID: "acme", Limit: 50,
})
// 翻页：
page, err = client.ContinueRead(ctx, id, page.NextCursor)
```

`ReadOptions.TenantID` 是**目标资源租户**，与身份租户不同会得到
`tenant_mismatch`；这是跨租户访问的显式判定点。

## 2. 身份模型

`Identity` 携带：`RequestID`（贯穿日志与审计）、`TenantID`、`CallerID`、
`Scopes`、`NotBefore`、`ExpiresAt`。

校验规则（`Identity.ValidAt`）：

- 缺少 RequestID/TenantID/CallerID，或当前时间落在 `[NotBefore, ExpiresAt)`
  之外 → `unauthenticated`。
- 授权所需的 scope 必须同时出现在**身份的 Scopes** 与**租户策略的
  AllowedScopes** 中，二者缺一即 `permission_denied`。

## 3. 策略模型与“一请求一版本”

`Policy` 是不可变快照：`Version`（单调递增）、`TenantID`、`AllowedScopes`、
`EffectiveAt`、`ExpiresAt`、`Revoked`。

- `MemoryPolicyStore.Update` 每次生成新版本并**保留全部历史快照**；
  `GetAtVersion` 可精确取回任意旧版本。
- 一个请求在首页读取时解析策略并把 `PolicyVersion` **固定进游标**。
  判定顺序固定为：
  身份有效 → 租户匹配 → 策略存在 → 策略在有效期且未废止 → 权限范围。
- 续读（`ContinueRead`）用游标里的版本号调用 `GetAtVersion` 重新校验：
  - 旧版本仍有效 → 即使已经发布新版本，也继续按旧版本完成（结论不漂移）；
  - 旧版本被废止/过期/被删除 → 返回 `policy_stale`，**绝不静默切换**到新版本。
- 并发 `Update` 下，任何 `Get/GetAtVersion` 都返回某个完整快照，
  因此每个请求只可能按一个确定版本完成判定。

## 4. 读取一致性规则

- **快照水位**：每次 `Upsert` 提交后全库水位 +1，记录按修订保存。
  `Read` 首页取当前水位 W 并固定进游标；`Scan` 只可见“提交序号 ≤ W”的
  最新修订。读取期间的新写入（插入或更新）不会混入，也不会改变排序。
- **确定性排序**：按记录 ID 字典序遍历；游标记录“上一页最后一条 ID”，
  下一页从它**严格之后**开始。因此续读不会重复已读记录，也不会静默跳过记录。
- **会话固定**：游标（不透明 base64 token，前缀 `tdc1:`）同时固定
  ReadID、租户、数据集、快照水位、策略版本、过滤条件、页大小与位置。
  跨租户持游标续读得到 `tenant_mismatch`；游标损坏或缺字段得到
  `invalid_cursor`；水位高于已提交值（不可能来自合法会话）得到
  `invalid_cursor`。
- **无法继续时明确失败**：策略版本失效是 `policy_stale`，
  不是空结果，也不是自动换版本。
- 过滤条件：`Filter.Prefix`（ID 前缀）与 `Filter.MinVersion`（记录版本下限）。

## 5. 错误分类（稳定且互不混淆）

| ErrorKind（归一化字符串） | 含义 | 是否重试 |
| --- | --- | --- |
| `unauthenticated` | 身份缺失/过期/未生效 | 否 |
| `tenant_mismatch` | 访问非本租户资源 | 否 |
| `permission_denied` | 租户匹配但 scope 不足 | 否 |
| `policy_stale` | 固定的策略版本失效 | 否 |
| `not_found` | 数据集/策略版本不存在 | 否 |
| `invalid_cursor` | 游标损坏、字段缺失或水位非法 | 否 |
| `capacity_exceeded` | 超过并发上限被拒绝 | 否 |
| `unavailable` | 暂时性内部故障 | 是（有限次） |
| `invalid_argument` | SDK 入参非法（编程错误） | 否 |

用法：

```go
se, _ := tenantdata.AsError(err)
switch se.Kind { /* 稳定分支处理 */ }
// 或：errors.Is(err, tenantdata.ErrPermissionDenied)
// 归一化字符串：tenantdata.Normalize(err).String()
```

结构化错误始终携带 `RequestID` 与 `PolicyVersion`，`Error()` 输出
`kind=... request=... policy_version=...: detail` 形式，便于日志与审计对齐。

## 6. 并发保护与有限重试

- **并发保护**：`Config.MaxConcurrent`（默认 32）个信号量槽位，非阻塞获取；
  超额请求立即返回 `capacity_exceeded`，不会进入读取链路、也不写成功审计。
- **有限重试**：仅 `unavailable` 可重试；指数退避，
  最多 `Config.MaxRetry` 次（含首次，默认 3）。鉴权与数据集存在性检查在
  重试循环之外完成，因此重试不会改变授权结论或策略版本。
- **审计幂等**：审计事件在请求**终态确定后**恰好写一条，
  记录 `attempts` 实际尝试次数。重试成功不会产生多条成功审计；
  确定性失败 `attempts=1`；重试耗尽仍只有一条 `unavailable`。
- 存储层可用 `MemoryDataStore.FailNextScans(n)` / `SetFaultHook` 注入
  暂时性故障，供嵌入方编写自己的重试测试。

## 7. 审计与日志

`AuditEvent` 字段：时间、RequestID、ReadID、租户、调用方、数据集、
PolicyVersion、Action、CursorPos（本页最后一条 ID）、Page、Allowed、
ErrorKind（归一化）、Attempts。

调用方可实现 `AuditSink` 把事件转发到自有审计管道；`MemoryAuditSink`
提供线程安全的内存留痕与按 RequestID 检索。日志（`Logger.Printf`）在
成功/失败两条路径上都打印 **请求标识、策略版本、游标位置、归一化
错误原因**（失败时还包含尝试次数与细节）。

## 8. 本地验证

```bash
# 全量单测（含并发读取、策略切换、游标失效、越权、有限重试）
go test ./... -count=1

# 竞态检测（并发测试在 -race 下运行）
go test ./... -race -count=1

# 查看可运行的嵌入示例（ExampleClient，输出被测试自动校验）
go test ./... -run ExampleClient -v

# 静态检查
go vet ./...
```

测试覆盖对应关系：

| 场景 | 测试 |
| --- | --- |
| 错误分类稳定、四类错误不混淆 | `TestErrorKindsDistinctAndStable` |
| 身份/身份有效期 | `TestIdentityValidAt`、`TestExpiredIdentityRejected` |
| 策略快照不可变、版本判定 | `TestPolicyStoreVersionsAreImmutable`、`TestAuthorizeErrorClassification` |
| 分页快照一致、不重不漏 | `TestPaginationIsStableAcrossWrites`、`TestPaginationFilterPrefixAndMinVersion` |
| 游标损坏/跨租户/重放 | `TestCursorRejectsGarbageAndCrossTenant`、`TestCursorReplayDoesNotRepeat` |
| 策略切换固定旧版本 / 废止失败 | `TestContinuePinnedToOldPolicyVersion`、`TestContinueFailsWhenPinnedPolicyRevoked` |
| 策略高频并发更新 | `TestConcurrentPolicyUpdatesEachRequestPinnedToOneVersion` |
| 多租户并发读 + 并发写 | `TestConcurrentReadsConsistentUnderWrites`（-race） |
| 有限重试与审计幂等 | `TestRetryTransientThenSuccessSingleAudit`、`TestRetryExhaustedIsUnavailable` |
| 确定性失败不重试 | `TestDeterministicErrorsAreNotRetried` |
| 过量并发拒绝 | `TestConcurrencyCapacityProtection` |
| 策略过期 | `TestExpiredPolicyRejectedThroughClient` |
| 日志必含字段 | `TestLogsContainRequiredContext` |
