# tenant-data-access-sdk-go

可本地嵌入的多租户数据访问 SDK。用本地数据集模拟租户数据，提供身份校验、
版本化策略判定、分页续读、有限重试、并发保护与审计留痕，供其他调用方直接嵌入。

## 快速开始

```go
ps := tenantdata.NewPolicyStore()
st := tenantdata.NewStore()
al := tenantdata.NewAuditLog()
client := tenantdata.NewClient(ps, st, al,
    tenantdata.WithMaxConcurrent(64),
    tenantdata.WithMaxRetries(2),
)

// 发布策略（版本号必须递增）
_ = ps.Set(tenantdata.Policy{
    TenantID: "t1", Version: 1,
    AllowedScopes: []string{"read"},
    ValidUntil: time.Now().Add(time.Hour),
})

// 写入本地模拟数据
st.Put("t1", "user/1", "alice")

// 带身份读取
page, err := client.Read(ctx, tenantdata.Identity{
    RequestID: "req-1", TenantID: "t1", Caller: "svc-a",
    Scopes: []string{"read"}, ExpiresAt: time.Now().Add(time.Minute),
}, tenantdata.ReadRequest{TenantID: "t1", Scope: "read", Limit: 100})
```

## 身份与策略模型

- **Identity**：一次请求的身份，含 `RequestID`、`TenantID`、`Caller`、`Scopes`
  （权限范围）与 `ExpiresAt`（有效期）。身份过期返回 `IDENTITY_EXPIRED`。
- **Policy**：按租户版本化管理，含 `Version`、`AllowedScopes`、`ValidUntil`。
  `PolicyStore.Set` 要求版本号严格递增，防止旧版本覆盖新版本。
- **判定顺序固定**：身份有效期 → 租户匹配 → 取策略快照 → 策略有效期 →
  权限范围（身份持有且策略放行）→ 游标校验 → 并发准入 → 数据读取。
- **策略快照**：一次请求只在判定时取一次策略版本，之后全部按该版本完成；
  并发更新策略时，不同请求各自看到确定的版本，不会中途切换。

## 读取一致性规则

- 首页请求以当前写入水位线（watermark）为**快照上限**，游标记录
  `{SnapshotSeq, LastSeq, PolicyVersion}`。
- 续读只返回 `Seq <= SnapshotSeq` 的记录：读取期间的新写入**不会**混入本次
  分页结果；记录只增不删（同 key 更新保留原 Seq），续读**不会重复也不会
  静默跳过**快照内记录。
- 游标绑定签发时的策略版本：策略切换后续读旧游标会明确失败
  （`CURSOR_INVALID`），而不是静默按新策略继续。调用方需重新发起首页读取。
- 游标格式非法同样返回 `CURSOR_INVALID`。

## 错误分类

所有错误为 `*tenantdata.Error`，含 `Code`、`Reason`、`RequestID`、
`PolicyVersion`，分类稳定、可区分：

| Code | 含义 | 可重试 |
|---|---|---|
| `TENANT_MISMATCH` | 身份租户与目标租户不一致 | 否 |
| `IDENTITY_EXPIRED` | 请求身份已过有效期 | 否 |
| `PERMISSION_DENIED` | 权限范围不足或无对应策略 | 否 |
| `POLICY_EXPIRED` | 策略已过有效期 | 否 |
| `NOT_FOUND` | 租户数据集不存在 | 否 |
| `CAPACITY_EXCEEDED` | 并发容量不足，被拒绝 | 否 |
| `UNAVAILABLE` | 暂时性内部故障 | 是 |
| `CURSOR_INVALID` | 游标失效（策略变更或格式非法） | 否 |

`Error.Retryable()` 仅对 `UNAVAILABLE` 返回 true。

## 并发与重试限制

- **并发保护**：`WithMaxConcurrent(n)` 限制并发读取数，超出立即以
  `CAPACITY_EXCEEDED` 拒绝，不排队。
- **有限重试**：仅 `UNAVAILABLE` 会重试，最多 `WithMaxRetries(n)` 次
  （默认 2 次），间隔 `WithRetryBackoff(d)`。重试尊重 `ctx` 取消。
- **审计唯一性**：每个逻辑请求（含内部重试）只写**一条**最终审计事件
  （`ALLOW`/`DENY`，含尝试次数），重试不会重复产生成功审计，也不会改变
  最终结论。

## 审计

`AuditLog.Events()` 返回全部审计事件，字段含请求标识、租户、调用方、
判定结果、策略版本、游标、错误分类、记录数与尝试次数。

## 本地验证

```sh
go build ./...
go vet ./...
go test ./...          # 单元测试
go test -race ./...    # 并发竞态检查
go test -v ./tenantdata/  # 查看日志：请求标识、策略版本、游标位置、归一化错误原因
```

测试覆盖：并发读取结果一致、读取期间写入的可见性、策略切换导致游标失效、
越权访问的错误分类、暂时故障的有限重试与审计唯一性、并发容量拒绝、
条件过滤与游标续读。
