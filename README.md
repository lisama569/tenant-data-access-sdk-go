# tenant-data-access-sdk-go

可本地嵌入的多租户数据访问 SDK：用本地内存数据模拟租户数据集，
覆盖身份校验、版本化策略判定、快照隔离读取、游标续读、有限重试、
并发保护与审计留痕。

- 包：`github.com/lisama569/tenant-data-access-sdk-go/tenantdata`
- 详细设计（身份/策略模型、一致性规则、错误分类、并发与重试限制）：
  [`docs/DESIGN.md`](docs/DESIGN.md)

## 本地验证

```bash
go test ./... -count=1          # 全量单测
go test ./... -race -count=1    # 含竞态检测
go vet ./...
```

可运行嵌入示例见 `tenantdata.ExampleClient`（输出由 `go test` 自动断言）。
