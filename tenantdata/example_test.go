package tenantdata_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	tenantdata "github.com/lisama569/tenant-data-access-sdk-go/tenantdata"
)

// stdLogger 把 SDK 的可解释日志桥接到标准库 log。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Printf(format string, args ...any) { s.l.Printf(format, args...) }

// ExampleClient 演示调用方如何把 SDK 直接嵌入到自身进程：
// 装配本地策略存储、本地数据集、内存审计留痕，再完成鉴权读取与游标续读。
func ExampleClient() {
	ctx := context.Background()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cfg := tenantdata.DefaultConfig()
	cfg.Now = func() time.Time { return now }
	cfg.RetryBaseDelay = time.Millisecond

	policies := tenantdata.NewMemoryPolicyStore()
	data := tenantdata.NewMemoryDataStore()
	audit := tenantdata.NewMemoryAuditSink()
	client := tenantdata.NewClient(cfg, policies, data, audit)
	client.SetLogger(stdLogger{l: log.New(os.Stderr, "", 0)})

	// 1) 植入租户策略 v1：允许 read。
	_ = policies.Put(tenantdata.Policy{
		Version:       1,
		TenantID:      "acme",
		AllowedScopes: []string{tenantdata.ScopeRead},
		EffectiveAt:   now.Add(-time.Minute),
	})
	// 2) 创建数据集并写入 4 条记录。
	_ = data.CreateDataset(ctx, "orders")
	for i := 0; i < 4; i++ {
		_ = data.Upsert(ctx, "orders", tenantdata.Record{
			ID:       fmt.Sprintf("order-%d", i),
			TenantID: "acme",
			Payload:  fmt.Sprintf("payload-%d", i),
		})
	}

	// 3) 构造带租户、调用方、权限和有效期的请求身份，首页读 2 条。
	id := tenantdata.Identity{
		RequestID: "req-embed-1",
		TenantID:  "acme",
		CallerID:  "billing-service",
		Scopes:    []string{tenantdata.ScopeRead},
		ExpiresAt: now.Add(time.Hour),
	}
	page, err := client.Read(ctx, id, tenantdata.ReadOptions{
		Dataset:  "orders",
		TenantID: "acme",
		Limit:    2,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("page1:", ids(page.Records), "policy_v", page.PolicyVersion)

	// 4) 游标续读（即使期间有写入/策略更新，会话仍固定在 v1 与首页快照）。
	page, err = client.ContinueRead(ctx, id, page.NextCursor)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("page2:", ids(page.Records), "more", page.More)

	// 5) 审计留痕：每个请求终态恰好一条，归一化错误原因为 ok。
	for _, e := range audit.Events() {
		fmt.Printf("audit request=%s policy_v=%d page=%d result=%s attempts=%d\n",
			e.RequestID, e.PolicyVersion, e.Page, e.ErrorKind, e.Attempts)
	}

	// Output:
	// page1: [order-0 order-1] policy_v 1
	// page2: [order-2 order-3] more false
	// audit request=req-embed-1 policy_v=1 page=1 result=ok attempts=1
	// audit request=req-embed-1 policy_v=1 page=2 result=ok attempts=1
}

func ids(rs []tenantdata.Record) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}
