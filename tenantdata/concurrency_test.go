package tenantdata

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// 大量租户并发全量翻页，同时持续写入：每个会话都必须恰好读全快照内记录。
func TestConcurrentReadsConsistentUnderWrites(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxConcurrent = 64
	cfg.DefaultPageSize = 7
	cfg.MaxPageSize = 7
	f := newFixture(t, cfg)

	tenants := []string{"acme", "beta", "gamma"}
	ctx := context.Background()
	if err := f.data.CreateDataset(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	for i, tn := range tenants {
		seedPolicy(t, f, tn, 1, ScopeRead)
		seedRecordsOnly(t, f, "orders", tn, 20+i*5)
	}

	stop := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		i := 100
		for {
			select {
			case <-stop:
				return
			default:
				tn := tenants[i%len(tenants)]
				// 使用与初始记录(0..29)不重叠的高区间 ID，且按租户错开，
				// 避免跨租户同 ID 修订造成测试夹具层面的混淆。
				_ = f.data.Upsert(ctx, "orders", Record{ID: "w" + tn + "-" + strconv.Itoa(i), TenantID: tn})
				i++
			}
		}
	}()

	const perTenant = 12
	var wg sync.WaitGroup
	errs := make(chan error, len(tenants)*perTenant)
	for g := 0; g < perTenant; g++ {
		for _, tn := range tenants {
			wg.Add(1)
			go func(tenant string, g int) {
				defer wg.Done()
				id := validIdentity(tenant, reqID("conc-"+tenant, g), ScopeRead)
				res, err := f.client.Read(ctx, id, ReadOptions{Dataset: "orders", TenantID: tenant, Limit: 7})
				if err != nil {
					errs <- err
					return
				}
				wm := res.Watermark
				all := append([]string{}, recordIDs(res.Records)...)
				pv := res.PolicyVersion
				for res.NextCursor != "" {
					res, err = f.client.ContinueRead(ctx, id, res.NextCursor)
					if err != nil {
						errs <- err
						return
					}
					if res.Watermark != wm || res.PolicyVersion != pv {
						errs <- errInconsistent(tenant, wm, res.Watermark, pv, res.PolicyVersion)
						return
					}
					all = append(all, recordIDs(res.Records)...)
				}
				// 基准：在固定水位 wm 下直接扫描存储，SDK 结果必须与其完全一致。
				baseRecs, _, _, berr := f.data.Scan(ctx, "orders", tenant, wm, Filter{}, "", 100000)
				if berr != nil {
					errs <- berr
					return
				}
				wantIDs := recordIDs(baseRecs)
				if len(all) != len(wantIDs) {
					errs <- errCount(tenant, len(wantIDs), len(all))
					return
				}
				for i := range wantIDs {
					if all[i] != wantIDs[i] {
						errs <- newError(KindInvalidArgument, "", 0,
							"%s snapshot mismatch at %d: got %s want %s", tenant, i, all[i], wantIDs[i])
						return
					}
				}
				assertNoDup(tenant, all, errs)
			}(tn, g)
		}
	}
	wg.Wait()
	close(stop)
	writerWG.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// 日志必须包含请求标识、策略版本、游标位置与归一化错误原因。
func TestLogsContainRequiredContext(t *testing.T) {
	f := newFixture(t, DefaultConfig())
	seedPolicy(t, f, "acme", 1, ScopeRead)
	seedRecords(t, f, "orders", "acme", 3)

	// 成功路径日志。
	_, err := f.client.Read(context.Background(), validIdentity("acme", "req-log-ok", ScopeRead),
		ReadOptions{Dataset: "orders", TenantID: "acme"})
	if err != nil {
		t.Fatal(err)
	}

	// 归一化错误路径日志：访问不存在的数据集。
	_, e2 := f.client.Read(context.Background(), validIdentity("acme", "req-log-404", ScopeRead),
		ReadOptions{Dataset: "ghost", TenantID: "acme"})
	if e2 == nil {
		t.Fatal("expected not_found")
	}

	joined := strings.Join(f.log.Lines(), "\n")
	for _, want := range []string{
		"request=req-log-ok", "policy_version=1", `cursor_pos="rec-0002"`, "error=ok",
		"request=req-log-404", "error=not_found",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("log missing %q\nfull logs:\n%s", want, joined)
		}
	}
}

func errInconsistent(tenant string, wm1, wm2 int64, pv1, pv2 uint64) error {
	return newError(KindInvalidArgument, "", 0,
		"%s session drifted: watermark %d->%d policy %d->%d", tenant, wm1, wm2, pv1, pv2)
}

func errCount(tenant string, want, got int) error {
	return newError(KindInvalidArgument, "", 0, "%s record count = %d, want %d", tenant, got, want)
}

func assertNoDup(tenant string, ids []string, errs chan<- error) {
	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			errs <- newError(KindInvalidArgument, "", 0, "%s duplicate id %s", tenant, id)
			return
		}
		seen[id] = true
	}
}
