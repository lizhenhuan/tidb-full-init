// manager_test.go：冒烟反馈三项的 L1 钉子（连接路径桩已在 api 测试覆盖）。
package job

import (
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/ddlsync"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	store, err := OpenStore("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return NewManager(store)
}

// TestQualifiedTableKey 跨库同名表在一个 job 内不互相覆盖（冒烟 #1）。
func TestQualifiedTableKey(t *testing.T) {
	m := newTestManager(t)
	j := &Job{Type: "ddl-sync", Name: "j", Status: "success"}
	if err := m.Store.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range []string{"db_a.users", "db_b.users"} {
		if err := m.Store.UpsertDDLResult(j.ID, &DDLResultRow{
			Table: tbl, Status: "success", DDL: "CREATE TABLE " + tbl}); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.Store.ListDDLResults(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("same-name tables across dbs collapsed: %d rows", len(rows))
	}
}

// TestResultRowQualified 拼装钉子（P3-1：resultRow 必须产出全限定 table，
// 回退成短名则此处红）。
func TestResultRowQualified(t *testing.T) {
	row := resultRow("smoke_b", ddlsync.TableResult{Table: "dup_t", Status: "success"})
	if row.Table != "smoke_b.dup_t" {
		t.Fatalf("qualified assembly regressed: %q", row.Table)
	}
}

// TestRecomputeTerminal 重跑成功后任务终态刷新（冒烟 #2）。
func TestRecomputeTerminal(t *testing.T) {
	m := newTestManager(t)
	j := &Job{Type: "ddl-sync", Name: "j", Status: "failed"}
	if err := m.Store.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	_ = m.Store.UpsertDDLResult(j.ID, &DDLResultRow{Table: "d.ok1", Status: "success"})
	_ = m.Store.UpsertDDLResult(j.ID, &DDLResultRow{Table: "d.ok2", Status: "skipped"})
	_ = m.Store.UpsertDDLResult(j.ID, &DDLResultRow{Table: "d.bad", Status: "failed"})

	// 仍有 failed → 保持 failed
	if err := m.recomputeTerminal(j.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := m.Store.GetJob(j.ID)
	if got.Status != "failed" {
		t.Fatalf("with failed rows want failed, got %s", got.Status)
	}

	// 最后一行重跑成功 → success
	_ = m.Store.UpsertDDLResult(j.ID, &DDLResultRow{Table: "d.bad", Status: "success"})
	if err := m.recomputeTerminal(j.ID); err != nil {
		t.Fatal(err)
	}
	got, _ = m.Store.GetJob(j.ID)
	if got.Status != "success" {
		t.Fatalf("all non-failed want success, got %s (summary=%s)", got.Status, got.Summary)
	}
	if got.ProgressPercent != 100 {
		t.Fatalf("terminal progress want 100, got %d", got.ProgressPercent)
	}
}
