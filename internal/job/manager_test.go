// manager_test.go：冒烟反馈三项的 L1 钉子（连接路径桩已在 api 测试覆盖）。
package job

import (
	"strings"
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
// 回退成短名则此处红；同时钉 overwrite DROP 重建 warning 透传，P2-1 标注）。
func TestResultRowQualified(t *testing.T) {
	row := resultRow("smoke_b", ddlsync.TableResult{Table: "dup_t", Status: "success",
		Warnings: []string{"已按 overwrite DROP 重建：目标原表（含数据）已被丢弃，本行为不可恢复"}})
	if row.Table != "smoke_b.dup_t" {
		t.Fatalf("qualified assembly regressed: %q", row.Table)
	}
	if len(row.Warnings) != 1 || !strings.Contains(row.Warnings[0], "overwrite") {
		t.Fatalf("overwrite drop warning must pass through: %+v", row.Warnings)
	}
}

// TestNoPrimaryKeySemantics P3-B 钉子：nil 集合=查询失败→视为有 PK；
// 非 nil 空集=有效「全部无 PK」；命中集合=有 PK。
func TestNoPrimaryKeySemantics(t *testing.T) {
	if noPrimaryKey(nil, "t") {
		t.Fatal("nil set (query failed) must be treated as HAS pk — no false warnings")
	}
	if !noPrimaryKey(map[string]bool{}, "t") {
		t.Fatal("empty non-nil set = genuinely no pk")
	}
	if noPrimaryKey(map[string]bool{"t": true}, "t") {
		t.Fatal("table in pk set must have pk")
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
