// store_test.go：warnings 落库钉子（#t8 重开缺陷——store 三处静默丢
// warnings，REST/console 路径必丢；CLI 内存直读可见导致 L2 漏检）。
package job

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// openRaw 直接开原始连接（绕过 OpenStore 的建表/迁移，造旧库用）。
func openRaw(path string) (*sql.DB, error) { return sql.Open("sqlite", path) }

// TestWarningsRoundTrip 钉子：Upsert 带 warnings → Get/List 读回非空；
// 覆盖 upsert 时 warnings 一并更新（清空不残留旧行值）。
func TestWarningsRoundTrip(t *testing.T) {
	s, err := OpenStore("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	j := &Job{Type: "ddl-sync", Name: "j", Status: "running"}
	if err := s.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	want := []string{"已按 overwrite DROP 重建：目标原表（含数据）已被丢弃，本行为不可恢复"}
	if err := s.UpsertDDLResult(j.ID, &DDLResultRow{
		Table: "db.t", Status: "success", Warnings: want,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDDLResult(j.ID, "db.t")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if len(got.Warnings) != 1 || got.Warnings[0] != want[0] {
		t.Fatalf("GetDDLResult dropped warnings: %+v", got.Warnings)
	}
	rows, err := s.ListDDLResults(j.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list: %v %v", rows, err)
	}
	if len(rows[0].Warnings) != 1 || rows[0].Warnings[0] != want[0] {
		t.Fatalf("ListDDLResults dropped warnings: %+v", rows[0].Warnings)
	}
	// GetJob 聚合路径（GET /jobs/{id} 实际出口）同读
	full, err := s.GetJob(j.ID)
	if err != nil || full == nil {
		t.Fatalf("getjob: %v %v", full, err)
	}
	if len(full.DDLResults) != 1 || len(full.DDLResults[0].Warnings) != 1 {
		t.Fatalf("GetJob.ddlResults dropped warnings: %+v", full.DDLResults)
	}
	// 重跑 upsert 无 warnings → 不残留旧值（语义：行状态整体替换）
	if err := s.UpsertDDLResult(j.ID, &DDLResultRow{Table: "db.t", Status: "failed", ErrorText: "x"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetDDLResult(j.ID, "db.t")
	if got == nil || len(got.Warnings) != 0 {
		t.Fatalf("upsert without warnings must clear stale ones: %+v", got.Warnings)
	}
}

// TestLegacyDBWarningsMigration 旧库（无 warnings_json 列）跑 OpenStore
// 迁移后可读写：CREATE TABLE IF NOT EXISTS 不作用于已存在的表，靠
// migrateDDLResultsWarnings 补列。
func TestLegacyDBWarningsMigration(t *testing.T) {
	dir := t.TempDir()
	path := "file:" + filepath.Join(dir, "console.db")

	// 手工造旧库：ddl_results 无 warnings_json
	old, err := openRaw(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`CREATE TABLE jobs (
	  id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, type TEXT NOT NULL,
	  status TEXT NOT NULL DEFAULT 'pending', summary TEXT NOT NULL DEFAULT '',
	  progress_percent INTEGER NOT NULL DEFAULT 0, progress_text TEXT NOT NULL DEFAULT '',
	  error_text TEXT NOT NULL DEFAULT '', oracle_env_json TEXT,
	  payload_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);`); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`CREATE TABLE ddl_results (
	  job_id INTEGER NOT NULL REFERENCES jobs(id), table_name TEXT NOT NULL,
	  target_table TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
	  ddl TEXT NOT NULL DEFAULT '', fixed_ddl TEXT NOT NULL DEFAULT '',
	  error_code TEXT NOT NULL DEFAULT '', error_text TEXT NOT NULL DEFAULT '',
	  compat_json TEXT NOT NULL DEFAULT '[]', updated_at TEXT NOT NULL,
	  PRIMARY KEY (job_id, table_name));`); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`INSERT INTO jobs (name,type,status,payload_json,created_at,updated_at)
	  VALUES('old','ddl-sync','success','{}','t','t')`); err != nil {
		t.Fatal(err)
	}
	if _, err := old.Exec(`INSERT INTO ddl_results (job_id,table_name,status,updated_at)
	  VALUES(1,'legacy.t','success','t')`); err != nil {
		t.Fatal(err)
	}
	old.Close()

	// OpenStore 触发迁移：旧行可读、新行带 warnings 可写读
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal("OpenStore migration failed:", err)
	}
	t.Cleanup(func() { s.Close() })
	prev, err := s.GetDDLResult(1, "legacy.t")
	if err != nil || prev == nil {
		t.Fatalf("legacy row unreadable after migration: %v %v", prev, err)
	}
	if len(prev.Warnings) != 0 {
		t.Fatalf("legacy row must default to no warnings: %+v", prev.Warnings)
	}
	if err := s.UpsertDDLResult(1, &DDLResultRow{
		Table: "legacy.t2", Status: "success",
		Warnings: []string{"无主键：结构已创建，后续全量/增量任务将被管道 ValidateTable 拒绝（已知差异）"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDDLResult(1, "legacy.t2")
	if err != nil || got == nil || len(got.Warnings) != 1 {
		t.Fatalf("post-migration write/read: %v %v", got, err)
	}

	// 再开一次：迁移幂等（列已存在不报错不重复）
	s.Close()
	s2, err := OpenStore(dir)
	if err != nil {
		t.Fatal("re-open (idempotent migration) failed:", err)
	}
	defer s2.Close()
	got, err = s2.GetDDLResult(1, "legacy.t2")
	if err != nil || got == nil || len(got.Warnings) != 1 {
		t.Fatalf("re-open read: %v %v", got, err)
	}
}
