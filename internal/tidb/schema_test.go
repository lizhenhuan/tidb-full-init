package tidb

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// 真 TiDB 集成测试（M2 复审 P2 接线验证）：SchemaFetcher 的 NotFound 语义
// —— 表删除后返回 *model.TableNotFoundError（applier 整组 DLQ 的判据），
// 而非普通错误（fatal）。未设 TIDB_TEST_DSN 时跳过：
//
//	TIDB_TEST_DSN='root:root123@tcp(10.168.2.232:4000)/' go test ./internal/tidb/ -run Integration -v
func TestIntegrationSchemaFetcherNotFound(t *testing.T) {
	dsn := os.Getenv("TIDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TIDB_TEST_DSN not set; skipping integration test")
	}
	sf, err := NewSchemaFetcher(dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer sf.Close()

	ctx := context.Background()
	_ = ctx
	const db, tbl = "e2e_schema_test", "orders"
	mustExec := func(q string) {
		t.Helper()
		if _, err := sf.db.Exec(q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(`CREATE DATABASE IF NOT EXISTS ` + db)
	mustExec(`CREATE TABLE IF NOT EXISTS ` + db + `.` + tbl + ` (id BIGINT PRIMARY KEY, amount DECIMAL(10,2), memo VARCHAR(64))`)
	defer mustExec(`DROP DATABASE IF EXISTS ` + db)

	info, _, err := sf.GetTableInfo(db, tbl)
	if err != nil {
		t.Fatalf("GetTableInfo: %v", err)
	}
	if len(info.Columns) != 3 || len(info.PKNames) != 1 || info.PKNames[0] != "id" {
		t.Fatalf("schema = %+v, want 3 cols / pk=id", info)
	}

	// 删除表后：必须是 *TableNotFoundError（不是普通错误）
	mustExec(`DROP TABLE ` + db + `.` + tbl)
	_, _, err = sf.GetTableInfo(db, tbl)
	if err == nil {
		t.Fatal("GetTableInfo on dropped table must fail")
	}
	var nf *model.TableNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("dropped-table error must be *model.TableNotFoundError, got %T: %v", err, err)
	}
	if nf.Database != db || nf.Table != tbl {
		t.Errorf("NotFound fields = %s.%s, want %s.%s", nf.Database, nf.Table, db, tbl)
	}
	if !strings.Contains(err.Error(), "not found in source schema") {
		t.Errorf("error text = %q", err.Error())
	}

	// 从未存在的表同样走 NotFound
	var nf2 *model.TableNotFoundError
	if _, _, err := sf.GetTableInfo(db, "never_existed"); !errors.As(err, &nf2) {
		t.Fatalf("never-existed table must be NotFound, got %T: %v", err, err)
	}
}
