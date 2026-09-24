package verify

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// 真 TiDB 镜像表比对（T2.4）：源表 vs 镜像表必须 Equal，
// 并验证 UPDATE/DELETE 注入能被逐列/缺失检测。未设 TIDB_TEST_DSN 跳过：
//
//	TIDB_TEST_DSN='root:root123@tcp(10.168.2.232:4000)/' go test ./internal/verify/ -run Integration -v
func TestIntegrationTiDBMirrorCompare(t *testing.T) {
	dsn := os.Getenv("TIDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TIDB_TEST_DSN not set; skipping integration test")
	}
	ctx := context.Background()
	db := "verify_test_" + fmt.Sprintf("%d%d", time.Now().UnixNano()%1e6, os.Getpid()%1000)
	const tbl, tbl2 = "orders", "orders2"

	// DDL/注入走独立连接（TiDBSource 面向读）
	ddlDB := openRaw(t, dsn)
	exec(t, ddlDB, `CREATE DATABASE IF NOT EXISTS `+db)
	for _, tb := range []string{tbl, tbl2} {
		exec(t, ddlDB, `CREATE TABLE `+db+`.`+tb+` (
			id BIGINT PRIMARY KEY,
			amount DECIMAL(10,2),
			memo VARCHAR(64),
			created DATETIME(6),
			ts DATE)`)
		exec(t, ddlDB, fmt.Sprintf(`INSERT INTO %s.%s VALUES
			(1, 10.50, 'a', '2026-09-24 10:30:05.123456', '2026-09-24'),
			(2, 20.00, NULL, '2026-01-01 00:00:00', '2026-01-01'),
			(3, -0.10, 'c', NULL, NULL)`, db, tb))
	}
	defer exec(t, ddlDB, `DROP DATABASE IF EXISTS `+db)

	src, err := NewTiDBSource(ctx, TiDBConfig{DSN: dsn, Database: db, Table: tbl})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	// 镜像表比对（源 vs 目标独立实体）
	dst, err := NewTiDBSource(ctx, TiDBConfig{DSN: dsn, Database: db, Table: tbl2})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	rep, err := Compare(ctx, src, dst, Options{ChunkSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Equal() {
		t.Fatalf("mirror compare must be equal: %+v", rep)
	}
	if len(rep.Chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(rep.Chunks))
	}

	// 注入 UPDATE → 行级列差异
	exec(t, ddlDB, `UPDATE `+db+`.`+tbl2+` SET amount = 99.99 WHERE id = 2`)
	rep2, err := Compare(ctx, src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Equal() || rep2.RowDiffCount != 1 || len(rep2.RowDiffs) != 1 {
		t.Fatalf("update diff = %+v", rep2)
	}
	d := rep2.RowDiffs[0]
	if d.PK != "2" || d.Columns[0].Column != "amount" {
		t.Fatalf("diff detail = %+v", d)
	}

	// 注入 DELETE → 缺失
	exec(t, ddlDB, `DELETE FROM `+db+`.`+tbl2+` WHERE id = 3`)
	rep3, err := Compare(ctx, src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep3.MissingCount != 1 || rep3.MissingInTarget[0] != "3" {
		t.Fatalf("missing = %+v", rep3)
	}

	// 抽样（此刻镜像仍缺 id=3 → 抽满必命中）
	repS, err := Sample(ctx, src, dst, 3)
	if err != nil {
		t.Fatal(err)
	}
	if repS.MissingCount != 1 {
		t.Fatalf("sample = %+v", repS)
	}
}

// 真 TiDB vs 真 Oracle 跨端比对（T2.4 核心）：同一批数据两侧表示不同
// （DECIMAL 文本 vs NUMBER、DATETIME(6) vs TIMESTAMP），规范化后必须 Equal；
// 注入差异必须被定位。需同时设 TIDB_TEST_DSN 与 ORACLE_FREE_*（同
// oracle_integration_test 约定），随机 schema 防并发互踩。
func TestIntegrationTiDBVsOracle(t *testing.T) {
	tdsn := os.Getenv("TIDB_TEST_DSN")
	ohost := os.Getenv("ORACLE_FREE_HOST")
	if tdsn == "" || ohost == "" {
		t.Skip("TIDB_TEST_DSN / ORACLE_FREE_HOST not set; skipping cross-db integration test")
	}
	ctx := context.Background()
	suffix := fmt.Sprintf("%d%d", time.Now().UnixNano()%1e6, os.Getpid()%1000)
	mysqlDB := "verify_x_" + suffix
	oraSchema := "vx" + suffix // ≤30 字符
	const tbl = "orders"

	ddlDB := openRaw(t, tdsn)
	exec(t, ddlDB, `CREATE DATABASE IF NOT EXISTS `+mysqlDB)
	exec(t, ddlDB, `CREATE TABLE `+mysqlDB+`.`+tbl+` (
		id BIGINT PRIMARY KEY,
		amount DECIMAL(10,2),
		memo VARCHAR(64),
		created DATETIME(6))`)
	defer exec(t, ddlDB, `DROP DATABASE IF EXISTS `+mysqlDB)

	ora := openOracle(t, ctx)
	oexec(t, ora, fmt.Sprintf(`CREATE USER "%s" IDENTIFIED BY "pw_%s"`, oraSchema, suffix))
	defer oexec(t, ora, fmt.Sprintf(`DROP USER "%s" CASCADE`, oraSchema))
	oexec(t, ora, fmt.Sprintf(`GRANT CREATE SESSION, CREATE TABLE, UNLIMITED TABLESPACE TO "%s"`, oraSchema))
	// 小写加引号建 schema/表（与 oraclesink 生产建表约定一致；OracleSource 亦按小写引号寻址）
	oexec(t, ora, fmt.Sprintf(`CREATE TABLE "%s"."%s" (
		ID NUMBER(20,0) PRIMARY KEY,
		AMOUNT NUMBER(10,2),
		MEMO VARCHAR2(64),
		CREATED TIMESTAMP(6))`, oraSchema, tbl))
	oexec(t, ora, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (1, 10.50, 'a', TIMESTAMP '2026-09-24 10:30:05.123456')`, oraSchema, tbl))
	oexec(t, ora, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (2, 20.00, NULL, TIMESTAMP '2026-01-01 00:00:00')`, oraSchema, tbl))
	exec(t, ddlDB, fmt.Sprintf(`INSERT INTO %s.%s VALUES
		(1, 10.50, 'a', '2026-09-24 10:30:05.123456'),
		(2, 20.00, NULL, '2026-01-01 00:00:00')`, mysqlDB, tbl))

	src, err := NewTiDBSource(ctx, TiDBConfig{DSN: tdsn, Database: mysqlDB, Table: tbl})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := NewOracleSource(ctx, OracleConfig{
		Host: ohost, Port: 1521,
		Service:  envOr("ORACLE_FREE_SERVICE", "FREEPDB1"),
		User:     envOr("ORACLE_FREE_USER", "system"),
		Password: os.Getenv("ORACLE_FREE_PASSWORD"),
		Schema:   oraSchema, Table: tbl,
	}, src.PKNames())
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()

	rep, err := Compare(ctx, src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Equal() {
		t.Fatalf("cross-db compare must be equal: %+v diffs=%+v", rep, rep.RowDiffs)
	}

	// 注入差异：Oracle 侧改 amount → 列级定位
	oexec(t, ora, fmt.Sprintf(`UPDATE "%s"."%s" SET AMOUNT = 99.99 WHERE ID = 1`, oraSchema, tbl))
	dst2, err := NewOracleSource(ctx, OracleConfig{
		Host: ohost, Port: 1521,
		Service:  envOr("ORACLE_FREE_SERVICE", "FREEPDB1"),
		User:     envOr("ORACLE_FREE_USER", "system"),
		Password: os.Getenv("ORACLE_FREE_PASSWORD"),
		Schema:   oraSchema, Table: tbl,
	}, src.PKNames())
	if err != nil {
		t.Fatal(err)
	}
	defer dst2.Close()
	rep2, err := Compare(ctx, src, dst2, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Equal() || rep2.RowDiffCount != 1 {
		t.Fatalf("oracle-side diff not detected: %+v", rep2)
	}
	if d := rep2.RowDiffs[0]; d.PK != "1" || !strings.EqualFold(d.Columns[0].Column, "amount") {
		t.Fatalf("diff detail = %+v", d)
	}
}
