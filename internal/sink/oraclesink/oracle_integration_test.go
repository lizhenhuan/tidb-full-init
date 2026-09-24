package oraclesink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	goorav2 "github.com/sijms/go-ora/v2"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// 本地集成冒烟测试（M1.5 P0#4 用例）：连接用户与目标 schema 分离，
// 走 system 登录 + TargetSchema="shop"（小写加引号创建的 schema）。
// 依赖 leader 起的本地 oracle-free 容器；未设 ORACLE_FREE_* 环境变量时跳过：
//
//	ORACLE_FREE_HOST=127.0.0.1 ORACLE_FREE_PORT=1521 \
//	ORACLE_FREE_SERVICE=FREEPDB1 ORACLE_FREE_USER=system \
//	ORACLE_FREE_PASSWORD=<password> go test ./internal/sink/oraclesink/ -run Integration -v
func integrationEnv(t *testing.T) Config {
	host := os.Getenv("ORACLE_FREE_HOST")
	if host == "" {
		t.Skip("ORACLE_FREE_HOST not set; skipping local integration test")
	}
	return Config{
		Host:     host,
		Port:     1521,
		Service:  envOr("ORACLE_FREE_SERVICE", "FREEPDB1"),
		User:     envOr("ORACLE_FREE_USER", "system"),
		Password: os.Getenv("ORACLE_FREE_PASSWORD"),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// shopOrdersMeta 连接用户=system、目标 schema="shop"（P0#4：二者分离）。
func shopOrdersMeta() model.TableMeta {
	return model.TableMeta{
		Database:     "lzh_db", // 源 TiDB 库名，与目标 schema 无关
		TargetSchema: "shop",
		Table:        "orders",
		Columns: []model.ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "amount", MySQLType: "decimal(10,2)"},
			{Name: "memo", MySQLType: "text"},
		},
		PKNames: []string{"id"},
	}
}

func ev(op model.OpType, id, amount, memo string) model.RowEvent {
	return model.RowEvent{
		Op: op, Database: "lzh_db", Table: "orders",
		PKNames: []string{"id"},
		After:   map[string]string{"id": id, "amount": amount, "memo": memo},
		Topic:   "t.orders", Partition: 0,
	}
}

func TestIntegrationWriteBatchShopSchema(t *testing.T) {
	cfg := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	s, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer s.Close()

	meta := shopOrdersMeta()
	if err := s.Prepare(ctx, meta); err != nil {
		t.Fatalf("Prepare (table should already exist): %v", err)
	}

	// 表是带引号小写建的（"id"/"amount"/"memo"），验证 SQL 同样用引号小写。
	q := func(sql string, args ...any) string {
		var v string
		if err := s.db.QueryRowContext(ctx, sql, args...).Scan(&v); err != nil {
			t.Fatalf("query %s: %v", sql, err)
		}
		return v
	}
	cleanup := func() { s.db.ExecContext(ctx, `DELETE FROM "shop"."orders" WHERE "id" >= 9000`) }
	cleanup()
	defer cleanup()

	// 1) 插入（P0#4：DDL/MERGE 里 "shop" 全部带引号，连接用户是 system）
	res, err := s.WriteBatch(ctx, meta, []model.RowEvent{ev(model.OpInsert, "9001", "123.45", "hello")})
	if err != nil || res.Applied != 1 || len(res.Failed) != 0 {
		t.Fatalf("insert: res=%+v err=%v", res, err)
	}
	if got := q(`SELECT TO_CHAR("amount") FROM "shop"."orders" WHERE "id"=9001`); got != "123.45" {
		t.Fatalf("amount: want 123.45, got %q", got)
	}

	// 2) MERGE 更新（幂等路径）+ NULL 写入（P0#3：空串绑 nil，CLOB 为 NULL）
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{ev(model.OpUpdate, "9001", "99.99", "")})
	if err != nil || res.Applied != 1 {
		t.Fatalf("update: res=%+v err=%v", res, err)
	}
	if got := q(`SELECT NVL(TO_CHAR("memo"), '<NULL>') FROM "shop"."orders" WHERE "id"=9001`); got != "<NULL>" {
		t.Fatalf("memo: want NULL, got %q", got)
	}

	// 3) P0#1：同批一好一坏 → 坏行 RowFailure、好行照常提交
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{
		ev(model.OpInsert, "9002", "not-a-number", "bad"),
		ev(model.OpInsert, "9003", "7.77", "good"),
	})
	if err != nil {
		t.Fatalf("mixed batch err: %v", err)
	}
	if res.Applied != 1 || len(res.Failed) != 1 || res.Failed[0].Retryable {
		t.Fatalf("mixed batch: res=%+v", res)
	}
	if res.Failed[0].Table != "orders" || res.Failed[0].Topic != "t.orders" {
		t.Fatalf("RowFailure missing table/topic: %+v", res.Failed[0])
	}
	if got := q(`SELECT COUNT(*) FROM "shop"."orders" WHERE "id"=9003`); got != "1" {
		t.Fatalf("good row not committed: %s", got)
	}

	// 4) PK 变更：UPDATE 拆 DELETE(旧)+MERGE(新)（D2）
	pk := ev(model.OpUpdate, "9010", "5.00", "moved")
	pk.Before = map[string]string{"id": "9009"}
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{pk})
	if err != nil || res.Applied != 1 {
		t.Fatalf("pk-change: res=%+v err=%v", res, err)
	}
	if got := q(`SELECT COUNT(*) FROM "shop"."orders" WHERE "id"=9010`); got != "1" {
		t.Fatalf("pk-changed row missing: %s", got)
	}

	// 5) DELETE；目标行不存在不报错（D2）
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{ev(model.OpDelete, "9010", "", "")})
	if err != nil || res.Applied != 1 {
		t.Fatalf("delete: res=%+v err=%v", res, err)
	}
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{ev(model.OpDelete, "8888", "", "")})
	if err != nil || res.Applied != 1 {
		t.Fatalf("delete nonexistent: res=%+v err=%v", res, err)
	}

	// 5b) delete 侧数据错误（复审 P2#6）：NUMBER 主键绑非数字 → deleteStmt
	// exec 抛 ORA-01722 → RowFailure 跳行 + deleteStmt 重建，同批后续好行提交。
	// 与 upsert 侧（用例 3）对称，验证 reprepare 对两条语句都生效。
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{
		ev(model.OpDelete, "not-a-number", "", ""),
		ev(model.OpInsert, "9004", "42.00", "after-bad-delete"),
	})
	if err != nil {
		t.Fatalf("mixed delete batch err: %v", err)
	}
	if res.Applied != 1 || len(res.Failed) != 1 {
		t.Fatalf("mixed delete batch: res=%+v", res)
	}
	if res.Failed[0].Retryable || !strings.Contains(res.Failed[0].Reason, "ORA-01722") {
		t.Fatalf("delete-side failure should be ORA-01722 row-data: %+v", res.Failed[0])
	}
	if got := q(`SELECT COUNT(*) FROM "shop"."orders" WHERE "id"=9004`); got != "1" {
		t.Fatalf("row after bad delete not committed: %s", got)
	}

	// 6) 可重试错误路径（复审 P1#1b/P1#3）：另一会话持有目标行锁，
	// 短超时 ctx 触发失败 → 返回 WriteError{Retryable:true} 且 res 清空。
	//
	// ⚠️ 真库实测（Oracle 26ai Free + go-ora v2）：ctx 取消发生在被行锁
	// 阻塞的语句上时，事务**不保证回滚**——本批先写成功的行可能已被提交
	// （database/sql 的 awaitDone 自动回滚与仍在阻塞的驱动调用存在竞态）。
	// 因此契约语义是：返回错误 = "本批结果未知，必须整批重放"，正确性由
	// 幂等 MERGE + 仅在成功后前进 offset 保证（D2），applier 不得把
	// error+Applied=0 理解为"什么都没写"。下方仅记录实际落库状态供观察。
	res, err = s.WriteBatch(ctx, meta, []model.RowEvent{ev(model.OpInsert, "9005", "11.11", "locked")})
	if err != nil || res.Applied != 1 {
		t.Fatalf("seed for lock test: res=%+v err=%v", res, err)
	}
	lockConn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatalf("lock conn: %v", err)
	}
	lockTx, err := lockConn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lock tx: %v", err)
	}
	if _, err := lockTx.Exec(`UPDATE "shop"."orders" SET "amount"=1 WHERE "id"=9005`); err != nil {
		t.Fatalf("hold lock: %v", err)
	}

	lockCtx, lockCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer lockCancel()
	res, err = s.WriteBatch(lockCtx, meta, []model.RowEvent{
		ev(model.OpInsert, "9006", "22.22", "never"),
		ev(model.OpUpdate, "9005", "33.33", "blocked"),
	})
	if err == nil {
		lockTx.Rollback()
		lockConn.Close()
		t.Fatalf("expected lock-timeout error, got nil (res=%+v)", res)
	}
	var we *WriteError
	if !errors.As(err, &we) || !we.Retryable {
		lockTx.Rollback()
		lockConn.Close()
		t.Fatalf("lock timeout must classify retryable, got %v", err)
	}
	if res.Applied != 0 || len(res.Failed) != 0 {
		lockTx.Rollback()
		lockConn.Close()
		t.Fatalf("failed batch must return empty result, got %+v", res)
	}
	// 释放锁，用全新连接观察实际落库状态（见上方 ⚠️：可能存在已提交行，
	// 这是驱动行为记录，不是断言）。
	if rbErr := lockTx.Rollback(); rbErr != nil {
		t.Logf("release lock tx: %v", rbErr)
	}
	lockConn.Close()
	time.Sleep(2 * time.Second) // 给 database/sql 的异步回滚/驱动收尾留时间
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer verifyCancel()
	vs, err := New(verifyCtx, cfg)
	if err != nil {
		t.Fatalf("verify conn: %v", err)
	}
	defer vs.Close()
	var n9006 int
	var amt9005 string
	if err := vs.db.QueryRowContext(verifyCtx,
		`SELECT COUNT(*) FROM "shop"."orders" WHERE "id"=9006`).Scan(&n9006); err != nil {
		t.Fatalf("verify 9006: %v", err)
	}
	vs.db.QueryRowContext(verifyCtx,
		`SELECT NVL(TO_CHAR("amount"),'<none>') FROM "shop"."orders" WHERE "id"=9005`).Scan(&amt9005)
	t.Logf("post-cancel DB state (driver reality, informational): id=9006 count=%d, id=9005 amount=%s",
		n9006, amt9005)
	if n9006 != 0 {
		t.Logf("NOTE: partial row 9006 SURVIVED ctx-cancel (known go-ora behavior, replay-safe via idempotent MERGE)")
	}
}

// TestIntegrationQuotedSchemaLogin 连接用户大小写语义（leader 实测发现）：
// system（大写默认）可直接登录；小写引号用户 "shop" 未加引号登录会 ORA-01017。
// 此用例固化「连接用户与目标 schema 是两个独立配置」的结论。
func TestIntegrationQuotedSchemaLogin(t *testing.T) {
	cfg := integrationEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 小写用户名不加引号 → ORA-01017（证明不能拿 TargetSchema 直接当登录用户拼连接串）。
	// shop 用户密码从环境变量取（复审 P2#3：口令不入库），未设置则跳过该子用例。
	shopPwd := os.Getenv("ORACLE_FREE_SHOP_PASSWORD")
	if shopPwd == "" {
		t.Log("ORACLE_FREE_SHOP_PASSWORD not set; skipping unquoted-login subcase")
		return
	}
	bad := cfg
	bad.User, bad.Password = "shop", shopPwd
	url := goorav2.BuildUrl(bad.Host, bad.Port, bad.Service, bad.User, bad.Password, nil)
	if _, err := sQueryOne(ctx, url); err == nil {
		t.Log("unquoted lowercase login unexpectedly succeeded (Oracle 版本差异)")
	} else {
		t.Logf("unquoted lowercase login rejected as expected: %v", err)
	}
}

func sQueryOne(ctx context.Context, url string) (string, error) {
	db, err := sql.Open("oracle", url)
	if err != nil {
		return "", err
	}
	defer db.Close()
	var v string
	if err := db.QueryRowContext(ctx, "SELECT 'ok' FROM DUAL").Scan(&v); err != nil {
		return "", fmt.Errorf("ping-query: %w", err)
	}
	return v, nil
}
