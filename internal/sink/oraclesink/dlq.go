package oraclesink

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lizhenhuan/tidb-full-init/internal/sink"
)

// DLQ 死信落库（设计文档 D11）：数据错误的消息按 Kafka 坐标
// (topic, partition, offset) 幂等 MERGE 进 "kafka_dlq"——重启/整窗重放
// 后同一消息至多一条死信记录。表建在登录用户自己 schema 下。
type DLQ struct {
	db *sql.DB
}

// NewDLQ 基于 Sink 已打开的连接构造（共享连接池）。
func (s *Sink) NewDLQ() *DLQ { return &DLQ{db: s.db} }

// EnsureTable 幂等建表（Oracle 无 IF NOT EXISTS，先查 USER_TABLES）。
func (d *DLQ) EnsureTable(ctx context.Context) error {
	var n int
	if err := d.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'kafka_dlq'`).Scan(&n); err != nil {
		return fmt.Errorf("oraclesink dlq check table: %w", err)
	}
	if n > 0 {
		return nil
	}
	if _, err := d.db.ExecContext(ctx, CreateDLQTable); err != nil {
		return fmt.Errorf("oraclesink dlq create table: %w", err)
	}
	return nil
}

// reason 上限 4000 字符（VARCHAR2(4000 CHAR)），防御 ORA-12899。
func truncReason(r string) string {
	if len(r) > 4000 {
		return r[:4000]
	}
	return r
}

const dlqMergeSQL = `MERGE INTO "kafka_dlq" t
USING (SELECT :topic AS topic, :pid AS partition_id, :oid AS offset_id FROM dual) s
ON (t."topic" = s.topic AND t."partition_id" = s.partition_id AND t."offset_id" = s.offset_id)
WHEN MATCHED THEN UPDATE SET t."reason" = :reason, t."raw_payload" = :raw, t."created_at" = SYSTIMESTAMP
WHEN NOT MATCHED THEN INSERT ("topic","partition_id","offset_id","reason","raw_payload")
VALUES (:topic, :pid, :oid, :reason, :raw)`

// Write 单事务写入一批死信（applier.DLQWriter 接口）。
// 失败返回错误只影响日志可见性（applier 侧计数在前、不因 DLQ 失败回退位点）。
func (d *DLQ) Write(ctx context.Context, failures []sink.RowFailure) error {
	if len(failures) == 0 {
		return nil
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, dlqMergeSQL)
	if err != nil {
		return fmt.Errorf("oraclesink dlq prepare: %w", err)
	}
	defer stmt.Close()

	for _, f := range failures {
		raw := f.Raw
		if raw == nil {
			raw = []byte{}
		}
		if _, err := stmt.ExecContext(ctx,
			sql.Named("topic", f.Topic),
			sql.Named("pid", f.Partition),
			sql.Named("oid", f.Offset),
			sql.Named("reason", truncReason(f.Reason)),
			sql.Named("raw", string(raw))); err != nil {
			return classify(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return classify(err)
	}
	return nil
}
