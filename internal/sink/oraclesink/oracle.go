// Package oraclesink 是 Sink 的 Oracle 实现（设计文档 D1/D2/D11）。
// go-ora/v2 纯 Go 驱动；单批 = 单个 Oracle 事务；MERGE 幂等 upsert。
package oraclesink

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"

	goorav2 "github.com/sijms/go-ora/v2"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect"
	oracle "github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink"
)

// Config Oracle 连接配置。
type Config struct {
	Host     string
	Port     int
	Service  string // service name
	User     string
	Password string
}

// Sink Oracle 写入端。
type Sink struct {
	db *sql.DB
	d  dialect.TargetDialect
}

// New 打开 Oracle 连接并做 session 准备（NLS utf8）。
func New(ctx context.Context, cfg Config) (*Sink, error) {
	url := goorav2.BuildUrl(cfg.Host, cfg.Port, cfg.Service, cfg.User, cfg.Password, nil)
	db, err := sql.Open("oracle", url)
	if err != nil {
		return nil, fmt.Errorf("oraclesink open: %w", err)
	}
	db.SetMaxOpenConns(4)
	d := oracle.New()
	if err := d.Prepare(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("oraclesink ping: %w", err)
	}
	return &Sink{db: db, d: d}, nil
}

// Prepare 确保目标表存在：先查 ALL_TABLES 再 CREATE（Oracle 无 IF NOT EXISTS，D5）。
// 建表保留原大小写并加引号，因此查询用原文而非 UPPER。
func (s *Sink) Prepare(ctx context.Context, meta model.TableMeta) error {
	if err := s.d.ValidateTable(meta); err != nil {
		return err
	}
	var n int
	q := "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :tname"
	if err := s.db.QueryRowContext(ctx, q, meta.Database, meta.Table).Scan(&n); err != nil {
		return fmt.Errorf("oraclesink check table: %w", err)
	}
	if n > 0 {
		return nil
	}
	ddl, err := s.d.RenderCreateTable(meta)
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("oraclesink create table: %w (ddl=%s)", err, ddl)
	}
	return nil
}

// WriteBatch 单批 = 单 Oracle 事务（D11）。PK 变更的 UPDATE 拆为
// DELETE(旧 PK) + MERGE(新行)（D2）。返回行级失败分类。
func (s *Sink) WriteBatch(ctx context.Context, meta model.TableMeta, events []model.RowEvent) (sink.FlushResult, error) {
	res := sink.FlushResult{}
	upsertSQL, err := s.d.RenderUpsert(meta)
	if err != nil {
		return res, err
	}
	deleteSQL, err := s.d.RenderDelete(meta)
	if err != nil {
		return res, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, classify(err, "", 0, 0, &res) // 连接级失败全部可重试
	}
	defer tx.Rollback()

	upsertStmt, err := tx.PrepareContext(ctx, upsertSQL)
	if err != nil {
		return res, fmt.Errorf("oraclesink prepare upsert: %w", err)
	}
	defer upsertStmt.Close()
	deleteStmt, err := tx.PrepareContext(ctx, deleteSQL)
	if err != nil {
		return res, fmt.Errorf("oraclesink prepare delete: %w", err)
	}
	defer deleteStmt.Close()

	for _, ev := range events {
		// PK 变更：先删旧行（D2）
		if ev.PKChanged() {
			oldArgs := bindPKArgs(&ev, true)
			if _, err := deleteStmt.ExecContext(ctx, oldArgs...); err != nil {
				return res, classify(err, ev.Topic, ev.Partition, ev.Offset, &res)
			}
		}
		switch ev.Op {
		case model.OpDelete:
			args := bindPKArgs(&ev, false)
			if _, err := deleteStmt.ExecContext(ctx, args...); err != nil {
				return res, classify(err, ev.Topic, ev.Partition, ev.Offset, &res)
			}
		case model.OpInsert, model.OpUpdate:
			args, err := s.bindRowArgs(&meta, &ev)
			if err != nil {
				res.Failed = append(res.Failed, sink.RowFailure{
					Topic: ev.Topic, Partition: ev.Partition, Offset: ev.Offset,
					Table: meta.Table, Reason: err.Error(), Retryable: false,
				})
				continue // 数据错误 → DLQ，不中断整批（行级错误不回滚已写行？
				// 注：单事务语义下数据错误将回滚整批，由 M2 applier 按
				// 分类决定整批重试或逐行降级，此处先返回失败明细。
			}
			if _, err := upsertStmt.ExecContext(ctx, args...); err != nil {
				return res, classify(err, ev.Topic, ev.Partition, ev.Offset, &res)
			}
		}
		res.Applied++
	}
	if err := tx.Commit(); err != nil {
		return res, classify(err, "", 0, 0, &res)
	}
	return res, nil
}

func (s *Sink) Close() error { return s.db.Close() }

// bindRowArgs 构造 MERGE 命名参数（含类型转换：Base64→RAW/BLOB 等）。
func (s *Sink) bindRowArgs(meta *model.TableMeta, ev *model.RowEvent) ([]any, error) {
	args := make([]any, 0, len(meta.Columns))
	for _, col := range meta.Columns {
		raw, ok := ev.After[col.Name]
		if !ok {
			return nil, fmt.Errorf("event missing column %q for %s.%s", col.Name, ev.Database, ev.Table)
		}
		v, err := convertValue(col.MySQLType, raw)
		if err != nil {
			return nil, err
		}
		args = append(args, sql.Named(col.Name, v))
	}
	return args, nil
}

func bindPKArgs(ev *model.RowEvent, useOld bool) []any {
	args := make([]any, 0, len(ev.PKNames))
	for _, pk := range ev.PKNames {
		v := ev.After[pk]
		if useOld {
			if old, ok := ev.Before[pk]; ok {
				v = old
			}
		}
		args = append(args, sql.Named(pk, v))
	}
	return args
}

// convertValue canal-json 字符串呈现 → go-ora 绑定值。
func convertValue(mysqlType, raw string) (any, error) {
	base := strings.ToLower(mysqlType)
	if idx := strings.IndexAny(base, "( "); idx > 0 {
		base = base[:idx]
	}
	switch {
	case strings.HasSuffix(base, "blob") || strings.HasSuffix(base, "binary"):
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("decode base64 binary value: %w", err)
		}
		return b, nil
	default:
		return raw, nil // 数值/时间由 Oracle 隐式转换，canal 字符串呈现安全
	}
}

// classify 依据 ORA- 错误号分类（D11）：
// 可重试：网络/锁（ORA-12170/00054/00031 等）；
// 数据错误：ORA-00001 唯一冲突/01722 数字转换/12899 值过宽 等 → DLQ。
func classify(err error, topic string, partition int, offset int64, res *sink.FlushResult) error {
	code := oraErrorCode(err)
	retryable := map[int]bool{
		12170: true, // 网络超时
		3113:  true, // 连接断开
		3114:  true,
		54:    true, // 资源忙（锁）
		31:    true, // 死锁标记
	}[code]
	if res != nil && !retryable {
		res.Failed = append(res.Failed, sink.RowFailure{
			Topic: topic, Partition: partition, Offset: offset,
			Reason: err.Error(), Retryable: false,
		})
	}
	return &WriteError{Err: err, Code: code, Retryable: retryable}
}

// WriteError 携带 ORA 错误码的分类错误。
type WriteError struct {
	Err       error
	Code      int
	Retryable bool
}

func (e *WriteError) Error() string { return e.Err.Error() }
func (e *WriteError) Unwrap() error { return e.Err }

// oraErrorCode 提取 ORA-xxxxx 错误码，无则 0。
func oraErrorCode(err error) int {
	msg := err.Error()
	i := strings.Index(msg, "ORA-")
	if i < 0 {
		return 0
	}
	code := 0
	for _, c := range msg[i+4:] {
		if c < '0' || c > '9' {
			break
		}
		code = code*10 + int(c-'0')
	}
	return code
}

var _ sink.Sink = (*Sink)(nil)

// CreateDLQTable 死信表 DDL（设计文档 D11，M1 定稿）。
// 数据错误的消息写入后前进 offset；主键 (topic, partition_id, offset_id)
// 保证同一消息至多一条死信记录。
const CreateDLQTable = `CREATE TABLE "kafka_dlq" (
  "topic"        VARCHAR2(256 CHAR) NOT NULL,
  "partition_id" NUMBER(10) NOT NULL,
  "offset_id"    NUMBER(19) NOT NULL,
  "db_name"      VARCHAR2(128 CHAR),
  "table_name"   VARCHAR2(128 CHAR),
  "reason"       VARCHAR2(4000 CHAR),
  "raw_payload"  CLOB,
  "created_at"   TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT "PK_KAFKA_DLQ" PRIMARY KEY ("topic", "partition_id", "offset_id")
)`
