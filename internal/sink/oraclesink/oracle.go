// Package oraclesink 是 Sink 的 Oracle 实现（设计文档 D1/D2/D11）。
// go-ora/v2 纯 Go 驱动；单批 = 单个 Oracle 事务；MERGE 幂等 upsert。
package oraclesink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

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
	if err := s.db.QueryRowContext(ctx, q, meta.Schema(), meta.Table).Scan(&n); err != nil {
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
// DELETE(旧 PK) + MERGE(新行)（D2）。
//
// 失败契约（M1.5 P0#1 定稿 + 跟进复审 P1#1 三态化）：
//   - retryable（网络/锁/ctx 取消）→ 整体回滚，返回 *WriteError{Retryable:true}；
//     语义是"本批结果未知"（ctx 取消下事务可能已提交部分行），applier
//     必须整批重放，绝不部分续传；
//   - row-data（白名单 ORA 码，见 classify）→ 记 RowFailure（带 Kafka
//     坐标与表名，Raw 由 applier 填充）跳过该行，其余行照常提交；
//   - fatal（默认：表不存在/权限/未知错误）→ 返回错误中止任务，不前进 offset；
//   - Applied 只统计随本事务真正提交的行；返回错误时 Applied/Failed 归零。
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
		return res, classify(err) // 连接级失败全部可重试
	}
	defer tx.Rollback()

	upsertStmt, err := tx.PrepareContext(ctx, upsertSQL)
	if err != nil {
		return res, fmt.Errorf("oraclesink prepare upsert: %w", err)
	}
	// 闭包捕获变量本身：reprepare 重赋值后 defer 关闭的是最终句柄
	//（旧句柄在 reprepare 内已关闭，重复 Close 无害）——复审 P2#4。
	defer func() { upsertStmt.Close() }()
	deleteStmt, err := tx.PrepareContext(ctx, deleteSQL)
	if err != nil {
		return res, fmt.Errorf("oraclesink prepare delete: %w", err)
	}
	defer func() { deleteStmt.Close() }()

	applyOne := func(ev *model.RowEvent) error {
		// PK 变更：先删旧行（D2）
		if ev.PKChanged() {
			oldArgs, err := s.bindPKArgs(&meta, ev, true)
			if err != nil {
				return dataError(err) // 绑定/转换失败：数据错误，非可重试
			}
			if _, err := deleteStmt.ExecContext(ctx, oldArgs...); err != nil {
				return classify(err)
			}
		}
		switch ev.Op {
		case model.OpDelete:
			args, err := s.bindPKArgs(&meta, ev, false)
			if err != nil {
				return dataError(err)
			}
			if _, err := deleteStmt.ExecContext(ctx, args...); err != nil {
				return classify(err)
			}
		case model.OpInsert, model.OpUpdate:
			args, err := s.bindRowArgs(&meta, ev)
			if err != nil {
				return dataError(err)
			}
			if _, err := upsertStmt.ExecContext(ctx, args...); err != nil {
				return classify(err)
			}
		}
		return nil
	}

	// reprepare 在数据错误后重建语句：真库实测 go-ora 的 prepared
	// statement 在一次 exec 报 ORA-01722 后，同语句下一次 exec 会抛
	// ORA-03120（语句状态被污染），必须重建才能继续本批剩余行。
	reprepare := func(stmt **sql.Stmt, query string) error {
		if *stmt != nil {
			(*stmt).Close()
		}
		ns, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return err
		}
		*stmt = ns
		return nil
	}

	for i := range events {
		if err := applyOne(&events[i]); err != nil {
			var we *WriteError
			if errors.As(err, &we) && we.RowData {
				// 行级数据错误 → 记 RowFailure 跳过该行，其余行照常提交（DLQ）。
				// 执行失败的语句状态不可信（ORA-01722 后复用会抛 ORA-03120），
				// 重建后继续。
				res.Failed = append(res.Failed, sink.RowFailure{
					Topic: events[i].Topic, Partition: events[i].Partition,
					Offset: events[i].Offset, Table: meta.Table,
					Reason: err.Error(), Retryable: false,
				})
				if rerr := reprepare(&upsertStmt, upsertSQL); rerr != nil {
					res.Applied, res.Failed = 0, nil
					return res, fmt.Errorf("oraclesink reprepare upsert: %w", rerr)
				}
				if rerr := reprepare(&deleteStmt, deleteSQL); rerr != nil {
					res.Applied, res.Failed = 0, nil
					return res, fmt.Errorf("oraclesink reprepare delete: %w", rerr)
				}
				continue
			}
			// retryable 或 fatal → 整批回滚（defer Rollback）返回错误：
			// retryable 由 applier 整批重放（幂等安全）；fatal 中止任务。
			// 已累计的 Applied/Failed 全部随事务作废，重放时重新推导，
			// 不能返回"已回滚但 Applied>0"的矛盾状态（复审 P1#3）。
			res.Applied = 0
			res.Failed = nil
			return res, err
		}
		res.Applied++
	}
	if err := tx.Commit(); err != nil {
		res.Applied, res.Failed = 0, nil // 提交失败同样整体作废
		return res, classify(err)
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

// bindPKArgs 构造 DELETE 的 PK 命名参数，并按列 MySQL 类型做与行写入
// 一致的显式转换（P0#2：日期类 PK 不能依赖 Oracle 隐式转换）。
func (s *Sink) bindPKArgs(meta *model.TableMeta, ev *model.RowEvent, useOld bool) ([]any, error) {
	args := make([]any, 0, len(ev.PKNames))
	for _, pk := range ev.PKNames {
		raw, ok := ev.After[pk]
		if useOld {
			if old, ok2 := ev.Before[pk]; ok2 {
				raw, ok = old, true
			}
		}
		if !ok {
			return nil, fmt.Errorf("event missing PK column %q for %s.%s", pk, ev.Database, ev.Table)
		}
		v, err := convertValue(pkMySQLType(meta, pk), raw)
		if err != nil {
			return nil, err
		}
		args = append(args, sql.Named(pk, v))
	}
	return args, nil
}

// pkMySQLType 从表元数据取列类型；元数据缺失时返回空串（走字符串绑定）。
func pkMySQLType(meta *model.TableMeta, col string) string {
	for _, c := range meta.Columns {
		if c.Name == col {
			return c.MySQLType
		}
	}
	return ""
}

// timeLayouts canal-json 时间字段的候选呈现格式（TiCDC：空格分隔；
// 兼容 ISO-8601 与纯日期）。
var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// convertValue canal-json 字符串呈现 → go-ora 绑定值。
//
// NULL 语义（P0#3）：decoder 把 canal-json 的 null 呈现为空串；
// Oracle 中 ''≡NULL，因此空串统一绑 nil —— 否则 "" 隐式转 NUMBER/TIMESTAMP
// 会抛 ORA-01722/01861，BLOB/RAW 会绑成 EMPTY_BLOB 而非 NULL。
//
// 时间语义（P0#2）：date/datetime/timestamp 显式解析为 time.Time 绑定，
// 不依赖 session NLS 日期格式（隐式转换受 NLS_DATE_FORMAT 支配会抛
// ORA-01861/01858）。time 仍按规范串写 VARCHAR2(18)（D4），数值类由
// Oracle 对规范数字串的隐式转换处理（不受 NLS 影响的形态）。
func convertValue(mysqlType, raw string) (any, error) {
	base := strings.ToLower(strings.TrimSpace(mysqlType))
	if idx := strings.IndexAny(base, "( "); idx > 0 {
		base = base[:idx]
	}
	if raw == "" {
		return nil, nil
	}
	switch {
	case strings.HasSuffix(base, "blob") || strings.HasSuffix(base, "binary"):
		b, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("decode base64 binary value: %w", err)
		}
		return b, nil
	case base == "date" || base == "datetime" || base == "timestamp":
		for _, layout := range timeLayouts {
			if t, err := time.Parse(layout, raw); err == nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("cannot parse %s value %q (layouts: %v)", mysqlType, raw, timeLayouts)
	case base == "year":
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %s value %q: %w", mysqlType, raw, err)
		}
		return n, nil
	default:
		return raw, nil
	}
}

// retryableORACode 暂时性错误的 ORA 码（D11 + M1.5 跟进复审 P1#2）：
// 网络/锁。死锁是 ORA-00060（00031 是 session marked for kill，不在此列）。
func retryableORACode(code int) bool {
	switch code {
	case 54, // 资源忙（锁等待）
		60,   // 死锁（Oracle 自动回滚其中一方，重放即可）
		1013, // 用户取消 / 锁等待超时
		4021, // 锁等待超时
		3113, // 连接断开
		3114, // 未连接
		3135, // 连接意外断开
		3136, // 连接被重置
		12170: // 网络超时
		return true
	}
	// 12500-12599：TNS 协议/网络层故障
	return code >= 12500 && code <= 12599
}

// rowDataORACodes 行级数据错误的 ORA 码白名单（跟进复审 P1#1）：
// 只有这些才允许"跳行 + DLQ + 前进 offset"；其余非可重试错误一律
// fatal（中止任务、不前进 offset），避免把表不存在/权限不足等配置
// 错误伪装成"成功 + 死信"静默丢数据。
var rowDataORACodes = map[int]bool{
	1:     true, // 唯一冲突（MERGE 语义下不应出现，保守保留）
	1722:  true, // 数字转换失败（坏值）
	1400:  true, // NOT NULL 列插 NULL
	1858:  true, // 非数字出现在日期数字处
	1861:  true, // 日期字面量与格式串不匹配
	1840:  true, // 输入值不够长
	12899: true, // 值超出列宽
}

// dataError 把本地绑定/转换失败包装为行级数据错误（→ DLQ）。
func dataError(err error) error {
	return &WriteError{Err: err, Code: 0, RowData: true}
}

// classify 依据 ORA- 错误号做三态分类（D11 + M1.5 跟进复审 P1#1），
// 仅用于驱动/服务端返回的错误；本地绑定错误用 dataError：
//   - retryable（网络/锁，含 ctx 取消/超时）→ applier 整批退避重放，
//     语义是"本批结果未知"，绝不部分续传；
//   - row-data（白名单：值转换/宽度/约束类）→ WriteBatch 记 RowFailure
//     跳过该行、其余行照常提交，applier 写 DLQ 后前进 offset；
//   - fatal（默认：表不存在 00942、非法标识符 00904、权限不足 01031、
//     未知错误等）→ 返回错误中止任务，不前进 offset，人工排查。
//
// 无 ORA 码（code=0）的驱动级错误分类（M2 复审 P1#2）：
//   - ctx 取消/超时、sql.ErrTxDone（ctx 取消与自动回滚的竞态残留，
//     真库实测）→ retryable，语义同"本批结果未知"；
//   - 瞬时网络层错误（不携带 ORA 码）：driver.ErrBadConn、net.Error
//     （i/o timeout 等）、io.EOF/ErrUnexpectedEOF、ECONNRESET/EPIPE/
//     ECONNREFUSED → retryable（MaxRetries 有界，不会死循环）；
//   - 其余未知 → fatal。
func classify(err error) error {
	code := oraErrorCode(err)
	if code == 0 {
		if isTransientDriverError(err) {
			return &WriteError{Err: err, Code: code, Retryable: true}
		}
		return &WriteError{Err: err, Code: code}
	}
	switch {
	case retryableORACode(code):
		return &WriteError{Err: err, Code: code, Retryable: true}
	case rowDataORACodes[code]:
		return &WriteError{Err: err, Code: code, RowData: true}
	default:
		return &WriteError{Err: err, Code: code}
	}
}

// isTransientDriverError 判断无 ORA 码错误的瞬时性（M2 复审 P1#2）：
// 一次网络抖动不应升级为任务中止。errors.Is/As 走链式查找，
// fmt.Errorf("%w") 包装与 database/sql 内层替换（driver.ErrBadConn）均可识别。
func isTransientDriverError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, sql.ErrTxDone) || errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var netErr net.Error // 覆盖 i/o timeout、连接被重置等 net 层错误
	return errors.As(err, &netErr)
}

// WriteError 携带 ORA 错误码的三态分类错误（见 classify）。
type WriteError struct {
	Err  error
	Code int
	// Retryable=true：暂时性错误（网络/锁/ctx 取消），整批退避重放。
	Retryable bool
	// RowData=true：行级数据错误（白名单 ORA 码），跳行 + DLQ + 前进 offset。
	// 两者皆 false：fatal（配置/权限/未知），中止任务、不前进 offset。
	RowData bool
}

func (e *WriteError) Error() string { return e.Err.Error() }
func (e *WriteError) Unwrap() error { return e.Err }

// IsRetryable 实现 sink.IsRetryable 的标记方法。
func (e *WriteError) IsRetryable() bool { return e.Retryable }

// IsRowData 实现 sink.IsRowData 的标记方法。
func (e *WriteError) IsRowData() bool { return e.RowData }

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
