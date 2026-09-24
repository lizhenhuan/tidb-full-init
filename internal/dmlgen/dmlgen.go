// Package dmlgen 增量 DML 驱动（#t6）：对真 TiDB 执行 INSERT/UPDATE/
// DELETE 并把每条变更编码成 canal-json 发往 Kafka——复刻 TiCDC 形状
// （encoder 生成，绝不手搓 JSON），供 tidb-sync 消费、tidb-verify 比对。
//
// 顺序契约：先落 TiDB 再发 Kafka。若中途崩溃，可能出现「TiDB 有、
// Oracle 无」——由幂等 MERGE + 重跑收敛（驱动只负责前进，不回滚）。
package dmlgen

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

// Config 驱动参数。
type Config struct {
	DSN         string // TiDB DSN（含库名）
	Database    string // 目标库（默认取 DSN 库名）
	Table       string // 目标表
	Brokers     []string
	Topic       string
	NInsert     int // INSERT 条数
	NUpdate     int // UPDATE 条数
	NDelete     int // DELETE 条数
	Seed        int64
	MaxKafkaTry int // Kafka 发送重试次数（默认 3）
}

// Stats 单轮统计。
type Stats struct {
	Inserted int
	Updated  int
	Deleted  int
}

type tiDBInfo struct {
	db      *sql.DB
	schema  string
	table   string
	pkCols  []string
	autoInc map[string]bool // auto_increment 列名集合
}

// Run 执行一轮 DML 混合流量：连接 TiDB → 读元数据 → 按 I/U/D 各自数量
// 执行 DML 并同步发 canal-json。任何一步失败立即返回（fail loud，
// 不静默少发——少发会导致 verify 假失败）。
func Run(ctx context.Context, cfg Config) (*Stats, error) {
	if cfg.NInsert < 0 || cfg.NUpdate < 0 || cfg.NDelete < 0 {
		return nil, fmt.Errorf("dmlgen: negative counts (%d/%d/%d)", cfg.NInsert, cfg.NUpdate, cfg.NDelete)
	}
	if len(cfg.Brokers) == 0 || cfg.Topic == "" {
		return nil, fmt.Errorf("dmlgen: brokers/topic required")
	}
	if cfg.MaxKafkaTry <= 0 {
		cfg.MaxKafkaTry = 3
	}
	rng := rand.New(rand.NewSource(cfg.Seed))

	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("dmlgen: open tidb: %w", err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(time.Minute)

	schema := cfg.Database
	if schema == "" {
		schema = parseSchemaFromDSN(cfg.Database, cfg.DSN)
	}

	var info *encoder.TableInfo
	var autoInc map[string]bool
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("dmlgen: ping tidb: %w", err)
	}
	if info, autoInc, err = fetchTableInfo(ctx, db, schema, cfg.Table); err != nil {
		return nil, err
	}
	tdb := &tiDBInfo{db: db, schema: schema, table: cfg.Table, autoInc: autoInc}
	for _, c := range info.Columns {
		if c.IsPrimaryKey {
			tdb.pkCols = append(tdb.pkCols, c.Name)
		}
	}
	if len(tdb.pkCols) == 0 {
		return nil, fmt.Errorf("dmlgen: %s.%s has no primary key (canal-json 需要 pk 定位行)", schema, cfg.Table)
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: cfg.Brokers, Topic: cfg.Topic,
		Balancer: kafka.CRC32Balancer{}, // Kafka 标准按 Key 哈希：同 PK 同分区（D3）
		Async:   false,
	})
	defer w.Close()

	st := &Stats{}
	// sendMsg 编码 + 发送（Key=PK 字符串，同 PK 同分区保序，D3）。
	sendMsg := func(m *encoder.CanalMessage, key string) error {
		payload, err := m.ToJSON()
		if err != nil {
			return fmt.Errorf("dmlgen: encode canal-json: %w", err)
		}
		return writeWithRetry(ctx, w, payload, []byte(key), cfg.MaxKafkaTry)
	}

	// INSERT：新行落库 + 发消息（pk 由 TiDB 自增或生成器给值）
	for i := 0; i < cfg.NInsert; i++ {
		msg, err := doInsert(ctx, tdb, info, rng)
		if err != nil {
			return st, err
		}
		if err := sendMsg(msg.msg, msg.key); err != nil {
			return st, err
		}
		st.Inserted++
	}
	// UPDATE：抽已有行 → 改一个非 PK 列 → 发消息（Old=变更列旧值）
	for i := 0; i < cfg.NUpdate; i++ {
		msg, err := doUpdate(ctx, tdb, info, rng)
		if err != nil {
			return st, err
		}
		if msg == nil {
			continue // 表空，无行可改（首次全量后不该发生；计数如实）
		}
		if err := sendMsg(msg.msg, msg.key); err != nil {
			return st, err
		}
		st.Updated++
	}
	// DELETE：抽已有行 → 删除 → 发消息（Data=被删整行）
	for i := 0; i < cfg.NDelete; i++ {
		msg, err := doDelete(ctx, tdb, info, rng)
		if err != nil {
			return st, err
		}
		if msg == nil {
			continue
		}
		if err := sendMsg(msg.msg, msg.key); err != nil {
			return st, err
		}
		st.Deleted++
	}
	return st, nil
}

// parseSchemaFromDSN 从 DSN 提取库名（/dbname?...）。
func parseSchemaFromDSN(fallback string, dsn string) string {
	q := -1
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '?' {
			q = i
			break
		}
	}
	s := dsn
	if q >= 0 {
		s = dsn[:q]
	}
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			name := s[i+1:]
			if name != "" {
				return name
			}
			break
		}
	}
	return fallback
}

// fetchTableInfo 读 information_schema 列元数据，形状与 encoder.TableInfo
// 对齐；同时返回 auto_increment 列集合。
func fetchTableInfo(ctx context.Context, db *sql.DB, schema, table string) (*encoder.TableInfo, map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, column_type, column_key, ordinal_position, extra
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, nil, fmt.Errorf("dmlgen: query columns: %w", err)
	}
	defer rows.Close()

	info := &encoder.TableInfo{Database: schema, Table: table}
	autoInc := make(map[string]bool)
	seen := 0
	for rows.Next() {
		var name, ctype, key, extra string
		var pos int
		if err := rows.Scan(&name, &ctype, &key, &pos, &extra); err != nil {
			return nil, nil, fmt.Errorf("dmlgen: scan column: %w", err)
		}
		// column_type 带修饰（unsigned/zerofill/枚举集合字面量）；
		// MySQLType 保留原样，生成器按前缀判型。
		info.Columns = append(info.Columns, encoder.ColumnInfo{
			ID: int64(pos), Name: name, MySQLType: ctype, IsPrimaryKey: key == "PRI",
		})
		if key == "PRI" {
			info.PKNames = append(info.PKNames, name)
		}
		if strings.Contains(strings.ToLower(extra), "auto_increment") {
			autoInc[name] = true
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("dmlgen: iterate columns: %w", err)
	}
	if seen == 0 {
		return nil, nil, fmt.Errorf("dmlgen: table %s.%s not found in information_schema", schema, table)
	}
	return info, autoInc, nil
}

// dmlMsg 一条 DML 对应的 Kafka 消息 + 分区键。
type dmlMsg struct {
	msg *encoder.CanalMessage
	key string
}

// doInsert 生成整行值（PK 冲突重试），落库后编码 canal-json。
func doInsert(ctx context.Context, t *tiDBInfo, info *encoder.TableInfo, rng *rand.Rand) (*dmlMsg, error) {
	backoff := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		vals := genRowValues(t, info, rng)
		cols := make([]string, 0, len(info.Columns))
		args := make([]interface{}, 0, len(info.Columns))
		for _, c := range info.Columns {
			cols = append(cols, quoteIdent(c.Name))
			args = append(args, vals[c.Name])
		}
		sqlStr := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			quoteIdent(t.schema), quoteIdent(t.table),
			joinQuoted(cols), placeholders(len(cols)))
		res, err := t.db.ExecContext(ctx, sqlStr, args...)
		if err == nil {
			if n, _ := res.RowsAffected(); n != 1 {
				return nil, fmt.Errorf("dmlgen: insert affected %d rows, want 1", n)
			}
			// 自增 PK：把 TiDB 分配的 id 回填进消息（LastInsertId 是
			// 会话级值，与单条 insert 一一对应）
			if len(t.pkCols) == 1 && t.autoInc[t.pkCols[0]] {
				if id, err := res.LastInsertId(); err == nil && id > 0 {
					vals[t.pkCols[0]] = fmt.Sprint(id)
				}
			}
			return buildRowMessage("INSERT", info, vals, nil, nil), nil
		}
		if isDupEntry(err) && attempt < 5 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			continue
		}
		return nil, fmt.Errorf("dmlgen: insert into %s.%s: %w", t.schema, t.table, err)
	}
}

// doUpdate 抽一行 → 选一个非 PK 列改值 → UPDATE → 编码消息。
// 表空返回 (nil, nil)。
func doUpdate(ctx context.Context, t *tiDBInfo, info *encoder.TableInfo, rng *rand.Rand) (*dmlMsg, error) {
	row, pkVals, err := sampleRow(ctx, t, info, rng)
	if err != nil || row == nil {
		return nil, err
	}
	// 选变更列：非 PK 优先；全 PK 表跳过（无列可改）
	var target *encoder.ColumnInfo
	for i := range info.Columns {
		if info.Columns[i].IsPrimaryKey {
			continue
		}
		target = &info.Columns[i]
		break
	}
	if target == nil {
		// 无非 PK 列可改：改回同值也算 UPDATE 事件（Old=After 同值）
		for i := range info.Columns {
			if info.Columns[i].Name == t.pkCols[0] {
				target = &info.Columns[i]
				break
			}
		}
		if target == nil {
			return nil, nil
		}
	}

	oldVal := row[target.Name]
	newVal := mutateValue(*target, oldVal, rng)
	_, err = t.db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s.%s SET %s = ? WHERE %s",
			quoteIdent(t.schema), quoteIdent(t.table), quoteIdent(target.Name), wherePK(t)),
		append([]interface{}{newVal}, pkArgs(t, pkVals)...)...)
	if err != nil {
		return nil, fmt.Errorf("dmlgen: update %s.%s: %w", t.schema, t.table, err)
	}

	// Data=改后整行（目标列替换新值）；Old=仅变更列旧值（TiCDC 形状）
	after := cloneMap(row)
	after[target.Name] = newVal
	return buildRowMessage("UPDATE", info, after, target, oldVal), nil
}

// doDelete 抽一行 → DELETE → 编码消息（Data=被删整行）。表空返回 (nil, nil)。
func doDelete(ctx context.Context, t *tiDBInfo, info *encoder.TableInfo, rng *rand.Rand) (*dmlMsg, error) {
	row, pkVals, err := sampleRow(ctx, t, info, rng)
	if err != nil || row == nil {
		return nil, err
	}
	res, err := t.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
			quoteIdent(t.schema), quoteIdent(t.table), wherePK(t)),
		pkArgs(t, pkVals)...)
	if err != nil {
		return nil, fmt.Errorf("dmlgen: delete %s.%s: %w", t.schema, t.table, err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("dmlgen: delete affected %d rows, want 1", n)
	}
	return buildRowMessage("DELETE", info, row, nil, nil), nil
}

// sampleRow 随机抽一行（ORDER BY RAND() LIMIT 1；测试量表级 ≤ 百万，
// 全表扫描可接受；抽不到 = 表空 → (nil, nil, nil)）。
// 返回 map[列名]值 + 按声明顺序的 PK 值切片。
func sampleRow(ctx context.Context, t *tiDBInfo, info *encoder.TableInfo, rng *rand.Rand) (map[string]interface{}, []interface{}, error) {
	cols := make([]string, len(info.Columns))
	for i, c := range info.Columns {
		cols[i] = quoteIdent(c.Name)
	}
	q := fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY RAND() LIMIT 1",
		joinCSV(cols), quoteIdent(t.schema), quoteIdent(t.table))
	rows, err := t.db.QueryContext(ctx, q)
	if err != nil {
		return nil, nil, fmt.Errorf("dmlgen: sample %s.%s: %w", t.schema, t.table, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, nil // 空
	}
	raw := make([]interface{}, len(info.Columns))
	ptrs := make([]interface{}, len(info.Columns))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, nil, fmt.Errorf("dmlgen: scan sampled row: %w", err)
	}
	row := make(map[string]interface{}, len(info.Columns))
	for i, c := range info.Columns {
		row[c.Name] = raw[i] // 保留原始类型（[]byte/time.Time），FormatValue 统一格式化
	}
	var pkVals []interface{}
	for _, pk := range t.pkCols {
		pkVals = append(pkVals, row[pk])
	}
	return row, pkVals, nil
}

// buildRowMessage 用 encoder 组 canal-json（绝不手搓）：
// BuildTypeMapping + SetRowData，与 snapshot 全量路径同一口径
// （FormatValue：binary→base64、时间→格式化字符串）。
// oldCol/oldVal 仅 UPDATE 提供（Old=仅变更列旧值，TiCDC 形状）。
// 返回消息 + Kafka 分区键（PK 值串，同 PK 同分区保序）。
func buildRowMessage(typ string, info *encoder.TableInfo, row map[string]interface{}, oldCol *encoder.ColumnInfo, oldVal interface{}) *dmlMsg {
	var m *encoder.CanalMessage
	switch typ {
	case "UPDATE":
		m = encoder.NewUpdateMessage(info)
	case "DELETE":
		m = encoder.NewDeleteMessage(info)
	default:
		m = encoder.NewInsertMessage(info)
	}
	m.BuildTypeMapping(info.Columns)

	data := make(map[string]string, len(info.Columns))
	pkParts := make([]string, 0, len(info.PKNames))
	for _, c := range info.Columns {
		data[c.Name] = encoder.FormatValue(c.MySQLType, row[c.Name])
		if c.IsPrimaryKey {
			pkParts = append(pkParts, data[c.Name])
		}
	}
	m.SetRowData(data)
	if oldCol != nil {
		m.SetOldData(map[string]string{oldCol.Name: encoder.FormatValue(oldCol.MySQLType, oldVal)})
	}
	return &dmlMsg{msg: m, key: strings.Join(pkParts, "|")}
}

// writeWithRetry Kafka 写入重试（瞬时 broker 抖动）。
func writeWithRetry(ctx context.Context, w *kafka.Writer, payload, key []byte, tries int) error {
	var err error
	backoff := 200 * time.Millisecond
	for i := 0; i < tries; i++ {
		if err = w.WriteMessages(ctx, kafka.Message{Key: key, Value: payload}); err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		log.Printf("[dmlgen] kafka write attempt %d/%d failed: %v", i+1, tries, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return fmt.Errorf("dmlgen: kafka write after %d tries: %w", tries, err)
}
