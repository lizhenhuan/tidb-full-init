package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// TiDBConfig TiDB 比对源配置。
type TiDBConfig struct {
	DSN      string // 如 root:pwd@tcp(host:4000)/（无需库名）
	Database string
	Table    string
}

// TiDBSource MySQL/TiDB 比对源。列类型来自 INFORMATION_SCHEMA，按 MySQL
// 语义规范化；DSN 建议 parseTime=true（BuildTiDBDSN 生成）。
type TiDBSource struct {
	db      *sql.DB
	database, table string
	cols    []Column
	pkNames []string
}

// BuildTiDBDSN 生成带比对所需参数的 DSN。
func BuildTiDBDSN(user, password, addr string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=true&loc=Local&timeout=10s",
		user, password, addr)
}

// NewTiDBSource 连接 TiDB 并读取表结构（列 + 主键）。
func NewTiDBSource(ctx context.Context, cfg TiDBConfig) (*TiDBSource, error) {
	dsn := cfg.DSN
	if !strings.Contains(dsn, "parseTime=true") {
		dsn += "?charset=utf8mb4&parseTime=true&loc=Local&timeout=10s"
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("verify tidb open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("verify tidb ping: %w", err)
	}
	s := &TiDBSource{db: db, database: cfg.Database, table: cfg.Table}
	if err := s.loadSchema(); err != nil {
		db.Close()
		return nil, err
	}
	if len(s.pkNames) == 0 {
		db.Close()
		return nil, fmt.Errorf("verify: %s.%s has no primary key", cfg.Database, cfg.Table)
	}
	return s, nil
}

func (s *TiDBSource) Close() error { return s.db.Close() }

func (s *TiDBSource) loadSchema() error {
	rows, err := s.db.Query(
		`SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY
		 FROM INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY ORDINAL_POSITION`, s.database, s.table)
	if err != nil {
		return fmt.Errorf("verify tidb columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, ctype, key string
		if err := rows.Scan(&name, &ctype, &key); err != nil {
			return fmt.Errorf("verify tidb scan: %w", err)
		}
		s.cols = append(s.cols, Column{Name: name, Kind: mysqlKind(ctype)})
		if key == "PRI" {
			s.pkNames = append(s.pkNames, name)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verify tidb rows: %w", err)
	}
	if len(s.cols) == 0 {
		return fmt.Errorf("verify: table %s.%s not found or no columns", s.database, s.table)
	}
	return nil
}

func (s *TiDBSource) Columns() []Column { return s.cols }
func (s *TiDBSource) PKNames() []string { return s.pkNames }

func (s *TiDBSource) quotedCols() []string {
	out := make([]string, len(s.cols))
	for i, c := range s.cols {
		out[i] = "`" + c.Name + "`"
	}
	return out
}

func (s *TiDBSource) Count(ctx context.Context) (int64, error) {
	var n int64
	q := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", s.database, s.table)
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("verify tidb count: %w", err)
	}
	return n, nil
}

// Stream 按 PK 升序全量流式输出（规范值）。
func (s *TiDBSource) Stream(ctx context.Context, fn func(pk string, row []string) error) error {
	order := make([]string, len(s.pkNames))
	for i, p := range s.pkNames {
		order[i] = "`" + p + "`"
	}
	q := fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s",
		strings.Join(s.quotedCols(), ","), s.database, s.table, strings.Join(order, ","))
	return s.queryRows(ctx, q, nil, fn)
}

// Get 点查（pk 为 \x01 连接的规范主键串）。
func (s *TiDBSource) Get(ctx context.Context, pk string) ([]string, error) {
	vals := SplitPK(pk)
	if len(vals) != len(s.pkNames) {
		return nil, fmt.Errorf("verify: pk %q has %d parts, want %d", pk, len(vals), len(s.pkNames))
	}
	where := make([]string, len(s.pkNames))
	for i, p := range s.pkNames {
		where[i] = "`" + p + "` = ?"
	}
	q := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s",
		strings.Join(s.quotedCols(), ","), s.database, s.table, strings.Join(where, " AND "))
	args := make([]any, len(vals))
	for i, v := range vals {
		args[i] = v
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("verify tidb get: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	row, err := s.scanRow(rows)
	if err != nil {
		return nil, err
	}
	return row, nil
}

// SamplePKs 随机抽主键。
func (s *TiDBSource) SamplePKs(ctx context.Context, n int) ([]string, error) {
	pkCols := make([]string, len(s.pkNames))
	for i, p := range s.pkNames {
		pkCols[i] = "`" + p + "`"
	}
	q := fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY RAND() LIMIT %d",
		strings.Join(pkCols, ","), s.database, s.table, n)
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("verify tidb sample: %w", err)
	}
	defer rows.Close()
	var pks []string
	for rows.Next() {
		vals := make([]any, len(s.pkNames))
		ptrs := make([]any, len(s.pkNames))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		parts := make([]string, len(s.pkNames))
		for i, pk := range s.pkNames {
			k := kindOf(s.cols, pk)
			cv, err := canonical(vals[i], k)
			if err != nil {
				return nil, err
			}
			parts[i] = cv
		}
		pks = append(pks, strings.Join(parts, pkSep))
	}
	return pks, rows.Err()
}

func (s *TiDBSource) queryRows(ctx context.Context, q string, args []any, fn func(pk string, row []string) error) error {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("verify tidb stream: %w", err)
	}
	defer rows.Close()
	pkIdx := pkIndexesOf(s.cols, s.pkNames)
	for _, i := range pkIdx {
		if i < 0 {
			return fmt.Errorf("verify tidb: pk column not found in %s.%s", s.database, s.table)
		}
	}
	for rows.Next() {
		row, err := s.scanRow(rows)
		if err != nil {
			return err
		}
		parts := make([]string, len(pkIdx))
		for i, idx := range pkIdx {
			parts[i] = row[idx]
		}
		if err := fn(strings.Join(parts, pkSep), row); err != nil {
			return err
		}
	}
	return rows.Err()
}

// scanRow 把 driver 值规范化为 canonical 字符串行。
func (s *TiDBSource) scanRow(rows *sql.Rows) ([]string, error) {
	vals := make([]any, len(s.cols))
	ptrs := make([]any, len(s.cols))
	for i := range vals {
		vals[i] = new(any)
		ptrs[i] = vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, fmt.Errorf("verify tidb scan: %w", err)
	}
	out := make([]string, len(s.cols))
	for i := range s.cols {
		v := *(vals[i].(*any))
		cv, err := canonical(v, s.cols[i].Kind)
		if err != nil {
			return nil, fmt.Errorf("verify tidb canonicalize %s.%s: %w", s.table, s.cols[i].Name, err)
		}
		out[i] = cv
	}
	return out, nil
}

func kindOf(cols []Column, name string) Kind {
	for _, c := range cols {
		if c.Name == name {
			return c.Kind
		}
	}
	return KindString
}

func pkIndexesOf(cols []Column, pkNames []string) []int {
	idx := make([]int, len(pkNames))
	for i, name := range pkNames {
		found := false
		for j, c := range cols {
			if strings.EqualFold(c.Name, name) {
				idx[i] = j
				found = true
				break
			}
		}
		if !found {
			idx[i] = -1
		}
	}
	return idx
}

// SplitPK 拆分复合主键规范键。
func SplitPK(pk string) []string { return strings.Split(pk, pkSep) }
