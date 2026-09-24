package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	goorav2 "github.com/sijms/go-ora/v2"
)

// OracleConfig Oracle 比对源配置（目标端）。
type OracleConfig struct {
	Host     string
	Port     int
	Service  string
	User     string
	Password string
	Schema   string // 目标 schema（小写加引号创建，如 "shop"）
	Table    string
}

// OracleSource Oracle 比对源。列类型来自 ALL_TAB_COLUMNS；主键列名沿用
// 比对对端（源端）——sink 建表保持原列名，因此两端同名。标识符全部按
// 小写加引号处理（与 oraclesink 建表约定一致）。
type OracleSource struct {
	db           *sql.DB
	schema, table string
	cols         []Column
	pkNames      []string
}

// NewOracleSource 连接 Oracle 并读取列结构。pkNames 由源端提供（同名假设
// 在 checkCompatible 校验）。Oracle 端不查约束字典：sink 建表的主键即源端主键。
func NewOracleSource(ctx context.Context, cfg OracleConfig, pkNames []string) (*OracleSource, error) {
	url := goorav2.BuildUrl(cfg.Host, cfg.Port, cfg.Service, cfg.User, cfg.Password, nil)
	db, err := sql.Open("oracle", url)
	if err != nil {
		return nil, fmt.Errorf("verify oracle open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("verify oracle ping: %w", err)
	}
	s := &OracleSource{db: db, schema: cfg.Schema, table: cfg.Table, pkNames: pkNames}
	if err := s.loadColumns(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *OracleSource) Close() error { return s.db.Close() }

func (s *OracleSource) loadColumns(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT COLUMN_NAME, DATA_TYPE, DATA_SCALE
		 FROM ALL_TAB_COLUMNS
		 WHERE OWNER = :1 AND TABLE_NAME = :2
		 ORDER BY COLUMN_ID`, s.schema, s.table)
	if err != nil {
		return fmt.Errorf("verify oracle columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, dataType string
		var scale sql.NullInt64
		if err := rows.Scan(&name, &dataType, &scale); err != nil {
			return fmt.Errorf("verify oracle scan: %w", err)
		}
		var sp *int
		if scale.Valid {
			v := int(scale.Int64)
			sp = &v
		}
		s.cols = append(s.cols, Column{Name: name, Kind: oracleKind(dataType, sp)})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("verify oracle rows: %w", err)
	}
	if len(s.cols) == 0 {
		return fmt.Errorf("verify: oracle table %s.%s not found or no columns", s.schema, s.table)
	}
	return nil
}

func (s *OracleSource) Columns() []Column { return s.cols }
func (s *OracleSource) PKNames() []string { return s.pkNames }

func (s *OracleSource) q(name string) string { return `"` + name + `"` }

// resolve 把（来自源端的）列名解析为本表实际列名（大小写不敏感）。
func (s *OracleSource) resolve(name string) string {
	for _, c := range s.cols {
		if strings.EqualFold(c.Name, name) {
			return c.Name
		}
	}
	return name
}

func (s *OracleSource) qualified() string {
	return s.q(s.schema) + "." + s.q(s.table)
}

func (s *OracleSource) colList() string {
	qs := make([]string, len(s.cols))
	for i, c := range s.cols {
		qs[i] = s.q(c.Name)
	}
	return strings.Join(qs, ",")
}

func (s *OracleSource) Count(ctx context.Context) (int64, error) {
	var n int64
	q := "SELECT COUNT(*) FROM " + s.qualified()
	if err := s.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("verify oracle count: %w", err)
	}
	return n, nil
}

func (s *OracleSource) Stream(ctx context.Context, fn func(pk string, row []string) error) error {
	order := make([]string, len(s.pkNames))
	for i, p := range s.pkNames {
		order[i] = s.q(s.resolve(p))
	}
	q := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
		s.colList(), s.qualified(), strings.Join(order, ","))
	return s.queryRows(ctx, q, nil, fn)
}

func (s *OracleSource) Get(ctx context.Context, pk string) ([]string, error) {
	vals := SplitPK(pk)
	if len(vals) != len(s.pkNames) {
		return nil, fmt.Errorf("verify: pk %q has %d parts, want %d", pk, len(vals), len(s.pkNames))
	}
	where := make([]string, len(s.pkNames))
	args := make([]any, len(vals))
	for i, p := range s.pkNames {
		where[i] = s.q(s.resolve(p)) + " = :" + fmt.Sprint(i+1)
		args[i] = vals[i]
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		s.colList(), s.qualified(), strings.Join(where, " AND "))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("verify oracle get: %w", err)
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

func (s *OracleSource) SamplePKs(ctx context.Context, n int) ([]string, error) {
	// 抽样主键始终由源端提供；Oracle 端不应被调用
	return nil, fmt.Errorf("verify: SamplePKs on oracle source is not supported; sample from the TiDB side")
}

func (s *OracleSource) queryRows(ctx context.Context, q string, args []any, fn func(pk string, row []string) error) error {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("verify oracle stream: %w", err)
	}
	defer rows.Close()
	pkIdx := pkIndexesOf(s.cols, s.pkNames)
	for _, i := range pkIdx {
		if i < 0 {
			return fmt.Errorf("verify oracle: pk column not found in %s.%s", s.schema, s.table)
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

func (s *OracleSource) scanRow(rows *sql.Rows) ([]string, error) {
	vals := make([]any, len(s.cols))
	ptrs := make([]any, len(s.cols))
	for i := range vals {
		vals[i] = new(any)
		ptrs[i] = vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, fmt.Errorf("verify oracle scan: %w", err)
	}
	out := make([]string, len(s.cols))
	for i := range s.cols {
		v := *(vals[i].(*any))
		cv, err := canonical(v, s.cols[i].Kind)
		if err != nil {
			return nil, fmt.Errorf("verify oracle canonicalize %s.%s: %w", s.table, s.cols[i].Name, err)
		}
		out[i] = cv
	}
	return out, nil
}
