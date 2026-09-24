// introspect.go：TiDB information_schema 元数据读取。
//
// 与管道 internal/tidb.SchemaFetcher 的分工：那边取列 ID/类型做行编码，
// 这里额外取默认值/注释/字符集/索引/FK/分区做 DDL 生成——同一
// information_schema 口径，无重复逻辑交叉。
package ddlsync

import (
	"database/sql"
	"fmt"
	"strings"
)

// Introspector TiDB 元数据读取器（db 为已连通的 *sql.DB，mysql 驱动）。
type Introspector struct {
	db *sql.DB
}

// NewIntrospector 构造读取器。
func NewIntrospector(db *sql.DB) *Introspector { return &Introspector{db: db} }

// FetchTable 读单表完整元数据（列按 ORDINAL_POSITION）。
// 表不存在或不是 BASE TABLE（视图一期不转换）返回 (nil, nil)，
// 调用方按 skipped 处理。
func (in *Introspector) FetchTable(database, table string) (*TableMetaFull, error) {
	var tableType string
	var comment, tblCollation sql.NullString
	err := in.db.QueryRow(
		`SELECT TABLE_TYPE, TABLE_COMMENT, TABLE_COLLATION
		 FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, database, table).
		Scan(&tableType, &comment, &tblCollation)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ddlsync: query %s.%s: %w", database, table, err)
	}
	if tableType != "BASE TABLE" {
		return nil, nil // 视图/临时表：一期不转换
	}

	meta := &TableMetaFull{
		Database:  database,
		Table:     table,
		Comment:   comment.String,
		Collation: tblCollation.String,
		Charset:   charsetOfCollation(tblCollation.String),
	}

	cols, err := in.fetchColumns(database, table)
	if err != nil {
		return nil, err
	}
	meta.Columns = cols
	for _, c := range cols {
		if c.IsPK {
			meta.PKNames = append(meta.PKNames, c.Name)
		}
	}

	uniques, indexes, err := in.fetchIndexes(database, table)
	if err != nil {
		return nil, err
	}
	meta.Uniques, meta.Indexes = uniques, indexes

	fks, err := in.fetchFKs(database, table)
	if err != nil {
		return nil, err
	}
	meta.ForeignKeys = fks

	if err := in.fetchPartition(database, table, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (in *Introspector) fetchColumns(database, table string) ([]ColumnMeta, error) {
	rows, err := in.db.Query(
		`SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
		        COLUMN_COMMENT, IFNULL(CHARACTER_SET_NAME,''), IFNULL(COLLATION_NAME,''),
		        COLUMN_KEY, IFNULL(EXTRA,'')
		 FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY ORDINAL_POSITION`, database, table)
	if err != nil {
		return nil, fmt.Errorf("ddlsync: query columns %s.%s: %w", database, table, err)
	}
	defer rows.Close()

	var cols []ColumnMeta
	for rows.Next() {
		var c ColumnMeta
		var nullable, columnKey string
		var def sql.NullString
		if err := rows.Scan(&c.Name, &c.MySQLType, &nullable, &def,
			&c.Comment, &c.Charset, &c.Collation, &columnKey, &c.Extra); err != nil {
			return nil, fmt.Errorf("ddlsync: scan column: %w", err)
		}
		c.Nullable = nullable == "YES"
		// COLUMN_DEFAULT 为 NULL 表示无显式默认（可空列隐式 NULL）；
		// TiDB 对 CURRENT_TIMESTAMP/表达式默认给原文。
		if def.Valid {
			c.HasDefault = true
			c.Default = def.String
		}
		c.IsPK = columnKey == "PRI"
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func (in *Introspector) fetchIndexes(database, table string) (uniques, indexes []IndexMeta, err error) {
	rows, err := in.db.Query(
		`SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
		 FROM information_schema.STATISTICS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY INDEX_NAME, SEQ_IN_INDEX`, database, table)
	if err != nil {
		return nil, nil, fmt.Errorf("ddlsync: query indexes %s.%s: %w", database, table, err)
	}
	defer rows.Close()

	byName := map[string]*IndexMeta{}
	var order []string
	for rows.Next() {
		var name string
		var column sql.NullString // 表达式索引部分 COLUMN_NAME 为 NULL
		var nonUnique, seq int
		if err := rows.Scan(&name, &nonUnique, &seq, &column); err != nil {
			return nil, nil, fmt.Errorf("ddlsync: scan index: %w", err)
		}
		idx, ok := byName[name]
		if !ok {
			idx = &IndexMeta{Name: name, Unique: nonUnique == 0}
			byName[name] = idx
			order = append(order, name)
		}
		// 表达式列记空串：convert 阶段识别为表达式索引 → 兼容项（一期不执行）。
		idx.Columns = append(idx.Columns, column.String)
	}
	for _, name := range order {
		idx := byName[name]
		if name == "PRIMARY" {
			continue // PK 走 PKNames
		}
		if idx.Unique {
			uniques = append(uniques, *idx)
		} else {
			indexes = append(indexes, *idx)
		}
	}
	return uniques, indexes, rows.Err()
}

func (in *Introspector) fetchFKs(database, table string) ([]FKMeta, error) {
	rows, err := in.db.Query(
		`SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME, k.REFERENCED_TABLE_SCHEMA,
		        k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME,
		        IFNULL(r.DELETE_RULE,'NO ACTION'), IFNULL(r.UPDATE_RULE,'NO ACTION')
		 FROM information_schema.KEY_COLUMN_USAGE k
		 JOIN information_schema.REFERENTIAL_CONSTRAINTS r
		   ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
		  AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
		 WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ?
		 ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION`, database, table)
	if err != nil {
		// TiDB 旧版可能无 REFERENTIAL_CONSTRAINTS 数据：FK 一期只记录，
		// 查询失败降级为空（不阻断 DDL 生成）
		return nil, nil
	}
	defer rows.Close()

	byName := map[string]*FKMeta{}
	var order []string
	for rows.Next() {
		var name, column, refSchema, refTable, refColumn, delRule, updRule string
		if err := rows.Scan(&name, &column, &refSchema, &refTable, &refColumn, &delRule, &updRule); err != nil {
			return nil, fmt.Errorf("ddlsync: scan fk: %w", err)
		}
		fk, ok := byName[name]
		if !ok {
			fk = &FKMeta{Name: name, RefDatabase: refSchema, RefTable: refTable,
				OnDeleteCascade: delRule == "CASCADE", OnUpdateCascade: updRule == "CASCADE"}
			byName[name] = fk
			order = append(order, name)
		}
		fk.Columns = append(fk.Columns, column)
		fk.RefColumns = append(fk.RefColumns, refColumn)
	}
	var fks []FKMeta
	for _, name := range order {
		fks = append(fks, *byName[name])
	}
	return fks, rows.Err()
}

// fetchPartition 分区判定 + 方法/表达式（P1 只记录兼容项；P2.5 落地
// RANGE/HASH/LIST → Oracle 语法转换）。
func (in *Introspector) fetchPartition(database, table string, meta *TableMetaFull) error {
	var method, expr sql.NullString
	err := in.db.QueryRow(
		`SELECT PARTITION_METHOD, IFNULL(PARTITION_EXPRESSION,'')
		 FROM information_schema.PARTITIONS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		   AND PARTITION_METHOD IS NOT NULL LIMIT 1`, database, table).
		Scan(&method, &expr)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return fmt.Errorf("ddlsync: query partition %s.%s: %w", database, table, err)
	}
	meta.PartitionExpr = strings.TrimSpace(method.String) + " " + strings.TrimSpace(expr.String)
	return nil
}

// charsetOfCollation utf8mb4_bin → utf8mb4（表字符集从排序规则推导）。
func charsetOfCollation(collation string) string {
	if i := strings.IndexByte(collation, '_'); i > 0 {
		return collation[:i]
	}
	return collation
}
