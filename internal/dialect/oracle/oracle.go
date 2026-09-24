// Package oracle 是 TargetDialect 的 Oracle 实现（设计文档 D4/D5/D8）。
package oracle

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// Dialect 实现 dialect.TargetDialect。
type Dialect struct {
	// maxIdentLen 标识符字节上限（schema/表/列/约束名）。传统 30；
	// Oracle ≥12.2 支持 128（ddl-sync 按探测版本通过 NewWithIdentLimit
	// 传入，数据管道默认仍 30，P2 接同一探测）。
	maxIdentLen int
}

// New 返回 Oracle 方言实例（标识符上限 30，现网口径）。
func New() *Dialect { return &Dialect{maxIdentLen: DefaultMaxIdentLen} }

// NewWithIdentLimit 按目标 Oracle 版本能力构造（≥12.2 传 128）。
func NewWithIdentLimit(maxIdent int) *Dialect {
	if maxIdent <= 0 {
		maxIdent = DefaultMaxIdentLen
	}
	return &Dialect{maxIdentLen: maxIdent}
}

// DefaultMaxIdentLen Oracle 传统标识符上限（12.2 之前为 30）。
const DefaultMaxIdentLen = 30

// MaxIdentLen 返回当前生效的标识符上限（ddl-sync 记录进报告供验收首查）。
func (d *Dialect) MaxIdentLen() int { return d.maxIdentLen }

func (d *Dialect) Name() string { return "oracle" }

// ErrUnsupportedType 一期硬拒绝的类型（ENUM/SET 等，设计文档 D4）。
type ErrUnsupportedType struct {
	Table    string
	Column   string
	Type     string
	Reason   string
}

func (e *ErrUnsupportedType) Error() string {
	return fmt.Sprintf("oracle: unsupported type %q on %s.%s: %s", e.Type, e.Table, e.Column, e.Reason)
}

// oracleReserved 常见 Oracle 保留字（非全集；因所有标识符一律强制加引号，
// 保留字天然安全，此表仅用于文档化与测试）。
var oracleReserved = map[string]bool{
	"ACCESS": true, "ADD": true, "ALL": true, "ALTER": true, "AND": true, "ANY": true,
	"AS": true, "ASC": true, "AUDIT": true, "BETWEEN": true, "BY": true, "CHAR": true,
	"CHECK": true, "CLUSTER": true, "COLUMN": true, "COMMENT": true, "COMPRESS": true,
	"CONNECT": true, "CREATE": true, "CURRENT": true, "DATE": true, "DECIMAL": true,
	"DEFAULT": true, "DELETE": true, "DESC": true, "DISTINCT": true, "DROP": true,
	"ELSE": true, "EXCLUSIVE": true, "EXISTS": true, "FILE": true, "FLOAT": true,
	"FOR": true, "FROM": true, "GRANT": true, "GROUP": true, "HAVING": true,
	"IDENTIFIED": true, "IMMEDIATE": true, "IN": true, "INCREMENT": true, "INDEX": true,
	"INITIAL": true, "INSERT": true, "INTEGER": true, "INTERSECT": true, "INTO": true,
	"IS": true, "LEVEL": true, "LIKE": true, "LOCK": true, "LONG": true, "MAXEXTENTS": true,
	"MINUS": true, "MLSLABEL": true, "MODE": true, "MODIFY": true, "NOAUDIT": true,
	"NOCOMPRESS": true, "NOT": true, "NOWAIT": true, "NULL": true, "NUMBER": true,
	"OF": true, "OFFLINE": true, "ON": true, "ONLINE": true, "OPTION": true, "OR": true,
	"ORDER": true, "PCTFREE": true, "PRIOR": true, "PUBLIC": true, "RAW": true,
	"RENAME": true, "RESOURCE": true, "REVOKE": true, "ROW": true, "ROWID": true,
	"ROWNUM": true, "ROWS": true, "SELECT": true, "SESSION": true, "SET": true,
	"SHARE": true, "SIZE": true, "SMALLINT": true, "START": true, "SUCCESSFUL": true,
	"SYNONYM": true, "SYSDATE": true, "TABLE": true, "THEN": true, "TO": true,
	"TRIGGER": true, "UID": true, "UNION": true, "UNIQUE": true, "UPDATE": true,
	"USER": true, "VALIDATE": true, "VALUES": true, "VARCHAR": true, "VARCHAR2": true,
	"VIEW": true, "WHENEVER": true, "WHERE": true, "WITH": true,
}

// QuoteIdentifier 一律双引号引用并转义内嵌双引号（注入对抗），
// 保留原名大小写，规避保留字冲突。
func (d *Dialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *Dialect) qualified(meta model.TableMeta) string {
	// P0#4：目标 schema 由 TableMeta.Schema() 决定（TargetSchema 优先）。
	return d.QuoteIdentifier(meta.Schema()) + "." + d.QuoteIdentifier(meta.Table)
}

// typeRe 解析 MySQL 类型定义：名称 + 可选 (precision[,scale]) + 修饰符。
var typeRe = regexp.MustCompile(`^\s*([a-zA-Z]+)\s*(?:\((\d+)(?:\s*,\s*(\d+))?\))?\s*(unsigned)?\s*$`)

type parsedType struct {
	Name     string // 小写基础类型名
	Prec     int
	Scale    int
	HasPrec  bool
	Unsigned bool
}

func parseType(mysqlType string) (parsedType, error) {
	m := typeRe.FindStringSubmatch(strings.TrimSpace(mysqlType))
	if m == nil {
		return parsedType{}, fmt.Errorf("cannot parse mysql type %q", mysqlType)
	}
	pt := parsedType{Name: strings.ToLower(m[1]), Unsigned: m[4] == "unsigned"}
	if m[2] != "" {
		pt.HasPrec = true
		pt.Prec, _ = strconv.Atoi(m[2])
		if m[3] != "" {
			pt.Scale, _ = strconv.Atoi(m[3])
		}
	}
	return pt, nil
}

// oracleNumberPrecision 各整数类型的十进制位数上限。
var oracleNumberPrecision = map[string]int{
	"tinyint": 3, "smallint": 5, "mediumint": 8, "int": 10, "integer": 10, "bigint": 19,
}

// MapType 单列类型映射（设计文档 D4 矩阵）。
func (d *Dialect) MapType(table, column, mysqlType string) (string, error) {
	// enum/set 的类型定义带引号参数（enum('a','b')），先于正则拦截，
	// 避免落进 "cannot parse" 这类误导性错误。
	if base := baseName(mysqlType); base == "enum" || base == "set" {
		return "", &ErrUnsupportedType{
			Table: table, Column: column, Type: mysqlType,
			Reason: "一期硬拒绝：canal-json 呈现为序号/位掩码，写入无意义；二期由 TiDB schema 还原后写 VARCHAR2",
		}
	}
	pt, err := parseType(mysqlType)
	if err != nil {
		return "", err
	}
	switch pt.Name {
	case "bool", "boolean":
		return "NUMBER(3)", nil
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		prec := oracleNumberPrecision[pt.Name]
		if pt.Unsigned {
			prec++ // 无符号上限多一位十进制
		}
		return fmt.Sprintf("NUMBER(%d)", prec), nil
	case "bit":
		return "NUMBER", nil
	case "decimal", "numeric":
		if !pt.HasPrec {
			return "NUMBER", nil
		}
		if pt.Prec > 38 {
			// 有损映射边界（#t6 复审 H2）：Oracle NUMBER 上限 38 位十进制，
			// 精度截断而 scale 原样保留——DECIMAL(65,30) 映射为 NUMBER(38,30)，
			// 整数位预算从 35 位缩到 8 位。超预算值写入报 ORA-01438，
			// oraclesink 将其归为行级数据错误（跳行 + DLQ + 前进 offset）：
			// 单行损失在 kafka_dlq 可见、不中止管道。需要无损承载时应收紧
			// 源列精度，勿依赖此截断。
			pt.Prec = 38 // 超精度截断告警由 ValidateTable 补充
		}
		if pt.Scale > 0 {
			return fmt.Sprintf("NUMBER(%d,%d)", pt.Prec, pt.Scale), nil
		}
		return fmt.Sprintf("NUMBER(%d,0)", pt.Prec), nil
	case "float":
		return "BINARY_FLOAT", nil
	case "double", "real":
		return "BINARY_DOUBLE", nil
	case "varchar", "char", "nvarchar", "nchar":
		if !pt.HasPrec || pt.Prec*4 > 4000 {
			return "CLOB", nil // utf8 安全上限：n*4 字节 ≤ 4000，否则 CLOB
		}
		return fmt.Sprintf("VARCHAR2(%d CHAR)", pt.Prec), nil
	case "tinytext", "text", "mediumtext", "longtext":
		return "CLOB", nil
	case "varbinary":
		if !pt.HasPrec || pt.Prec > 2000 {
			return "", &ErrUnsupportedType{
				Table: table, Column: column, Type: mysqlType,
				Reason: "varbinary 长度缺失或超过 RAW 上限 2000，请改用 blob",
			}
		}
		return fmt.Sprintf("RAW(%d)", pt.Prec), nil
	case "binary":
		if pt.HasPrec && pt.Prec <= 2000 {
			return fmt.Sprintf("RAW(%d)", pt.Prec), nil
		}
		return "BLOB", nil
	case "tinyblob", "blob", "mediumblob", "longblob":
		return "BLOB", nil
	case "date":
		return "DATE", nil
	case "datetime", "timestamp":
		return "TIMESTAMP", nil
	case "time":
		// 无损且驱动安全：规范串 [-]HH:MM:SS[.ffffff]（设计文档 D4/P1-6）
		return "VARCHAR2(18)", nil
	case "year":
		return "NUMBER(4)", nil
	case "json":
		return "CLOB", nil
	default:
		return "", &ErrUnsupportedType{
			Table: table, Column: column, Type: mysqlType,
			Reason: "未知 MySQL 类型，请人工确认后扩展映射矩阵",
		}
	}
}

func (d *Dialect) validateIdentifier(kind, name string) error {
	if name == "" {
		return fmt.Errorf("oracle: empty %s identifier", kind)
	}
	if len(name) > d.maxIdentLen {
		return fmt.Errorf("oracle: %s identifier %q exceeds %d bytes (目标版本标识符上限)", kind, name, d.maxIdentLen)
	}
	if strings.ContainsAny(name, "\x00") {
		return fmt.Errorf("oracle: %s identifier %q contains NUL", kind, name)
	}
	return nil
}

// ValidateTable 任务创建前的表级校验（设计文档 D4）。
// 校验目标端 schema（Schema()：TargetSchema 优先），而非源库文名——
// 目标 OWNER 才是真正落库的标识符（复审 P2#5）。
func (d *Dialect) ValidateTable(meta model.TableMeta) error {
	if err := d.validateIdentifier("schema", meta.Schema()); err != nil {
		return err
	}
	if err := d.validateIdentifier("table", meta.Table); err != nil {
		return err
	}
	if len(meta.PKNames) == 0 {
		return fmt.Errorf("oracle: table %s.%s has no primary key (一期硬约束)", meta.Schema(), meta.Table)
	}
	// P1#8：仅主键列的表无法生成合法的 UPDATE SET，任务创建/DDL 预览阶段
	// 就要挡住，不能等首次写入才炸（复审意见：从 RenderUpsert 前移）。
	if len(meta.Columns) == len(meta.PKNames) {
		return fmt.Errorf("oracle: table %s.%s contains only PK columns; "+
			"MERGE upsert requires at least one non-PK column", meta.Schema(), meta.Table)
	}
	for _, col := range meta.Columns {
		if err := d.validateIdentifier("column", col.Name); err != nil {
			return err
		}
		if _, err := d.MapType(meta.Table, col.Name, col.MySQLType); err != nil {
			return err
		}
	}
	return nil
}

// ValidateIdentifiers 校验 schema/表/列名不超标识符上限（ddl-sync 生成
// 路径用：与管道 ValidateTable 的标识符检查同口径，但不带 PK/全-PK
// 约束——DDL 同步允许无 PK 表建结构，数据管道任务创建时另行硬校验）。
func (d *Dialect) ValidateIdentifiers(schema, table string, columns []string) error {
	if err := d.validateIdentifier("schema", schema); err != nil {
		return err
	}
	if err := d.validateIdentifier("table", table); err != nil {
		return err
	}
	for _, c := range columns {
		if err := d.validateIdentifier("column", c); err != nil {
			return err
		}
	}
	return nil
}

// PKConstraintName 约束名 "PK_<table>"，超标识符上限时截断并追加短哈希
// （P1#7：表名 28 字符时约束名会 31 → ORA-00972；上限随实例化
// maxIdentLen，30/128 同逻辑）。导出供 ddlsync 复用同一命名口径。
func (d *Dialect) PKConstraintName(table string) string {
	name := "PK_" + table
	if len(name) <= d.maxIdentLen {
		return name
	}
	sum := fnv32a(table)
	return name[:d.maxIdentLen-4] + fmt.Sprintf("%04X", sum&0xFFFF)
}

func fnv32a(s string) uint32 {
	const prime = 16777619
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

// RenderCreateTable 生成建表 DDL（设计文档 D5）。
func (d *Dialect) RenderCreateTable(meta model.TableMeta) (string, error) {
	if err := d.ValidateTable(meta); err != nil {
		return "", err
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "CREATE TABLE %s (\n", d.qualified(meta))
	for i, col := range meta.Columns {
		oracleType, err := d.MapType(meta.Table, col.Name, col.MySQLType)
		if err != nil {
			return "", err
		}
		comma := ","
		if i == len(meta.Columns)-1 {
			comma = ""
		}
		fmt.Fprintf(&sb, "  %s %s%s\n", d.QuoteIdentifier(col.Name), oracleType, comma)
	}
	pks := make([]string, 0, len(meta.PKNames))
	for _, pk := range meta.PKNames {
		pks = append(pks, d.QuoteIdentifier(pk))
	}
	fmt.Fprintf(&sb, "  ,CONSTRAINT %s PRIMARY KEY (%s)\n)",
		d.QuoteIdentifier(d.PKConstraintName(meta.Table)), strings.Join(pks, ", "))
	return sb.String(), nil
}

// RenderUpsert 生成按 PK 的 MERGE INTO 幂等 upsert（设计文档 D2）。
// 绑定变量按列名命名，支持 go-ora 命名参数。
func (d *Dialect) RenderUpsert(meta model.TableMeta) (string, error) {
	if err := d.ValidateTable(meta); err != nil {
		return "", err
	}
	target := d.qualified(meta)
	var src, on, set, cols, vals strings.Builder
	for i, col := range meta.Columns {
		c := d.QuoteIdentifier(col.Name)
		b := ":" + col.Name
		comma := ","
		if i == len(meta.Columns)-1 {
			comma = ""
		}
		fmt.Fprintf(&src, "%s AS %s%s", b, c, comma)
		fmt.Fprintf(&cols, "%s%s", c, comma)
		fmt.Fprintf(&vals, "%s%s", b, comma)
		if !isPK(col.Name, meta.PKNames) {
			fmt.Fprintf(&set, "t.%s = s.%s%s", c, c, comma)
		}
	}
	setStr := strings.TrimSuffix(set.String(), ",")
	for i, pk := range meta.PKNames {
		c := d.QuoteIdentifier(pk)
		comma := " AND "
		if i == len(meta.PKNames)-1 {
			comma = ""
		}
		fmt.Fprintf(&on, "t.%s = s.%s%s", c, c, comma)
	}
	return fmt.Sprintf(
		"MERGE INTO %s t USING (SELECT %s FROM DUAL) s ON (%s) "+
			"WHEN MATCHED THEN UPDATE SET %s "+
			"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
		target, src.String(), on.String(), setStr, cols.String(), vals.String()), nil
}

// RenderDelete 生成按 PK 的删除语句（目标行不存在不视为错误，D2）。
func (d *Dialect) RenderDelete(meta model.TableMeta) (string, error) {
	if err := d.ValidateTable(meta); err != nil {
		return "", err
	}
	var where strings.Builder
	for i, pk := range meta.PKNames {
		comma := " AND "
		if i == len(meta.PKNames)-1 {
			comma = ""
		}
		fmt.Fprintf(&where, "%s = :%s%s", d.QuoteIdentifier(pk), pk, comma)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", d.qualified(meta), where.String()), nil
}

// RenderAlterTable 二期预留（设计文档 D8）。
func (d *Dialect) RenderAlterTable(meta model.TableMeta, change dialect.SchemaChange) (string, error) {
	return "", dialect.ErrNotImplemented
}

// Prepare 连接级准备（设计文档 D5）。一期无实际 session 级设置：
// 时间类型在 oraclesink 中已显式解析为 time.Time 绑定（M1.5 P0#2），
// 不依赖 NLS 日期格式；接口保留给 sqlserver/db2 与二期 NLS 兜底。
func (d *Dialect) Prepare(ctx context.Context) error { return nil }

func isPK(name string, pkNames []string) bool {
	for _, pk := range pkNames {
		if pk == name {
			return true
		}
	}
	return false
}

// baseName 返回去掉空白与括号参数的基础类型名（小写）。
func baseName(mysqlType string) string {
	s := strings.ToLower(strings.TrimSpace(mysqlType))
	if i := strings.IndexAny(s, "( "); i > 0 {
		return s[:i]
	}
	return s
}

// IsReserved 暴露保留字判断供测试使用。
func IsReserved(name string) bool { return oracleReserved[strings.ToUpper(name)] }
