// convert.go：TableMetaFull → Oracle DDL 语句集。
//
// 类型映射唯一权威 = dialect.MapType（设计 §0：ddl-sync 建的表就是数据
// 管道要写的表；一致性守卫测试钉住两路径逐列一致）。从 t2o 移植的
// 呈现逻辑（Apache-2.0）：标识符大小写三态、默认值引用转换、
// COMMENT ON DDL——t2o 的 GenSchemaName/GenTableName 大小写分支
// copy-paste bug（rule.go:605/630）在此按 origin/lower/upper 三分支
// 正交实现，不复制原实现。
package ddlsync

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
)

// NameCase 标识符大小写模式（t2o 三态：origin/lower/upper）。
type NameCase int

const (
	// NameCaseOrigin 保留源端原名（默认；与数据管道引号小写口径一致）。
	NameCaseOrigin NameCase = iota
	NameCaseLower
	NameCaseUpper
)

// ConvertOptions DDL 生成选项。
type ConvertOptions struct {
	NameCase     NameCase
	TargetSchema string // 目标 Oracle OWNER（空 = 源库名）
	// DropIfExists 预置 DROP TABLE。仅当调用方确认目标表确实存在时置真
	// （P2-A：无条件生成 + 目标不存在 → ORA-00942；Oracle 无 DROP IF
	// EXISTS，存在性由 syncOne 的 ALL_TABLES 检查决定）。高危：丢目标数据。
	DropIfExists bool
	// SkipBadColumns 容忍列级类型不兼容：跳过该列、相关约束/索引一并
	// 省略，并在语句集头部插入注释标记（供 failed 结果回填 DDL 作为
	// FixedDDL 编辑器基线，绝不直接执行——调用方只用于展示）。
	SkipBadColumns bool
}

// Statement 单条 DDL（按执行顺序输出）。
type Statement struct {
	Kind string // drop | create | index | comment
	SQL  string
}

// Convert 生成单表 DDL 语句集 + 补充兼容项（表达式索引等 convert 期
// 才能判定的项）。存在列级 DATA_TYPE 不兼容时返回错误（调用方标记
// failed，compat 已由 CheckCompat 给出，FixedDDL 重跑是恢复路径）。
func Convert(meta *TableMetaFull, d *oracle.Dialect, opts ConvertOptions) ([]Statement, []CompatItem, error) {
	schema := opts.TargetSchema
	if schema == "" {
		schema = meta.Database
	}
	colNames := make([]string, 0, len(meta.Columns))
	for _, c := range meta.Columns {
		colNames = append(colNames, c.Name)
	}
	// 标识符上限校验与管道 ValidateTable 同口径（dry-run 阶段即可见，
	// 不等 ORA-00972）。截断只发生在约束/索引名（fitIdent），表/列名
	// 从不静默截断。
	if err := d.ValidateIdentifiers(schema, meta.Table, colNames); err != nil {
		return nil, nil, err
	}
	nc := func(name string) string {
		switch opts.NameCase {
		case NameCaseLower:
			return strings.ToLower(name)
		case NameCaseUpper:
			return strings.ToUpper(name)
		default:
			return name
		}
	}

	// 先逐列映射（错误即 failed；此路径与管道 RenderCreateTable 同源）
	type colDDL struct{ name, typ, mods string }
	cols := make([]colDDL, 0, len(meta.Columns))
	colTypes := make(map[string]string, len(meta.Columns)) // 列名→目标类型（LOB 索引判定）
	skipped := map[string]bool{}
	for _, c := range meta.Columns {
		oracleType, err := d.MapType(meta.Table, c.Name, c.MySQLType)
		if err != nil {
			if !opts.SkipBadColumns {
				return nil, nil, err
			}
			skipped[c.Name] = true
			continue
		}
		mods, err := columnMods(c, oracleType)
		if err != nil {
			return nil, nil, fmt.Errorf("ddlsync: column %s.%s default %q: %w", meta.Table, c.Name, c.Default, err)
		}
		colTypes[c.Name] = oracleType
		cols = append(cols, colDDL{name: d.QuoteIdentifier(nc(c.Name)), typ: oracleType, mods: mods})
	}

	var extra []CompatItem
	var stmts []Statement
	tableName := d.QuoteIdentifier(nc(schema)) + "." + d.QuoteIdentifier(nc(meta.Table))

	if len(skipped) > 0 {
		var names []string
		for _, c := range meta.Columns {
			if skipped[c.Name] {
				names = append(names, c.Name)
			}
		}
		stmts = append(stmts, Statement{Kind: "comment",
			SQL: fmt.Sprintf("-- 以下列因目标类型不兼容被跳过（本 DDL 仅作 FixedDDL 编辑基线，修正后方可执行）: %s",
				strings.Join(names, ", "))})
	}

	if opts.DropIfExists {
		stmts = append(stmts, Statement{Kind: "drop",
			SQL: fmt.Sprintf("DROP TABLE %s", tableName)})
	}

	// CREATE TABLE：列 + NOT NULL/DEFAULT + PK + UK（内联约束）。
	// 全列跳过时不生成 CREATE（该 DDL 仅作展示基线）。
	pkNames := filterSkipped(meta.PKNames, skipped)
	validUKs := make([]IndexMeta, 0, len(meta.Uniques))
	for _, uk := range meta.Uniques {
		if !anySkipped(uk.Columns, skipped) {
			validUKs = append(validUKs, uk)
		}
	}
	// indexedCols：已生效的索引列集（PK + 已生成 UK + 已建二级索引），
	// UK 去重（ORA-02261）与二级索引去重（ORA-01408）共用。
	indexedCols := [][]string{}
	if len(pkNames) > 0 {
		indexedCols = append(indexedCols, pkNames)
	}
	if len(cols) > 0 {
		var sb strings.Builder
		fmt.Fprintf(&sb, "CREATE TABLE %s (\n", tableName)
		for i, c := range cols {
			comma := ","
			// 尾列不带尾逗号：后续约束行自带前导逗号（与管道
			// RenderCreateTable 同构；冒烟 Bug2：尾逗号+前导逗号=双逗号
			// ORA-00917）。
			if i == len(cols)-1 {
				comma = ""
			}
			fmt.Fprintf(&sb, "  %s %s%s%s\n", c.name, c.typ, c.mods, comma)
		}
		if len(pkNames) > 0 {
			pks := make([]string, 0, len(pkNames))
			for _, pk := range pkNames {
				pks = append(pks, d.QuoteIdentifier(nc(pk)))
			}
			fmt.Fprintf(&sb, "  ,CONSTRAINT %s PRIMARY KEY (%s)\n",
				d.QuoteIdentifier(d.PKConstraintName(nc(meta.Table))), strings.Join(pks, ", "))
		}
		for _, uk := range validUKs {
			if isExpressionIndex(uk, meta) {
				extra = append(extra, CompatItem{
					Level:        "table",
					PreCheckType: PreCheckDataType,
					SourceValue:  fmt.Sprintf("UNIQUE INDEX %s", uk.Name),
					TargetValue:  "",
					Suggest:      "唯一索引含表达式列，一期不转换；目标未创建该约束（可用 FixedDDL 手工补）",
				})
				continue
			}
			// ORA-02261（#t14）：内联 UNIQUE 列集与 PK/已生成 UNIQUE 精确
			// 相同时 Oracle 拒绝（northwind.purchase_orders：PK(id) +
			// UNIQUE INDEX id(id)，生成器/ORM 常见）——与二级索引的
			// ORA-01408 去重同族，剔除并记 compat。
			if dupIndexColumns(uk.Columns, indexedCols) {
				extra = append(extra, CompatItem{
					Level:        "table",
					PreCheckType: PreCheckDataType,
					SourceValue:  fmt.Sprintf("UNIQUE INDEX %s", uk.Name),
					TargetValue:  "",
					Suggest:      "唯一约束列集与主键/已有唯一约束重复，Oracle 拒绝（ORA-02261）；已跳过",
				})
				continue
			}
			indexedCols = append(indexedCols, uk.Columns)
			colsUK := make([]string, 0, len(uk.Columns))
			for _, c := range uk.Columns {
				colsUK = append(colsUK, d.QuoteIdentifier(nc(c)))
			}
			fmt.Fprintf(&sb, "  ,CONSTRAINT %s UNIQUE (%s)\n",
				// Oracle 约束名 schema 全局：MySQL 约束名仅表内唯一，跨表
				// 同名（role_id 等）→ ORA-02264，UK 名纳入表名限定
				d.QuoteIdentifier(fitIdent(d, "UK_"+meta.Table+"_"+uk.Name)), strings.Join(colsUK, ", "))
		}
		sb.WriteString(")")
		stmts = append(stmts, Statement{Kind: "create", SQL: sb.String()})
	}

	// 二级索引（引用被跳过列的索引一并省略）。
	//   - 索引名 schema 全局：MySQL 索引名仅表内唯一，跨表同名（project_id
	//     等）→ ORA-00955，索引名纳入表名限定（IDX_<table>_<idx>）；
	//   - ORA-01408：列集与 PK/UK/已建索引完全相同的重复索引 Oracle 拒绝，
	//     跳过并记 compat；
	//   - ORA-02327：LOB 列不能建 B-tree 索引，跳过并记 compat。
	// （indexedCols 已含 PK + 已生成 UK 列集：UK 生成时逐条累积）
	for _, idx := range meta.Indexes {
		if anySkipped(idx.Columns, skipped) {
			continue
		}
		if isExpressionIndex(idx, meta) {
			extra = append(extra, CompatItem{
				Level:        "table",
				PreCheckType: PreCheckDataType,
				SourceValue:  fmt.Sprintf("INDEX %s", idx.Name),
				TargetValue:  "",
				Suggest:      "表达式索引一期不转换；目标未创建该索引（可用 FixedDDL 手工补）",
			})
			continue
		}
		if hasLobColumn(idx.Columns, colTypes) {
			extra = append(extra, CompatItem{
				Level:        "table",
				PreCheckType: PreCheckDataType,
				SourceValue:  fmt.Sprintf("INDEX %s", idx.Name),
				TargetValue:  "",
				Suggest:      "索引含 LOB 列，Oracle B-tree 不支持（ORA-02327）；已跳过，需要时可用 Oracle TEXT 或函数索引",
			})
			continue
		}
		if dupIndexColumns(idx.Columns, indexedCols) {
			extra = append(extra, CompatItem{
				Level:        "table",
				PreCheckType: PreCheckDataType,
				SourceValue:  fmt.Sprintf("INDEX %s", idx.Name),
				TargetValue:  "",
				Suggest:      "索引列集与主键/唯一约束/已建索引重复，Oracle 拒绝重复索引（ORA-01408）；已跳过",
			})
			continue
		}
		indexedCols = append(indexedCols, idx.Columns)
		idxCols := make([]string, 0, len(idx.Columns))
		for _, c := range idx.Columns {
			idxCols = append(idxCols, d.QuoteIdentifier(nc(c)))
		}
		stmts = append(stmts, Statement{Kind: "index", SQL: fmt.Sprintf(
			"CREATE INDEX %s ON %s (%s)",
			d.QuoteIdentifier(fitIdent(d, "IDX_"+meta.Table+"_"+idx.Name)), tableName, strings.Join(idxCols, ", "))})
	}

	// 注释（COMMENT ON，t2o 口径）
	if meta.Comment != "" {
		stmts = append(stmts, Statement{Kind: "comment", SQL: fmt.Sprintf(
			"COMMENT ON TABLE %s IS '%s'", tableName, escapeSingleQuotes(meta.Comment))})
	}
	for _, c := range meta.Columns {
		if c.Comment != "" && !skipped[c.Name] {
			stmts = append(stmts, Statement{Kind: "comment", SQL: fmt.Sprintf(
				"COMMENT ON COLUMN %s.%s IS '%s'", tableName,
				d.QuoteIdentifier(nc(c.Name)), escapeSingleQuotes(c.Comment))})
		}
	}
	return stmts, extra, nil
}

// columnMods 生成 DEFAULT / NOT NULL 子句。
// 语法序铁律：Oracle 列定义 DEFAULT 必须在 NOT NULL 之前
// （`NOT NULL DEFAULT ...` → ORA-03076；#t8 重开 P1，leader #t12
// 用例池 11/11 ready 表全挂，均为 NOT NULL+默认值 timestamp 列）。
// 默认值口径（t2o ChangeTableColumnDefaultValue 移植 + 收敛）：
//   - 无显式默认：可空列省略（隐式 NULL），NOT NULL 列省略（无默认）；
//   - 函数/关键字默认（CURRENT_TIMESTAMP 等）：不引号，TIMESTAMP 列
//     CURRENT_TIMESTAMP → SYSTIMESTAMP；
//   - b'..' 位串字面量 → 十进制；
//   - 字符串字面量：单引号 + '' doubling。
func columnMods(c ColumnMeta, oracleType string) (string, error) {
	mods := ""
	if c.HasDefault {
		def := strings.TrimSpace(c.Default)
		switch {
		case def == "", strings.EqualFold(def, "NULL"):
			mods = " DEFAULT NULL"
		case isFunctionDefault(def):
			if strings.EqualFold(def, "CURRENT_TIMESTAMP") ||
				strings.EqualFold(def, "now()") ||
				strings.EqualFold(def, "current_timestamp()") {
				if strings.HasPrefix(oracleType, "TIMESTAMP") {
					mods = " DEFAULT SYSTIMESTAMP"
				}
				// 非 TIMESTAMP 目标列：丢弃时间默认（mods 保持 ""；
				// 记兼容项由上层补充属 P2）
			} else if isStringTarget(oracleType) && !strings.Contains(def, "(") {
				// 裸标识符默认（无括号）落在字符串目标列上：
				// information_schema 对字符串默认可能去引号存储
				// （MySQL 5.7 口径；TiDB 8 侧 L2 实测定案），按字面量
				// 加引号更安全——误引号最多 ORA-00904 可见，漏引号会把
				// 字面量当列引用。
				mods = fmt.Sprintf(" DEFAULT '%s'", escapeSingleQuotes(def))
			} else {
				// 其他函数默认：原样透传（与 t2o 口径一致；坏函数由
				// Oracle 执行期报错可见）
				mods = fmt.Sprintf(" DEFAULT %s", def)
			}
		case strings.HasPrefix(def, "b'") && strings.HasSuffix(def, "'"):
			bin := def[2 : len(def)-1]
			n, err := strconv.ParseUint(bin, 2, 64)
			if err != nil {
				return "", fmt.Errorf("bad bit default %q: %w", def, err)
			}
			mods = fmt.Sprintf(" DEFAULT %d", n)
		default:
			// 日期时间字面量默认（'0001-01-01 00:00:00' 等）：裸引号字面量
			// 在 TIMESTAMP/DATE 列上走 NLS 隐式转换 → ORA-01847；改 ANSI
			// 字面量 TIMESTAMP '...' / DATE '...'（leader 真库实证可过）。
			if dateLitRe.MatchString(def) {
				switch {
				case strings.HasPrefix(oracleType, "TIMESTAMP"):
					mods = fmt.Sprintf(" DEFAULT TIMESTAMP '%s'", def)
				case oracleType == "DATE":
					if len(def) == 10 { // DATE 字面量仅接受 YYYY-MM-DD
						mods = fmt.Sprintf(" DEFAULT DATE '%s'", def)
					} else { // DATE 列 + 带时间：TO_DATE 显式格式，不赌 NLS
						mods = fmt.Sprintf(" DEFAULT TO_DATE('%s', 'YYYY-MM-DD HH24:MI:SS')", def)
					}
				default: // 非日期目标列：普通引号字面量（日期串只是巧合字符串）
					mods = fmt.Sprintf(" DEFAULT '%s'", escapeSingleQuotes(def))
				}
			} else {
				mods = fmt.Sprintf(" DEFAULT '%s'", escapeSingleQuotes(def))
			}
		}
	}
	if !c.Nullable {
		mods += " NOT NULL" // 恒末尾（ORA-03076 语法序）
	}
	return mods, nil
}

// isFunctionDefault 函数/关键字默认的粗判：标识符开头且全为字母数字/
// 下划线/括号（无引号包裹的值在 information_schema 里仅函数与关键字）。
var funcDefaultRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\(\d*\))?$`)

func isFunctionDefault(def string) bool {
	return funcDefaultRe.MatchString(def)
}

// dateLitRe 日期时间字面量默认（information_schema 对 date/datetime/
// timestamp 列的默认值呈现）：YYYY-MM-DD[ HH:MM:SS[.fff]]。
var dateLitRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d{1,9})?)?$`)

// hasLobColumn 索引列集中含 LOB 目标类型列（B-tree 不能索引 LOB，ORA-02327）。
func hasLobColumn(cols []string, colTypes map[string]string) bool {
	for _, c := range cols {
		switch colTypes[c] {
		case "CLOB", "NCLOB", "BLOB":
			return true
		}
	}
	return false
}

// dupIndexColumns 列集与已有索引（PK/UK/已建二级索引）完全相同（ORA-01408）。
func dupIndexColumns(cols []string, existing [][]string) bool {
	for _, e := range existing {
		if sameColumns(e, cols) {
			return true
		}
	}
	return false
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// isStringTarget 目标列为字符类型（字面量默认应加引号的判定）。
func isStringTarget(oracleType string) bool {
	return strings.HasPrefix(oracleType, "VARCHAR2") ||
		strings.HasPrefix(oracleType, "NVARCHAR2") ||
		strings.HasPrefix(oracleType, "CHAR") ||
		oracleType == "CLOB" || oracleType == "NCLOB"
}

// filterSkipped / anySkipped：SkipBadColumns 模式下剔除引用被跳过
// 列的 PK / 索引（引用不存在列的 DDL 在 Oracle 必然 ORA-00904）。
func filterSkipped(names []string, skipped map[string]bool) []string {
	if len(skipped) == 0 {
		return names
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if !skipped[n] {
			out = append(out, n)
		}
	}
	return out
}

func anySkipped(names []string, skipped map[string]bool) bool {
	if len(skipped) == 0 {
		return false
	}
	for _, n := range names {
		if skipped[n] {
			return true
		}
	}
	return false
}

// isExpressionIndex 索引任一列不在表列集（含空串表达式占位）= 表达式索引。
func isExpressionIndex(idx IndexMeta, meta *TableMetaFull) bool {
	names := map[string]bool{}
	for _, c := range meta.Columns {
		names[c.Name] = true
	}
	for _, c := range idx.Columns {
		if c == "" || !names[c] {
			return true
		}
	}
	return false
}

// fitIdent 约束/索引名超标识符上限时截断+短哈希（与 PKConstraintName
// 同算法，fitIdent 供 UK_/IDX_ 前缀名复用）。
func fitIdent(d *oracle.Dialect, name string) string {
	if len(name) <= d.MaxIdentLen() {
		return name
	}
	sum := fnv32a(name)
	return name[:d.MaxIdentLen()-4] + fmt.Sprintf("%04X", sum&0xFFFF)
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
