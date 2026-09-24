// apply.go：DDL 执行与逐表结果（报告口径见设计 §3；FixedDDL 重跑语义
// 对齐 TMS 线上：base64、按表、幂等、只重跑 failed）。
package ddlsync

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
)

// 表结果三态（契约 DdlResult.status 同源）。
const (
	StatusSuccess = "success"
	StatusFailed  = "failed"
	StatusSkipped = "skipped"
)

// TableResult 单表同步结果（ddl_results 表/报告的行结构）。
type TableResult struct {
	Table       string
	TargetTable string
	Status      string
	DDL         string // 生成语句按序拼接（; 分隔）
	ErrorCode   string // ORA-\d+（执行失败时）
	ErrorText   string
	Compat      []CompatItem
	Warnings    []string // 如 overwrite DROP 重建（目标数据已丢弃）显式标注
}

// RunConfig 一次同步任务的输入。
type RunConfig struct {
	Database     string   // 源 TiDB 库
	Tables       []string // 源表名
	TargetSchema string   // 目标 Oracle OWNER
	NameCase     NameCase
	// Overwrite 高危：DROP+CREATE 丢目标数据。默认 false；CLI/REST 层
	// 只应对 failed/skipped 表的重跑路径透出该开关（设计 review #3）。
	Overwrite bool
	// EffectiveMaxIdent 实际生效的标识符上限（探测值；入报告供验收首查）。
	EffectiveMaxIdent int
	// DryRun 只生成语句不执行（CLI 预览模式：探测/兼容预检/存在性
	// 检查照常——均为只读查询；通过预检的表 status=success 表示
	// 「可转换」，报告由调用方标注 dry-run）。
	DryRun bool
}

// Result 整批结果（含任务级环境信息）。
type Result struct {
	Tables    []TableResult
	MaxIdent  int    // 实际生效上限（验收首查项）
	OracleVer string // 探测到的 Oracle 版本串
}

// ProbeOracle 探测目标 Oracle 版本并构造方言（≥12.2 → 标识符 128，
// 否则 30；探测失败回退 30 保守值并记录版本串为空）。
func ProbeOracle(ctx context.Context, oraDB *sql.DB) (ver string, maxIdent int) {
	maxIdent = oracle.DefaultMaxIdentLen
	var v sql.NullString
	// PRODUCT_COMPONENT_VERSION 无需 DBA 权限
	if err := oraDB.QueryRowContext(ctx,
		`SELECT VERSION FROM PRODUCT_COMPONENT_VERSION
		 WHERE PRODUCT LIKE 'Oracle%' AND ROWNUM = 1`).Scan(&v); err != nil {
		return "", maxIdent
	}
	ver = v.String
	major, minor, ok := parseOracleMajorMinor(ver)
	if ok && (major > 12 || (major == 12 && minor >= 2)) {
		maxIdent = 128
	}
	return ver, maxIdent
}

var verRe = regexp.MustCompile(`^(\d+)\.(\d+)`)

func parseOracleMajorMinor(ver string) (major, minor int, ok bool) {
	m := verRe.FindStringSubmatch(strings.TrimSpace(ver))
	if m == nil {
		return 0, 0, false
	}
	fmt.Sscanf(m[1], "%d", &major)
	fmt.Sscanf(m[2], "%d", &minor)
	return major, minor, true
}

// Run 同步一批表：introspect → compat 预检 → convert → 逐表执行。
// 单表失败不中断其余表（t2o error_log_detail 语义的内存版；
// 落库版随 P2 JobManager）。
func Run(ctx context.Context, tidbDB, oraDB *sql.DB, cfg RunConfig) (*Result, error) {
	ver, maxIdent := ProbeOracle(ctx, oraDB)
	cfg.EffectiveMaxIdent = maxIdent
	d := oracle.NewWithIdentLimit(maxIdent)
	in := NewIntrospector(tidbDB)

	res := &Result{MaxIdent: maxIdent, OracleVer: ver}
	for _, table := range cfg.Tables {
		res.Tables = append(res.Tables, syncOne(ctx, in, oraDB, d, cfg, table))
	}
	return res, nil
}

func syncOne(ctx context.Context, in *Introspector, oraDB *sql.DB, d *oracle.Dialect, cfg RunConfig, table string) TableResult {
	meta, err := in.FetchTable(cfg.Database, table)
	if err != nil {
		return TableResult{Table: table, Status: StatusFailed, ErrorText: err.Error()}
	}
	if meta == nil {
		return TableResult{Table: table, Status: StatusSkipped,
			ErrorText: "source table not found or not a base table"}
	}
	target := cfg.TargetSchema
	if target == "" {
		target = cfg.Database
	}

	compat := CheckCompat(meta, d)
	r := TableResult{Table: table, TargetTable: target + "." + table, Compat: compat}

	switch {
	case HasTableCharsetIssue(compat):
		r.Status = StatusSkipped // 表级字符集不兼容：跳过（skipped 两态之一）
		return r
	case HasDataTypeIssue(compat):
		// 列级类型不兼容（ENUM/SET/未知）：failed + compat（FixedDDL 恢复路径）。
		// failed 仍回传本次生成的 DDL 基线（SkipBadColumns：跳过不兼容列，
		// 语句头部注释标记，前端 FixedDDL 编辑器据此起稿）。
		r.Status = StatusFailed
		r.ErrorText = "column type unsupported by target mapping (see compat items)"
		if stmts, extra, err := Convert(meta, d, ConvertOptions{
			NameCase: cfg.NameCase, TargetSchema: cfg.TargetSchema,
			Overwrite: cfg.Overwrite, SkipBadColumns: true}); err == nil {
			r.DDL = joinStatements(stmts)
			// P3-3：表达式索引等 convert 期 compat 项不随基线丢弃
			r.Compat = append(r.Compat, extra...)
		}
		return r
	}

	stmts, extra, err := Convert(meta, d, ConvertOptions{
		NameCase: cfg.NameCase, TargetSchema: cfg.TargetSchema, Overwrite: cfg.Overwrite})
	r.Compat = append(r.Compat, extra...)
	if err != nil {
		r.Status = StatusFailed
		r.ErrorText = err.Error()
		return r
	}
	r.DDL = joinStatements(stmts)

	// 目标存在性检查（只读，dry-run 同样执行）：
	// 已存在且非 overwrite → skipped（skipped 两态之二）；已存在且
	// overwrite → DROP 重建高危标注（设计 §2：报告对被 DROP 表显式标注）。
	var n int
	q := `SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :o AND TABLE_NAME = :t`
	exists := oraDB.QueryRowContext(ctx, q,
		sql.Named("o", target), sql.Named("t", table)).Scan(&n) == nil && n > 0
	if exists {
		if !cfg.Overwrite {
			r.Status = StatusSkipped
			r.ErrorCode = "ORA-00955"
			r.ErrorText = "table already exists in target (overwrite=false)"
			return r
		}
		r.Warnings = append(r.Warnings,
			"已按 overwrite DROP 重建：目标原表（含数据）已被丢弃，本行为不可恢复")
	}

	if cfg.DryRun {
		r.Status = StatusSuccess // 可转换（未执行）；dry-run 由报告外层标注
		return r
	}
	for _, s := range stmts {
		if _, err := oraDB.ExecContext(ctx, s.SQL); err != nil {
			r.Status = StatusFailed
			r.ErrorCode = oraCodeOf(err)
			r.ErrorText = err.Error()
			return r // 语句有序：前缀失败后其余语句大概率无效，失败即停
		}
	}
	r.Status = StatusSuccess
	return r
}

// joinStatements 语句集 → ddl_results.ddl 文本（";\n" 分隔，尾部 ";"）。
func joinStatements(stmts []Statement) string {
	if len(stmts) == 0 {
		return ""
	}
	sqls := make([]string, 0, len(stmts))
	for _, s := range stmts {
		sqls = append(sqls, s.SQL)
	}
	return strings.Join(sqls, ";\n") + ";"
}

// ApplyFixedDDL 重跑单表：base64(UTF-8) DDL 按分号拆分逐条执行。
// 语义对齐 TMS 线上（structure_service 先 decode 再 Exec）。
// 语句有序：第 i 条失败即停，错误显式标注「已生效 i 条」（P2-2 现场
// 复现：部分生效 + 非幂等，下次重跑撞 ORA-00955——结果必须可见）。
func ApplyFixedDDL(ctx context.Context, oraDB *sql.DB, sqlText string) error {
	stmts := splitStatements(sqlText)
	for i, stmt := range stmts {
		if _, err := oraDB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ddlsync: fixed ddl 第 %d/%d 条失败（前 %d 条已生效、部分执行非幂等，重跑前请先清理或改写 DDL）%q…: %w",
				i+1, len(stmts), i, trunc(stmt, 60), err)
		}
	}
	return nil
}

// DecodeFixedDDL base64 → UTF-8 文本（与前端 TextEncoder 口径一致）。
func DecodeFixedDDL(b64 string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return "", fmt.Errorf("ddlsync: decode fixed ddl: %w", err)
	}
	if !utf8.Valid(b) {
		return "", fmt.Errorf("ddlsync: fixed ddl is not valid UTF-8")
	}
	return string(b), nil
}

// splitStatements 引号/注释感知切分（P2-2：裸 ';' 会切坏字符串字面量、
// COMMENT ON 文本与 '--' 注释里的分号 → ORA-01756）。规则：单引号字符串
// 内（含 '' 转义）不切；'--' 行注释吞到行尾；仅引号外的 ';' 是语句边界。
// PL/SQL 匿名块不支持——FixedDDL 编辑器是 DDL 语句序列，不设 BEGIN/END
// 嵌套状态（如需，二期加 /* */ 块注释感知）。
func splitStatements(sqlText string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		switch {
		case inQuote:
			cur.WriteByte(c)
			if c == '\'' {
				if i+1 < len(sqlText) && sqlText[i+1] == '\'' { // '' 转义
					cur.WriteByte(sqlText[i+1])
					i++
				} else {
					inQuote = false
				}
			}
		case c == '\'':
			inQuote = true
			cur.WriteByte(c)
		case c == '-' && i+1 < len(sqlText) && sqlText[i+1] == '-':
			j := strings.IndexByte(sqlText[i:], '\n')
			if j < 0 {
				cur.WriteString(sqlText[i:])
				i = len(sqlText)
			} else {
				cur.WriteString(sqlText[i : i+j+1])
				i += j
			}
		case c == ';':
			if s := strings.TrimSpace(cur.String()); s != "" {
				out = append(out, s)
			}
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// oraCodeOf 从错误文本提取 ORA-\d+（口径同 oraclesink.oraErrorCode；
// 该函数未导出，此处按同实现复用）。
func oraCodeOf(err error) string {
	msg := err.Error()
	i := strings.Index(msg, "ORA-")
	if i < 0 {
		return ""
	}
	j := i + 4
	for j < len(msg) && msg[j] >= '0' && msg[j] <= '9' {
		j++
	}
	if j == i+4 {
		return ""
	}
	return msg[i:j]
}
