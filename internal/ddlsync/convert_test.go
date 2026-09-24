// convert_test.go：Convert 黄金断言 + 默认值边界 + SkipBadColumns 基线。
package ddlsync

import (
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
)

func goldenMeta() *TableMetaFull {
	return &TableMetaFull{
		Database: "shop", Table: "orders", Charset: "utf8mb4", Collation: "utf8mb4_bin",
		Comment: "订单表",
		Columns: []ColumnMeta{
			{Name: "id", MySQLType: "bigint", Nullable: false, IsPK: true, Comment: "主键"},
			{Name: "order_no", MySQLType: "varchar(32)", Nullable: false, HasDefault: false, Comment: "存在 ' 引号'"},
			{Name: "amount", MySQLType: "decimal(10,2)", Nullable: true, HasDefault: true, Default: "0.00"},
			{Name: "status", MySQLType: "varchar(16)", Nullable: true, HasDefault: true, Default: "new"},
			{Name: "flag", MySQLType: "bit(8)", Nullable: true, HasDefault: true, Default: "b'101'"},
			{Name: "created_at", MySQLType: "datetime", Nullable: true, HasDefault: true, Default: "CURRENT_TIMESTAMP"},
		// NOT NULL + 默认值并存（#t8 再重开 P1：黄金此前无此形态，漏检
		// ORA-03076——语法序 DEFAULT 必须在前）
		{Name: "updated_at", MySQLType: "timestamp", Nullable: false, HasDefault: true, Default: "CURRENT_TIMESTAMP"},
			{Name: "note", MySQLType: "text", Nullable: true},
		},
		PKNames: []string{"id"},
		Uniques: []IndexMeta{{Name: "uk_order", Columns: []string{"order_no"}}},
		Indexes: []IndexMeta{{Name: "idx_status", Columns: []string{"status"}}},
	}
}

func TestConvertGolden(t *testing.T) {
	d := oracle.NewWithIdentLimit(128)
	stmts, extra, err := Convert(goldenMeta(), d, ConvertOptions{TargetSchema: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	if len(extra) != 0 {
		t.Fatalf("unexpected compat extras: %+v", extra)
	}
	var got []string
	for _, s := range stmts {
		got = append(got, s.Kind+" | "+s.SQL)
	}
	want := []string{
		"create | CREATE TABLE \"shop\".\"orders\" (\n" +
			"  \"id\" NUMBER(19) NOT NULL,\n" +
			"  \"order_no\" VARCHAR2(32 CHAR) NOT NULL,\n" +
			"  \"amount\" NUMBER(10,2) DEFAULT '0.00',\n" +
			"  \"status\" VARCHAR2(16 CHAR) DEFAULT 'new',\n" +
			"  \"flag\" NUMBER DEFAULT 5,\n" +
			"  \"created_at\" TIMESTAMP DEFAULT SYSTIMESTAMP,\n" +
			"  \"updated_at\" TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,\n" +
			"  \"note\" CLOB\n" +
			"  ,CONSTRAINT \"PK_orders\" PRIMARY KEY (\"id\")\n" +
			"  ,CONSTRAINT \"UK_orders_uk_order\" UNIQUE (\"order_no\")\n" +
			")",
		"index | CREATE INDEX \"IDX_orders_idx_status\" ON \"shop\".\"orders\" (\"status\")",
		"comment | COMMENT ON TABLE \"shop\".\"orders\" IS '订单表'",
		"comment | COMMENT ON COLUMN \"shop\".\"orders\".\"id\" IS '主键'",
		"comment | COMMENT ON COLUMN \"shop\".\"orders\".\"order_no\" IS '存在 '' 引号'''",
	}
	if len(got) != len(want) {
		t.Fatalf("stmt count %d != %d:\n%s", len(got), len(want), strings.Join(got, "\n---\n"))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("stmt #%d:\n got:  %s\n want: %s", i, got[i], want[i])
		}
	}
}

// TestConvertNoDoubleComma 冒烟 Bug2 回归：尾列与约束行之间不得出现
// 双逗号（NOT NULL,\n  ,CONSTRAINT → ORA-00917）。黄金钉住的旧输出即
// 错误行为，此测试独立钉住「无 ',\n  ,'」。
func TestConvertNoDoubleComma(t *testing.T) {
	d := oracle.NewWithIdentLimit(128)
	for _, m := range []*TableMetaFull{goldenMeta(), goldenMetaNoUK()} {
		stmts, _, err := Convert(m, d, ConvertOptions{TargetSchema: "shop"})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range stmts {
			if strings.Contains(s.SQL, ",\n  ,") || strings.Contains(s.SQL, ",\n,") {
				t.Errorf("double comma in %s:\n%s", m.Table, s.SQL)
			}
		}
	}
}

// goldenMetaNoUK 无 UK 只有 PK（约束紧跟尾列的另一形态）。
func goldenMetaNoUK() *TableMetaFull {
	m := goldenMeta()
	m.Uniques = nil
	m.Indexes = nil
	return m
}

// TestConvertDropOnlyWhenExists P2-A 钉子：DROP 仅由 DropIfExists 决定
// （存在性检查在 syncOne；无条件 DROP + 目标不存在 → ORA-00942）。
func TestConvertDropOnlyWhenExists(t *testing.T) {
	d := oracle.NewWithIdentLimit(30)
	stmts, _, err := Convert(goldenMeta(), d, ConvertOptions{TargetSchema: "shop", DropIfExists: true})
	if err != nil {
		t.Fatal(err)
	}
	if stmts[0].Kind != "drop" || !strings.HasPrefix(stmts[0].SQL, "DROP TABLE \"shop\".\"orders\"") {
		t.Fatalf("DropIfExists must prepend DROP, got %+v", stmts[0])
	}
	// 目标不存在（DropIfExists=false，syncOne 对新建表的取值）→ 绝无 DROP
	stmts, _, err = Convert(goldenMeta(), d, ConvertOptions{TargetSchema: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stmts {
		if s.Kind == "drop" || strings.HasPrefix(s.SQL, "DROP TABLE") {
			t.Fatalf("new table must not emit DROP (ORA-00942): %+v", s)
		}
	}
}

func TestConvertNameCase(t *testing.T) {
	d := oracle.NewWithIdentLimit(30)
	for _, tc := range []struct {
		nc   NameCase
		want string
	}{
		{NameCaseLower, `CREATE TABLE "shop"."orders"`},
		{NameCaseUpper, `CREATE TABLE "SHOP"."ORDERS"`},
		{NameCaseOrigin, `CREATE TABLE "Shop"."orders"`}, // origin：TargetSchema 原样保留
	} {
		stmts, _, err := Convert(goldenMeta(), d, ConvertOptions{TargetSchema: "Shop", NameCase: tc.nc})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stmts[0].SQL, tc.want) {
			t.Errorf("nameCase %v: want %q in %q", tc.nc, tc.want, stmts[0].SQL)
		}
	}
}

func TestColumnModsEdges(t *testing.T) {
	cases := []struct {
		name       string
		col        ColumnMeta
		oracleType string
		want       string
	}{
		{"no default not null", ColumnMeta{Nullable: false}, "NUMBER(19)", " NOT NULL"},
		{"no default nullable", ColumnMeta{Nullable: true}, "CLOB", ""},
		{"explicit null", ColumnMeta{Nullable: true, HasDefault: true, Default: "NULL"}, "CLOB", " DEFAULT NULL"},
		{"empty string default", ColumnMeta{Nullable: true, HasDefault: true, Default: ""}, "VARCHAR2(8 CHAR)", " DEFAULT NULL"},
		{"ct timestamp col", ColumnMeta{Nullable: true, HasDefault: true, Default: "CURRENT_TIMESTAMP"}, "TIMESTAMP", " DEFAULT SYSTIMESTAMP"},
		{"now() timestamp col", ColumnMeta{Nullable: true, HasDefault: true, Default: "now()"}, "TIMESTAMP", " DEFAULT SYSTIMESTAMP"},
		{"ct on non-timestamp dropped", ColumnMeta{Nullable: true, HasDefault: true, Default: "CURRENT_TIMESTAMP"}, "VARCHAR2(16 CHAR)", ""},
		{"ct non-timestamp not-null keeps nn", ColumnMeta{Nullable: false, HasDefault: true, Default: "current_timestamp()"}, "NUMBER(3)", " NOT NULL"},
		{"bit default", ColumnMeta{Nullable: true, HasDefault: true, Default: "b'0'"}, "NUMBER", " DEFAULT 0"},
		{"bit default 8bit", ColumnMeta{Nullable: true, HasDefault: true, Default: "b'11111111'"}, "NUMBER", " DEFAULT 255"},
		{"string quotes", ColumnMeta{Nullable: true, HasDefault: true, Default: "o'k"}, "VARCHAR2(8 CHAR)", " DEFAULT 'o''k'"},
		{"func passthrough", ColumnMeta{Nullable: true, HasDefault: true, Default: "uuid()"}, "VARCHAR2(64 CHAR)", " DEFAULT uuid()"},
	}
	for _, c := range cases {
		got, err := columnMods(c.col, c.oracleType)
		if err != nil {
			t.Errorf("%s: err %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
	// 坏位串必须报错（failed 可见）
	if _, err := columnMods(ColumnMeta{HasDefault: true, Default: "b'2'"}, "NUMBER"); err == nil {
		t.Error("bad bit default b'2' should error")
	}
}

// TestColumnModsDefaultBeforeNotNull P1 钉子（#t8 再重开，leader #t12 用例
// 池 11/11 撞 ORA-03076）：NOT NULL 与默认值并存时 DEFAULT 必须在 NOT NULL
// 之前（Oracle 列定义语法序）。覆盖全部默认值分支 × NOT NULL 组合。
func TestColumnModsDefaultBeforeNotNull(t *testing.T) {
	cases := []struct {
		name       string
		col        ColumnMeta
		oracleType string
	}{
		{"ct timestamp", ColumnMeta{Nullable: false, HasDefault: true, Default: "CURRENT_TIMESTAMP"}, "TIMESTAMP"},
		{"decimal literal", ColumnMeta{Nullable: false, HasDefault: true, Default: "0.00"}, "NUMBER(10,2)"},
		{"bit literal", ColumnMeta{Nullable: false, HasDefault: true, Default: "b'101'"}, "NUMBER"},
		{"string literal", ColumnMeta{Nullable: false, HasDefault: true, Default: "o'k"}, "VARCHAR2(8 CHAR)"},
		{"explicit NULL", ColumnMeta{Nullable: false, HasDefault: true, Default: "NULL"}, "CLOB"},
		{"empty string default", ColumnMeta{Nullable: false, HasDefault: true, Default: ""}, "VARCHAR2(8 CHAR)"},
		{"func passthrough", ColumnMeta{Nullable: false, HasDefault: true, Default: "uuid()"}, "VARCHAR2(64 CHAR)"},
		{"ct dropped non-timestamp", ColumnMeta{Nullable: false, HasDefault: true, Default: "current_timestamp()"}, "NUMBER(3)"},
	}
	for _, c := range cases {
		mods, err := columnMods(c.col, c.oracleType)
		if err != nil {
			t.Fatalf("%s: err %v", c.name, err)
		}
		if !strings.HasSuffix(mods, "NOT NULL") {
			t.Errorf("%s: NOT NULL must be the trailing clause: %q", c.name, mods)
		}
		// 有默认值且未被丢弃的分支：DEFAULT 必须出现在 NOT NULL 之前
		if c.col.Default != "current_timestamp()" { // 该分支时间默认被丢弃，无 DEFAULT 合法
			di := strings.Index(mods, "DEFAULT")
			ni := strings.LastIndex(mods, "NOT NULL")
			if di < 0 || di > ni {
				t.Errorf("%s: DEFAULT must precede NOT NULL (ORA-03076): %q", c.name, mods)
			}
		}
	}
}

// TestConvertSkipBadColumns failed 回填 DDL 基线：不兼容列被跳过并注释
// 标记；引用该列的 PK/UK/索引/注释剔除；其余列正常生成。
func TestConvertSkipBadColumns(t *testing.T) {
	m := goldenMeta()
	m.Columns = append(m.Columns, ColumnMeta{Name: "tags", MySQLType: "set('a','b')", Nullable: true, Comment: "集"})
	m.Uniques = append(m.Uniques, IndexMeta{Name: "uk_tags", Columns: []string{"tags"}})
	m.Indexes = append(m.Indexes, IndexMeta{Name: "idx_tags", Columns: []string{"tags"}})
	m.PKNames = []string{"id"}

	d := oracle.NewWithIdentLimit(128)
	stmts, _, err := Convert(m, d, ConvertOptions{TargetSchema: "shop", SkipBadColumns: true})
	if err != nil {
		t.Fatal(err)
	}
	joined := joinStatements(stmts)
	if !strings.Contains(joined, "-- 以下列因目标类型不兼容被跳过") || !strings.Contains(joined, "tags") {
		t.Errorf("missing skip marker:\n%s", joined)
	}
	if strings.Contains(joined, `"tags"`) {
		t.Errorf("skipped column must not appear as column def:\n%s", joined)
	}
	if strings.Contains(joined, "uk_tags") || strings.Contains(joined, "idx_tags") {
		t.Errorf("constraints referencing skipped column must be dropped:\n%s", joined)
	}
	if strings.Contains(joined, "IS '集'") {
		t.Errorf("comment on skipped column must be dropped:\n%s", joined)
	}
	// 其余结构保留
	for _, keep := range []string{`"order_no" VARCHAR2(32 CHAR)`, `"PK_orders"`, `UK_orders_uk_order`, `IDX_orders_idx_status`} {
		if !strings.Contains(joined, keep) {
			t.Errorf("missing %s in baseline:\n%s", keep, joined)
		}
	}

	// 不开 SkipBadColumns：set 列硬拒绝（与守卫测试同口径）
	if _, _, err := Convert(m, d, ConvertOptions{}); err == nil {
		t.Error("convert without SkipBadColumns must reject set column")
	}
}

// TestConvertExpressionIndexCompat 表达式索引 → 兼容项、不生成 DDL。
func TestConvertExpressionIndexCompat(t *testing.T) {
	m := goldenMeta()
	m.Indexes = append(m.Indexes, IndexMeta{Name: "idx_expr", Columns: []string{"", "status"}}) // 空串 = 表达式部分
	stmts, extra, err := Convert(m, oracle.NewWithIdentLimit(30), ConvertOptions{TargetSchema: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	if len(extra) != 1 || extra[0].PreCheckType != PreCheckDataType || !strings.Contains(extra[0].SourceValue, "idx_expr") {
		t.Fatalf("expression index must yield exactly one compat item, got %+v", extra)
	}
	joined := joinStatements(stmts)
	if strings.Contains(joined, "idx_expr") {
		t.Errorf("expression index DDL must not be generated:\n%s", joined)
	}
}

// TestSkipDuplicateUniqueConstraint #t14 钉子（ORA-02261）：内联 UNIQUE
// 约束列集与 PK（northwind.purchase_orders：PK(id)+UNIQUE INDEX id(id)）
// 或已生成 UNIQUE 精确相同时剔除 + compat；被剔除 UK 不再作为二级索引
// 去重的种子（种子里只有真实生效的约束）。
func TestSkipDuplicateUniqueConstraint(t *testing.T) {
	m := goldenMeta() // PK=[id]
	m.Uniques = []IndexMeta{
		{Name: "id", Columns: []string{"id"}},             // = PK 列集（#t14 现场）
		{Name: "order_no", Columns: []string{"order_no"}}, // 正常 UK，保留
		{Name: "order_no2", Columns: []string{"order_no"}}, // = 已生成 UK 列集
	}
	// 二级索引与保留 UK 同列集 → 走 ORA-01408 剔除（种子=已生效约束）
	m.Indexes = append(m.Indexes, IndexMeta{Name: "idx_order_no", Columns: []string{"order_no"}})

	stmts, extra, err := Convert(m, oracle.NewWithIdentLimit(128), ConvertOptions{TargetSchema: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	joined := joinStatements(stmts)
	if n := strings.Count(joined, "CONSTRAINT"); n != 2 { // PK + 仅一条 UK
		t.Fatalf("want exactly PK + 1 UNIQUE constraint, got %d:\n%s", n, joined)
	}
	if !strings.Contains(joined, `"UK_orders_order_no"`) || strings.Contains(joined, `"UK_orders_id"`) {
		t.Fatalf("must keep distinct UK, drop PK-duplicate UK:\n%s", joined)
	}
	// compat：2 条 UK 剔除 + 1 条二级索引剔除
	if len(extra) != 3 {
		t.Fatalf("want 3 compat items (dup UK×2 + dup idx×1), got %d: %+v", len(extra), extra)
	}
	var ukDup, idxDup int
	for _, it := range extra {
		if strings.Contains(it.Suggest, "ORA-02261") {
			ukDup++
		}
		if strings.Contains(it.Suggest, "ORA-01408") {
			idxDup++
		}
	}
	if ukDup != 2 || idxDup != 1 {
		t.Fatalf("compat breakdown wrong: ukDup=%d idxDup=%d: %+v", ukDup, idxDup, extra)
	}
}

// TestIndexConstraintNamesTableQualified P1 钉子（#t12 扫描 ORA-00955/
// ORA-02264）：Oracle 索引名与约束名 schema 全局，MySQL 表内同名的
// 索引/UK 跨表必撞——生成名必须纳入表名限定，两表互不相同。
func TestIndexConstraintNamesTableQualified(t *testing.T) {
	d := oracle.NewWithIdentLimit(128)
	mk := func(table string) *TableMetaFull {
		m := goldenMeta()
		m.Table = table
		m.Uniques = []IndexMeta{{Name: "role_id", Columns: []string{"order_no"}}}
		m.Indexes = []IndexMeta{{Name: "status", Columns: []string{"status"}}}
		return m
	}
	seen := map[string]bool{}
	for _, tbl := range []string{"col_restrictions", "project_role_restrictions"} {
		stmts, _, err := Convert(mk(tbl), d, ConvertOptions{TargetSchema: "shop"})
		if err != nil {
			t.Fatal(err)
		}
		joined := joinStatements(stmts)
		for _, want := range []string{"IDX_" + tbl + "_status", "UK_" + tbl + "_role_id"} {
			if !strings.Contains(joined, `"`+want+`"`) {
				t.Errorf("%s: missing qualified name %q:\n%s", tbl, want, joined)
			}
			if seen[want] {
				t.Errorf("cross-table collision on %q", want)
			}
			seen[want] = true
		}
	}
}

// TestSkipDuplicateAndLobIndex P2 钉子（ORA-01408 / ORA-02327）：列集与
// PK/UK/已建索引完全相同的索引剔除并记 compat；LOB 列索引剔除并记 compat。
func TestSkipDuplicateAndLobIndex(t *testing.T) {
	m := goldenMeta()
	m.Indexes = append(m.Indexes,
		IndexMeta{Name: "dup_pk", Columns: []string{"id"}},         // = PK 列集
		IndexMeta{Name: "dup_uk", Columns: []string{"order_no"}},   // = UK 列集
		IndexMeta{Name: "st2", Columns: []string{"status"}},        // = 同表已建索引列集
		IndexMeta{Name: "idx_note", Columns: []string{"note"}},     // CLOB 列
	)
	stmts, extra, err := Convert(m, oracle.NewWithIdentLimit(128), ConvertOptions{TargetSchema: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	joined := joinStatements(stmts)
	if n := strings.Count(joined, "CREATE INDEX"); n != 1 {
		t.Fatalf("only the unique non-LOB index must survive, got %d:\n%s", n, joined)
	}
	if !strings.Contains(joined, `"IDX_orders_idx_status"`) {
		t.Fatalf("surviving index wrong:\n%s", joined)
	}
	if len(extra) != 4 {
		t.Fatalf("want 4 compat items (dup_pk/dup_uk/st2/idx_note), got %d: %+v", len(extra), extra)
	}
	for _, it := range extra {
		if it.PreCheckType != PreCheckDataType || it.Level != "table" {
			t.Errorf("compat item malformed: %+v", it)
		}
	}
}

// TestDateLiteralDefault P2 钉子（ORA-01847）：日期时间字面量默认在
// TIMESTAMP/DATE 列必须走 ANSI 字面量/TO_DATE，不得裸引号赌 NLS。
func TestDateLiteralDefault(t *testing.T) {
	cases := []struct{ def, typ, want string }{
		{"0001-01-01 00:00:00", "TIMESTAMP", ` DEFAULT TIMESTAMP '0001-01-01 00:00:00'`},
		{"2024-01-02", "DATE", ` DEFAULT DATE '2024-01-02'`},
		{"2024-01-02 03:04:05", "DATE", ` DEFAULT TO_DATE('2024-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS')`},
		{"2024-01-02 03:04:05.123", "TIMESTAMP", ` DEFAULT TIMESTAMP '2024-01-02 03:04:05.123'`},
	}
	for _, c := range cases {
		got, err := columnMods(ColumnMeta{Nullable: true, HasDefault: true, Default: c.def}, c.typ)
		if err != nil || got != c.want {
			t.Errorf("%s on %s: got %q err %v, want %q", c.def, c.typ, got, err, c.want)
		}
	}
	// 非日期目标列：同字面量保持普通引号字面量
	got, err := columnMods(ColumnMeta{Nullable: true, HasDefault: true, Default: "2024-01-02"}, "VARCHAR2(32 CHAR)")
	if err != nil || got != ` DEFAULT '2024-01-02'` {
		t.Errorf("varchar col must keep quoted literal: %q %v", got, err)
	}
	// 与 NOT NULL 组合：DEFAULT（含日期字面量）仍在前
	got, err = columnMods(ColumnMeta{Nullable: false, HasDefault: true, Default: "0001-01-01 00:00:00"}, "TIMESTAMP")
	if err != nil || got != ` DEFAULT TIMESTAMP '0001-01-01 00:00:00' NOT NULL` {
		t.Errorf("combined with NOT NULL: %q %v", got, err)
	}
}

// TestFitIdent 约束/索引名超限截断 + 短哈希（30/128 两档）。
func TestFitIdent(t *testing.T) {
	d30, d128 := oracle.NewWithIdentLimit(30), oracle.NewWithIdentLimit(128)
	name := "IDX_" + strings.Repeat("x", 60)
	got30, got128 := fitIdent(d30, name), fitIdent(d128, name)
	if len(got30) != 30 || len(got128) != 64 { // 128 > 64=4+60 → 原名
		t.Fatalf("fitIdent lengths: 30→%d, 128→%d", len(got30), len(got128))
	}
	if got128 != name {
		t.Errorf("fitIdent 128 must keep original, got %q", got128)
	}
	if got30[:26] != name[:26] {
		t.Errorf("fitIdent 30 must keep prefix, got %q", got30)
	}
	// 同名同哈希（确定性）；异名哈希大概率不同（碰撞不测）
	if fitIdent(d30, name) != got30 {
		t.Error("fitIdent must be deterministic")
	}
}
