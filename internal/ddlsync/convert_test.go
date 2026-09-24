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
			"  ,CONSTRAINT \"UK_uk_order\" UNIQUE (\"order_no\")\n" +
			")",
		"index | CREATE INDEX \"IDX_idx_status\" ON \"shop\".\"orders\" (\"status\")",
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
	for _, keep := range []string{`"order_no" VARCHAR2(32 CHAR)`, `"PK_orders"`, `UK_uk_order`, `IDX_idx_status`} {
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
