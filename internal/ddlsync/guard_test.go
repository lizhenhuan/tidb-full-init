// guard_test.go：一致性守卫测试（#t9 首条，leader 门：两侧方言必须以
// 同一显式 maxIdentLen 构造，禁止默认 New() 对 30）。
//
// ddl-sync 生成的表就是数据管道随后要写的表：Convert 的逐列 Oracle
// 类型必须与管道 RenderCreateTable 完全一致（含 30/128 两档）。
// 断言方式：ddlsync CREATE 语句的每个列行必须以管道对应列行为前缀
// （管道只渲染 name+type；ddlsync 额外追加 NOT NULL/DEFAULT 修饰）。
package ddlsync

import (
	"regexp"
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// guardCorpus 守卫覆盖的列类型集（#t6 十一轮验收的核心映射面）。
var guardCorpus = []string{
	"varchar(32)", "varchar(2000)", "char(8)", "nvarchar(40)",
	"text", "mediumtext", "longtext", "tinytext",
	"tinyblob", "blob", "mediumblob", "longblob", "varbinary(64)", "binary(16)",
	"tinyint", "tinyint(1)", "smallint", "mediumint", "int", "int unsigned",
	"bigint", "bigint unsigned", "integer",
	"decimal(10,2)", "decimal(65,30)", "numeric", "float", "double",
	"date", "datetime", "datetime(3)", "timestamp", "timestamp(6)",
	"time", "time(3)", "year", "bit(1)", "bit(8)", "json",
	"bool", "boolean",
}

func guardMetas() (*model.TableMeta, *TableMetaFull) {
	pm := &model.TableMeta{Database: "shop", Table: "guard_t", Columns: []model.ColumnMeta{{Name: "c0", MySQLType: "bigint"}}, PKNames: []string{"c0"}}
	dm := &TableMetaFull{Database: "shop", Table: "guard_t", Charset: "utf8mb4", Columns: []ColumnMeta{{Name: "c0", MySQLType: "bigint", IsPK: true, Nullable: false}}, PKNames: []string{"c0"}}
	for i, t := range guardCorpus {
		name := "c" + string(rune('a'+i/26)) + string(rune('a'+i%26)) // caa…（≤30 稳）
		pm.Columns = append(pm.Columns, model.ColumnMeta{Name: name, MySQLType: t})
		dm.Columns = append(dm.Columns, ColumnMeta{Name: name, MySQLType: t, Nullable: true})
	}
	return pm, dm
}

// columnDefs 抽 CREATE TABLE 语句体里的列定义行（去尾逗号）。
func columnDefs(createSQL string) []string {
	body := createSQL[strings.Index(createSQL, "(\n")+2 : strings.LastIndex(createSQL, ")")]
	re := regexp.MustCompile(`(?m)^  "[^"]+" .+$`)
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if re.MatchString(line) {
			out = append(out, strings.TrimSuffix(strings.TrimRight(line, " "), ","))
		}
	}
	return out
}

func TestConsistencyGuardConvertMatchesPipeline(t *testing.T) {
	// leader 门：两侧同一显式 maxIdentLen（30 与 128 各跑一遍）。
	for _, n := range []int{30, 128} {
		pm, dm := guardMetas()
		pipe := oracle.NewWithIdentLimit(n)
		dls := oracle.NewWithIdentLimit(n)

		pipeSQL, err := pipe.RenderCreateTable(*pm)
		if err != nil {
			t.Fatalf("maxIdent=%d pipeline render: %v", n, err)
		}
		stmts, _, err := Convert(dm, dls, ConvertOptions{TargetSchema: "shop"})
		if err != nil {
			t.Fatalf("maxIdent=%d convert: %v", n, err)
		}
		var create string
		for _, s := range stmts {
			if s.Kind == "create" {
				create = s.SQL
			}
		}
		if create == "" {
			t.Fatalf("maxIdent=%d: no create statement", n)
		}

		pipeCols := columnDefs(pipeSQL)
		dlsCols := columnDefs(create)
		if len(pipeCols) != len(dlsCols) {
			t.Fatalf("maxIdent=%d: column count %d != %d\npipeline:\n%s\nconvert:\n%s",
				n, len(pipeCols), len(dlsCols), pipeSQL, create)
		}
		for i := range pipeCols {
			// 管道行 = `  "name" TYPE`；ddlsync 行 = 管道行 + 可选 mods。
			if !strings.HasPrefix(dlsCols[i], pipeCols[i]) {
				t.Errorf("maxIdent=%d col #%d diverged:\n pipeline: %s\n ddlsync:  %s",
					n, i, pipeCols[i], dlsCols[i])
			}
		}

		// PK 约束名同口径（同 maxIdent 下两侧必须一致）。
		if !strings.Contains(create, `"PK_guard_t"`) {
			t.Errorf("maxIdent=%d: pk constraint name mismatch in %q", n, create)
		}
	}
}

// TestConsistencyGuardUnsupportedTypesBothFail ENUM/SET 与未知类型
// 两条路径必须同时硬拒绝（不允许 DDL 侧「宽进」建出管道写不了的表）。
func TestConsistencyGuardUnsupportedTypesBothFail(t *testing.T) {
	for _, bad := range []string{"enum('a','b')", "set('x','y')", "point"} {
		pm := &model.TableMeta{Database: "shop", Table: "t", Columns: []model.ColumnMeta{
			{Name: "id", MySQLType: "bigint"}, {Name: "c", MySQLType: bad}}, PKNames: []string{"id"}}
		dm := &TableMetaFull{Database: "shop", Table: "t", Charset: "utf8mb4", Columns: []ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true}, {Name: "c", MySQLType: bad, Nullable: true}}, PKNames: []string{"id"}}

		for _, n := range []int{30, 128} {
			if _, err := oracle.NewWithIdentLimit(n).RenderCreateTable(*pm); err == nil {
				t.Errorf("type %s maxIdent=%d: pipeline should reject", bad, n)
			}
			if _, _, err := Convert(dm, oracle.NewWithIdentLimit(n), ConvertOptions{}); err == nil {
				t.Errorf("type %s maxIdent=%d: convert should reject", bad, n)
			}
		}
	}
}

// TestConsistencyGuardIdentifierLimit 标识符超限行为两侧同口径：
// maxIdent=30 + 40 字符表名 → 双方都拒绝；maxIdent=128 → 双方都通过。
func TestConsistencyGuardIdentifierLimit(t *testing.T) {
	long := strings.Repeat("t", 40)
	pm := &model.TableMeta{Database: "shop", Table: long, Columns: []model.ColumnMeta{
		{Name: "id", MySQLType: "bigint"}, {Name: "v", MySQLType: "varchar(10)"}}, PKNames: []string{"id"}}
	dm := &TableMetaFull{Database: "shop", Table: long, Charset: "utf8mb4", Columns: []ColumnMeta{
		{Name: "id", MySQLType: "bigint", IsPK: true}, {Name: "v", MySQLType: "varchar(10)", Nullable: true}}, PKNames: []string{"id"}}

	if _, err := oracle.NewWithIdentLimit(30).RenderCreateTable(*pm); err == nil {
		t.Error("maxIdent=30: pipeline should reject 40-char table")
	}
	if _, _, err := Convert(dm, oracle.NewWithIdentLimit(30), ConvertOptions{}); err == nil {
		t.Error("maxIdent=30: convert should reject 40-char table")
	}
	if _, err := oracle.NewWithIdentLimit(128).RenderCreateTable(*pm); err != nil {
		t.Errorf("maxIdent=128 pipeline: %v", err)
	}
	if _, _, err := Convert(dm, oracle.NewWithIdentLimit(128), ConvertOptions{}); err != nil {
		t.Errorf("maxIdent=128 convert: %v", err)
	}
}
