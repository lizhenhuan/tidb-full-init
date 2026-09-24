// compat_test.go：兼容项断言（ENUM/SET 列级、表级字符集、FK、分区）。
package ddlsync

import (
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
)

func TestCheckCompatEnumColumnLevel(t *testing.T) {
	m := &TableMetaFull{
		Database: "shop", Table: "t", Charset: "utf8mb4",
		Columns: []ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "color", MySQLType: "enum('r','g')", Nullable: true},
			{Name: "tags", MySQLType: "set('a','b')", Nullable: true},
			{Name: "ok", MySQLType: "varchar(8)", Nullable: true},
		},
		PKNames: []string{"id"},
	}
	items := CheckCompat(m, oracle.NewWithIdentLimit(128))
	if len(items) != 2 {
		t.Fatalf("want 2 DATA_TYPE items, got %+v", items)
	}
	for _, it := range items {
		if it.Level != "column" || it.PreCheckType != PreCheckDataType {
			t.Errorf("ENUM/SET must be column-level DATA_TYPE, got %+v", it)
		}
	}
	if !HasDataTypeIssue(items) {
		t.Error("HasDataTypeIssue must be true (→ failed)")
	}
	if HasTableCharsetIssue(items) {
		t.Error("HasTableCharsetIssue must be false")
	}
}

func TestCheckCompatTableCharsetSkips(t *testing.T) {
	m := &TableMetaFull{
		Database: "shop", Table: "t", Charset: "latin1",
		Columns: []ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "c", MySQLType: "enum('a')", Nullable: true}, // 表级早退：列级不再报
		},
		PKNames: []string{"id"},
	}
	items := CheckCompat(m, oracle.NewWithIdentLimit(30))
	if len(items) != 1 || items[0].Level != "table" || items[0].PreCheckType != PreCheckCharset {
		t.Fatalf("want single table-level CHARSET item, got %+v", items)
	}
	if !HasTableCharsetIssue(items) {
		t.Error("HasTableCharsetIssue must be true (→ skipped)")
	}
	if HasDataTypeIssue(items) {
		t.Error("table charset issue must not surface as DATA_TYPE")
	}
}

func TestCheckCompatCharsetValues(t *testing.T) {
	for cs, want := range map[string]string{"utf8": "AL32UTF8", "utf8mb4": "AL32UTF8", "gbk": "ZHS16GBK"} {
		if targetCharsetSupported[cs] != want {
			t.Errorf("charset %s: want %s got %s", cs, want, targetCharsetSupported[cs])
		}
	}
}

func TestCheckCompatFKAndPartitionRecordOnly(t *testing.T) {
	m := &TableMetaFull{
		Database: "shop", Table: "t", Charset: "utf8mb4",
		Columns:      []ColumnMeta{{Name: "id", MySQLType: "bigint", IsPK: true}, {Name: "pid", MySQLType: "bigint", Nullable: true}},
		PKNames:      []string{"id"},
		ForeignKeys:  []FKMeta{{Name: "fk_parent", Columns: []string{"pid"}, RefDatabase: "shop", RefTable: "parent", RefColumns: []string{"id"}}},
		PartitionExpr: "RANGE (id)",
	}
	items := CheckCompat(m, oracle.NewWithIdentLimit(30))
	var fk, part bool
	for _, it := range items {
		switch it.PreCheckType {
		case PreCheckFK:
			fk = true
			if !strings.Contains(it.SourceValue, "fk_parent") || !strings.Contains(it.Suggest, "未创建") {
				t.Errorf("FK item content: %+v", it)
			}
		case PreCheckPartition:
			part = true
			if !strings.Contains(it.Suggest, "未创建分区") {
				t.Errorf("partition item content: %+v", it)
			}
		}
	}
	if !fk || !part {
		t.Fatalf("FK/partition must both be recorded, fk=%v part=%v: %+v", fk, part, items)
	}
	// 记录型兼容项不改变执行判定
	if HasDataTypeIssue(items) || HasTableCharsetIssue(items) {
		t.Error("FK/partition items must not flip status")
	}
}

func TestCheckCompatColumnCharsetMismatch(t *testing.T) {
	m := &TableMetaFull{
		Database: "shop", Table: "t", Charset: "utf8mb4",
		Columns: []ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "g", MySQLType: "varchar(8)", Nullable: true, Charset: "gbk"},
			{Name: "lat", MySQLType: "varchar(8)", Nullable: true, Charset: "latin1"},
		},
		PKNames: []string{"id"},
	}
	items := CheckCompat(m, oracle.NewWithIdentLimit(30))
	// gbk 有映射 → 不报；latin1 无映射 → 列级 CHARSET
	if len(items) != 1 || items[0].Column != "lat" || items[0].PreCheckType != PreCheckCharset {
		t.Fatalf("want single column-level CHARSET for latin1, got %+v", items)
	}
	if HasTableCharsetIssue(items) {
		t.Error("column-level charset issue must not flip table skip")
	}
}
