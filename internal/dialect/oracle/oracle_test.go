package oracle_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect"
	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

func TestMapTypeMatrix(t *testing.T) {
	d := oracle.New()
	tests := []struct {
		mysql string
		want  string
	}{
		{"tinyint", "NUMBER(3)"},
		{"tinyint unsigned", "NUMBER(4)"},
		{"bool", "NUMBER(3)"},
		{"boolean", "NUMBER(3)"},
		{"smallint", "NUMBER(5)"},
		{"smallint unsigned", "NUMBER(6)"},
		{"mediumint", "NUMBER(8)"},
		{"int", "NUMBER(10)"},
		{"integer", "NUMBER(10)"},
		{"int unsigned", "NUMBER(11)"},
		{"bigint", "NUMBER(19)"},
		{"bigint unsigned", "NUMBER(20)"},
		{"bit", "NUMBER"},
		{"bit(8)", "NUMBER"},
		{"decimal(10,2)", "NUMBER(10,2)"},
		{"decimal", "NUMBER"},
		{"decimal(65,30)", "NUMBER(38,30)"}, // 超精度截断
		{"numeric(5)", "NUMBER(5,0)"},
		{"float", "BINARY_FLOAT"},
		{"double", "BINARY_DOUBLE"},
		{"real", "BINARY_DOUBLE"},
		{"varchar(255)", "VARCHAR2(255 CHAR)"},
		{"char(10)", "VARCHAR2(10 CHAR)"},
		{"varchar(2000)", "CLOB"}, // 2000*4 > 4000
		{"varchar(1000)", "VARCHAR2(1000 CHAR)"},
		{"text", "CLOB"},
		{"mediumtext", "CLOB"},
		{"longtext", "CLOB"},
		{"varbinary(100)", "RAW(100)"},
		{"binary(16)", "RAW(16)"},
		{"blob", "BLOB"},
		{"longblob", "BLOB"},
		{"date", "DATE"},
		{"datetime", "TIMESTAMP"},
		{"datetime(6)", "TIMESTAMP"},
		{"timestamp", "TIMESTAMP"},
		{"time", "VARCHAR2(18)"},
		{"time(6)", "VARCHAR2(18)"},
		{"year", "NUMBER(4)"},
		{"json", "CLOB"},
	}
	for _, tt := range tests {
		t.Run(tt.mysql, func(t *testing.T) {
			got, err := d.MapType("t", "c", tt.mysql)
			if err != nil {
				t.Fatalf("MapType(%q): %v", tt.mysql, err)
			}
			if got != tt.want {
				t.Errorf("MapType(%q) = %q, want %q", tt.mysql, got, tt.want)
			}
		})
	}
}

func TestMapTypeUnsupported(t *testing.T) {
	d := oracle.New()
	for _, mt := range []string{"enum('a','b','c')", "set('x','y')", "geometry", "varbinary(5000)"} {
		t.Run(mt, func(t *testing.T) {
			_, err := d.MapType("t", "c", mt)
			if err == nil {
				t.Fatalf("MapType(%q) should fail", mt)
			}
			var unsupported *oracle.ErrUnsupportedType
			if !errors.As(err, &unsupported) {
				t.Errorf("expect ErrUnsupportedType, got %T: %v", err, err)
			}
		})
	}
}

func TestValidateTable(t *testing.T) {
	d := oracle.New()
	base := func() model.TableMeta {
		return model.TableMeta{
			Database: "shop", Table: "orders", PKNames: []string{"id"},
			Columns: []model.ColumnMeta{
				{Name: "id", MySQLType: "bigint", SQLType: -5, IsPK: true},
				{Name: "memo", MySQLType: "text", SQLType: 2005},
			},
		}
	}
	if err := d.ValidateTable(base()); err != nil {
		t.Fatalf("valid table rejected: %v", err)
	}

	noPK := base()
	noPK.PKNames = nil
	if err := d.ValidateTable(noPK); err == nil {
		t.Error("table without PK should be rejected")
	}

	withEnum := base()
	withEnum.Columns = append(withEnum.Columns, model.ColumnMeta{Name: "st", MySQLType: "enum('a','b')"})
	if err := d.ValidateTable(withEnum); err == nil {
		t.Error("enum column should be rejected")
	}

	longName := base()
	longName.Table = strings.Repeat("x", 31)
	if err := d.ValidateTable(longName); err == nil {
		t.Error(">30 byte identifier should be rejected")
	}
}

func TestQuoteIdentifierInjection(t *testing.T) {
	d := oracle.New()
	tests := []struct {
		in, want string
	}{
		{"id", `"id"`},
		{"order", `"order"`},                         // 保留字 → 引号安全
		{`weird"col`, `"weird""col"`},                // 内嵌引号 → 转义，无法逃逸
		{`x"); DROP TABLE t; --`, `"x""); DROP TABLE t; --"`},
	}
	for _, tt := range tests {
		got := d.QuoteIdentifier(tt.in)
		if got != tt.want {
			t.Errorf("QuoteIdentifier(%q) = %s, want %s", tt.in, got, tt.want)
		}
	}
	if !oracle.IsReserved("SELECT") || oracle.IsReserved("id") {
		t.Error("reserved word table broken")
	}
}

func testMeta() model.TableMeta {
	return model.TableMeta{
		Database: "shop", Table: "orders", PKNames: []string{"id"},
		Columns: []model.ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "amount", MySQLType: "decimal(10,2)"},
			{Name: "memo", MySQLType: "text"},
		},
	}
}

func TestRenderCreateTable(t *testing.T) {
	d := oracle.New()
	ddl, err := d.RenderCreateTable(testMeta())
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`CREATE TABLE "shop"."orders"`,
		`"id" NUMBER(19)`,
		`"amount" NUMBER(10,2)`,
		`"memo" CLOB`,
		`PRIMARY KEY ("id")`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("ddl missing %q:\n%s", want, ddl)
		}
	}
}

func TestRenderUpsert(t *testing.T) {
	d := oracle.New()
	sqlStr, err := d.RenderUpsert(testMeta())
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		`MERGE INTO "shop"."orders" t`,
		"WHEN MATCHED THEN UPDATE SET",
		"WHEN NOT MATCHED THEN INSERT",
		"t.\"id\" = s.\"id\"",
		":id",
		":amount",
		":memo",
	} {
		if !strings.Contains(sqlStr, want) {
			t.Errorf("upsert missing %q:\n%s", want, sqlStr)
		}
	}
	// UPDATE SET 不得包含 PK 自身
	if strings.Contains(sqlStr, `t."id" = s."id",`) {
		t.Errorf("upsert should not set PK column:\n%s", sqlStr)
	}
}

func TestRenderDelete(t *testing.T) {
	d := oracle.New()
	sqlStr, err := d.RenderDelete(testMeta())
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	want := `DELETE FROM "shop"."orders" WHERE "id" = :id`
	if sqlStr != want {
		t.Errorf("delete = %s, want %s", sqlStr, want)
	}

	composite := testMeta()
	composite.PKNames = []string{"a", "b"}
	sqlStr, _ = d.RenderDelete(composite)
	if !strings.Contains(sqlStr, `"a" = :a AND "b" = :b`) {
		t.Errorf("composite PK delete wrong: %s", sqlStr)
	}
}

func TestRenderAlterTableNotImplemented(t *testing.T) {
	d := oracle.New()
	_, err := d.RenderAlterTable(testMeta(), dialect.SchemaChange{})
	if err != dialect.ErrNotImplemented {
		t.Errorf("expect ErrNotImplemented, got %v", err)
	}
}
