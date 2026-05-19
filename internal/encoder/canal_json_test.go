package encoder_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

func TestNewInsertMessage(t *testing.T) {
	info := &encoder.TableInfo{
		Database: "testdb",
		Table:    "users",
		PKNames:  []string{"id"},
		Columns: []encoder.ColumnInfo{
			{Name: "id", MySQLType: "int", IsPrimaryKey: true},
			{Name: "name", MySQLType: "varchar"},
			{Name: "age", MySQLType: "int"},
		},
	}

	msg := encoder.NewInsertMessage(info)
	msg.BuildTypeMapping(info.Columns)

	if msg.Database != "testdb" {
		t.Errorf("expected database testdb, got %s", msg.Database)
	}
	if msg.Table != "users" {
		t.Errorf("expected table users, got %s", msg.Table)
	}
	if msg.Type != "INSERT" {
		t.Errorf("expected type INSERT, got %s", msg.Type)
	}
	if msg.IsDDL {
		t.Error("expected IsDDL to be false")
	}
	if msg.TiDB == nil {
		t.Error("expected _tidb extension to be present")
	}
	if msg.TiDB.CommitTs != 0 {
		t.Errorf("expected commitTs=0 for snapshot, got %d", msg.TiDB.CommitTs)
	}
	if msg.Old != nil {
		t.Error("expected old to be null for INSERT")
	}

	msg.SetRowData(map[string]string{"id": "1", "name": "Alice", "age": "30"})
	data, err := msg.ToJSON()
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	mysqlType, ok := parsed["mysqlType"].(map[string]interface{})
	if !ok {
		t.Fatal("mysqlType should be a map")
	}
	if mysqlType["id"] != "int" {
		t.Errorf("expected mysqlType.id=int, got %v", mysqlType["id"])
	}
	if mysqlType["name"] != "varchar" {
		t.Errorf("expected mysqlType.name=varchar, got %v", mysqlType["name"])
	}

	sqlType, ok := parsed["sqlType"].(map[string]interface{})
	if !ok {
		t.Fatal("sqlType should be a map")
	}
	if sqlType["id"] != float64(4) {
		t.Errorf("expected sqlType.id=4, got %v", sqlType["id"])
	}
	if sqlType["name"] != float64(12) {
		t.Errorf("expected sqlType.name=12, got %v", sqlType["name"])
	}

	tidb, ok := parsed["_tidb"].(map[string]interface{})
	if !ok {
		t.Fatal("_tidb extension missing")
	}
	if tidb["commitTs"] != float64(0) {
		t.Errorf("expected _tidb.commitTs=0, got %v", tidb["commitTs"])
	}
}

func TestMapCanalJSONType(t *testing.T) {
	tests := []struct {
		col      encoder.ColumnInfo
		wantMT   string
		wantCode int
	}{
		{encoder.ColumnInfo{Name: "a", MySQLType: "tinyint"}, "tinyint", -6},
		{encoder.ColumnInfo{Name: "a", MySQLType: "smallint"}, "smallint", 5},
		{encoder.ColumnInfo{Name: "a", MySQLType: "int"}, "int", 4},
		{encoder.ColumnInfo{Name: "a", MySQLType: "bigint"}, "bigint", -5},
		{encoder.ColumnInfo{Name: "a", MySQLType: "bigint", IsUnsigned: true}, "bigint unsigned", -5},
		{encoder.ColumnInfo{Name: "a", MySQLType: "float"}, "float", 7},
		{encoder.ColumnInfo{Name: "a", MySQLType: "double"}, "double", 8},
		{encoder.ColumnInfo{Name: "a", MySQLType: "decimal"}, "decimal", 3},
		{encoder.ColumnInfo{Name: "a", MySQLType: "varchar"}, "varchar", 12},
		{encoder.ColumnInfo{Name: "a", MySQLType: "char"}, "char", 1},
		{encoder.ColumnInfo{Name: "a", MySQLType: "text"}, "text", 2005},
		{encoder.ColumnInfo{Name: "a", MySQLType: "blob"}, "blob", 2004},
		{encoder.ColumnInfo{Name: "a", MySQLType: "date"}, "date", 91},
		{encoder.ColumnInfo{Name: "a", MySQLType: "time"}, "time", 92},
		{encoder.ColumnInfo{Name: "a", MySQLType: "datetime"}, "datetime", 93},
		{encoder.ColumnInfo{Name: "a", MySQLType: "timestamp"}, "timestamp", 93},
		{encoder.ColumnInfo{Name: "a", MySQLType: "year"}, "year", 12},
		{encoder.ColumnInfo{Name: "a", MySQLType: "enum"}, "enum", 4},
		{encoder.ColumnInfo{Name: "a", MySQLType: "set"}, "set", -7},
		{encoder.ColumnInfo{Name: "a", MySQLType: "json"}, "json", 12},
		{encoder.ColumnInfo{Name: "a", MySQLType: "bool"}, "tinyint", -6},
		{encoder.ColumnInfo{Name: "a", MySQLType: "boolean"}, "tinyint", -6},
		{encoder.ColumnInfo{Name: "a", MySQLType: "mediumint"}, "mediumint", 4},
		{encoder.ColumnInfo{Name: "a", MySQLType: "unknown_type"}, "unknown_type", 12},
	}

	for _, tt := range tests {
		t.Run(tt.col.MySQLType, func(t *testing.T) {
			gotMT, gotCode := encoder.MapCanalJSONType(tt.col)
			if gotMT != tt.wantMT {
				t.Errorf("mysqlType: got %q, want %q", gotMT, tt.wantMT)
			}
			if gotCode != tt.wantCode {
				t.Errorf("sqlType: got %d, want %d", gotCode, tt.wantCode)
			}
		})
	}
}

func TestNewDDLMessage(t *testing.T) {
	msg := encoder.NewDDLMessage("testdb", "ALTER TABLE users ADD COLUMN age INT")

	if !msg.IsDDL {
		t.Error("expected IsDDL to be true")
	}
	if msg.Type != "QUERY" {
		t.Errorf("expected type QUERY, got %s", msg.Type)
	}
	if msg.Table != "" {
		t.Errorf("expected empty table, got %s", msg.Table)
	}
	if msg.SQL != "ALTER TABLE users ADD COLUMN age INT" {
		t.Errorf("unexpected SQL: %s", msg.SQL)
	}

	data, err := msg.ToJSON()
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if !strings.Contains(string(data), `"isDdl":true`) {
		t.Error("DDL message should have isDdl:true")
	}
}

func TestEncodeBinaryValue(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{"tab", []byte{'\t'}, `\t`},
		{"newline", []byte{'\n'}, `\n`},
		{"carriage_return", []byte{'\r'}, `\r`},
		{"ampersand", []byte{'&'}, `\u0026`},
		{"less_than", []byte{'<'}, `\u003c`},
		{"greater_than", []byte{'>'}, `\u003e`},
		{"control_char", []byte{0x05}, `\u0005`},
		{"printable", []byte("hello"), "hello"},
		{"mixed", []byte{'a', '\n', 'b'}, "a\\nb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encoder.EncodeBinaryValue(tt.input)
			// Note: want values use Go string representation where \\n = backslash + n
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatValue(t *testing.T) {
	val := encoder.FormatValue("int", int64(42))
	if val != "42" {
		t.Errorf("expected '42', got %q", val)
	}

	val = encoder.FormatValue("varchar", "hello")
	if val != "hello" {
		t.Errorf("expected 'hello', got %q", val)
	}

	val = encoder.FormatValue("int", nil)
	if val != "" {
		t.Errorf("expected empty string for nil, got %q", val)
	}

	val = encoder.FormatValue("blob", []byte{0x00, 0x01})
	if !strings.Contains(val, `\u0000`) {
		t.Errorf("binary encoding should escape control chars: %q", val)
	}
}
