package decoder_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/decoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

func loadGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return b
}

// TestCanalJSONGolden 覆盖 TiCDC 官方样例形态与本仓库全量快照消息。
func TestCanalJSONGolden(t *testing.T) {
	tests := []struct {
		name      string
		file      string
		op        model.OpType
		db, table string
		after     map[string]string
		before    map[string]string
		commitTs  uint64
		pkChanged bool
	}{
		{
			name: "TiCDC INSERT", file: "ticdc_insert.json", op: model.OpInsert,
			db: "test", table: "tp_int",
			after:    map[string]string{"id": "1", "ti_tinyint": "1", "ti_unsigned": "65535"},
			commitTs: 16396247046269759,
		},
		{
			name: "TiCDC UPDATE 普通列变更（old 仅含变更列）", file: "ticdc_update.json", op: model.OpUpdate,
			db: "test", table: "tp_int",
			after: map[string]string{"id": "1", "ti_tinyint": "2"},
			before: map[string]string{"ti_tinyint": "1"}, commitTs: 16396247046269760,
		},
		{
			name: "TiCDC UPDATE 主键变更（D2: 拆 DELETE+MERGE）", file: "ticdc_update_pk_change.json", op: model.OpUpdate,
			db: "test", table: "tp_int",
			after: map[string]string{"id": "2", "ti_tinyint": "1"},
			before: map[string]string{"id": "1"}, commitTs: 16396247046269761,
			pkChanged: true,
		},
		{
			name: "TiCDC DELETE（data 为被删行，old 为 null）", file: "ticdc_delete.json", op: model.OpDelete,
			db: "test", table: "tp_int",
			after: map[string]string{"id": "1"}, commitTs: 16396247046269762,
		},
		{
			name: "全量快照 INSERT（commitTs=0，memo 为 JSON null → 空串）", file: "snapshot_insert.json", op: model.OpInsert,
			db: "shop", table: "orders",
			after: map[string]string{"order_id": "42", "amount": "12.34", "memo": "", "payload": "aGVsbG8="},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evAny, err := decoder.CanalJSON(loadGolden(t, tt.file))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			row, ok := evAny.(*model.RowEvent)
			if !ok {
				t.Fatalf("expect *RowEvent, got %T", evAny)
			}
			if row.Op != tt.op {
				t.Errorf("op = %s, want %s", row.Op, tt.op)
			}
			if row.Database != tt.db || row.Table != tt.table {
				t.Errorf("db.table = %s.%s, want %s.%s", row.Database, row.Table, tt.db, tt.table)
			}
			if row.CommitTs != tt.commitTs {
				t.Errorf("commitTs = %d, want %d", row.CommitTs, tt.commitTs)
			}
			for k, want := range tt.after {
				if got := row.After[k]; got != want {
					t.Errorf("after[%q] = %q, want %q", k, got, want)
				}
			}
			if len(row.After) != len(tt.after) {
				t.Errorf("after has %d cols, want %d: %v", len(row.After), len(tt.after), row.After)
			}
			for k, want := range tt.before {
				if got := row.Before[k]; got != want {
					t.Errorf("before[%q] = %q, want %q", k, got, want)
				}
			}
			if got := row.PKChanged(); got != tt.pkChanged {
				t.Errorf("PKChanged = %v, want %v", got, tt.pkChanged)
			}
		})
	}
}

func TestCanalJSONDDL(t *testing.T) {
	evAny, err := decoder.CanalJSON(loadGolden(t, "ticdc_ddl.json"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	ddl, ok := evAny.(*model.DDLEvent)
	if !ok {
		t.Fatalf("expect *DDLEvent, got %T", evAny)
	}
	if ddl.Database != "test" || ddl.SQL != "alter table tp_int add column c4 int" {
		t.Errorf("unexpected ddl event: %+v", ddl)
	}
	if ddl.CommitTs != 16396247046269763 {
		t.Errorf("commitTs = %d", ddl.CommitTs)
	}
}

func TestCanalJSONErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"malformed json", `{"type":`},
		{"missing database", `{"table":"t","type":"INSERT","pkNames":["id"],"data":[{"id":"1"}]}`},
		{"unsupported type", `{"database":"d","table":"t","pkNames":["id"],"type":"TRUNCATE","data":[{"id":"1"}]}`},
		{"no pkNames", `{"database":"d","table":"t","pkNames":null,"type":"INSERT","data":[{"id":"1"}]}`},
		{"zero rows", `{"database":"d","table":"t","pkNames":["id"],"type":"INSERT","data":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decoder.CanalJSON([]byte(tt.input)); err == nil {
				t.Fatalf("expect error for %s", tt.name)
			}
		})
	}
}

// FuzzCanalJSON L1.4：任意输入不 panic、不死循环（CI 定期跑 -fuzz）。
func FuzzCanalJSON(f *testing.F) {
	for _, golden := range []string{
		"ticdc_insert.json", "ticdc_update.json", "ticdc_update_pk_change.json",
		"ticdc_delete.json", "ticdc_ddl.json", "snapshot_insert.json",
	} {
		f.Add(string(mustRead(f, golden)))
	}
	f.Add(`{"type":`)
	f.Add(`{"data":[{"a":null}],"type":"INSERT","database":"d","table":"t","pkNames":["a"]}`)
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = decoder.CanalJSON([]byte(input))
	})
}

func mustRead(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestPKChangedSemantics 直接验证 D2 主键变更判定规则：
// old 仅含变更列，未出现在 old 中的 PK 视为未变更。
func TestPKChangedSemantics(t *testing.T) {
	tests := []struct {
		name   string
		op     model.OpType
		before map[string]string
		want   bool
	}{
		{"old 不含 PK 列 = 未变更", model.OpUpdate, map[string]string{"c": "1"}, false},
		{"old 含 PK 且值不同", model.OpUpdate, map[string]string{"id": "1"}, true},
		{"old 含 PK 且值相同", model.OpUpdate, map[string]string{"id": "9"}, false},
		{"INSERT 恒为 false", model.OpInsert, map[string]string{"id": "1"}, false},
		{"DELETE 恒为 false", model.OpDelete, map[string]string{"id": "1"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &model.RowEvent{
				Op: tt.op, PKNames: []string{"id"},
				Before: tt.before, After: map[string]string{"id": "9", "c": "2"},
			}
			if got := ev.PKChanged(); got != tt.want {
				t.Errorf("PKChanged = %v, want %v", got, tt.want)
			}
		})
	}
}
