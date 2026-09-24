// dmlgen 单元测试：消息形状契约（不依赖真 TiDB/Kafka）。
// 核心断言：dmlgen 组出的 canal-json 经 decoder.CanalJSON 解码后
// Op/After/Before 与预期一致——这是「增量驱动 → applier」链路的形状证明。
package dmlgen

import (
	"encoding/base64"
	"math/rand"
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/decoder"
	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

func testTableInfo() *encoder.TableInfo {
	return &encoder.TableInfo{
		Database: "db1", Table: "t1", PKNames: []string{"id"},
		Columns: []encoder.ColumnInfo{
			{ID: 1, Name: "id", MySQLType: "bigint(20)", IsPrimaryKey: true},
			{ID: 2, Name: "v", MySQLType: "varchar(255)"},
		},
	}
}

// TestBuildRowMessageRoundTrip 三种 DML 的 encoder→JSON→decoder 全链路。
func TestBuildRowMessageRoundTrip(t *testing.T) {
	info := testTableInfo()

	decode := func(t *testing.T, m *dmlMsg) *model.RowEvent {
		t.Helper()
		payload, err := m.msg.ToJSON()
		if err != nil {
			t.Fatalf("ToJSON: %v", err)
		}
		ev, err := decoder.CanalJSON(payload)
		if err != nil {
			t.Fatalf("decode: %v\npayload: %s", err, payload)
		}
		re, ok := ev.(*model.RowEvent)
		if !ok {
			t.Fatalf("want RowEvent, got %T", ev)
		}
		return re
	}

	// INSERT：Data=新整行，Old=nil
	ins := buildRowMessage("INSERT", info, map[string]interface{}{
		"id": int64(7), "v": "alice",
	}, nil, nil)
	if ins.key != "7" {
		t.Errorf("insert key = %q, want 7", ins.key)
	}
	ev := decode(t, ins)
	if ev.Op != model.OpInsert {
		t.Errorf("op = %q, want INSERT", ev.Op)
	}
	if ev.After["id"] != "7" || ev.After["v"] != "alice" {
		t.Errorf("After = %+v", ev.After)
	}
	if ev.Before != nil {
		t.Errorf("Before = %+v, want nil", ev.Before)
	}

	// UPDATE：Data=改后整行，Old=仅变更列旧值（TiCDC 形状）
	vCol := info.Columns[1]
	upd := buildRowMessage("UPDATE", info, map[string]interface{}{
		"id": int64(7), "v": "bob",
	}, &vCol, "alice")
	if upd.key != "7" {
		t.Errorf("update key = %q, want 7", upd.key)
	}
	ev = decode(t, upd)
	if ev.Op != model.OpUpdate {
		t.Errorf("op = %q, want UPDATE", ev.Op)
	}
	if ev.After["v"] != "bob" || ev.After["id"] != "7" {
		t.Errorf("After = %+v", ev.After)
	}
	if len(ev.Before) != 1 || ev.Before["v"] != "alice" {
		t.Errorf("Before = %+v, want {v:alice} 仅变更列", ev.Before)
	}

	// DELETE：Data=被删整行，Old=nil
	del := buildRowMessage("DELETE", info, map[string]interface{}{
		"id": int64(7), "v": "bob",
	}, nil, nil)
	ev = decode(t, del)
	if ev.Op != model.OpDelete {
		t.Errorf("op = %q, want DELETE", ev.Op)
	}
	if ev.After["id"] != "7" {
		t.Errorf("After = %+v（DELETE Data=被删整行）", ev.After)
	}
	if ev.Before != nil {
		t.Errorf("Before = %+v, want nil", ev.Before)
	}
}

// TestBuildRowMessageBinaryBase64 二进制列走 base64（与 snapshot 全量
// 路径同口径，FormatValue），不是原始字节串。
func TestBuildRowMessageBinaryBase64(t *testing.T) {
	info := &encoder.TableInfo{
		Database: "db1", Table: "t1", PKNames: []string{"id"},
		Columns: []encoder.ColumnInfo{
			{ID: 1, Name: "id", MySQLType: "bigint(20)", IsPrimaryKey: true},
			{ID: 2, Name: "b", MySQLType: "varbinary(16)"},
		},
	}
	raw := []byte{0x01, 0x02, 0xff, 0xfe}
	m := buildRowMessage("INSERT", info, map[string]interface{}{
		"id": int64(1), "b": raw,
	}, nil, nil)
	if got := m.msg.Data[0]["b"]; got != base64.StdEncoding.EncodeToString(raw) {
		t.Errorf("binary value = %q, want base64 %q", got, base64.StdEncoding.EncodeToString(raw))
	}
}

// TestParseSchemaFromDSN DSN 库名提取。
func TestParseSchemaFromDSN(t *testing.T) {
	cases := []struct {
		dsn, want string
	}{
		{"root:pw@tcp(1.2.3.4:4000)/shop?parseTime=true", "shop"},
		{"root:pw@tcp(1.2.3.4:4000)/shop", "shop"},
		{"root:pw@tcp(1.2.3.4:4000)/", "fallback"},
	}
	for _, c := range cases {
		if got := parseSchemaFromDSN("fallback", c.dsn); got != c.want {
			t.Errorf("parseSchemaFromDSN(%q) = %q, want %q", c.dsn, got, c.want)
		}
	}
}

// TestGenValueSmoke 各类型生成值可序列化、宽度不超限。
func TestGenValueSmoke(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	types := []string{
		"int(11)", "bigint(20) unsigned", "decimal(10,2)", "float",
		"varchar(10)", "char(8)", "text", "datetime", "timestamp",
		"date", "time", "year(4)", "varbinary(8)", "json",
		"enum('a','b')", "tinyint(1)",
	}
	for _, mt := range types {
		for i := 0; i < 20; i++ {
			v := genValue(mt, rng)
			s := encoder.FormatValue(mt, v)
			// 宽度约束只对字符串/二进制类型断言（数值 (N) 是显示宽度）；
			// 二进制比较解码后字节数（base64 会膨胀 4/3）
			if strings.HasSuffix(mt, "char(10)") || strings.HasSuffix(mt, "char(8)") {
				if w := widthOf(mt); w > 0 && len(s) > w {
					t.Errorf("genValue(%q) = %q exceeds width %d", mt, s, w)
				}
			}
			if strings.HasSuffix(mt, "binary(8)") {
				if raw, err := base64.StdEncoding.DecodeString(s); err == nil && len(raw) > 8 {
					t.Errorf("genValue(%q) = %d bytes, exceeds 8", mt, len(raw))
				}
			}
		}
	}
}

// TestMutateValueChanged mutateValue 必须产出可区分的新值。
func TestMutateValueChanged(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	col := encoder.ColumnInfo{Name: "v", MySQLType: "varchar(255)"}
	old := "same"
	for i := 0; i < 10; i++ {
		nv := mutateValue(col, old, rng)
		if encoder.FormatValue(col.MySQLType, nv) == old {
			t.Fatalf("mutateValue returned identical value %q", old)
		}
	}
}
