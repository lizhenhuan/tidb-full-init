package snapshot

// 行键主键解码单测（#t6 复审 H1）：聚簇复合主键（int+varchar 等）、
// 负数 int handle、二进制列 base64、不支持类型的 fail-loud。

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/tikv"
)

// --- 测试用编码器：与 TiDB util/codec 键格式对齐 ---

func encKeyInt(v int64) []byte {
	return tikv.EncodeInt([]byte{flagInt}, v)
}

// encKeyBytes 复刻 codec.EncodeBytes：flag + 8 字节分组 + 尾随 marker
// （= 9-有效字节数），恰 8 字节整倍数追加空组收尾。
func encKeyBytes(s string) []byte {
	b := []byte{flagBytes}
	vals := []byte(s)
	for {
		var buf [8]byte
		n := copy(buf[:], vals)
		vals = vals[n:]
		b = append(b, buf[:]...)
		b = append(b, byte(8-n+1))
		if n < 8 {
			return b
		}
	}
}

func rowKey(suffix []byte) []byte {
	return append(tikv.GenTableRecordPrefix(42), suffix...)
}

func ti(columns []encoder.ColumnInfo, pkNames ...string) *encoder.TableInfo {
	for i := range columns {
		for _, pk := range pkNames {
			if columns[i].Name == pk {
				columns[i].IsPrimaryKey = true
			}
		}
	}
	return &encoder.TableInfo{
		Database: "db", Table: "t",
		Columns: columns, PKNames: pkNames,
	}
}

// --- fillPKFromKey ---

// H1 主场景：复合 int 主键聚簇表（part_range(id, region_id) 形态）。
func TestFillPKFromKeyCompositeInt(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint"},
		{Name: "region_id", MySQLType: "int"},
		{Name: "amount", MySQLType: "decimal(10,2)"},
	}, "id", "region_id")
	key := rowKey(append(encKeyInt(42), encKeyInt(7)...))

	row := map[string]string{"amount": "1.23"}
	if err := fillPKFromKey(key, info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if row["id"] != "42" || row["region_id"] != "7" {
		t.Fatalf("pk = id:%q region_id:%q, want 42/7", row["id"], row["region_id"])
	}
}

// varchar + int 复合主键（非整数列在前）。
func TestFillPKFromKeyVarcharInt(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "name", MySQLType: "varchar(32)"},
		{Name: "seq", MySQLType: "int"},
	}, "name", "seq")
	key := rowKey(append(encKeyBytes("alice"), encKeyInt(-3)...))

	row := map[string]string{"seq": "-3"} // seq 已在 value（不缺失），name 回填
	if err := fillPKFromKey(key, info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if row["name"] != "alice" {
		t.Fatalf("name = %q, want alice", row["name"])
	}
}

// 单列整数主键：int handle 路径，含负值（旧实现 handle>0 会静默跳过）。
func TestFillPKFromKeyIntHandleNegative(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint"},
		{Name: "v", MySQLType: "int"},
	}, "id")
	key := rowKey(tikv.EncodeInt(nil, -5))

	row := map[string]string{"v": "1"}
	if err := fillPKFromKey(key, info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if row["id"] != "-5" {
		t.Fatalf("id = %q, want -5", row["id"])
	}
}

// binary 主键按 rowcodec 同口径 base64。
func TestFillPKFromKeyBinaryBase64(t *testing.T) {
	raw := "\x01\x02\xff"
	info := ti([]encoder.ColumnInfo{
		{Name: "b", MySQLType: "binary(8)"},
		{Name: "v", MySQLType: "int"},
	}, "b")
	key := rowKey(encKeyBytes(raw))

	row := map[string]string{"v": "1"}
	if err := fillPKFromKey(key, info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if want := base64.StdEncoding.EncodeToString([]byte(raw)); row["b"] != want {
		t.Fatalf("b = %q, want %q", row["b"], want)
	}
}

// 非聚簇表（主键列在 value 已有）：不做任何事。
func TestFillPKFromKeyNoMissing(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint"},
		{Name: "v", MySQLType: "int"},
	}, "id")
	row := map[string]string{"id": "999", "v": "1"}
	if err := fillPKFromKey(rowKey(tikv.EncodeInt(nil, 1)), info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if row["id"] != "999" {
		t.Fatalf("id overwritten: %q", row["id"])
	}
}

// 空字符串主键。
func TestFillPKFromKeyEmptyStringPK(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "s", MySQLType: "varchar(8)"},
		{Name: "v", MySQLType: "int"},
	}, "s")
	row := map[string]string{"v": "1"}
	if err := fillPKFromKey(rowKey(encKeyBytes("")), info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if row["s"] != "" {
		t.Fatalf("s = %q, want empty", row["s"])
	}
}

// 8 字节整倍数字符串主键：空组收尾必须正确停住（多列连排自终结）。
func TestFillPKFromKeyExact8ByteString(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "s", MySQLType: "varchar(8)"},
		{Name: "n", MySQLType: "int"},
	}, "s", "n")
	key := rowKey(append(encKeyBytes("abcdefgh"), encKeyInt(5)...))
	row := map[string]string{}
	if err := fillPKFromKey(key, info, row); err != nil {
		t.Fatalf("fill: %v", err)
	}
	if row["s"] != "abcdefgh" || row["n"] != "5" {
		t.Fatalf("got s=%q n=%q, want abcdefgh/5", row["s"], row["n"])
	}
}

// 不支持的 PK 类型（decimal flag 6）：必须报错，不得静默。
func TestFillPKFromKeyUnsupportedFlagFails(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "d", MySQLType: "decimal(10,2)"},
		{Name: "v", MySQLType: "int"},
	}, "d")
	suffix := append([]byte{flagBytes + 5 /* =6 decimal */}, 1, 2, 3)
	err := fillPKFromKey(rowKey(suffix), info, map[string]string{"v": "1"})
	if err == nil {
		t.Fatal("unsupported flag must error (fail-loud, no silent row drop)")
	}
	if !strings.Contains(err.Error(), "unsupported key codec flag") {
		t.Fatalf("err = %v, want unsupported-flag message", err)
	}
}

// 截断载荷：报错而非 panic/错值。
func TestFillPKFromKeyTruncatedPayload(t *testing.T) {
	info := ti([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint"},
		{Name: "id2", MySQLType: "bigint"},
	}, "id", "id2")
	if err := fillPKFromKey(rowKey([]byte{flagInt, 1, 2}), info, map[string]string{}); err == nil {
		t.Fatal("truncated payload must error")
	}
}
