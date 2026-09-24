package snapshot

// rowcodec 新格式 ground-truth 回归（#t6 复审 J1）。
// 编码器按 TiDB v8.5 pkg/util/rowcodec（encoder.go/row.go）忠实复刻：
//   VER|FLAGS|NOT_NULL_CNT(2LE)|NULL_CNT(2LE)|NOT_NULL_IDS|NULL_IDS|
//   NOT_NULL_OFFSETS(2LE, END 偏移)|DATA|CHECKSUM?
// 关键语义：offsets 只数列数据（不含 checksum 尾巴，row.go fromBytes
// data=rowData[cursor:cursor+lastOffset]）；空串列数据 0 字节
// （start==end）；NULL 列只在 NULL_IDS 里出现、无数据。
// J1 现场：wform 2000 行 F_Description=''（空串列丢）、base ts-0001/2/3
//（NULL 列丢）→ applier "event missing column"。

import (
	"encoding/binary"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

type encCol struct {
	id    int64
	value []byte // nil = NULL 列
}

// encodeRowV2 按真实 TiDB 语义编码一行（small 模式，colID<256）。
// withChecksum 时追加 1B header(ver=2) + 4B LE CRC 占位（解码器不校验，
// 只要求不泄漏进数据）。
func encodeRowV2(withChecksum bool, cols ...encCol) []byte {
	var notNull, null []encCol
	for _, c := range cols {
		if c.value == nil {
			null = append(null, c)
		} else {
			notNull = append(notNull, c)
		}
	}
	// 编码器按 colID 升序排（reformatCols 的 sorter）
	sortEnc := func(s []encCol) {
		for i := 1; i < len(s); i++ {
			for j := i; j > 0 && s[j].id < s[j-1].id; j-- {
				s[j], s[j-1] = s[j-1], s[j]
			}
		}
	}
	sortEnc(notNull)
	sortEnc(null)

	var out []byte
	out = append(out, 128) // CodecVer
	var flags byte
	if withChecksum {
		flags |= rowFlagChecksum
	}
	out = append(out, flags)
	out = binary.LittleEndian.AppendUint16(out, uint16(len(notNull)))
	out = binary.LittleEndian.AppendUint16(out, uint16(len(null)))
	for _, c := range notNull {
		out = append(out, byte(c.id))
	}
	for _, c := range null {
		out = append(out, byte(c.id))
	}
	var data []byte
	for _, c := range notNull {
		data = append(data, c.value...)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(data))) // END 偏移
	}
	out = append(out, data...)
	if withChecksum {
		out = append(out, 2)                        // checksum header：VER=2（v8.4+ raw handle）
		out = binary.LittleEndian.AppendUint32(out, 0xDEADBEEF) // CRC 占位
	}
	return out
}

func encStr(s string) []byte  { return []byte(s) }
func encIntLE(v int64) []byte { return []byte{byte(v)} } // 1 字节 LE（值域内）

func j1Columns() []encoder.ColumnInfo {
	return []encoder.ColumnInfo{
		{ID: 1, Name: "id", MySQLType: "bigint"},
		{ID: 2, Name: "F_Description", MySQLType: "varchar(500)"},
		{ID: 3, Name: "F_CreatorTime", MySQLType: "datetime"},
		{ID: 4, Name: "F_CreatorUserId", MySQLType: "varchar(50)"},
	}
}

func TestDecodeRowNewFormatNullAndEmpty(t *testing.T) {
	cols := j1Columns()

	cases := []struct {
		name string
		csum bool
		row  []encCol
		want map[string]string // 只断言关键列存在与值
		// wantMissing：解码后必须仍缺失的列（由 reader 层 fillAbsentColumns
		// 在 fillPKFromKey 之后回填，解码层不得越俎代庖——聚簇主键列的
		// 合法缺失要留给 key 回填）。
		wantMissing []string
	}{
		{
			name: "empty string column (wform F_Description='')",
			row: []encCol{
				{1, encIntLE(7)},
				{2, encStr("")}, // 空串：0 字节数据
				{3, encStr("2026-01-01 00:00:00")},
				{4, encStr("admin")},
			},
			want: map[string]string{"id": "7", "F_Description": "", "F_CreatorUserId": "admin"},
		},
		{
			name: "NULL columns (base ts-0001: F_CreatorTime/F_CreatorUserId NULL)",
			row: []encCol{
				{1, encIntLE(1)},
				{2, encStr("hello")},
				{3, nil}, // NULL：只在 nullIDs
				{4, nil},
			},
			want: map[string]string{"id": "1", "F_Description": "hello", "F_CreatorTime": "", "F_CreatorUserId": ""},
		},
		{
			name: "checksum trailer must not leak into last column",
			csum: true,
			row: []encCol{
				{1, encIntLE(9)},
				{2, encStr("x")},
				{3, encStr("2026-01-02 03:04:05")},
				{4, encStr("u1")}, // 最后一个 not-null 列
			},
			want: map[string]string{"id": "9", "F_CreatorUserId": "u1"},
		},
		{
			name: "checksum + last not-null column is empty string",
			csum: true,
			row: []encCol{
				{1, encIntLE(3)},
				{2, encStr("")}, // 空 + 恰为最后 not-null 列
			},
			want: map[string]string{"id": "3", "F_Description": ""},
		},
		{
			name: "checksum + trailing NULL column (data ends at last offset)",
			csum: true,
			row: []encCol{
				{1, encIntLE(4)},
				{2, encStr("desc")},
				{4, nil}, // NULL 在最后：data 止于 F_Description 的 offset
			},
			want: map[string]string{"id": "4", "F_Description": "desc", "F_CreatorUserId": ""},
		},
		{
			name: "column entirely absent from row (ALTER ADD COLUMN old row)",
			row: []encCol{
				{1, encIntLE(5)},
				{2, encStr("legacy")},
				// 列 3/4 完全不在行里：不在 nullIDs、无数据（旧行早于
				// ALTER ADD COLUMN）。解码层应保持缺失，由 reader 层
				// fillAbsentColumns 在 fillPKFromKey 之后回填 ""。
			},
			want:        map[string]string{"id": "5", "F_Description": "legacy"},
			wantMissing: []string{"F_CreatorTime", "F_CreatorUserId"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeRowNewFormat(encodeRowV2(tc.csum, tc.row...), cols)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			for k, want := range tc.want {
				if v, ok := got[k]; !ok {
					t.Errorf("column %q missing from result (J1: applier will DLQ whole row); got=%v", k, got)
				} else if v != want {
					t.Errorf("column %q = %q, want %q", k, v, want)
				}
			}
			for _, k := range tc.wantMissing {
				if _, ok := got[k]; ok {
					t.Errorf("column %q present in decode result, want left absent for reader-level backfill", k)
				}
			}
		})
	}
}

// reader 层回填：缺失列 → ""，已有列（含空串、NULL 语义 ""）不动。
// 顺序契约：必须在 fillPKFromKey 之后调用，否则聚簇主键列会被 "" 挡住
// key 回填——这里直接验证组合语义。
func TestFillAbsentColumns(t *testing.T) {
	cols := j1Columns()
	row := map[string]string{"F_Description": ""} // 空串保持原样

	fillAbsentColumns(row, cols)
	for _, name := range []string{"id", "F_CreatorTime", "F_CreatorUserId"} {
		if v, ok := row[name]; !ok || v != "" {
			t.Errorf("column %q = %q(ok=%v), want \"\" backfilled", name, v, ok)
		}
	}
	if row["F_Description"] != "" {
		t.Errorf("F_Description must stay empty string, got %q", row["F_Description"])
	}
}
