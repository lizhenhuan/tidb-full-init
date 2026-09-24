package snapshot

import (
	"strings"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

// buildNewFormatRow 构造最小新格式 rowcodec 值：ver|flags|notNullCnt|nullCnt|colIDs。
// 非 large 模式下列 ID 为单字节（见 readColIDs）。
func buildNewFormatRow(flags byte, colIDs ...int64) []byte {
	out := []byte{128, flags, byte(len(colIDs)), 0, 0, 0}
	for _, id := range colIDs {
		out = append(out, byte(id))
	}
	// 尾部填充哑数据（列偏移与值对本测试无关，ExtractRowColumnIDs 不读取）
	out = append(out, 0, 0)
	return out
}

// buildOldFormatRow 构造最小旧格式值：colID varint + Nil flag(0) 序列。
func buildOldFormatRow(colIDs ...int64) []byte {
	var out []byte
	for _, id := range colIDs {
		out = append(out, byte(id)) // readVarint 为原生 LEB128（非 zigzag）
		out = append(out, 0)           // Nil
	}
	return out
}

func TestValidateColumnIDs(t *testing.T) {
	cols := []encoder.ColumnInfo{
		{ID: 1, Name: "id"}, {ID: 2, Name: "name"}, {ID: 3, Name: "age"},
	}

	t.Run("IDs match, passes", func(t *testing.T) {
		rows := [][]byte{
			buildNewFormatRow(0, 1, 2, 3),
			buildNewFormatRow(0, 1, 3), // Nullable columns may be missing
		}
		if err := ValidateColumnIDs(rows, cols, false); err != nil {
			t.Fatalf("expect pass, got %v", err)
		}
	})

	t.Run("New format has extra ID, rejected in ordinal mode", func(t *testing.T) {
		rows := [][]byte{buildNewFormatRow(0, 1, 2, 7)}
		err := ValidateColumnIDs(rows, cols, false)
		if err == nil {
			t.Fatal("expect rejection")
		}
		if !strings.Contains(err.Error(), "DROP COLUMN") {
			t.Errorf("error should mention cause: %v", err)
		}
	})

	t.Run("Old format has extra ID, rejected", func(t *testing.T) {
		rows := [][]byte{buildOldFormatRow(1, 2, 9)}
		if err := ValidateColumnIDs(rows, cols, false); err == nil {
			t.Fatal("expect rejection")
		}
	})

	// L7：真实列 ID 模式下，行数据里的未知 ID 只可能是被 DROP COLUMN
	// 的残留——跳过（告警）而非拒绝，否则带 DDL 历史的表永远无法快照。
	t.Run("Dropped-column remnant skipped in real-ID mode", func(t *testing.T) {
		rows := [][]byte{buildNewFormatRow(0, 1, 3, 7)}
		if err := ValidateColumnIDs(rows, cols, true); err != nil {
			t.Fatalf("real-ID mode must skip dropped remnants, got %v", err)
		}
		old := [][]byte{buildOldFormatRow(1, 3, 9)}
		if err := ValidateColumnIDs(old, cols, true); err != nil {
			t.Fatalf("real-ID mode old format must skip too, got %v", err)
		}
	})

	t.Run("Old format matches, passes", func(t *testing.T) {
		rows := [][]byte{buildOldFormatRow(1, 2)}
		if err := ValidateColumnIDs(rows, cols, false); err != nil {
			t.Fatalf("expect pass, got %v", err)
		}
	})

	t.Run("Empty sample, passes directly", func(t *testing.T) {
		if err := ValidateColumnIDs(nil, cols, false); err != nil {
			t.Fatalf("expect pass, got %v", err)
		}
	})
}

func TestExtractRowColumnIDsEmpty(t *testing.T) {
	if got := ExtractRowColumnIDs(nil); len(got) != 0 {
		t.Errorf("nil value should yield empty set, got %v", got)
	}
	if got := ExtractRowColumnIDs([]byte{}); len(got) != 0 {
		t.Errorf("empty value should yield empty set, got %v", got)
	}
}
