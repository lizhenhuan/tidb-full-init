package verify

import (
	"testing"
	"time"
)

// 跨库规范化契约：MySQL 侧表示与 Oracle 侧表示 → 同一 canonical 串。
// 这是 TiDB↔Oracle 比对正确性的核心（T2.4）。
func TestCanonicalCrossDialect(t *testing.T) {
	mk := time.Date(2026, 9, 24, 10, 30, 0, 0, time.Local)
	dt := time.Date(2026, 9, 24, 10, 30, 5, 123456000, time.Local)
	oracleDT := time.Date(2026, 9, 24, 10, 30, 5, 123456000, time.Local)

	cases := []struct {
		name            string
		kind            Kind
		mysqlVal, oraVal any
		want            string
	}{
		// 整数：MySQL int64 vs Oracle NUMBER(20,0)（int64/string）
		{"int", KindInt, int64(42), "42", "42"},
		{"int-string", KindInt, []byte("42"), "42", "42"},
		{"int-neg", KindInt, int64(-7), int64(-7), "-7"},
		// 定点：DECIMAL 文本 vs NUMBER 带小数（尾零/精度差抹平）
		{"decimal-trailing-zero", KindDecimal, []byte("123.40"), float64(123.4), "617/5"},
		{"decimal-string", KindDecimal, []byte("0.10"), "0.1", "1/10"},
		{"decimal-neg-zero", KindDecimal, []byte("-0.00"), "0", "0"},
		{"decimal-exponent", KindDecimal, []byte("1.23E+3"), "1230", "1230"},
		{"decimal-string-ora", KindDecimal, "123.45", float64(123.45), "2469/20"},
		// 时间：尾零截断（datetime(6) 存 .123456000 vs TIMESTAMP(6) .123456）
		{"time-frac-trim", KindTime, time.Date(2026, 9, 24, 10, 30, 5, 123456000, time.Local), dt, "2026-09-24 10:30:05.123456"},
		{"time-trailing-zeros", KindTime, time.Date(2026, 9, 24, 10, 30, 5, 100000000, time.Local), "2026-09-24 10:30:05.1", "2026-09-24 10:30:05.1"},
		{"time-no-frac", KindTime, time.Date(2026, 9, 24, 10, 30, 5, 0, time.Local), "2026-09-24 10:30:05", "2026-09-24 10:30:05"},
		{"time-string", KindTime, "2026-09-24 10:30:05.123456", oracleDT, "2026-09-24 10:30:05.123456"},
		// 日期：Oracle DATE（时间部分丢弃，映射约定）
		{"date", KindDate, mk, mk, "2026-09-24"},
		// 单精度浮点（L6）：两侧统一 32 位精度——float32 直读、驱动
		// 拓宽成 float64、文本协议三种形态必须同 canonical。
		{"float32-direct", KindFloat32, float32(1.1), float32(1.1), "11/10"},
		{"float32-widened-f64", KindFloat32, float64(float32(1.1)), float32(1.1), "11/10"},
		{"float32-text", KindFloat32, []byte("1.1"), float32(1.1), "11/10"},
		{"float32-int-val", KindFloat32, float32(3), float64(3), "3"},
		// BIT（L10）：MySQL 侧大端位串/uint64，Oracle NUMBER int64——按
		// 无符号整数归一，不再走 base64 假 DIFF。
		{"bit-bytes", KindBit, []byte{0x01, 0x00}, int64(256), "256"},
		{"bit-zero", KindBit, []byte{0x00, 0x00}, "0", "0"},
		{"bit-uint64", KindBit, uint64(65535), int64(65535), "65535"},
		// NULL
		{"null", KindString, nil, nil, nullSentinel},
		// 字符串
		{"string", KindString, []byte("héllo"), "héllo", "héllo"},
		{"empty-string", KindString, "", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, err := canonical(c.mysqlVal, c.kind)
			if err != nil {
				t.Fatalf("mysql side: %v", err)
			}
			o, err := canonical(c.oraVal, c.kind)
			if err != nil {
				t.Fatalf("oracle side: %v", err)
			}
			if m != o {
				t.Fatalf("canonical mismatch: mysql %q vs oracle %q", m, o)
			}
			if m != c.want {
				t.Fatalf("canonical = %q, want %q", m, c.want)
			}
		})
	}
}

func TestCanonicalNumberRejects(t *testing.T) {
	for _, bad := range []any{true, struct{}{}} {
		if _, err := canonical(bad, KindInt); err == nil {
			t.Fatalf("canonical(%T) should fail", bad)
		}
	}
	if _, err := canonical([]byte("abc"), KindDecimal); err == nil {
		t.Fatal("canonical(abc) as decimal should fail")
	}
}

// 精度隔离（L6）：KindFloat（BINARY_DOUBLE）保持 64 位精度不被 32 位
// 圆整吞掉，否则真差异会被错判相等。
func TestCanonicalFloatPrecisionSeparation(t *testing.T) {
	full, err := canonical(float64(1.100000023841858), KindFloat)
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := canonical(float64(float32(1.1)), KindFloat32)
	if err != nil {
		t.Fatal(err)
	}
	if full == narrow {
		t.Fatalf("KindFloat must preserve 64-bit precision: both = %q", full)
	}
}

func TestKindInference(t *testing.T) {
	for mt, want := range map[string]Kind{
		"bigint": KindInt, "int unsigned": KindInt, "tinyint(1)": KindInt,
		"decimal(10,2)": KindDecimal, "numeric(5)": KindDecimal,
		"float": KindFloat32, "double": KindFloat,
		"datetime": KindTime, "datetime(6)": KindTime, "timestamp": KindTime,
		"date": KindDate, "bit(16)": KindBit, "bit": KindBit,
		"binary(16)": KindBytes, "blob": KindBytes,
		"varchar(64)": KindString, "text": KindString,
	} {
		if got := mysqlKind(mt); got != want {
			t.Errorf("mysqlKind(%q) = %v, want %v", mt, got, want)
		}
	}
	s2 := 2
	zero := 0
	for _, c := range []struct {
		dt  string
		sc *int
		want Kind
	}{
		{"NUMBER", nil, KindDecimal},
		{"NUMBER", &s2, KindDecimal},
		{"NUMBER", &zero, KindInt},
		{"FLOAT", nil, KindFloat},
		{"BINARY_DOUBLE", nil, KindFloat},
		{"BINARY_FLOAT", nil, KindFloat32},
		{"TIMESTAMP(6)", nil, KindTime},
		{"DATE", nil, KindDate},
		{"VARCHAR2", nil, KindString},
		{"CLOB", nil, KindString},
		{"BLOB", nil, KindBytes},
	} {
		if got := oracleKind(c.dt, c.sc); got != c.want {
			t.Errorf("oracleKind(%q,%v) = %v, want %v", c.dt, c.sc, got, c.want)
		}
	}
}
