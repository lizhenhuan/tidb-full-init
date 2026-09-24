package dmlgen

// J2：非自增主键顺序分配的纯函数单测（MAX 解析与类型判别）。

import (
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestParseMaxPKValue(t *testing.T) {
	cases := []struct {
		in      string
		want    uint64
		wantErr bool
	}{
		{"", 1, false},              // 空表（MAX 为 NULL）
		{"0", 1, false},             // max=0 → 从 1 起
		{"3438891053255012", 3438891053255013, false}, // part_hash 现场
		{"9223372036854775807", 9223372036854775808, false}, // 有符号上限 → 无符号域内 +1
		{"18446744073709551615", 0, true},  // +1 溢出无符号域 → 错误（值域耗尽由 seqMax 兜底）
		{"-5", 1, false},            // 全负 id：正区间必然空闲
		{" 42 ", 43, false},         // 驱动返回可能带空白
		{"abc", 0, true},            // 解析失败 fail-loud
	}
	for _, tc := range cases {
		got, err := parseMaxPKValue(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseMaxPKValue(%q) = %d, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseMaxPKValue(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseMaxPKValue(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestIsIntegerCol(t *testing.T) {
	yes := []string{"bigint", "bigint unsigned", "int(11)", "tinyint(1)", "smallint", "mediumint", "integer"}
	for _, s := range yes {
		if !isIntegerCol(s) {
			t.Errorf("isIntegerCol(%q) = false, want true", s)
		}
	}
	no := []string{"varchar(32)", "decimal(10,2)", "datetime", "char(8)", "bit(1)"}
	for _, s := range no {
		if isIntegerCol(s) {
			t.Errorf("isIntegerCol(%q) = true, want false", s)
		}
	}
}

// TestGenValueDecimalRange #t12 现场钉子：decimal 值必须按列精度生成
// （payment.amount decimal(5,2) 上限 999.99——固定 5 位整数生成整批
// Out of range 把 dmlgen 直接拦停）。
func TestGenValueDecimalRange(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	cases := []struct {
		typ    string
		maxInt int64
		frac   int
	}{
		{"decimal(5,2)", 1000, 2},
		{"decimal(3,0)", 1000, 0},           // 整数 3 位、无小数
		{"decimal(1,1)", 1, 1},              // 最窄：0.0~0.9
		{"decimal(10,2)", 100000000, 2},     // 旧固定生成的形状仍合法
		{"decimal", 10000000000, 0},         // 无宽度缺省 decimal(10,0)
		{"decimal(65,30)", 1000000000000000, 9}, // 超宽：截断到 int64/9 位小数，仍在列值域内
	}
	for i := 0; i < 500; i++ {
		for _, tc := range cases {
			v, ok := genValue(tc.typ, rng).(string)
			if !ok {
				t.Fatalf("%s: genValue must yield string, got %T", tc.typ, v)
			}
			s := v
			if strings.HasPrefix(s, "-") {
				t.Errorf("gen must be non-negative: %q", s)
				continue
			}
			intPart, fracPart := s, ""
			if dot := strings.Index(s, "."); dot >= 0 {
				intPart, fracPart = s[:dot], s[dot+1:]
			}
			if len(fracPart) != tc.frac {
				t.Errorf("%s: frac digits %d != %d (%q)", tc.typ, len(fracPart), tc.frac, v)
			}
			iv, err := strconv.ParseInt(intPart, 10, 64)
			if err != nil {
				t.Fatalf("%s: int part unparsable %q", tc.typ, v)
			}
			if iv >= tc.maxInt {
				t.Errorf("%s: %q out of range (int part must be < %d)", tc.typ, v, tc.maxInt)
			}
		}
	}
}

// L11a：fsp>0 的 time/datetime 生成值必须带 fsp 位小数（含尾零口径），
// 与 MySQL 文本显示逐位一致；fsp=0 无小数点。
func TestGenValueFsp(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	hmsRe := regexp.MustCompile(`^\d{2}:\d{2}:\d{2}(\.\d+)?$`)
	dtRe := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$`)
	for i := 0; i < 200; i++ {
		for mt, wantFracLen := range map[string]int{
			"time(6)": 6, "time(3)": 3, "time": 0,
			"datetime(6)": 6, "timestamp(3)": 3, "datetime": 0,
		} {
			v, _ := genValue(mt, rng).(string)
			var frac string
			if j := strings.LastIndex(v, "."); j >= 0 {
				frac = v[j+1:]
			}
			if len(frac) != wantFracLen {
				t.Fatalf("genValue(%q) = %q, frac len %d, want %d", mt, v, len(frac), wantFracLen)
			}
			re := hmsRe
			if strings.HasPrefix(mt, "datetime") || strings.HasPrefix(mt, "timestamp") {
				re = dtRe
			}
			if !re.MatchString(v) {
				t.Fatalf("genValue(%q) = %q not matching %v", mt, v, re)
			}
		}
	}
}

// L11a：JSON 生成值用 TiDB String() 分隔符（", "/": "），插入后源端读回
// 的形态与消息一致（CLOB 逐字节比）。
func TestGenValueJSONSpacing(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	v, _ := genValue("json", rng).(string)
	if !strings.Contains(v, `"k": `) || !strings.Contains(v, `, "s": `) {
		t.Fatalf("genValue(json) = %q, want TiDB spacing {\"k\": N, \"s\": \"...\"}", v)
	}
}
