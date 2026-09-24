package dmlgen

// J2：非自增主键顺序分配的纯函数单测（MAX 解析与类型判别）。

import "testing"

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
