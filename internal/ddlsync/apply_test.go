// apply_test.go：纯函数单元（版本探测解析、ORA 码提取、FixedDDL 编解码、
// 语句拆分）。Run/syncOne 的 DB 路径由 L2 真库集成覆盖（门控 leader 环境）。
package ddlsync

import (
	"strings"
	"testing"
)

func TestParseOracleMajorMinor(t *testing.T) {
	cases := []struct {
		ver            string
		major, minor   int
		ok             bool
	}{
		{"19.0.0.0.0", 19, 0, true},
		{"12.2.0.1.0", 12, 2, true},
		{"12.1.0.2.0", 12, 1, true},
		{"11.2.0.4.0 - 64bit", 11, 2, true},
		{"unknown", 0, 0, false},
		{"", 0, 0, false},
	}
	for _, c := range cases {
		maj, min, ok := parseOracleMajorMinor(c.ver)
		if ok != c.ok || (ok && (maj != c.major || min != c.minor)) {
			t.Errorf("%q: got (%d,%d,%v) want (%d,%d,%v)",
				c.ver, maj, min, ok, c.major, c.minor, c.ok)
		}
	}
	// 128 阈值：12.2 起生效
	for ver, want128 := range map[string]bool{"12.2.0.1": true, "12.1.0.2": false, "19.0.0.0": true, "11.2.0.4": false} {
		maj, min, ok := parseOracleMajorMinor(ver)
		if !ok {
			t.Fatalf("%q unparseable", ver)
		}
		got := maj > 12 || (maj == 12 && min >= 2)
		if got != want128 {
			t.Errorf("%s: maxIdent128=%v want %v", ver, got, want128)
		}
	}
}

func TestOraCodeOf(t *testing.T) {
	cases := map[string]string{
		"ORA-00955: name is already used by an existing object": "ORA-00955",
		"ORA-01438: value larger than specified precision":     "ORA-01438",
		"sql error: ORA-00904: invalid identifier":              "ORA-00904",
		"some other error":                                      "",
		"ORA- without digits":                                   "",
		"multi ORA-12345 then ORA-00001":                        "ORA-12345",
	}
	for msg, want := range cases {
		if got := oraCodeOf(errString(msg)); got != want {
			t.Errorf("%q: got %q want %q", msg, got, want)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestSplitStatements(t *testing.T) {
	in := "DROP TABLE x;\n\nCREATE TABLE x (id NUMBER);\n  \nCOMMENT ON TABLE x IS 'a';"
	got := splitStatements(in)
	if len(got) != 3 || strings.TrimSpace(got[0]) != "DROP TABLE x" {
		t.Fatalf("got %#v", got)
	}
	if len(splitStatements("")) != 0 || len(splitStatements(" ; ; ")) != 0 {
		t.Error("empty input must yield no statements")
	}
}

func TestDecodeFixedDDL(t *testing.T) {
	// "CREATE TABLE t (id NUMBER);" → base64
	b64 := "Q1JFQVRFIFRBQkxFIHQgKGlkIE5VTUJFUik7"
	got, err := DecodeFixedDDL(b64)
	if err != nil || got != "CREATE TABLE t (id NUMBER);" {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := DecodeFixedDDL("!!not-base64!!"); err == nil {
		t.Error("invalid base64 must error")
	}
	// base64(JSON of non-UTF8 bytes)：GBK "中文" = D6 D0 CE C4
	if _, err := DecodeFixedDDL("1tDMyw=="); err == nil || !strings.Contains(err.Error(), "UTF-8") {
		t.Errorf("non-UTF-8 payload must error, got %v", err)
	}
	// 与前端 TextEncoder 口径：UTF-8 多字节
	if s, err := DecodeFixedDDL("5Lit5paH"); err != nil || s != "中文" {
		t.Errorf("utf8 roundtrip: %q %v", s, err)
	}
}

func TestJoinStatements(t *testing.T) {
	if joinStatements(nil) != "" {
		t.Error("nil must join to empty")
	}
	got := joinStatements([]Statement{{SQL: "A"}, {SQL: "B"}})
	if got != "A;\nB;" {
		t.Errorf("got %q", got)
	}
}

func TestEnumOrSet(t *testing.T) {
	for _, yes := range []string{"enum('a')", "SET('a','b')", "set"} {
		if !enumOrSet(yes) {
			t.Errorf("%s should be enum/set", yes)
		}
	}
	for _, no := range []string{"varchar(8)", "bigint", "tinyint(1)"} {
		if enumOrSet(no) {
			t.Errorf("%s should not be enum/set", no)
		}
	}
}
