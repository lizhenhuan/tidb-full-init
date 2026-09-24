// apply_test.go：纯函数单元（版本探测解析、ORA 码提取、FixedDDL 编解码、
// 语句拆分）。Run/syncOne 的 DB 路径由 L2 真库集成覆盖（门控 leader 环境）。
package ddlsync

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
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

// TestSplitStatementsP22 P2-2 现场（leader 复现）：注释值里的 ';' 不得
// 切断语句；'' 转义、'--' 注释里的 ';'、基线头部注释标记同理。
func TestSplitStatementsP22(t *testing.T) {
	// leader 复现用例：COMMENT 值含分号
	in := "CREATE TABLE \"shop\".\"p22\" (\"c\" VARCHAR2(10));\n" +
		"COMMENT ON COLUMN \"shop\".\"p22\".\"c\" IS 'a;b';"
	got := splitStatements(in)
	if len(got) != 2 || !strings.Contains(got[1], "'a;b'") {
		t.Fatalf("P2-2 leader case: got %#v", got)
	}

	// '' 转义后跟分号值
	got = splitStatements("COMMENT ON TABLE t IS 'it''s;fine';\nDROP TABLE t")
	if len(got) != 2 || !strings.Contains(got[0], "'it''s;fine'") {
		t.Fatalf("escaped quote: got %#v", got)
	}

	// '--' 注释含分号：不产生额外语句（注释行随下一条语句整体提交，
	// Oracle 引擎正确解析前导注释）
	got = splitStatements("-- skip: a;b;c\nCREATE TABLE t (id NUMBER);")
	if len(got) != 1 || !strings.Contains(got[0], "CREATE TABLE t (id NUMBER)") {
		t.Fatalf("line comment: got %#v", got)
	}

	// 基线头部注释标记 + 正文语句
	got = splitStatements("-- 以下列因目标类型不兼容被跳过: tags\nCREATE TABLE t (id NUMBER);\nCREATE INDEX i ON t (id);")
	if len(got) != 2 {
		t.Fatalf("baseline marker: got %#v", got)
	}

	// 尾部纯注释（用户手编 FixedDDL 常见）不自成语句
	got = splitStatements("CREATE TABLE t (id NUMBER);\n-- done")
	if len(got) != 1 {
		t.Fatalf("trailing comment must not be a statement: %#v", got)
	}
	// 多行纯注释块同理
	got = splitStatements("DROP TABLE t;\n-- a\n-- b;c")
	if len(got) != 1 {
		t.Fatalf("comment block must not be a statement: %#v", got)
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

// ---- #t13 CheckOwnerSchema：假驱动（database/sql/driver 最小实现）----

var (
	ownerFakeMu    sync.Mutex
	ownerFakeUsers = map[string]string{} // dsn → 假装的连接用户
	ownerFakeOnce  sync.Once
)

func ownerFakeDB(t *testing.T, user string) *sql.DB {
	t.Helper()
	ownerFakeMu.Lock()
	dsn := fmt.Sprintf("ownerfake-%d", len(ownerFakeUsers))
	ownerFakeUsers[dsn] = user
	ownerFakeMu.Unlock()
	ownerFakeOnce.Do(func() {
		sql.Register("ownerfake", ownerFakeDriver{})
	})
	db, err := sql.Open("ownerfake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

type ownerFakeDriver struct{}

func (ownerFakeDriver) Open(dsn string) (driver.Conn, error) { return ownerFakeConn{dsn}, nil }

type ownerFakeConn struct{ dsn string }

func (ownerFakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (ownerFakeConn) Close() error                        { return nil }
func (ownerFakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }

func (c ownerFakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	ownerFakeMu.Lock()
	user := ownerFakeUsers[c.dsn]
	ownerFakeMu.Unlock()
	return &ownerFakeRows{vals: []driver.Value{user}}, nil
}

type ownerFakeRows struct {
	vals []driver.Value
	done bool
}

func (ownerFakeRows) Columns() []string { return []string{"USER"} }
func (ownerFakeRows) Close() error      { return nil }
func (r *ownerFakeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	copy(dest, r.vals)
	r.done = true
	return nil
}

// TestCheckOwnerSchema #t13 钉子：连接用户与目标 OWNER 一致（大小写
// 不敏感）放行；不一致必须在任何表执行前整体拒绝，可读错误含双方
// 名字与后果（索引落连接用户 schema、跨任务撞 ORA-00955）。
func TestCheckOwnerSchema(t *testing.T) {
	ctx := context.Background()
	// 一致：大小写不敏感（Oracle 未加引号标识符缺省大写折叠）
	for _, tc := range [][2]string{
		{"DDL_SMOKE", "ddl_smoke"},
		{"shop", "shop"},
		{"LZH_PRISMA_MYSQL", "lzh_prisma_mysql"},
	} {
		if err := CheckOwnerSchema(ctx, ownerFakeDB(t, tc[0]), tc[1]); err != nil {
			t.Errorf("user %q vs target %q must pass: %v", tc[0], tc[1], err)
		}
	}
	// 不一致：拒绝 + 可读错误（#t13 现场：system 连、target-schema 另一 OWNER）
	err := CheckOwnerSchema(ctx, ownerFakeDB(t, "SYSTEM"), "lzh_prisma_mysql")
	if err == nil {
		t.Fatal("user/target mismatch must be rejected")
	}
	for _, want := range []string{"SYSTEM", "lzh_prisma_mysql", "ORA-00955"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("readable error must mention %q: %v", want, err)
		}
	}
}
