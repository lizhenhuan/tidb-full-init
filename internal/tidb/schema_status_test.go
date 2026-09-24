package tidb

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

func cols(names ...string) []encoder.ColumnInfo {
	out := make([]encoder.ColumnInfo, len(names))
	for i, n := range names {
		out[i] = encoder.ColumnInfo{ID: int64(i + 1), Name: n}
	}
	return out
}

// 真实列 ID 覆盖序号兜底：按名字（大小写不敏感）对齐，DROP COLUMN 后
// 序号与真实 ID 错位的表由此解堵。
func TestApplyRealColumnIDsOverrides(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schema/db1/t1" {
			http.NotFound(w, r)
			return
		}
		fmt.Fprint(w, `{"id":42,"cols":[
			{"id":3,"name":{"O":"id","L":"id"},"state":"public"},
			{"id":1,"name":{"O":"Name","L":"name"},"state":"public"},
			{"id":7,"name":{"O":"dropped","L":"dropped"},"state":"write only"}
		]}`)
	}))
	defer srv.Close()

	sf := &SchemaFetcher{statusURL: srv.URL}
	c := cols("id", "name") // 序号口径 1,2
	if err := sf.applyRealColumnIDs("db1", "t1", c); err != nil {
		t.Fatal(err)
	}
	if c[0].ID != 3 || c[1].ID != 1 {
		t.Fatalf("IDs = [%d %d], want [3 1]", c[0].ID, c[1].ID)
	}
}

// 全有或全无：任一列在 status schema 缺失即整体放弃且不改动原 IDs
// （混合 ID 集会解出脏数据）。
func TestApplyRealColumnIDsAllOrNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"cols":[{"id":3,"name":{"O":"id","L":"id"}}]}`)
	}))
	defer srv.Close()

	sf := &SchemaFetcher{statusURL: srv.URL}
	c := cols("id", "memo")
	if err := sf.applyRealColumnIDs("db1", "t1", c); err == nil {
		t.Fatal("missing column must fail the whole override")
	}
	if c[0].ID != 1 || c[1].ID != 2 {
		t.Fatalf("IDs must stay untouched on failure, got [%d %d]", c[0].ID, c[1].ID)
	}
}

// L5：TiDB v8.5.8 的 status API 列状态是数字（public=5），另有构建给
// 字符串、232 端省略——三种形态都要兼容，非 public 数字（DDL 中间态）跳过。
func TestApplyRealColumnIDsNumericState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"cols":[
			{"id":1,"name":{"O":"id","L":"id"},"state":5},
			{"id":4,"name":{"O":"c","L":"c"},"state":2},
			{"id":3,"name":{"O":"b","L":"b"},"state":"public"}
		]}`)
	}))
	defer srv.Close()

	// 只有 public 列可命中；state=2（write only）的列 c 对不上 → 整体失败。
	sf := &SchemaFetcher{statusURL: srv.URL}
	c := cols("id", "b", "c")
	if err := sf.applyRealColumnIDs("db1", "t1", c); err == nil {
		t.Fatal("non-public column must be skipped and fail all-or-nothing")
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"cols":[
			{"id":1,"name":{"O":"id","L":"id"},"state":5},
			{"id":3,"name":{"O":"b","L":"b"},"state":"public"},
			{"id":4,"name":{"O":"zz","L":"zz"},"state":2}
		]}`)
	}))
	defer srv2.Close()
	sf2 := &SchemaFetcher{statusURL: srv2.URL}
	c2 := cols("id", "b")
	if err := sf2.applyRealColumnIDs("db1", "t1", c2); err != nil {
		t.Fatalf("numeric public state must work: %v", err)
	}
	if c2[0].ID != 1 || c2[1].ID != 3 {
		t.Fatalf("IDs = [%d %d], want [1 3]", c2[0].ID, c2[1].ID)
	}
}

func TestApplyRealColumnIDsErrors(t *testing.T) {
	sf := &SchemaFetcher{statusURL: ""}
	if err := sf.applyRealColumnIDs("db1", "t1", cols("id")); err == nil {
		t.Fatal("empty status URL must fail")
	}

	// HTTP 非 200（如表已删除）→ 放弃，不致命。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "table not found", http.StatusNotFound)
	}))
	defer srv.Close()
	sf = &SchemaFetcher{statusURL: srv.URL}
	if err := sf.applyRealColumnIDs("db1", "t1", cols("id")); err == nil {
		t.Fatal("404 must fail")
	}

	// 同一 ID 映射到多列 → 放弃。
	dup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"cols":[
			{"id":3,"name":{"O":"a","L":"a"}},
			{"id":3,"name":{"O":"b","L":"b"}}
		]}`)
	}))
	defer dup.Close()
	sf = &SchemaFetcher{statusURL: dup.URL}
	if err := sf.applyRealColumnIDs("db1", "t1", cols("a", "b")); err == nil {
		t.Fatal("duplicate column id must fail")
	}
}

func TestStatusBaseURL(t *testing.T) {
	t.Setenv("TIDB_STATUS_ADDR", "http://10.0.0.9:18080")
	if got := statusBaseURL("root:pw@tcp(127.0.0.1:4000)/"); got != "http://10.0.0.9:18080" {
		t.Fatalf("env override ignored: %q", got)
	}
	t.Setenv("TIDB_STATUS_ADDR", "")
	if got := statusBaseURL("root:pw@tcp(127.0.0.1:4000)/"); got != "http://127.0.0.1:10080" {
		t.Fatalf("derived URL = %q", got)
	}
}
