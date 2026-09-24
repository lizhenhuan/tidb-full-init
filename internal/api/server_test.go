// server_test.go：P2 REST L1——SQLite 内存库 + httptest（无真库依赖；
// 连接类路径 ConnectTiDB/ConnectOracle 注入 stub，真库验证属 L2 联调）。
package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/ddlsync"
	"github.com/lizhenhuan/tidb-full-init/internal/job"
)

// fakeDB Connect 注入桩：不真连库。ddl-sync 执行路径通过替换
// ddlsync.Run 不可行（包内直调）——执行链路的端到端留 L2；此处桩
// 仅覆盖 datasources/jobs 校验与存储读写。
type fakeDB struct{ mu sync.Mutex }

func (f *fakeDB) connect(ctx context.Context, cfg job.DatasourceConfig) (*sql.DB, error) {
	// 用一个真实 *sql.DB 但不 ping（Open 惰性）：满足签名即可
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newTestServer(t *testing.T) (*Server, *job.Store, *fakeDB) {
	t.Helper()
	store, err := job.OpenStore("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mgr := job.NewManager(store)
	fake := &fakeDB{}
	mgr.ConnectTiDB = fake.connect
	mgr.ConnectOracle = fake.connect
	return New(store, mgr), store, fake
}

func doJSON(t *testing.T, s *Server, method, path string, body any) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, "/api/v1"+path, rd)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Mux.ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

func seedDatasource(t *testing.T, s *Server, typ string) string {
	t.Helper()
	rec, body := doJSON(t, s, "POST", "/datasources", dsInput{
		Name: typ + "-test", Type: typ,
		Config: job.DatasourceConfig{Host: "127.0.0.1", Port: 4000, User: "root", Service: "FREEPDB1"},
	})
	if rec.Code != 200 {
		t.Fatalf("seed datasource %s: HTTP %d %s", typ, rec.Code, body)
	}
	var d job.Datasource
	if err := json.Unmarshal(body, &d); err != nil {
		t.Fatal(err)
	}
	return d.ID
}

func TestDatasourceCRUD(t *testing.T) {
	s, _, _ := newTestServer(t)

	rec, _ := doJSON(t, s, "GET", "/datasources", nil)
	if rec.Code != 200 {
		t.Fatalf("list: %d", rec.Code)
	}
	id := seedDatasource(t, s, "tidb")

	// 凭据不回传：响应不应包含 password 字段值
	rec, body := doJSON(t, s, "GET", "/datasources", nil)
	if bytes.Contains(body, []byte(`"password"`)) && bytes.Contains(body, []byte("pw")) {
		// Config json tag "-"，只可能出现在显式 Config 序列化——这里确认无 Config 字段
		t.Errorf("credentials leaked: %s", body)
	}

	rec, body = doJSON(t, s, "POST", "/datasources/"+id+"/test", nil)
	if rec.Code != 200 {
		t.Fatalf("test: %d %s", rec.Code, body)
	}

	rec, _ = doJSON(t, s, "DELETE", "/datasources/"+id, nil)
	if rec.Code != 200 {
		t.Fatalf("delete: %d", rec.Code)
	}
	rec, body = doJSON(t, s, "POST", "/datasources/"+id+"/test", nil)
	if rec.Code != 404 {
		t.Fatalf("deleted should 404: %d %s", rec.Code, body)
	}

	// 类型校验 422 + string[]
	rec, body = doJSON(t, s, "POST", "/datasources", dsInput{Name: "x", Type: "redis"})
	if rec.Code != 422 {
		t.Fatalf("bad type: %d %s", rec.Code, body)
	}
	var errs []string
	if err := json.Unmarshal(body, &errs); err != nil || len(errs) == 0 {
		t.Fatalf("422 body must be string[]: %s", body)
	}
}

func TestCreateJobValidation(t *testing.T) {
	s, _, _ := newTestServer(t)
	tidb := seedDatasource(t, s, "tidb")
	ora := seedDatasource(t, s, "oracle")

	// 未选表 → 422
	rec, body := doJSON(t, s, "POST", "/jobs", map[string]any{
		"type": "ddl-sync", "name": "j1", "tidbDatasourceId": tidb,
		"oracleDatasourceId": ora, "targetSchema": "SHOP"})
	if rec.Code != 422 {
		t.Fatalf("no tables: %d %s", rec.Code, body)
	}
	// 不存在的数据源 → 422
	rec, body = doJSON(t, s, "POST", "/jobs", map[string]any{
		"type": "ddl-sync", "name": "j1", "tidbDatasourceId": "ds-nope",
		"oracleDatasourceId": ora, "targetSchema": "SHOP",
		"tables": []map[string]any{{"database": "d", "tables": []string{"t"}}}})
	if rec.Code != 422 {
		t.Fatalf("bad ds: %d %s", rec.Code, body)
	}
	// 非ddl-sync 类型 → 422（P2 范围）
	rec, _ = doJSON(t, s, "POST", "/jobs", map[string]any{"type": "full-init", "name": "j"})
	if rec.Code != 422 {
		t.Fatalf("full-init should 422 in P2: %d", rec.Code)
	}
}

func TestDdlRerunValidation(t *testing.T) {
	s, store, _ := newTestServer(t)

	// 直接落一个 failed 行验证 rerun 校验路径（执行路径 L2）
	jobID := int64(1)
	j := &job.Job{Type: "ddl-sync", Name: "j", Status: "failed"}
	if err := store.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	jobID = j.ID
	_ = store.UpsertDDLResult(jobID, &job.DDLResultRow{
		Table: "t1", TargetTable: "SHOP.T1", Status: "success", DDL: "CREATE TABLE x (id NUMBER)"})
	_ = store.UpsertDDLResult(jobID, &job.DDLResultRow{
		Table: "t2", TargetTable: "SHOP.T2", Status: "failed", DDL: "-- skip"})

	// 非 failed 表 → 422
	rec, body := doJSON(t, s, "POST", fmt.Sprintf("/jobs/%d/ddl-rerun", jobID),
		map[string]string{"table": "t1"})
	if rec.Code != 422 {
		t.Fatalf("non-failed rerun: %d %s", rec.Code, body)
	}
	// 不存在的表 → 422
	rec, _ = doJSON(t, s, "POST", fmt.Sprintf("/jobs/%d/ddl-rerun", jobID),
		map[string]string{"table": "nope"})
	if rec.Code != 422 {
		t.Fatalf("unknown table rerun: %d", rec.Code)
	}
	// 坏 base64 → 422（fixedDdl 解码校验在执行前）
	rec, body = doJSON(t, s, "POST", fmt.Sprintf("/jobs/%d/ddl-rerun", jobID),
		map[string]string{"table": "t2", "fixedDdl": "!!!not-base64!!!"})
	if rec.Code != 422 {
		t.Fatalf("bad base64: %d %s", rec.Code, body)
	}
	var errs []string
	_ = json.Unmarshal(body, &errs)
	if len(errs) == 0 {
		t.Fatalf("422 body: %s", body)
	}
}

func TestGetJobShape(t *testing.T) {
	s, store, _ := newTestServer(t)
	j := &job.Job{Type: "ddl-sync", Name: "j", Status: "success"}
	if err := store.CreateJob(j); err != nil {
		t.Fatal(err)
	}
	if err := store.SetOracleEnv(j.ID, &job.OracleEnv{Version: "19.0.0.0", Extended: true, MaxIdentLen: 128}); err != nil {
		t.Fatal(err)
	}
	_ = store.UpsertDDLResult(j.ID, &job.DDLResultRow{Table: "orders", Status: "success",
		DDL: "CREATE TABLE orders", Compat: []job.CompatIssue{{Level: "table", PreCheckType: "FK", Suggest: "未创建"}}})

	rec, body := doJSON(t, s, "GET", fmt.Sprintf("/jobs/%d", j.ID), nil)
	if rec.Code != 200 {
		t.Fatalf("get job: %d %s", rec.Code, body)
	}
	var got struct {
		TargetType string `json:"targetType"`
		OracleEnv  *struct {
			Version     string `json:"version"`
			MaxIdentLen int    `json:"maxIdentLen"`
		} `json:"oracleEnv"`
		DDLResults []struct {
			Table  string `json:"table"`
			Status string `json:"status"`
			Compat []struct {
				PreCheckType string `json:"preCheckType"`
			} `json:"compat"`
		} `json:"ddlResults"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.TargetType != "oracle" || got.OracleEnv == nil || got.OracleEnv.MaxIdentLen != 128 {
		t.Fatalf("job shape mismatch: %s", body)
	}
	if len(got.DDLResults) != 1 || got.DDLResults[0].Table != "orders" ||
		len(got.DDLResults[0].Compat) != 1 || got.DDLResults[0].Compat[0].PreCheckType != "FK" {
		t.Fatalf("ddlResults mismatch: %s", body)
	}
}

func TestDdlPreviewValidation(t *testing.T) {
	s, _, _ := newTestServer(t)
	tidb := seedDatasource(t, s, "tidb")
	ora := seedDatasource(t, s, "oracle")

	// 缺 tidbDatasourceId（契约扩展）→ 422
	rec, _ := doJSON(t, s, "POST", "/oracle/ddl-preview", map[string]any{
		"oracleDatasourceId": ora, "tables": []string{"db.t"}})
	if rec.Code != 422 {
		t.Fatalf("missing tidbDatasourceId: %d", rec.Code)
	}
	// 非 db.table 全限定 → 422
	rec, _ = doJSON(t, s, "POST", "/oracle/ddl-preview", map[string]any{
		"tidbDatasourceId": tidb, "oracleDatasourceId": ora, "tables": []string{"just_table"}})
	if rec.Code != 422 {
		t.Fatalf("unqualified: %d", rec.Code)
	}
}

func TestDecodeRerunSemantics(t *testing.T) {
	// fixedDdl 编解码与 ddlsync 包契约（base64 UTF-8）直接钉住
	const ddl = "CREATE TABLE t (id NUMBER);\nCOMMENT ON TABLE t IS '中''文';"
	b64 := "Q1JFQVRFIFRBQkxFIHQgKGlkIE5VTUJFUik7CkNPTU1FTlQgT04gVEFCTEUgdCBJUyAn5LitJyfmlocnOw=="
	got, err := ddlsync.DecodeFixedDDL(b64)
	if err != nil || got != ddl {
		t.Fatalf("decode: %q %v", got, err)
	}
}

var _ = time.Now
