// Package api：M3 console REST（P2 范围 = ddl-sync 闭环所需端点）。
//
// 实现：/datasources CRUD+test、/tidb/schemas、/jobs（仅 ddl-sync 类型；
// full-init/dml-sync 返回 422 待 M3 后续）、/jobs/{id}、
// /jobs/{id}/ddl-rerun、/oracle/ddl-preview。
// 错误契约：校验失败 422 + string[]；其余非 2xx + 文本 body（前端
// client.ts request() 口径）。
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/ddlsync"
	"github.com/lizhenhuan/tidb-full-init/internal/job"
)

// Server REST 服务。
type Server struct {
	Mux    *http.ServeMux
	store  *job.Store
	mgr    *job.Manager
}

// New 构造（路由挂载 /api/v1 前缀，与 vite 代理对齐）。
func New(store *job.Store, mgr *job.Manager) *Server {
	s := &Server{store: store, mgr: mgr, Mux: http.NewServeMux()}
	// Go 1.22+ 方法模式："GET /api/v1/..."（同路径不同方法各注册一次）
	r := func(method, path string, h http.HandlerFunc) {
		s.Mux.HandleFunc(method+" /api/v1"+path, h)
	}
	r(http.MethodGet, "/datasources", s.listDatasources)
	r(http.MethodPost, "/datasources", s.createDatasource)
	r(http.MethodPost, "/datasources/{id}/test", s.testDatasource)
	r(http.MethodDelete, "/datasources/{id}", s.deleteDatasource)
	r(http.MethodGet, "/tidb/schemas", s.listSchemas)
	r(http.MethodGet, "/jobs", s.listJobs)
	r(http.MethodPost, "/jobs", s.createJob)
	r(http.MethodGet, "/jobs/{id}", s.getJob)
	r(http.MethodPost, "/jobs/{id}/ddl-rerun", s.ddlRerun)
	r(http.MethodPost, "/oracle/ddl-preview", s.ddlPreview)
	return s
}

// ---- 通用 ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, msg)
}

func writeValidation(w http.ResponseWriter, errs []string) {
	writeJSON(w, http.StatusUnprocessableEntity, errs)
}

func pathID(r *http.Request) (string, bool) {
	id := r.PathValue("id")
	return id, id != ""
}

func jobID(r *http.Request) (int64, bool) {
	id, ok := pathID(r)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(id, 10, 64)
	return n, err == nil
}

// ---- datasources ----

func (s *Server) listDatasources(w http.ResponseWriter, r *http.Request) {
	ds, err := s.store.ListDatasources()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if ds == nil {
		ds = []*job.Datasource{}
	}
	writeJSON(w, 200, ds)
}

type dsInput struct {
	Name     string             `json:"name"`
	Type     string             `json:"type"`
	Endpoint string             `json:"endpoint"`
	Detail   string             `json:"detail,omitempty"`
	Config   job.DatasourceConfig `json:"config"`
}

func (s *Server) createDatasource(w http.ResponseWriter, r *http.Request) {
	var in dsInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, 400, "bad json: "+err.Error())
		return
	}
	var errs []string
	switch in.Type {
	case "tidb", "kafka", "oracle":
	default:
		errs = append(errs, "type 必须是 tidb|kafka|oracle")
	}
	if in.Name == "" {
		errs = append(errs, "name 不能为空")
	}
	if in.Type != "kafka" && in.Config.Host == "" {
		errs = append(errs, "host 不能为空")
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if in.Endpoint == "" && in.Config.Host != "" {
		in.Endpoint = fmt.Sprintf("%s:%d", in.Config.Host, in.Config.Port)
	}
	d := &job.Datasource{Name: in.Name, Type: in.Type, Endpoint: in.Endpoint,
		Detail: in.Detail, Status: "unknown", Config: in.Config}
	if err := s.store.UpsertDatasource(d); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, d)
}

func (s *Server) testDatasource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		writeErr(w, 400, "missing id")
		return
	}
	d, err := s.store.GetDatasource(id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if d == nil {
		writeErr(w, 404, "datasource not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), connectTimeout)
	defer cancel()
	status := "ok"
	var connect func(context.Context, job.DatasourceConfig) (*sql.DB, error)
	switch d.Type {
	case "tidb":
		connect = s.mgr.ConnectTiDB
	case "oracle":
		connect = s.mgr.ConnectOracle
	default: // kafka：P2 范围外，标记 unknown（前端 kafka 管理页后续接）
		status = "unknown"
	}
	if connect != nil {
		db, err := connect(ctx, d.Config)
		if err != nil {
			status = "failed" // 失败原因不落 detail（保持展示字段语义）
		} else {
			db.Close()
		}
	}
	d.Status = status
	_ = s.store.UpdateDatasourceStatus(id, status)
	writeJSON(w, 200, d)
}

func (s *Server) deleteDatasource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r)
	if !ok {
		writeErr(w, 400, "missing id")
		return
	}
	if err := s.store.DeleteDatasource(id); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

// ---- schemas / jobs ----

func (s *Server) listSchemas(w http.ResponseWriter, r *http.Request) {
	dsID := r.URL.Query().Get("datasource")
	if dsID == "" {
		writeValidation(w, []string{"datasource 参数必填"})
		return
	}
	schemas, errs, err := s.mgr.ListSchemas(r.Context(), dsID)
	if err != nil {
		writeErr(w, 502, err.Error())
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	writeJSON(w, 200, schemas)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListJobs()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if jobs == nil {
		jobs = []*job.Job{}
	}
	writeJSON(w, 200, jobs)
}

type createJobInput struct {
	Type              string          `json:"type"`
	Name              string          `json:"name"`
	TiDBDatasourceID  string          `json:"tidbDatasourceId"`
	OracleDatasourceID string         `json:"oracleDatasourceId"`
	TargetSchema      string          `json:"targetSchema"`
	Tables            []job.DBTables  `json:"tables"`
	Overwrite         bool            `json:"overwrite"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var in createJobInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, 400, "bad json: "+err.Error())
		return
	}
	if in.Type != "ddl-sync" {
		writeValidation(w, []string{fmt.Sprintf("任务类型 %s 暂不支持（P2 仅 ddl-sync）", in.Type)})
		return
	}
	j, errs, err := s.mgr.CreateDdlSyncJob(r.Context(), &job.DdlSyncPayload{
		Type: in.Type, Name: in.Name,
		TiDBDatasourceID: in.TiDBDatasourceID, OracleDatasourceID: in.OracleDatasourceID,
		TargetSchema: in.TargetSchema, Tables: in.Tables, Overwrite: in.Overwrite,
	})
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	writeJSON(w, 200, j)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(r)
	if !ok {
		writeErr(w, 400, "bad job id")
		return
	}
	j, err := s.store.GetJob(id)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if j == nil {
		writeErr(w, 404, "job not found")
		return
	}
	if j.DDLResults == nil {
		j.DDLResults = []job.DDLResultRow{}
	}
	writeJSON(w, 200, j)
}

func (s *Server) ddlRerun(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(r)
	if !ok {
		writeErr(w, 400, "bad job id")
		return
	}
	var in struct {
		Table    string `json:"table"`
		FixedDdl string `json:"fixedDdl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, 400, "bad json: "+err.Error())
		return
	}
	if in.Table == "" {
		writeValidation(w, []string{"table 必填"})
		return
	}
	row, errs, err := s.mgr.DdlRerun(r.Context(), id, in.Table, in.FixedDdl)
	if err != nil {
		writeErr(w, 502, err.Error())
		return
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	writeJSON(w, 200, row)
}

// ddlPreview 预览（DryRun）。契约扩展：body 需带 tidbDatasourceId
// （前端 DdlSync 页有 TiDB 选择，原契约只有 oracleDatasourceId+tables
// 无法定位源端——前端 client.ts 需同步传参，见 P2 汇报）。
// tables 为 "db.table" 全限定名（DdlSync.tsx 口径）。
func (s *Server) ddlPreview(w http.ResponseWriter, r *http.Request) {
	var in struct {
		TiDBDatasourceID   string   `json:"tidbDatasourceId"`
		OracleDatasourceID string   `json:"oracleDatasourceId"`
		Tables             []string `json:"tables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, 400, "bad json: "+err.Error())
		return
	}
	var errs []string
	if in.TiDBDatasourceID == "" {
		errs = append(errs, "tidbDatasourceId 必填（契约扩展：预览需定位源 TiDB）")
	}
	if in.OracleDatasourceID == "" {
		errs = append(errs, "oracleDatasourceId 必填")
	}
	if len(in.Tables) == 0 {
		errs = append(errs, "tables 不能为空")
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}

	tidbDS, err := s.store.GetDatasource(in.TiDBDatasourceID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if tidbDS == nil {
		writeValidation(w, []string{"TiDB 数据源不存在"})
		return
	}
	oraDS, err := s.store.GetDatasource(in.OracleDatasourceID)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if oraDS == nil {
		writeValidation(w, []string{"Oracle 数据源不存在"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), previewTimeout)
	defer cancel()
	tidbDB, err := s.mgr.ConnectTiDB(ctx, tidbDS.Config)
	if err != nil {
		writeErr(w, 502, err.Error())
		return
	}
	defer tidbDB.Close()
	oraDB, err := s.mgr.ConnectOracle(ctx, oraDS.Config)
	if err != nil {
		writeErr(w, 502, err.Error())
		return
	}
	defer oraDB.Close()

	byDB := map[string][]string{}
	var order []string
	for _, t := range in.Tables {
		db, tbl, ok := strings.Cut(t, ".")
		if !ok || db == "" || tbl == "" {
			writeValidation(w, []string{fmt.Sprintf("表名 %q 需为 db.table 全限定", t)})
			return
		}
		if _, seen := byDB[db]; !seen {
			order = append(order, db)
		}
		byDB[db] = append(byDB[db], tbl)
	}

	type previewRow struct {
		SourceTable string             `json:"sourceTable"`
		TargetTable string             `json:"targetTable"`
		DDL         string             `json:"ddl"`
		Compat      []job.CompatIssue  `json:"compat"`
	}
	out := make([]previewRow, 0, len(in.Tables))
	for _, db := range order {
		res, err := ddlsync.Run(ctx, tidbDB, oraDB, ddlsync.RunConfig{
			Database: db, Tables: byDB[db], DryRun: true,
		})
		if err != nil {
			writeErr(w, 502, fmt.Sprintf("ddlsync %s: %v", db, err))
			return
		}
		for _, t := range res.Tables {
			row := previewRow{SourceTable: db + "." + t.Table, TargetTable: t.TargetTable, DDL: t.DDL}
			for _, c := range t.Compat {
				row.Compat = append(row.Compat, job.CompatIssue{
					Level: c.Level, Column: c.Column, PreCheckType: c.PreCheckType,
					SourceValue: c.SourceValue, TargetValue: c.TargetValue, Suggest: c.Suggest,
				})
			}
			out = append(out, row)
		}
	}
	writeJSON(w, 200, out)
}

const (
	connectTimeout = 10 * time.Second
	previewTimeout = 120 * time.Second
)
