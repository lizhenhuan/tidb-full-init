// Package job：M3 console 任务管理（P2）——SQLite 任务库（jobs +
// ddl_results + datasources，设计 §4）与 JobManager。
//
// 复用 checkpoint/sqlite.go 的 modernc.org/sqlite 纯 Go 口径（无 CGO，
// 与 go-ora 静态构建兼容）。凭据存本地 console 库：这是单机运维工具，
// 库文件权限即边界（README/文档已注明，不引入 KMS 花活）。
package job

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Datasource 数据源登记（REST /datasources 落库结构）。
type Datasource struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Type     string          `json:"type"` // tidb | kafka | oracle
	Endpoint string          `json:"endpoint"` // 展示用（不含凭据）
	Detail   string          `json:"detail,omitempty"` // TiDB PD 地址 / Oracle service
	Status   string          `json:"status"` // ok | failed | unknown
	LastCheckedAt string     `json:"lastCheckedAt,omitempty"`
	Config   DatasourceConfig `json:"-"` // 凭据不回传
}

// DatasourceConfig 连接配置（凭据，仅后端使用）。
type DatasourceConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Service  string `json:"service,omitempty"` // oracle
}

// Job 任务行（GET /jobs/{id} 响应同构，camelCase 契约）。
type Job struct {
	ID              int64           `json:"id"`
	Name            string          `json:"name"`
	Type            string          `json:"type"`
	TargetType      string          `json:"targetType,omitempty"`
	Status          string          `json:"status"` // pending|running|paused|success|failed
	CreatedAt       string          `json:"createdAt"`
	Summary         string          `json:"summary"`
	ProgressPercent int             `json:"progressPercent"`
	ProgressText    string          `json:"progressText"`
	ErrorText       string          `json:"errorText,omitempty"`
	OracleEnv       *OracleEnv      `json:"oracleEnv,omitempty"`
	DDLResults      []DDLResultRow  `json:"ddlResults,omitempty"`
	Payload         json.RawMessage `json:"-"`
}

// OracleEnv 任务级 Oracle 环境（验收首查项）。
type OracleEnv struct {
	Version     string `json:"version"`
	Extended    bool   `json:"extended"`
	MaxIdentLen int    `json:"maxIdentLen,omitempty"`
}

// DDLResultRow ddl_results 行（契约 DdlResult 同构）。
type DDLResultRow struct {
	Table      string          `json:"table"`
	TargetTable string         `json:"targetTable"`
	Status     string          `json:"status"` // success|failed|skipped
	DDL        string          `json:"ddl,omitempty"`
	FixedDDL   string          `json:"fixedDdl,omitempty"` // base64（重跑编辑稿）
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorText  string          `json:"errorText,omitempty"`
	Compat     []CompatIssue   `json:"compat,omitempty"`
	Warnings   []string        `json:"warnings,omitempty"`
}

// CompatIssue 契约 DdlCompatIssue 同构。
type CompatIssue struct {
	Level        string `json:"level"`
	Column       string `json:"column,omitempty"`
	PreCheckType string `json:"preCheckType"`
	SourceValue  string `json:"sourceValue"`
	TargetValue  string `json:"targetValue"`
	Suggest      string `json:"suggest"`
}

// Store SQLite 任务库。
type Store struct {
	db *sql.DB
}

// OpenStore 打开/初始化任务库（dir 空 = 内存库，测试用）。
func OpenStore(dir string) (*Store, error) {
	dsn := ":memory:"
	if dir != "" {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return nil, err
		}
		dsn = "file:" + filepath.Join(dir, "console.db") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) init() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS datasources (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, type TEXT NOT NULL,
  endpoint TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'unknown', last_checked_at TEXT NOT NULL DEFAULT '',
  config_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL, type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  summary TEXT NOT NULL DEFAULT '', progress_percent INTEGER NOT NULL DEFAULT 0,
  progress_text TEXT NOT NULL DEFAULT '', error_text TEXT NOT NULL DEFAULT '',
  oracle_env_json TEXT, payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ddl_results (
  job_id INTEGER NOT NULL REFERENCES jobs(id),
  table_name TEXT NOT NULL,
  target_table TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  ddl TEXT NOT NULL DEFAULT '',
  fixed_ddl TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_text TEXT NOT NULL DEFAULT '',
  compat_json TEXT NOT NULL DEFAULT '[]',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (job_id, table_name)
);`)
	return err
}

func nowISO() string { return time.Now().Format(time.RFC3339) }

// ---- datasources ----

// UpsertDatasource 新建（id 为空则生成）。
func (s *Store) UpsertDatasource(d *Datasource) error {
	if d.ID == "" {
		d.ID = fmt.Sprintf("ds-%x", time.Now().UnixNano())
	}
	cfg, err := json.Marshal(d.Config)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO datasources
		(id,name,type,endpoint,detail,status,last_checked_at,config_json,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET name=excluded.name, type=excluded.type,
		  endpoint=excluded.endpoint, detail=excluded.detail, status=excluded.status,
		  last_checked_at=excluded.last_checked_at, config_json=excluded.config_json`,
		d.ID, d.Name, d.Type, d.Endpoint, d.Detail, d.Status, d.LastCheckedAt, string(cfg), nowISO())
	return err
}

func (s *Store) scanDatasource(row interface{ Scan(...any) error }) (*Datasource, error) {
	var d Datasource
	var cfgJSON string
	if err := row.Scan(&d.ID, &d.Name, &d.Type, &d.Endpoint, &d.Detail, &d.Status, &d.LastCheckedAt, &cfgJSON); err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(cfgJSON), &d.Config)
	return &d, nil
}

// ListDatasources 全量（凭据不回传：Config 由 scan 填充但 JSON tag "-"）。
func (s *Store) ListDatasources() ([]*Datasource, error) {
	rows, err := s.db.Query(`SELECT id,name,type,endpoint,detail,status,last_checked_at,config_json FROM datasources ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Datasource
	for rows.Next() {
		d, err := s.scanDatasource(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDatasource 按 id（不存在返回 nil,nil）。
func (s *Store) GetDatasource(id string) (*Datasource, error) {
	row := s.db.QueryRow(`SELECT id,name,type,endpoint,detail,status,last_checked_at,config_json FROM datasources WHERE id = ?`, id)
	d, err := s.scanDatasource(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return d, err
}

// UpdateDatasourceStatus 连通性测试结果回写。
func (s *Store) UpdateDatasourceStatus(id, status string) error {
	_, err := s.db.Exec(`UPDATE datasources SET status = ?, last_checked_at = ? WHERE id = ?`,
		status, nowISO(), id)
	return err
}

func (s *Store) DeleteDatasource(id string) error {
	_, err := s.db.Exec(`DELETE FROM datasources WHERE id = ?`, id)
	return err
}

// ---- jobs ----

// CreateJob 落库 pending 任务，回填自增 id。
func (s *Store) CreateJob(j *Job) error {
	payload, err := json.Marshal(j.Payload)
	if err != nil {
		return err
	}
	now := nowISO()
	res, err := s.db.Exec(`INSERT INTO jobs (name,type,status,progress_text,payload_json,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?)`, j.Name, j.Type, j.Status, j.ProgressText, string(payload), now, now)
	if err != nil {
		return err
	}
	j.ID, err = res.LastInsertId()
	j.CreatedAt = now
	return err
}

// UpdateJobStatus 任务级状态/进度。
func (s *Store) UpdateJobStatus(id int64, status string, percent int, text, summary, errText string) error {
	_, err := s.db.Exec(`UPDATE jobs SET status=?, progress_percent=?, progress_text=?, summary=?, error_text=?, updated_at=? WHERE id=?`,
		status, percent, text, summary, errText, nowISO(), id)
	return err
}

// SetOracleEnv 探测结果（验收首查项）。
func (s *Store) SetOracleEnv(id int64, env *OracleEnv) error {
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`UPDATE jobs SET oracle_env_json=?, updated_at=? WHERE id=?`, string(b), nowISO(), id)
	return err
}

// GetJob 任务 + ddl_results（不存在返回 nil,nil）。
func (s *Store) GetJob(id int64) (*Job, error) {
	j := &Job{}
	var env, payload sql.NullString
	err := s.db.QueryRow(`SELECT id,name,type,status,created_at,summary,progress_percent,progress_text,error_text,oracle_env_json,payload_json
		FROM jobs WHERE id=?`, id).
		Scan(&j.ID, &j.Name, &j.Type, &j.Status, &j.CreatedAt, &j.Summary, &j.ProgressPercent, &j.ProgressText, &j.ErrorText, &env, &payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if j.Type == "ddl-sync" {
		j.TargetType = "oracle"
	}
	if env.Valid && env.String != "" {
		_ = json.Unmarshal([]byte(env.String), &j.OracleEnv)
	}
	j.Payload = json.RawMessage(payload.String)
	if j.Type == "ddl-sync" {
		if rows, err := s.ListDDLResults(id); err == nil {
			j.DDLResults = rows
		}
	}
	return j, nil
}

// ListJobs 全量（列表不带 ddl_results）。
func (s *Store) ListJobs() ([]*Job, error) {
	rows, err := s.db.Query(`SELECT id,name,type,status,created_at,summary,progress_percent,progress_text,error_text FROM jobs ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Job
	for rows.Next() {
		j := &Job{}
		if err := rows.Scan(&j.ID, &j.Name, &j.Type, &j.Status, &j.CreatedAt, &j.Summary, &j.ProgressPercent, &j.ProgressText, &j.ErrorText); err != nil {
			return nil, err
		}
		if j.Type == "ddl-sync" {
			j.TargetType = "oracle"
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ---- ddl_results ----

// UpsertDDLResult 单表结果（重跑 upsert 同入口，幂等）。
func (s *Store) UpsertDDLResult(jobID int64, r *DDLResultRow) error {
	compat, err := json.Marshal(r.Compat)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO ddl_results
		(job_id,table_name,target_table,status,ddl,fixed_ddl,error_code,error_text,compat_json,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(job_id, table_name) DO UPDATE SET
		  target_table=excluded.target_table, status=excluded.status, ddl=excluded.ddl,
		  fixed_ddl=excluded.fixed_ddl, error_code=excluded.error_code,
		  error_text=excluded.error_text, compat_json=excluded.compat_json,
		  updated_at=excluded.updated_at`,
		jobID, r.Table, r.TargetTable, r.Status, r.DDL, r.FixedDDL, r.ErrorCode, r.ErrorText, string(compat), nowISO())
	return err
}

// ListDDLResults 按表名序（报告口径稳定）。
func (s *Store) ListDDLResults(jobID int64) ([]DDLResultRow, error) {
	rows, err := s.db.Query(`SELECT table_name,target_table,status,ddl,fixed_ddl,error_code,error_text,compat_json
		FROM ddl_results WHERE job_id=? ORDER BY table_name`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DDLResultRow
	for rows.Next() {
		var r DDLResultRow
		var compat string
		if err := rows.Scan(&r.Table, &r.TargetTable, &r.Status, &r.DDL, &r.FixedDDL, &r.ErrorCode, &r.ErrorText, &compat); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(compat), &r.Compat)
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetDDLResult 单表行（重跑入口；不存在 nil,nil）。
func (s *Store) GetDDLResult(jobID int64, table string) (*DDLResultRow, error) {
	var r DDLResultRow
	var compat string
	err := s.db.QueryRow(`SELECT table_name,target_table,status,ddl,fixed_ddl,error_code,error_text,compat_json
		FROM ddl_results WHERE job_id=? AND table_name=?`, jobID, table).
		Scan(&r.Table, &r.TargetTable, &r.Status, &r.DDL, &r.FixedDDL, &r.ErrorCode, &r.ErrorText, &compat)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal([]byte(compat), &r.Compat)
	return &r, nil
}
