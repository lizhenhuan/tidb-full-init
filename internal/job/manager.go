// manager.go：JobManager——ddl-sync 任务编排 + FixedDDL 重跑。
//
// 执行模型（P2 最小闭环）：create → 落库 pending → goroutine 异步跑
// ddlsync.Run（单表失败不中断）→ 逐表 upsert ddl_results → 任务终态
// success（无 failed）/ failed（有 failed；skipped 不算失败）。
// 暂停/恢复/停止对 ddl-sync 无意义（有限批量任务）：/actions 对 ddl-sync
// 返回 422（其余任务类型的编排属 M3 后续）。
package job

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // 注册 mysql 驱动：ConnectTiDB 的 sql.Open("mysql") 依赖（tidb-console 链路此前无包引入）
	goorav2 "github.com/sijms/go-ora/v2"

	"github.com/lizhenhuan/tidb-full-init/internal/ddlsync"
)

// DdlSyncPayload 契约 DdlSyncPayload 同构（tables 按 db 分组）。
type DdlSyncPayload struct {
	Type              string          `json:"type"` // ddl-sync
	Name              string          `json:"name"`
	TiDBDatasourceID  string          `json:"tidbDatasourceId"`
	OracleDatasourceID string         `json:"oracleDatasourceId"`
	TargetSchema      string          `json:"targetSchema"`
	Tables            []DBTables      `json:"tables"`
	Overwrite         bool            `json:"overwrite,omitempty"`
}

// DBTables {database, tables[]} 分组。
type DBTables struct {
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
}

// Manager 任务管理器（store + 运行中任务注册表）。
type Manager struct {
	Store *Store

	mu     sync.Mutex // guards running map + cancel funcs
	cancel map[int64]context.CancelFunc

	// ConnectTiDB/ConnectOracle 注入（测试替换口）。nil = 生产实现。
	ConnectTiDB    func(ctx context.Context, cfg DatasourceConfig) (*sql.DB, error)
	ConnectOracle  func(ctx context.Context, cfg DatasourceConfig) (*sql.DB, error)
}

// NewManager 构造。
func NewManager(store *Store) *Manager {
	m := &Manager{Store: store, cancel: map[int64]context.CancelFunc{}}
	m.ConnectTiDB = func(ctx context.Context, cfg DatasourceConfig) (*sql.DB, error) {
		db, err := sql.Open("mysql", tidbDSN(cfg))
		if err != nil {
			return nil, err
		}
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return nil, fmt.Errorf("ping tidb: %w", err)
		}
		return db, nil
	}
	m.ConnectOracle = func(ctx context.Context, cfg DatasourceConfig) (*sql.DB, error) {
		db, err := sql.Open("oracle", goorav2.BuildUrl(cfg.Host, cfg.Port, cfg.Service, cfg.User, cfg.Password, nil))
		if err != nil {
			return nil, err
		}
		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return nil, fmt.Errorf("ping oracle: %w", err)
		}
		return db, nil
	}
	return m
}

func tidbDSN(cfg DatasourceConfig) string {
	// DSN 口径与 cmd/tidb-ddlsync 一致（net/url 交给驱动解析）
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/", cfg.User, cfg.Password, cfg.Host, cfg.Port)
}

// CreateDdlSyncJob 校验 payload → 落库 pending → 异步执行。
// 返回 422 语义错误列表（前端契约：string[]）。
func (m *Manager) CreateDdlSyncJob(ctx context.Context, p *DdlSyncPayload) (*Job, []string, error) {
	var errs []string
	if p.Name == "" {
		errs = append(errs, "任务名不能为空")
	}
	if p.TiDBDatasourceID == "" || p.OracleDatasourceID == "" {
		errs = append(errs, "需选择 TiDB 与 Oracle 数据源")
	}
	if p.TargetSchema == "" {
		errs = append(errs, "目标 Schema（OWNER）不能为空")
	}
	if len(p.Tables) == 0 {
		errs = append(errs, "需选择至少一张表")
	}
	for _, g := range p.Tables {
		if g.Database == "" || len(g.Tables) == 0 {
			errs = append(errs, "表分组缺少库名或表名")
		}
	}
	// 无 PK / ENUM 不在入口拒绝（口径 A）：DDL 任务管结构，数据任务创建
	// 时由管道 ValidateTable 硬校验；前端橙标 + warnings 提示。
	if len(errs) > 0 {
		return nil, errs, nil
	}
	if ds, err := m.Store.GetDatasource(p.TiDBDatasourceID); err != nil {
		return nil, nil, err
	} else if ds == nil {
		return nil, append(errs, "TiDB 数据源不存在"), nil
	}
	if ds, err := m.Store.GetDatasource(p.OracleDatasourceID); err != nil {
		return nil, nil, err
	} else if ds == nil {
		return nil, append(errs, "Oracle 数据源不存在"), nil
	}

	payload, _ := json.Marshal(p)
	j := &Job{Type: "ddl-sync", Name: p.Name, Status: "pending",
		ProgressText: "排队中", Payload: payload}
	if err := m.Store.CreateJob(j); err != nil {
		return nil, nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.cancel[j.ID] = cancel
	m.mu.Unlock()
	go m.runDdlSync(runCtx, j.ID, p)
	return j, nil, nil
}

// runDdlSync 异步主体：逐组执行 ddlsync.Run，逐表 upsert。
func (m *Manager) runDdlSync(ctx context.Context, jobID int64, p *DdlSyncPayload) {
	defer func() {
		m.mu.Lock()
		delete(m.cancel, jobID)
		m.mu.Unlock()
	}()
	fail := func(format string, args ...any) {
		_ = m.Store.UpdateJobStatus(jobID, "failed", 0, "", "", fmt.Sprintf(format, args...))
	}

	tidbDS, err := m.Store.GetDatasource(p.TiDBDatasourceID)
	if err != nil || tidbDS == nil {
		fail("tidb datasource unavailable: %v", err)
		return
	}
	oraDS, err := m.Store.GetDatasource(p.OracleDatasourceID)
	if err != nil || oraDS == nil {
		fail("oracle datasource unavailable: %v", err)
		return
	}

	_ = m.Store.UpdateJobStatus(jobID, "running", 5, "连接数据源", "", "")
	tidbDB, err := m.ConnectTiDB(ctx, tidbDS.Config)
	if err != nil {
		fail("%v", err)
		return
	}
	defer tidbDB.Close()
	oraDB, err := m.ConnectOracle(ctx, oraDS.Config)
	if err != nil {
		fail("%v", err)
		return
	}
	defer oraDB.Close()

	ver, maxIdent := ddlsync.ProbeOracle(ctx, oraDB)
	_ = m.Store.SetOracleEnv(jobID, &OracleEnv{
		Version: ver, Extended: maxIdent >= 128, MaxIdentLen: maxIdent})

	totalTables := 0
	for _, g := range p.Tables {
		totalTables += len(g.Tables)
	}
	done, failed := 0, 0
	for _, g := range p.Tables {
		res, err := ddlsync.Run(ctx, tidbDB, oraDB, ddlsync.RunConfig{
			Database: g.Database, Tables: g.Tables, TargetSchema: p.TargetSchema,
			Overwrite: p.Overwrite,
		})
		if err != nil {
			fail("ddlsync run %s: %v", g.Database, err)
			return
		}
		for _, t := range res.Tables {
			row := toRow(t)
			// 无 PK 表：结构可建，数据任务将被管道拒绝（口径 A warnings）
			if t.Status == ddlsync.StatusSuccess && hasNoPK(tidbDB, g.Database, t.Table) {
				row.Warnings = []string{"无主键：结构已创建，后续全量/增量任务将被管道 ValidateTable 拒绝（已知差异）"}
			}
			if err := m.Store.UpsertDDLResult(jobID, &row); err != nil {
				fail("persist result %s: %v", t.Table, err)
				return
			}
			if t.Status == ddlsync.StatusFailed {
				failed++
			}
			done++
			_ = m.Store.UpdateJobStatus(jobID, "running", 5+90*done/totalTables,
				fmt.Sprintf("已处理 %d/%d 表", done, totalTables), "", "")
		}
	}

	status, summary := "success", fmt.Sprintf("%d 表完成", totalTables)
	if failed > 0 {
		status = "failed"
		summary = fmt.Sprintf("%d/%d 表失败", failed, totalTables)
	}
	_ = m.Store.UpdateJobStatus(jobID, status, 100, "", summary, "")
}

func toRow(t ddlsync.TableResult) DDLResultRow {
	row := DDLResultRow{
		Table: t.Table, TargetTable: t.TargetTable, Status: t.Status,
		DDL: t.DDL, ErrorCode: t.ErrorCode, ErrorText: t.ErrorText,
	}
	for _, c := range t.Compat {
		row.Compat = append(row.Compat, CompatIssue{
			Level: c.Level, Column: c.Column, PreCheckType: c.PreCheckType,
			SourceValue: c.SourceValue, TargetValue: c.TargetValue, Suggest: c.Suggest,
		})
	}
	return row
}

// hasNoPK 源表无主键判定（information_schema，失败按有 PK 处理不误标）。
func hasNoPK(db *sql.DB, database, table string) bool {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY'`, database, table).Scan(&n)
	return err == nil && n == 0
}

// DdlRerun FixedDDL 重跑（仅 failed 表；fixedDdl base64，空则用原生成 DDL）。
// 幂等语义：重跑按 overwrite 处理已存在对象（DROP 由用户在 FixedDDL 里
// 自行加或依赖对象不存在）；成功后 upsert 该行。
func (m *Manager) DdlRerun(ctx context.Context, jobID int64, table, fixedDdlB64 string) (*DDLResultRow, []string, error) {
	row, err := m.Store.GetDDLResult(jobID, table)
	if err != nil {
		return nil, nil, err
	}
	if row == nil {
		return nil, []string{fmt.Sprintf("表 %s 不在本任务结果中", table)}, nil
	}
	if row.Status != "failed" {
		return nil, []string{"只能重跑 failed 表（当前 " + row.Status + "）"}, nil
	}

	sqlText := row.DDL
	if fixedDdlB64 != "" {
		decoded, err := ddlsync.DecodeFixedDDL(fixedDdlB64)
		if err != nil {
			return nil, []string{"fixedDdl base64/UTF-8 解码失败: " + err.Error()}, nil
		}
		sqlText = decoded
		row.FixedDDL = fixedDdlB64
	}
	if strings.TrimSpace(sqlText) == "" {
		return nil, []string{"无可执行 DDL（原结果未生成且未提供 fixedDdl）"}, nil
	}

	j, err := m.Store.GetJob(jobID)
	if err != nil || j == nil {
		return nil, nil, fmt.Errorf("job %d: %v", jobID, err)
	}
	var p DdlSyncPayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, nil, err
	}
	oraDS, err := m.Store.GetDatasource(p.OracleDatasourceID)
	if err != nil || oraDS == nil {
		return nil, nil, fmt.Errorf("oracle datasource: %v", err)
	}
	oraDB, err := m.ConnectOracle(ctx, oraDS.Config)
	if err != nil {
		return nil, nil, err
	}
	defer oraDB.Close()

	if err := ddlsync.ApplyFixedDDL(ctx, oraDB, sqlText); err != nil {
		row.Status = "failed"
		row.ErrorText = err.Error()
		row.ErrorCode = oraCodeOf(err)
	} else {
		row.Status = "success"
		row.ErrorCode, row.ErrorText = "", ""
	}
	if err := m.Store.UpsertDDLResult(jobID, row); err != nil {
		return nil, nil, err
	}
	return row, nil, nil
}

func oraCodeOf(err error) string {
	msg := err.Error()
	i := strings.Index(msg, "ORA-")
	if i < 0 {
		return ""
	}
	j := i + 4
	for j < len(msg) && msg[j] >= '0' && msg[j] <= '9' {
		j++
	}
	if j == i+4 {
		return ""
	}
	return msg[i:j]
}

// TableSummary / DBSchema 契约 TableSummary/DatabaseSchema 同构。
type TableSummary struct {
	Name          string   `json:"name"`
	RowCount      int64    `json:"rowCount"`
	ColumnCount   int      `json:"columnCount"`
	HasPrimaryKey bool     `json:"hasPrimaryKey"`
	HasEnumOrSet  bool     `json:"hasEnumOrSet"`
	EnumOrSetColumns []string `json:"enumOrSetColumns,omitempty"`
}

type DBSchema struct {
	Database string         `json:"database"`
	Tables   []TableSummary `json:"tables"`
}

// ListSchemas TiDB 数据源 → 库表摘要（含 ENUM/SET 列名与无 PK 标记，
// 前端树渲染/橙标数据源；行数为 information_schema 估算值）。
func (m *Manager) ListSchemas(ctx context.Context, tidbDatasourceID string) ([]DBSchema, []string, error) {
	ds, err := m.Store.GetDatasource(tidbDatasourceID)
	if err != nil {
		return nil, nil, err
	}
	if ds == nil {
		return nil, []string{"TiDB 数据源不存在"}, nil
	}
	db, err := m.ConnectTiDB(ctx, ds.Config)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()
	return fetchSchemas(ctx, db)
}

func fetchSchemas(ctx context.Context, db *sql.DB) ([]DBSchema, []string, error) {
	rows, err := db.QueryContext(ctx, `
SELECT t.TABLE_SCHEMA, t.TABLE_NAME, IFNULL(t.TABLE_ROWS,0),
       (SELECT COUNT(*) FROM information_schema.COLUMNS c
         WHERE c.TABLE_SCHEMA=t.TABLE_SCHEMA AND c.TABLE_NAME=t.TABLE_NAME),
       (SELECT COUNT(*) FROM information_schema.STATISTICS s
         WHERE s.TABLE_SCHEMA=t.TABLE_SCHEMA AND s.TABLE_NAME=t.TABLE_NAME
           AND s.INDEX_NAME='PRIMARY')
FROM information_schema.TABLES t
WHERE t.TABLE_TYPE='BASE TABLE'
  AND t.TABLE_SCHEMA NOT IN ('information_schema','mysql','performance_schema','sys','INFORMATION_SCHEMA','METRICS_SCHEMA','PERFORMANCE_SCHEMA','SYS')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	byDB := map[string]*DBSchema{}
	var order []string
	for rows.Next() {
		var schema, table string
		var rowCount int64
		var colCount, pkCount int
		if err := rows.Scan(&schema, &table, &rowCount, &colCount, &pkCount); err != nil {
			return nil, nil, err
		}
		dbs, ok := byDB[schema]
		if !ok {
			dbs = &DBSchema{Database: schema}
			byDB[schema] = dbs
			order = append(order, schema)
		}
		dbs.Tables = append(dbs.Tables, TableSummary{
			Name: table, RowCount: rowCount, ColumnCount: colCount, HasPrimaryKey: pkCount > 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// ENUM/SET 列名按表补齐（一次查询，避免逐表 N+1）
	erows, err := db.QueryContext(ctx, `
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS
WHERE LOWER(COLUMN_TYPE) LIKE 'enum%' OR LOWER(COLUMN_TYPE) LIKE 'set%'`)
	if err == nil {
		defer erows.Close()
		idx := map[string]*TableSummary{}
		for _, dbs := range byDB {
			for i := range dbs.Tables {
				idx[dbs.Database+"\x00"+dbs.Tables[i].Name] = &dbs.Tables[i]
			}
		}
		for erows.Next() {
			var schema, table, col string
			if err := erows.Scan(&schema, &table, &col); err == nil {
				if ts, ok := idx[schema+"\x00"+table]; ok {
					ts.HasEnumOrSet = true
					ts.EnumOrSetColumns = append(ts.EnumOrSetColumns, col)
				}
			}
		}
	}
	// 排序稳定性（列名顺序 = ORDINAL_POSITION 顺序，已是查询序）
	sort.Strings(order)
	out := make([]DBSchema, 0, len(order))
	for _, s := range order {
		out = append(out, *byDB[s])
	}
	return out, nil, nil
}

var _ = time.Now // time 预留（进度时间戳按需）
