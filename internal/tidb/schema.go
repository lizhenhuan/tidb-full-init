package tidb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// defaultStatusPort TiDB status 端口默认值（HTTP API，含 /schema/{db}/{table}）。
const defaultStatusPort = 10080

type SchemaFetcher struct {
	db *sql.DB
	// statusURL TiDB status API 基址（http://host:10080）；空 = 无法推导，
	// 真实列 ID 不可用，回退序号口径（L4）。
	statusURL string
}

func NewSchemaFetcher(dsn string) (*SchemaFetcher, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping tidb: %w", err)
	}

	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("query version: %w", err)
	}
	log.Printf("[schema] connected to TiDB %s", version)

	return &SchemaFetcher{db: db, statusURL: statusBaseURL(dsn)}, nil
}

func (sf *SchemaFetcher) Close() error {
	return sf.db.Close()
}

func (sf *SchemaFetcher) GetTableInfo(database, tableName string) (*encoder.TableInfo, int64, error) {
	columns, err := sf.getColumns(database, tableName)
	if err != nil {
		return nil, 0, err
	}

	// L4：用 status API 的真实列 ID 覆盖序号兜底。任一失败不致命——
	// 回退序号口径，由快照侧 D13 守卫拒绝有 DDL 历史的表（安全网仍在）。
	idSource := "ordinal(fallback)"
	if err := sf.applyRealColumnIDs(database, tableName, columns); err != nil {
		log.Printf("[schema] %s.%s: real column IDs unavailable (%v); "+
			"falling back to ORDINAL_POSITION as column ID — tables with DROP COLUMN history will be rejected by the snapshot guard (D13)",
			database, tableName, err)
	} else {
		idSource = "real(status API)"
	}

	tableID, err := sf.getTableID(database, tableName)
	if err != nil {
		return nil, 0, err
	}

	var pkNames []string
	for _, col := range columns {
		if col.IsPrimaryKey {
			pkNames = append(pkNames, col.Name)
		}
	}

	tableInfo := &encoder.TableInfo{
		Database: database,
		Table:    tableName,
		Columns:  columns,
		PKNames:  pkNames,
		// 真实列 ID 模式（L7）：行数据里的未知列 ID 只可能是被 DROP
		// COLUMN 的残留，快照侧按跳过处理而非拒绝扫描。
		RealColumnIDs: idSource == "real(status API)",
	}

	log.Printf("[schema] %s.%s: tableID=%d, columns=%d, pk=%v, column-ids=%s",
		database, tableName, tableID, len(columns), pkNames, idSource)

	return tableInfo, tableID, nil
}

// GetPartitionIDs returns the TIDB_PARTITION_IDs for a partitioned table.
// Returns an empty slice if the table is not partitioned.
func (sf *SchemaFetcher) GetPartitionIDs(database, tableName string) ([]int64, error) {
	rows, err := sf.db.Query(
		`SELECT DISTINCT TIDB_PARTITION_ID
		 FROM INFORMATION_SCHEMA.PARTITIONS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TIDB_PARTITION_ID IS NOT NULL
		 ORDER BY TIDB_PARTITION_ID`, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("query partitions: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan partition id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (sf *SchemaFetcher) getColumns(database, tableName string) ([]encoder.ColumnInfo, error) {
	rows, err := sf.db.Query(
		`SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY
		 FROM INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY ORDINAL_POSITION`, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	var columns []encoder.ColumnInfo
	for rows.Next() {
		var ordinal int
		var name, dataType, columnType, columnKey string
		if err := rows.Scan(&ordinal, &name, &dataType, &columnType, &columnKey); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		columns = append(columns, encoder.ColumnInfo{
			ID:           int64(ordinal),
			Name:         name,
			MySQLType:    columnType,
			IsPrimaryKey: columnKey == "PRI",
			IsUnsigned:   strings.Contains(strings.ToLower(columnType), "unsigned"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %w", err)
	}

	if len(columns) == 0 {
		// 三态区分（M2 复审 P2）：只有「库可见但表不可见」才判 NotFound
		// （schema drift 终态 → *model.TableNotFoundError，applier 整组 DLQ
		// 并前进 offset）。抓取账号对目标 schema 无可见权限时 TABLES 也得 0，
		// 但 SCHEMATA 同样得 0 —— 此种情况误判 dropped 会整组 DLQ 静默丢
		// 数据，必须走 fatal。「表可见但无列」= schema 异常，同样 fatal。
		// 查询本身的失败在上面已按普通错误返回（fatal）。
		var schemaVisible int
		if err := sf.db.QueryRow(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?`,
			database).Scan(&schemaVisible); err != nil {
			return nil, fmt.Errorf("check schema visibility %s: %w", database, err)
		}
		if schemaVisible == 0 {
			return nil, fmt.Errorf("schema %s not visible to this account (dropped or insufficient privilege; refusing NotFound/DLQ)", database)
		}
		var n int
		if err := sf.db.QueryRow(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
			database, tableName).Scan(&n); err != nil {
			return nil, fmt.Errorf("check table existence %s.%s: %w", database, tableName, err)
		}
		if n == 0 {
			return nil, &model.TableNotFoundError{Database: database, Table: tableName}
		}
		return nil, fmt.Errorf("table %s.%s exists but reports no columns (schema anomaly)", database, tableName)
	}

	return columns, nil
}

func (sf *SchemaFetcher) getTableID(database, tableName string) (int64, error) {
	var tableID int64
	err := sf.db.QueryRow(
		`SELECT TIDB_TABLE_ID
		 FROM INFORMATION_SCHEMA.TABLES
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		database, tableName).Scan(&tableID)
	if err != nil {
		return 0, fmt.Errorf("query table id: %w", err)
	}
	return tableID, nil
}

// statusBaseURL 从 DSN 推导 TiDB status API 基址（默认端口 10080）。
// 环境变量 TIDB_STATUS_ADDR（如 http://10.0.0.1:10080）可覆盖，DSN 解析
// 失败返回空串（真实列 ID 随之不可用）。
func statusBaseURL(dsn string) string {
	if u := os.Getenv("TIDB_STATUS_ADDR"); u != "" {
		return u
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil || cfg.Addr == "" {
		return ""
	}
	host := cfg.Addr
	if h, _, err := net.SplitHostPort(host); err == nil && h != "" {
		host = h
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(defaultStatusPort))
}

// statusTableInfo / statusColumnInfo 是 GET /schema/{db}/{table} 返回的
// table.Info 子集（列名是 CIStr：O=原始大小写，L=小写）。
type statusColumnInfo struct {
	ID    int64 `json:"id"`
	Name  struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"name"`
	State statusColumnState `json:"state"`
}

type statusTableInfo struct {
	Columns []statusColumnInfo `json:"cols"`
}

// statusColumnState 兼容各版本 status API 的列状态形态（L5）：TiDB v8.5.8
// 实测返回数字（public=5，即 model.StatePublic），另有构建给字符串
// （"public"），232 端则整体省略该字段。字段缺省的零值视为 public
// （information_schema 只暴露 public 列，名字对不上会走全有或全无兜底）。
type statusColumnState int8

const (
	stateUnset     statusColumnState = 0  // wire 上省略
	statePublicNum statusColumnState = 5  // TiDB model.StatePublic
	stateNonPublic statusColumnState = -1 // 明确非 public 的字符串形态
)

func (s *statusColumnState) UnmarshalJSON(b []byte) error {
	t := strings.TrimSpace(string(b))
	if t == "" || t == "null" {
		*s = stateUnset
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err == nil {
		*s = statusColumnState(n)
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err == nil {
		if strings.EqualFold(str, "public") {
			*s = statePublicNum
		} else {
			*s = stateNonPublic
		}
		return nil
	}
	*s = stateUnset
	return nil
}

func (s statusColumnState) isPublic() bool {
	return s == stateUnset || s == statePublicNum
}

// applyRealColumnIDs 用 status API 的真实列 ID 覆盖 columns 里的序号
// 兜底（L4）：information_schema 不暴露列 ID，序号与真实 ID 在经历过
// DROP COLUMN / ADD COLUMN 的表上错位，而 rowcodec 解码按列 ID 对齐。
// 全有或全无：任一列对不上即整体放弃（混合 ID 集会解出脏数据），
// 调用方回退序号口径并依赖 D13 守卫拒绝危险扫描。
func (sf *SchemaFetcher) applyRealColumnIDs(database, tableName string, columns []encoder.ColumnInfo) error {
	if sf.statusURL == "" {
		return fmt.Errorf("status address unknown")
	}
	u := fmt.Sprintf("%s/schema/%s/%s",
		strings.TrimSuffix(sf.statusURL, "/"), url.PathEscape(database), url.PathEscape(tableName))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("query %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("query %s: %s", u, resp.Status)
	}
	var ti statusTableInfo
	if err := json.NewDecoder(resp.Body).Decode(&ti); err != nil {
		return fmt.Errorf("decode %s: %w", u, err)
	}

	byName := make(map[string]int64, len(ti.Columns))
	for _, c := range ti.Columns {
		// 只认 public 列（DDL 中间态列不参与解码）。
		if !c.State.isPublic() {
			continue
		}
		name := c.Name.O
		if name == "" {
			name = c.Name.L
		}
		if name == "" {
			continue
		}
		byName[strings.ToLower(name)] = c.ID
	}
	if len(byName) == 0 {
		return fmt.Errorf("status schema reports no public columns")
	}

	// 先全部解析成功再一次性写回（中途失败不得留下半覆盖的 ID 集）。
	seen := make(map[int64]bool, len(columns))
	ids := make([]int64, len(columns))
	for i := range columns {
		id, ok := byName[strings.ToLower(columns[i].Name)]
		if !ok {
			return fmt.Errorf("column %s missing in status schema", columns[i].Name)
		}
		if seen[id] {
			return fmt.Errorf("duplicate column id %d from status schema", id)
		}
		seen[id] = true
		ids[i] = id
	}
	for i := range columns {
		columns[i].ID = ids[i]
	}
	return nil
}

func BuildDSN(host, port, user, password string) string {
	addr := host
	if !strings.Contains(addr, ":") {
		addr = addr + ":" + port
	}
	return fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=true&loc=Local&timeout=10s",
		user, password, addr)
}

// VerificationResult holds TiDB connection verification details.
type VerificationResult struct {
	Version string
	User    string
	Grants  []string
}

// VerifyConnection verifies TiDB credentials and connection by pinging the server,
// querying the version, and fetching the current user's grants.
// Returns a VerificationResult on success.
func VerifyConnection(dsn string) (*VerificationResult, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("authentication failed: %w", err)
	}

	var version string
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		return nil, fmt.Errorf("query version: %w", err)
	}

	var currentUser string
	if err := db.QueryRow("SELECT CURRENT_USER()").Scan(&currentUser); err != nil {
		return nil, fmt.Errorf("query current user: %w", err)
	}

	grants, _ := queryGrants(db) // non-fatal

	return &VerificationResult{
		Version: version,
		User:    currentUser,
		Grants:  grants,
	}, nil
}

// VerifyTableAccess verifies SELECT permission on a specific table by running
// SELECT * FROM database.table LIMIT 0.
func VerifyTableAccess(dsn string, database, tableName string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 0", database, tableName)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("SELECT permission denied on %s.%s: %w", database, tableName, err)
	}
	rows.Close()
	return nil
}

func queryGrants(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW GRANTS FOR CURRENT_USER()")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}
