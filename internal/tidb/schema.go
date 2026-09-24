package tidb

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

type SchemaFetcher struct {
	db *sql.DB
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

	return &SchemaFetcher{db: db}, nil
}

func (sf *SchemaFetcher) Close() error {
	return sf.db.Close()
}

func (sf *SchemaFetcher) GetTableInfo(database, tableName string) (*encoder.TableInfo, int64, error) {
	columns, err := sf.getColumns(database, tableName)
	if err != nil {
		return nil, 0, err
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
	}

	log.Printf("[schema] %s.%s: tableID=%d, columns=%d, pk=%v",
		database, tableName, tableID, len(columns), pkNames)

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
		// 区分「表不存在」（schema drift 终态 → *model.TableNotFoundError，
		// applier 整组 DLQ 并前进 offset）与「表存在但无列」（schema 异常 →
		// fatal）；查询本身的失败在上面已按普通错误返回（fatal）。
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
