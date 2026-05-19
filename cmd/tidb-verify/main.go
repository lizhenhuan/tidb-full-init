package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

var (
	pdAddress  string
	database   string
	tableName  string
	outputFile string
	username   string
	password   string
)

func init() {
	flag.StringVar(&pdAddress, "addr", "148.70.194.28:4000", "TiDB address (host:port)")
	flag.StringVar(&database, "database", "test", "Database name")
	flag.StringVar(&tableName, "table", "", "Table name (required)")
	flag.StringVar(&outputFile, "output", "", "Output file path (default: stdout)")
	flag.StringVar(&username, "username", "root", "TiDB username")
	flag.StringVar(&password, "password", "", "TiDB password")
}

func main() {
	flag.Parse()

	if tableName == "" {
		log.Fatal("-table is required")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local&timeout=5s",
		username, password, pdAddress, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Open connection: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Ping: %v", err)
	}
	log.Printf("Connected to TiDB %s/%s", pdAddress, database)

	columns, err := getTableSchema(db, database, tableName)
	if err != nil {
		log.Fatalf("Get schema: %v", err)
	}
	log.Printf("Table %s.%s: %d columns", database, tableName, len(columns))

	tableInfo := &encoder.TableInfo{
		Database: database,
		Table:    tableName,
		Columns:  columns,
	}
	for _, col := range columns {
		if col.IsPrimaryKey {
			tableInfo.PKNames = append(tableInfo.PKNames, col.Name)
		}
	}

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`.`%s`", database, tableName))
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		log.Fatalf("Get columns: %v", err)
	}

	var writer *os.File
	if outputFile != "" {
		writer, err = os.Create(outputFile)
		if err != nil {
			log.Fatalf("Create output file: %v", err)
		}
		defer writer.Close()
	} else {
		writer = os.Stdout
	}

	count := 0

	for rows.Next() {
		values := make([]interface{}, len(colNames))
		valuePtrs := make([]interface{}, len(colNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Scan row error: %v", err)
			continue
		}

		rowData := make(map[string]string)
		for i, col := range colNames {
			rowData[col] = formatColumnValue(values[i], getMySQLTypeForColumn(columns, col))
		}

		msg := encoder.NewInsertMessage(tableInfo)
		msg.BuildTypeMapping(columns)
		msg.SetRowData(rowData)

		writeJSON(writer, msg)
		count++
	}

	log.Printf("Done: %d rows exported for %s.%s", count, database, tableName)
}

func getTableSchema(db *sql.DB, database, tableName string) ([]encoder.ColumnInfo, error) {
	rows, err := db.Query(
		`SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY
		 FROM INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY ORDINAL_POSITION`, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer rows.Close()

	var columns []encoder.ColumnInfo
	for rows.Next() {
		var name, dataType, columnType, columnKey string
		if err := rows.Scan(&name, &dataType, &columnType, &columnKey); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		columns = append(columns, encoder.ColumnInfo{
			Name:         name,
			MySQLType:    columnType,
			IsPrimaryKey: columnKey == "PRI",
			IsUnsigned:   strings.Contains(strings.ToLower(columnType), "unsigned"),
		})
	}
	return columns, rows.Err()
}

func getMySQLTypeForColumn(columns []encoder.ColumnInfo, colName string) string {
	for _, c := range columns {
		if c.Name == colName {
			return c.MySQLType
		}
	}
	return "varchar"
}

func formatColumnValue(val interface{}, mysqlType string) string {
	if val == nil {
		return ""
	}

	switch v := val.(type) {
	case []byte:
		str := string(v)
		if encoder.IsBinaryType(mysqlType) {
			return encoder.EncodeBinaryValue(v)
		}
		return str
	case time.Time:
		lower := strings.ToLower(mysqlType)
		switch {
		case strings.HasPrefix(lower, "datetime"), strings.HasPrefix(lower, "timestamp"):
			return v.Format("2006-01-02 15:04:05")
		case strings.HasPrefix(lower, "date"):
			return v.Format("2006-01-02")
		case strings.HasPrefix(lower, "time"):
			return v.Format("15:04:05")
		default:
			return v.Format("2006-01-02 15:04:05")
		}
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		if strings.Contains(strings.ToLower(mysqlType), "decimal") {
			return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%f", v), "0"), ".")
		}
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func writeJSON(w *os.File, msg *encoder.CanalMessage) {
	data, err := msg.ToJSON()
	if err != nil {
		log.Printf("Marshal error: %v", err)
		return
	}
	fmt.Fprintln(w, string(data))
}

func init() {
	_ = json.Marshal
}
