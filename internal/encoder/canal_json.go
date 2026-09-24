package encoder

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type CanalMessage struct {
	ID        int64               `json:"id"`
	Database  string              `json:"database"`
	Table     string              `json:"table"`
	PKNames   []string            `json:"pkNames"`
	IsDDL     bool                `json:"isDdl"`
	Type      string              `json:"type"`
	ES        int64               `json:"es"`
	TS        int64               `json:"ts"`
	SQL       string              `json:"sql"`
	SQLType   map[string]int      `json:"sqlType"`
	MySQLType map[string]string   `json:"mysqlType"`
	Data      []map[string]string `json:"data"`
	Old       interface{}         `json:"old"`
	TiDB      *TiDBExtension      `json:"_tidb"`
}

type TiDBExtension struct {
	CommitTs    int64 `json:"commitTs"`
	WatermarkTs int64 `json:"watermarkTs,omitempty"`
}

type ColumnInfo struct {
	ID           int64
	Name         string
	MySQLType    string
	IsPrimaryKey bool
	IsUnsigned   bool
}

type TableInfo struct {
	Database string
	Table    string
	Columns  []ColumnInfo
	PKNames  []string
	// RealColumnIDs 列 ID 来自 TiDB status API 的真实 ID（而非
	// ORDINAL_POSITION 序号兜底）。此时行数据里的未知列 ID 只可能是
	// 被 DROP COLUMN 的残留，快照按跳过处理（L7）；序号兜底时保持
	// D13 严格拒绝（序号错位语义不明，跳过等于静默丢列）。
	RealColumnIDs bool
}

func NewInsertMessage(tableInfo *TableInfo) *CanalMessage {
	now := time.Now().UnixMilli()
	return &CanalMessage{
		ID:        0,
		Database:  tableInfo.Database,
		Table:     tableInfo.Table,
		PKNames:   tableInfo.PKNames,
		IsDDL:     false,
		Type:      "INSERT",
		ES:        now,
		TS:        now,
		SQL:       "",
		SQLType:   make(map[string]int),
		MySQLType: make(map[string]string),
		Data:      make([]map[string]string, 1),
		Old:       nil,
		TiDB:      &TiDBExtension{CommitTs: 0},
	}
}

func NewDDLMessage(database, sql string) *CanalMessage {
	now := time.Now().UnixMilli()
	return &CanalMessage{
		ID:        0,
		Database:  database,
		Table:     "",
		PKNames:   nil,
		IsDDL:     true,
		Type:      "QUERY",
		ES:        now,
		TS:        now,
		SQL:       sql,
		SQLType:   nil,
		MySQLType: nil,
		Data:      nil,
		Old:       nil,
		TiDB:      &TiDBExtension{CommitTs: 0},
	}
}

func (m *CanalMessage) SetSQLType(colName string, code int) {
	m.SQLType[colName] = code
}

func (m *CanalMessage) SetMySQLType(colName string, typ string) {
	m.MySQLType[colName] = typ
}

func (m *CanalMessage) SetRowData(row map[string]string) {
	m.Data[0] = row
}

func (m *CanalMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

func (m *CanalMessage) BuildTypeMapping(columns []ColumnInfo) {
	for _, col := range columns {
		mysqlType, sqlType := MapCanalJSONType(col)
		m.SetMySQLType(col.Name, mysqlType)
		m.SetSQLType(col.Name, sqlType)
	}
}

func MapCanalJSONType(col ColumnInfo) (mysqlType string, sqlTypeCode int) {
	mt := strings.ToLower(col.MySQLType)

	switch {
	case strings.HasPrefix(mt, "tinyint"):
		if col.IsUnsigned {
			return "tinyint unsigned", -6
		}
		return "tinyint", -6

	case strings.HasPrefix(mt, "smallint"):
		if col.IsUnsigned {
			return "smallint unsigned", 5
		}
		return "smallint", 5

	case strings.HasPrefix(mt, "mediumint"):
		return "mediumint", 4

	case strings.HasPrefix(mt, "int"), strings.HasPrefix(mt, "integer"):
		if col.IsUnsigned {
			return "int unsigned", 4
		}
		return "int", 4

	case strings.HasPrefix(mt, "bigint"):
		if col.IsUnsigned {
			return "bigint unsigned", -5
		}
		return "bigint", -5

	case strings.HasPrefix(mt, "float"):
		return "float", 7

	case strings.HasPrefix(mt, "double"), strings.HasPrefix(mt, "real"):
		return "double", 8

	case strings.HasPrefix(mt, "decimal"), strings.HasPrefix(mt, "numeric"):
		return "decimal", 3

	case strings.HasPrefix(mt, "varchar"):
		return "varchar", 12

	case strings.HasPrefix(mt, "char"):
		return "char", 1

	case containsAny(mt, "text", "tinytext", "mediumtext", "longtext"):
		return "text", 2005

	case containsAny(mt, "blob", "tinyblob", "mediumblob", "longblob"):
		return "blob", 2004

	case strings.HasPrefix(mt, "binary"):
		return "binary", 2004

	case strings.HasPrefix(mt, "varbinary"):
		return "varbinary", 2004

	case strings.HasPrefix(mt, "bit"):
		return "bit", -7

	case strings.HasPrefix(mt, "datetime"):
		return "datetime", 93

	case strings.HasPrefix(mt, "timestamp"):
		return "timestamp", 93

	case strings.HasPrefix(mt, "date"):
		return "date", 91

	case strings.HasPrefix(mt, "time"):
		return "time", 92

	case strings.HasPrefix(mt, "year"):
		return "year", 12

	case strings.HasPrefix(mt, "enum"):
		return "enum", 4

	case strings.HasPrefix(mt, "set"):
		return "set", -7

	case strings.HasPrefix(mt, "json"):
		return "json", 12

	case strings.HasPrefix(mt, "bool"), strings.HasPrefix(mt, "boolean"):
		return "tinyint", -6

	default:
		return mt, 12
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// EncodeBinaryValue encodes binary data as Base64 for Canal JSON compatibility.
// Flink Canal JSON format expects BLOB/BINARY values to be Base64-encoded.
func EncodeBinaryValue(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// encodeBinaryEscape encodes binary data as escaped hex string (legacy format).
func encodeBinaryEscape(data []byte) string {
	var sb strings.Builder
	for _, b := range data {
		switch {
		case b == '\t':
			sb.WriteString(`\t`)
		case b == '\n':
			sb.WriteString(`\n`)
		case b == '\r':
			sb.WriteString(`\r`)
		case b == '&':
			sb.WriteString(`\u0026`)
		case b == '<':
			sb.WriteString(`\u003c`)
		case b == '>':
			sb.WriteString(`\u003e`)
		case b < 32:
			sb.WriteString(fmt.Sprintf(`\u%04x`, b))
		case b >= 32 && b <= 127:
			sb.WriteByte(b)
		default:
			sb.WriteByte(b)
		}
	}
	return sb.String()
}

func IsBinaryType(mysqlType string) bool {
	lower := strings.ToLower(mysqlType)
	return strings.Contains(lower, "blob") ||
		strings.Contains(lower, "binary") ||
		strings.HasPrefix(lower, "bit")
}

// isBitType BIT 列（数值语义：dialect 映射 Oracle NUMBER），位串按
// 无符号整数呈现，不走 binary base64（见 FormatValue）。
func isBitType(mysqlType string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(mysqlType)), "bit")
}

func FormatValue(mysqlType string, value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case []byte:
		if isBitType(mysqlType) {
			// L11：BIT 文本协议读出的是大端位串（bit(16)=0x0100 →
			// []byte{0x01,0x00}），必须按无符号整数输出十进制——与
			// snapshot rowcodec 路径（decodeUintLE）、verify KindBit 同
			// 口径。此前误走 IsBinaryType→base64，sink 把 base64 串绑给
			// Oracle NUMBER 列抛 ORA-01722，UPDATE 整行进 DLQ。
			var u uint64
			for _, b := range v {
				u = u<<8 | uint64(b)
			}
			return strconv.FormatUint(u, 10)
		}
		if IsBinaryType(mysqlType) {
			return EncodeBinaryValue(v)
		}
		return string(v)
	case string:
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		// L8a：layout 用 .999999（自动省略尾零）——datetime(6)/timestamp(6)
		// 的微秒不能丢，丢了对端 verify 即假 DIFF（比对侧 canonicalTime
		// 同样截尾零，两侧口径一致）。
		if strings.HasPrefix(strings.ToLower(mysqlType), "datetime") ||
			strings.HasPrefix(strings.ToLower(mysqlType), "timestamp") {
			return v.Format("2006-01-02 15:04:05.999999")
		}
		if strings.HasPrefix(strings.ToLower(mysqlType), "date") {
			return v.Format("2006-01-02")
		}
		if strings.HasPrefix(strings.ToLower(mysqlType), "time") {
			return v.Format("15:04:05.999999")
		}
		return v.Format("2006-01-02 15:04:05.999999")
	default:
		return fmt.Sprintf("%v", v)
	}
}

// NewUpdateMessage 增量 UPDATE 消息（#t6 增量驱动用；与 TiCDC 形状一致：
// Data=新整行，Old=仅变更列的旧值）。
func NewUpdateMessage(tableInfo *TableInfo) *CanalMessage {
	m := NewInsertMessage(tableInfo)
	m.Type = "UPDATE"
	return m
}

// NewDeleteMessage 增量 DELETE 消息（Data=被删整行，Old=nil）。
func NewDeleteMessage(tableInfo *TableInfo) *CanalMessage {
	m := NewInsertMessage(tableInfo)
	m.Type = "DELETE"
	return m
}

// SetOldData 设置变更前数据（UPDATE=变更列旧值；DELETE 不需要）。
func (m *CanalMessage) SetOldData(old map[string]string) {
	m.Old = old
}
