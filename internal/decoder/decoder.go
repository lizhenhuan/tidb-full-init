// Package decoder 将 Kafka 中的 canal-json 消息解码为统一事件模型。
// 兼容两种生产者：本仓库 tidb-full-init 全量快照消息（INSERT-only、commitTs=0）
// 与 TiCDC changefeed canal-json 增量消息（INSERT/UPDATE/DELETE/QUERY DDL）。
package decoder

import (
	"encoding/json"
	"fmt"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// canalMessage 与 encoder.CanalMessage 字段兼容，独立定义避免反向依赖
// encoder 包（消费侧不应对生产侧产生编译耦合）。
type canalMessage struct {
	ID        int64                `json:"id"`
	Database  string               `json:"database"`
	Table     string               `json:"table"`
	PKNames   []string             `json:"pkNames"`
	IsDDL     bool                 `json:"isDdl"`
	Type      string               `json:"type"`
	ES        int64                `json:"es"`
	TS        int64                `json:"ts"`
	SQL       string               `json:"sql"`
	SQLType   map[string]int       `json:"sqlType"`
	MySQLType map[string]string    `json:"mysqlType"`
	Data      []map[string]*string `json:"data"` // TiCDC 中 NULL 呈现为 JSON null
	Old       map[string]*string   `json:"old"`  // TiCDC UPDATE 仅含变更列
	TiDB      *tidbExt              `json:"_tidb"`
}

type tidbExt struct {
	CommitTs    int64 `json:"commitTs"`
	WatermarkTs int64 `json:"watermarkTs"`
}

// CanalJSON 解码 canal-json 消息体（不含 Kafka 坐标）。
// 返回 *model.RowEvent（DML）或 *model.DDLEvent（isDdl=true）。
func CanalJSON(value []byte) (any, error) {
	var m canalMessage
	if err := json.Unmarshal(value, &m); err != nil {
		return nil, fmt.Errorf("canal-json unmarshal: %w", err)
	}
	if m.Database == "" || (m.Table == "" && !m.IsDDL) {
		return nil, fmt.Errorf("canal-json missing database/table: type=%q", m.Type)
	}

	var commitTs uint64
	if m.TiDB != nil && m.TiDB.CommitTs > 0 {
		commitTs = uint64(m.TiDB.CommitTs)
	}

	if m.IsDDL || m.Type == "QUERY" {
		return &model.DDLEvent{
			Database: m.Database, Table: m.Table, SQL: m.SQL, CommitTs: commitTs,
		}, nil
	}

	switch m.Type {
	case "INSERT", "UPDATE", "DELETE":
	default:
		return nil, fmt.Errorf("canal-json unsupported type %q", m.Type)
	}
	if len(m.Data) != 1 {
		return nil, fmt.Errorf("canal-json expects exactly 1 row, got %d (type=%q)", len(m.Data), m.Type)
	}

	ev := &model.RowEvent{
		Op:        model.OpType(m.Type),
		Database:  m.Database,
		Table:     m.Table,
		PKNames:   m.PKNames,
		After:     derefRow(m.Data[0]),
		Before:    derefRow(m.Old),
		SQLType:   m.SQLType,
		MySQLType: m.MySQLType,
		CommitTs:  commitTs,
	}
	if len(ev.PKNames) == 0 {
		return nil, fmt.Errorf("canal-json table %s.%s has no pkNames (一期要求主键表)", m.Database, m.Table)
	}
	return ev, nil
}

// derefRow 将 *string 行转换为 string 行；JSON null → 空字符串
// （Oracle 空串即 NULL，语义无损）。null 列保留为空值键，
// 供 sink 按列名绑定，避免误判"缺列"。
func derefRow(row map[string]*string) map[string]string {
	if row == nil {
		return nil
	}
	out := make(map[string]string, len(row))
	for k, v := range row {
		if v != nil {
			out[k] = *v
		} else {
			out[k] = "" // JSON null → 空串（Oracle 空串即 NULL）
		}
	}
	return out
}
