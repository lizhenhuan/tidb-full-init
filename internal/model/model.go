// Package model 定义消费链路的共享事件与元数据模型。
// 单独成包以避免 decoder/sink/dialect 之间的循环依赖。
package model

// OpType 事件操作类型。
type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
	OpDDL    OpType = "DDL"
)

// RowEvent 是 canal-json 消息解码后的统一行级事件。
// Before/After 保留 canal-json 的字符串呈现（NULL 呈现为空字符串，
// 与 Oracle 空串即 NULL 的语义一致，见设计文档 D2/D8）。
type RowEvent struct {
	Op        OpType
	Database  string
	Table     string
	PKNames   []string
	Before    map[string]string // 变更前镜像（canal-json old；仅 UPDATE/部分 DELETE）
	After     map[string]string // 变更后镜像（canal-json data；DELETE 时为被删行）
	SQLType   map[string]int    // java.sql.Types 编码
	MySQLType map[string]string
	CommitTs  uint64 // 全量消息为 0
	Topic     string // 来源 Kafka 坐标（由 consumer 填充）
	Partition int
	Offset    int64
}

// PKChanged 判断 UPDATE 是否发生主键变更（Flink CDC 语义：
// 变更时需先 DELETE 旧 PK 再 MERGE 新行，见设计文档 D2）。
// canal-json 的 old 仅包含发生变更的列，因此 PK 未出现在 old 中即视为未变更。
func (e *RowEvent) PKChanged() bool {
	if e.Op != OpUpdate || len(e.Before) == 0 {
		return false
	}
	for _, pk := range e.PKNames {
		if old, ok := e.Before[pk]; ok && old != e.After[pk] {
			return true
		}
	}
	return false
}

// DDLEvent 一期仅识别与计数，不应用（二期启用）。
type DDLEvent struct {
	Database string
	Table    string
	SQL      string
	CommitTs uint64
	Topic    string
	Partition int
	Offset    int64
}

// TableMeta 目标表元数据（以 TiDB SchemaFetcher 为权威来源，见设计文档 D5）。
type TableMeta struct {
	Database string
	Table    string
	Columns  []ColumnMeta
	PKNames  []string
}

// ColumnMeta 列元数据。
type ColumnMeta struct {
	Name      string
	MySQLType string // 如 "varchar(255)"、"bigint unsigned"
	SQLType   int    // java.sql.Types 编码（兜底用）
	IsPK      bool
}

// TopicPartition Kafka 位点坐标。
type TopicPartition struct {
	Topic     string
	Partition int
}
