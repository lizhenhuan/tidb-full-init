// Package dialect 定义目标端方言接口。一期实现 oracle，
// 预留 sqlserver / db2（新增实现即可接入，见设计文档 D8）。
package dialect

import (
	"context"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// SchemaChange 描述一次目标端结构变更（二期 DDL 同步使用，一期只定义）。
type SchemaChange struct {
	Database  string
	Table     string
	AddColumns    []model.ColumnMeta
	DropColumns   []string
	ModifyColumns []model.ColumnMeta
}

// TargetDialect 目标数据库方言：类型映射与 SQL 渲染。
type TargetDialect interface {
	// Name 返回方言标识："oracle" / "sqlserver" / "db2"。
	Name() string

	// QuoteIdentifier 标识符引用，必须处理保留字与注入（如内嵌引号转义）。
	QuoteIdentifier(name string) string

	// RenderCreateTable 由 TiDB 权威 TableMeta 生成建表 DDL（设计文档 D5）。
	RenderCreateTable(meta model.TableMeta) (string, error)

	// RenderUpsert 生成按 PK 的幂等 upsert 语句（Oracle: MERGE INTO）。
	RenderUpsert(meta model.TableMeta) (string, error)

	// RenderDelete 生成按 PK 的删除语句。
	RenderDelete(meta model.TableMeta) (string, error)

	// RenderAlterTable 二期 schema evolution 预留（一期定义不实现，
	// 实现方一期可返回 ErrNotImplemented）。
	RenderAlterTable(meta model.TableMeta, change SchemaChange) (string, error)

	// ValidateTable 任务创建前的表级校验（如一期拒绝 ENUM/SET 列，
	// 见设计文档 D4）。
	ValidateTable(meta model.TableMeta) error

	// Prepare 连接级准备（如 session 字符集设置），由 sink 在打开后调用。
	Prepare(ctx context.Context) error
}

// ErrNotImplemented 二期功能占位。
var ErrNotImplemented = errNotImplemented{}

type errNotImplemented struct{}

func (errNotImplemented) Error() string { return "dialect: not implemented in phase 1" }
