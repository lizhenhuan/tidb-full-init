package syncer

import (
	"github.com/lizhenhuan/tidb-full-init/internal/applier"
	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// FetchTableInfo 表结构来源（*tidb.SchemaFetcher 满足此签名；
// 抽成函数类型便于测试注入）。
type FetchTableInfo func(db, table string) (*encoder.TableInfo, int64, error)

// MetaProvider 把表结构来源适配为 applier.MetaProvider（D5：TiDB
// SchemaFetcher 为权威来源）。TargetSchema 即「库文名→Oracle 目标
// schema」的任务级映射（P0#4），为空回落 Database。
func MetaProvider(fetch FetchTableInfo, targetSchema string) applier.MetaProvider {
	return func(db, table string) (model.TableMeta, error) {
		info, _, err := fetch(db, table)
		if err != nil {
			return model.TableMeta{}, err
		}
		return TableMetaFrom(info, targetSchema), nil
	}
}

// TableMetaFrom encoder.TableInfo → model.TableMeta。
// SQLType 留 0：Oracle dialect 的类型映射只消费 MySQLType（兜底字段）。
func TableMetaFrom(info *encoder.TableInfo, targetSchema string) model.TableMeta {
	meta := model.TableMeta{
		Database:     info.Database,
		Table:        info.Table,
		TargetSchema: targetSchema,
		PKNames:      info.PKNames,
		Columns:      make([]model.ColumnMeta, len(info.Columns)),
	}
	for i, col := range info.Columns {
		meta.Columns[i] = model.ColumnMeta{
			Name:      col.Name,
			MySQLType: col.MySQLType,
			IsPK:      col.IsPrimaryKey,
		}
	}
	return meta
}
