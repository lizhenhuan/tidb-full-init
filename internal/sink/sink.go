// Package sink 定义写入目标端的接口（设计文档 D8）。
// 一期实现：oraclesink（幂等 MERGE 批量写）、mocksink（测试）。
package sink

import (
	"context"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// RowFailure 行级失败（设计文档 D11：可重试 vs DLQ）。
type RowFailure struct {
	Topic     string
	Partition int
	Offset    int64
	Table     string
	Reason    string
	// Retryable=true 为暂时性错误（网络/锁），原地退避重试；
	// false 为数据错误，写 DLQ 并前进 offset。
	Retryable bool
}

// FlushResult 一批写入的结果。
type FlushResult struct {
	Applied int          // 成功应用的行数
	Failed  []RowFailure // 行级失败明细
}

// Sink 写入目标。实现必须幂等：同一批事件重放不改变终态（D2）。
// WriteBatch 内部保证单批 = 单个目标端事务（D11）。
type Sink interface {
	// Prepare 确保目标表存在（建表走 dialect.RenderCreateTable，D5）。
	Prepare(ctx context.Context, meta model.TableMeta) error

	// WriteBatch 幂等批量写入。UPDATE 的 PK 变更事件由实现按
	// DELETE(旧 PK)+MERGE(新行) 拆分应用（D2）。
	WriteBatch(ctx context.Context, meta model.TableMeta, events []model.RowEvent) (FlushResult, error)

	Close() error
}
