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
	// Raw 原始消息体，由 pipeline 填充，供 DLQ 落库与前端死信列表展示。
	Raw []byte
	// Retryable=true 为暂时性错误（网络/锁），原地退避重试；
	// false 为数据错误，写 DLQ 并前进 offset。
	Retryable bool
}

// FlushResult 一批写入的结果。
type FlushResult struct {
	Applied int          // 本事务真正提交的行数（数据错误行跳过不计）
	Failed  []RowFailure // 行级失败明细
}

// Sink 写入目标。实现必须幂等：同一批事件重放不改变终态（D2）。
//
// WriteBatch 失败契约（M1.5 P0#1 定稿，D11）：
//   - 单批 = 单个目标端事务；
//   - 可重试错误（网络/锁等）→ 整体回滚，返回 *WriteError（或同语义错误），
//     由 applier 退避重试整批；
//   - 数据错误 → 记 RowFailure（Retryable=false）跳过该行，其余行照常提交；
//   - Applied 只统计真正提交的行。
type Sink interface {
	// Prepare 确保目标表存在（建表走 dialect.RenderCreateTable，D5）。
	Prepare(ctx context.Context, meta model.TableMeta) error

	// WriteBatch 幂等批量写入。UPDATE 的 PK 变更事件由实现按
	// DELETE(旧 PK)+MERGE(新行) 拆分应用（D2）。
	WriteBatch(ctx context.Context, meta model.TableMeta, events []model.RowEvent) (FlushResult, error)

	Close() error
}
