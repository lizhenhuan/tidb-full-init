// Package sink 定义写入目标端的接口（设计文档 D8）。
// 一期实现：oraclesink（幂等 MERGE 批量写）、mocksink（测试）。
package sink

import (
	"context"
	"errors"

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
// WriteBatch 失败契约（M1.5 P0#1 定稿 + 跟进复审 P1#1 三态化，D11）：
//   - 单批 = 单个目标端事务；
//   - retryable（网络/锁/ctx 取消，IsRetryable(err)=true）→ 整体回滚并返回
//     错误；语义是"本批结果未知"（ctx 取消下事务可能未完全回滚），applier
//     必须整批重放，绝不部分续传，error+Applied=0 不代表什么都没写；
//   - row-data（行级数据错误，IsRowData(err)=true）→ 记 RowFailure
//     （Retryable=false）跳过该行，其余行照常提交，applier 写 DLQ 后前进 offset；
//   - fatal（其余一切错误）→ 返回错误，applier 中止任务、不前进 offset；
//   - Applied 只统计真正提交的行。
type Sink interface {
	// Prepare 确保目标表存在（建表走 dialect.RenderCreateTable，D5）。
	Prepare(ctx context.Context, meta model.TableMeta) error

	// WriteBatch 幂等批量写入。UPDATE 的 PK 变更事件由实现按
	// DELETE(旧 PK)+MERGE(新行) 拆分应用（D2）。
	WriteBatch(ctx context.Context, meta model.TableMeta, events []model.RowEvent) (FlushResult, error)

	Close() error
}

// retryableError 实现方（如 oraclesink.WriteError）以此暴露可重试标记。
type retryableError interface {
	error
	IsRetryable() bool
}

// IsRetryable 判断 WriteBatch 返回的错误是否为可整批重试的暂时性错误
// （网络/锁/ctx 取消）。走 errors.As 语义的链式查找；未实现标记的错误
// 一律视为不可重试（宁可停任务排查，不可无界重放）。
func IsRetryable(err error) bool {
	var r retryableError
	return errors.As(err, &r) && r.IsRetryable()
}

// rowError 实现方以此暴露"行级数据错误"标记（可跳行 DLQ 并前进 offset）。
type rowError interface {
	error
	IsRowData() bool
}

// IsRowData 判断错误是否为行级数据错误（值转换/宽度/约束类白名单）。
// 与 IsRetryable 皆 false = fatal：中止任务、不前进 offset。
func IsRowData(err error) bool {
	var r rowError
	return errors.As(err, &r) && r.IsRowData()
}
