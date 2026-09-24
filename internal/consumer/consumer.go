// Package consumer 定义 Kafka 消费接口（设计文档 D3/D8/D10）。
// 一期采用手动分区分配（不用 consumer group 自动 rebalance），
// SQLite 为唯一权威位点，Kafka offset 提交仅用于观测 lag。
package consumer

import (
	"context"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// Message 一条 Kafka 消息（含坐标）。
type Message struct {
	TopicPartition model.TopicPartition
	Offset         int64
	Key            []byte
	Value          []byte
}

// Consumer Kafka 消费者抽象。
type Consumer interface {
	// Fetch 拉取下一条消息；ctx 取消时返回 ctx.Err()。
	Fetch(ctx context.Context) (Message, error)

	// Commit best-effort 提交 Kafka offset（仅观测用，非权威位点，D10）。
	Commit(ctx context.Context, msg Message) error

	Close() error
}
