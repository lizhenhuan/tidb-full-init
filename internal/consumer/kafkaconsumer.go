// Kafka Consumer 实现（M2 T2.1，设计文档 D3/D10）：手动分区分配，
// 不用 consumer group 自动 rebalance；SQLite 为唯一权威位点。
//
// 实现方式：每个 (topic, partition) 一个 kafka-go Reader（无 GroupID），
// 各自 goroutine 泵入统一 channel，Fetch 从 channel 出队。
// 启动位点由调用方（pipeline）从 OffsetStore 读取后经 Assign 的
// startOffsets 指定（值为"下一条待消费 offset" = 已应用 offset + 1）。
package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// KafkaConfig Kafka 消费配置。
type KafkaConfig struct {
	Brokers  []string
	MinBytes int // 每次拉取最小字节数（0 用 kafka-go 默认 1B）
	MaxBytes int // 每次拉取最大字节数（0 用 kafka-go 默认 10MB）
}

// KafkaConsumer 手动分区消费者（D3：并行单位 = Kafka 分区，分区内严格有序）。
type KafkaConsumer struct {
	cfg    KafkaConfig
	client *kafka.Client

	mu           sync.Mutex
	readers      []*kafka.Reader
	msgs         chan kafka.Message
	wg           sync.WaitGroup
	cancel       context.CancelFunc
	closed       bool
	startOffsets map[model.TopicPartition]int64
}

// NewKafkaConsumer 创建消费者。Assign 之前 Fetch 会阻塞。
func NewKafkaConsumer(cfg KafkaConfig) *KafkaConsumer {
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = 1
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 << 20
	}
	return &KafkaConsumer{
		cfg:          cfg,
		client:       &kafka.Client{Addr: kafka.TCP(cfg.Brokers...)},
		msgs:         make(chan kafka.Message, 1024),
		startOffsets: make(map[model.TopicPartition]int64),
	}
}

// SetStartOffsets 设置各分区起始位点，必须在 Assign 之前调用。
// 值 = 该分区下一条待消费的 offset（已应用 offset + 1；
// 无历史位点传 0 从头消费）。
func (c *KafkaConsumer) SetStartOffsets(next map[model.TopicPartition]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for tp, off := range next {
		c.startOffsets[tp] = off
	}
}

// Assign 手动分配分区（Consumer 接口）。每个分区一个 reader，
// 起始位点取 SetStartOffsets 的值（默认 0）。
func (c *KafkaConsumer) Assign(ctx context.Context, partitions []model.TopicPartition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("kafkaconsumer: already closed")
	}
	if len(partitions) == 0 {
		return fmt.Errorf("kafkaconsumer: no partitions to assign")
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	for _, tp := range partitions {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   c.cfg.Brokers,
			Topic:     tp.Topic,
			Partition: tp.Partition,
			MinBytes:  c.cfg.MinBytes,
			MaxBytes:  c.cfg.MaxBytes,
		})
		off := c.startOffsets[tp]
		if err := r.SetOffset(off); err != nil {
			r.Close()
			cancel()
			return fmt.Errorf("kafkaconsumer: seek %s/%d to %d: %w", tp.Topic, tp.Partition, off, err)
		}
		c.readers = append(c.readers, r)
		c.wg.Add(1)
		go func(rd *kafka.Reader) {
			defer c.wg.Done()
			for {
				m, err := rd.FetchMessage(runCtx)
				if err != nil {
					return // ctx 取消或 reader 关闭
				}
				select {
				case c.msgs <- m:
				case <-runCtx.Done():
					return
				}
			}
		}(r)
	}
	c.cancel = cancel
	return nil
}

// Fetch 拉取下一条消息（任意已分配分区）；ctx 取消时返回 ctx.Err()。
// 分区内顺序由单 Reader 保证；跨分区无序（D3：按分区并行）。
func (c *KafkaConsumer) Fetch(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case m, ok := <-c.msgs:
		if !ok {
			return Message{}, fmt.Errorf("kafkaconsumer: closed")
		}
		return Message{
			TopicPartition: model.TopicPartition{Topic: m.Topic, Partition: m.Partition},
			Offset:         m.Offset,
			Key:            m.Key,
			Value:          m.Value,
		}, nil
	}
}

// Commit best-effort 提交 Kafka offset（仅观测用，非权威位点，D10）。
// 一期手动分配模式无 consumer group，broker 不存 offset：本实现为 no-op，
// lag 观测走 HighWatermark 与权威位点（SQLite）的差值。
func (c *KafkaConsumer) Commit(context.Context, Message) error { return nil }

// HighWatermark 返回分区高水位（lag = HW - 已应用 offset - 1，D9/D10）。
func (c *KafkaConsumer) HighWatermark(ctx context.Context, tp model.TopicPartition) (int64, error) {
	resp, err := c.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			tp.Topic: []kafka.OffsetRequest{kafka.LastOffsetOf(tp.Partition)},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("kafkaconsumer list offsets: %w", err)
	}
	part := resp.Topics[tp.Topic]
	if len(part) == 0 {
		return 0, fmt.Errorf("kafkaconsumer: no offset info for %s/%d", tp.Topic, tp.Partition)
	}
	if part[0].Error != nil {
		return 0, fmt.Errorf("kafkaconsumer list offsets %s/%d: %w", tp.Topic, tp.Partition, part[0].Error)
	}
	return part[0].LastOffset + 1, nil // LastOffset=LEO-1（最后一条消息），高水位=LEO
}

// Close 停止所有分区 reader 并释放连接。
func (c *KafkaConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	for _, r := range c.readers {
		r.Close() // 关闭会使阻塞中的 FetchMessage 返回错误
	}
	c.wg.Wait()
	close(c.msgs)
	return nil
}

var _ Consumer = (*KafkaConsumer)(nil)
