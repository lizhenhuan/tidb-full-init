package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer   *kafka.Writer
	sent     atomic.Int64
	once     sync.Once
}

type KafkaProducerConfig struct {
	Brokers       string
	Topic         string
	Acks          int
	Compression   string
	BatchBytes    int
	BatchSize     int
	LingerMs      int
	MaxAttempts   int
	WriteTimeoutMs int // 单次写超时（默认 30000；broker 不 ack 时报错而非无限等）
	BatchTimeoutMs int // 未满批强制冲刷间隔（默认 500）
}

func NewKafkaProducer(cfg KafkaProducerConfig) (*KafkaProducer, error) {
	brokers := strings.Split(cfg.Brokers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}

	acks := kafka.RequireAll
	switch cfg.Acks {
	case 0:
		acks = kafka.RequireNone
	case 1:
		acks = kafka.RequireOne
	}

	compression := kafka.Compression(0)
	switch strings.ToLower(cfg.Compression) {
	case "snappy":
		compression = kafka.Snappy
	case "gzip":
		compression = kafka.Gzip
	case "lz4":
		compression = kafka.Lz4
	case "zstd":
		compression = kafka.Zstd
	}

	// 默认 1：Send 是逐条同步写，批凑不满时 kafka-go 只等 batch.timer
	// （每条白等一个 BatchTimeout，#t6 复审实测）。需要攒批的调用方应
	// 自己一次 WriteMessages 多条，而不是调大这里的 BatchSize。
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	writeTimeout := time.Duration(cfg.WriteTimeoutMs) * time.Millisecond
	if writeTimeout <= 0 {
		writeTimeout = 30 * time.Second
	}
	batchTimeout := time.Duration(cfg.BatchTimeoutMs) * time.Millisecond
	if batchTimeout <= 0 {
		batchTimeout = 500 * time.Millisecond
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		RequiredAcks: acks,
		Compression:  compression,
		MaxAttempts:  maxAttempts,
		// 超时兜底（#t6 复审 P0-2）：调用方 ctx 无 deadline 时，
		// broker 停止 ack 会让 WriteMessages 永久挂在 <-batch.done。
		// WriteTimeout 是整次写（含重试）的绝对 deadline。
		WriteTimeout: writeTimeout,
		BatchTimeout: batchTimeout,
		Async:        false,
	}

	log.Printf("[kafka] producer created: brokers=%v topic=%s batch=%d compression=%s",
		brokers, cfg.Topic, batchSize, cfg.Compression)

	return &KafkaProducer{writer: w}, nil
}

// Send 发送一条消息。超时由 Writer.WriteTimeout 兜底（调用方 ctx 无
// deadline 时 30s；broker 不 ack 报错而非永久阻塞）。
func (p *KafkaProducer) Send(ctx context.Context, msg *Message) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
	})
	if err != nil {
		return fmt.Errorf("kafka write: %w", err)
	}
	p.sent.Add(1)
	return nil
}

// EnsureTopic creates a Kafka topic if it does not exist.
func EnsureTopic(brokers string, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", brokers)
	if err != nil {
		return fmt.Errorf("connect to kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get kafka controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("connect to kafka controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// TopicAlreadyExists is not an error in our case
		if !strings.Contains(err.Error(), "TopicExists") &&
			!strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
		log.Printf("[kafka] topic %s already exists, skipping creation", topic)
		return nil
	}

	log.Printf("[kafka] topic %s created (partitions=%d)", topic, partitions)
	return nil
}

func (p *KafkaProducer) Close() error {
	var err error
	p.once.Do(func() {
		err = p.writer.Close()
		log.Printf("[kafka] producer closed, sent=%d", p.sent.Load())
	})
	return err
}
