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
	AsyncMode     bool
}

func NewKafkaProducer(cfg KafkaProducerConfig) (*KafkaProducer, error) {
	brokers := normalizeBrokers(cfg.Brokers)

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

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		RequiredAcks: acks,
		Compression:  compression,
		MaxAttempts:  maxAttempts,
		Async:        cfg.AsyncMode,
		BatchTimeout: 200 * time.Millisecond,
	}

	log.Printf("[kafka] producer created: brokers=%v topic=%s batch=%d compression=%s async=%v",
		brokers, cfg.Topic, batchSize, cfg.Compression, cfg.AsyncMode)

	return &KafkaProducer{writer: w}, nil
}

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

// normalizeBrokers strips protocol prefixes and splits comma-separated addresses.
func normalizeBrokers(raw string) []string {
	// Strip common URL prefixes
	raw = strings.TrimPrefix(raw, "kafka://")
	raw = strings.TrimPrefix(raw, "kafka:")
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// EnsureTopic creates a Kafka topic if it does not exist.
func EnsureTopic(brokers string, topic string, partitions int) error {
	brokerList := normalizeBrokers(brokers)
	if len(brokerList) == 0 {
		return fmt.Errorf("no kafka brokers specified")
	}

	var lastErr error
	for _, broker := range brokerList {
		conn, err := kafka.Dial("tcp", broker)
		if err != nil {
			lastErr = err
			continue
		}

		controller, err := conn.Controller()
		conn.Close()
		if err != nil {
			lastErr = fmt.Errorf("get kafka controller: %w", err)
			continue
		}

		controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
		if err != nil {
			lastErr = fmt.Errorf("connect to kafka controller: %w", err)
			continue
		}
		defer controllerConn.Close()

		topicConfigs := []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		}}

		err = controllerConn.CreateTopics(topicConfigs...)
		if err != nil {
			if strings.Contains(err.Error(), "TopicExists") ||
				strings.Contains(err.Error(), "already exists") {
				log.Printf("[kafka] topic %s already exists, skipping creation", topic)
				return nil
			}
			lastErr = fmt.Errorf("create topic %s: %w", topic, err)
			continue
		}

		log.Printf("[kafka] topic %s created (partitions=%d)", topic, partitions)
		return nil
	}
	return fmt.Errorf("ensure topic %s failed (tried %d brokers): %w", topic, len(brokerList), lastErr)
}

func (p *KafkaProducer) Close() error {
	var err error
	p.once.Do(func() {
		err = p.writer.Close()
		log.Printf("[kafka] producer closed, sent=%d", p.sent.Load())
	})
	return err
}
