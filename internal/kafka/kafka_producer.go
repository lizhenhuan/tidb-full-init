package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

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
		Async:        false,
	}

	log.Printf("[kafka] producer created: brokers=%v topic=%s batch=%d compression=%s",
		brokers, cfg.Topic, batchSize, cfg.Compression)

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
