package syncer

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"

	"github.com/lizhenhuan/tidb-full-init/internal/applier"
	"github.com/lizhenhuan/tidb-full-init/internal/checkpoint"
	"github.com/lizhenhuan/tidb-full-init/internal/consumer"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/oraclesink"
	"github.com/lizhenhuan/tidb-full-init/internal/tidb"
)

// ListTopicPartitions 发现 topics 的全部分区（手动分配用，D3）。
func ListTopicPartitions(ctx context.Context, brokers, topics []string) ([]model.TopicPartition, error) {
	client := kafka.Client{Addr: kafka.TCP(brokers...)}
	want := make(map[string]bool, len(topics))
	for _, t := range topics {
		want[t] = true
	}
	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("syncer: read partitions: %w", err)
	}
	out := make([]model.TopicPartition, 0)
	for _, t := range resp.Topics {
		if !want[t.Name] {
			continue
		}
		if t.Error != nil {
			return nil, fmt.Errorf("syncer: topic %s: %w", t.Name, t.Error)
		}
		for _, p := range t.Partitions {
			out = append(out, model.TopicPartition{Topic: t.Name, Partition: p.ID})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("syncer: no partitions found for topics %v (missing topics or unreachable broker)", topics)
	}
	return out, nil
}

// Deps 运行依赖注入点（生产路径留空由 Run 装配；测试注入 fake）。
type Deps struct {
	// Sink 目标端（留空 = oraclesink）。
	Sink sink.Sink
	// DLQ 死信写入（留空 = oraclesink.DLQ；设为 nil 且 DisableDLQ 才真正禁用）。
	DLQ applier.DLQWriter
	// DisableDLQ 显式禁用死信落库（仅数据错误计数）。
	DisableDLQ bool
	// Fetch 表结构来源（留空 = tidb.SchemaFetcher）。
	Fetch FetchTableInfo
	// Consumer 消费者（留空 = KafkaConsumer）。
	Consumer consumer.Consumer
	// ListPartitions 分区发现（留空 = ListTopicPartitions）。
	ListPartitions func(ctx context.Context, brokers, topics []string) ([]model.TopicPartition, error)
	// PrepareTables 是否为目标表执行 Prepare（建表）。默认 true；测试 mocksink 可关。
	PrepareTables *bool
}

// Result 一次运行的结果摘要。
type Result struct {
	Metrics applier.Metrics
	Topics  []string // 消费的 topic 列表
}

// Run 装配并运行消费管道，阻塞至 ctx 取消或致命错误。
// 位点唯一权威是 SQLite（D10）；起始位点语义复用 applier.Pipeline.Run：
// Load=-1（无记录）→ 0 从头消费，即「全量从 0、增量续传」。
func Run(ctx context.Context, cfg *Config, deps Deps) (*Result, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// 表结构来源（关闭责任在本地）
	if deps.Fetch == nil {
		dsn := tidb.BuildDSN(cfg.TiDB.Host, cfg.TiDB.Port, cfg.TiDB.Username, cfg.TiDB.Password)
		sf, err := tidb.NewSchemaFetcher(dsn)
		if err != nil {
			return nil, fmt.Errorf("syncer: create schema fetcher: %w", err)
		}
		defer sf.Close()
		deps.Fetch = sf.GetTableInfo
	}
	meta := MetaProvider(deps.Fetch, cfg.TargetSchema)

	// 目标端
	var closeSink func()
	if deps.Sink == nil {
		os, err := oraclesink.New(ctx, oraclesink.Config{
			Host: cfg.Oracle.Host, Port: cfg.Oracle.Port, Service: cfg.Oracle.Service,
			User: cfg.Oracle.User, Password: cfg.Oracle.Password,
		})
		if err != nil {
			return nil, fmt.Errorf("syncer: create oracle sink: %w", err)
		}
		deps.Sink = os
		closeSink = func() { os.Close() }
	}
	if closeSink != nil {
		defer closeSink()
	}

	// DLQ（oraclesink 时一并建死信表）
	if !deps.DisableDLQ && deps.DLQ == nil {
		if os, ok := deps.Sink.(*oraclesink.Sink); ok {
			d := os.NewDLQ()
			if err := d.EnsureTable(ctx); err != nil {
				return nil, fmt.Errorf("syncer: ensure dlq table: %w", err)
			}
			deps.DLQ = d
		}
	}

	// topic 派生 + 目标表 Prepare（幂等建表，D5）
	topicSet := map[string]bool{}
	prepare := true
	if deps.PrepareTables != nil {
		prepare = *deps.PrepareTables
	}
	for _, te := range cfg.Tables {
		for _, t := range te.Tables {
			topicSet[cfg.ResolveTopic(te, t)] = true
			if prepare {
				m, err := meta(te.Database, t)
				if err != nil {
					return nil, fmt.Errorf("syncer: meta %s.%s: %w", te.Database, t, err)
				}
				if ps, ok := deps.Sink.(interface {
					Prepare(ctx context.Context, meta model.TableMeta) error
				}); ok {
					if err := ps.Prepare(ctx, m); err != nil {
						return nil, fmt.Errorf("syncer: prepare %s.%s: %w", te.Database, t, err)
					}
					log.Printf("[syncer] target table ready: %s.%s -> %s.%s", te.Database, t, m.Schema(), m.Table)
				}
			}
		}
	}
	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}

	// 分区发现 + 位点 + 管道
	listParts := deps.ListPartitions
	if listParts == nil {
		listParts = ListTopicPartitions
	}
	partitions, err := listParts(ctx, []string{cfg.KafkaBootstrapServers}, topics)
	if err != nil {
		return nil, err
	}
	log.Printf("[syncer] consuming %d partition(s) across %d topic(s)", len(partitions), len(topics))

	if deps.Consumer == nil {
		deps.Consumer = consumer.NewKafkaConsumer(consumer.KafkaConfig{
			Brokers: []string{cfg.KafkaBootstrapServers},
		})
	}
	defer deps.Consumer.Close()

	store, err := checkpoint.NewSQLiteStore(ctx, cfg.CheckpointPath)
	if err != nil {
		return nil, fmt.Errorf("syncer: open checkpoint store: %w", err)
	}
	defer store.Close()

	bs, flush, retries, backoff := cfg.applierCfg()
	p := applier.New(deps.Consumer, deps.Sink, store, meta, deps.DLQ, applier.Config{
		BatchSize: bs, FlushInterval: flush, MaxRetries: retries, RetryBackoff: backoff,
	})

	runErr := p.Run(ctx, partitions)
	return &Result{Metrics: p.Metrics(), Topics: topics}, runErr
}
