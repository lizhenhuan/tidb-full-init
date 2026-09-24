// Package syncer 把 M2 的消费管道（consumer→applier→oraclesink）从测试
// 接线提升为可运行入口（M3 / #t6）：config 装配、SchemaFetcher→MetaProvider
// 适配、topic 派生与分区发现。内部语义全部复用冻结实现，不引入新行为。
package syncer

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// TiDBConfig 源端 TiDB（SchemaFetcher 权威来源，D5）。
type TiDBConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// OracleConfig 目标端 Oracle 连接。
type OracleConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Service  string `json:"service"`
	User     string `json:"user"`
	Password string `json:"password"`
}

// TableEntry 库表清单（与 initrunner 的 topic 解析规则对齐：
// Topic > 固定 KafkaTopic > 前缀拼接）。
type TableEntry struct {
	Database string   `json:"database"`
	Tables   []string `json:"tables"`
	Topic    string   `json:"topic,omitempty"`
}

// PipelineConfig applier 管道参数（0 用 applier 默认值）。
type PipelineConfig struct {
	BatchSize     int   `json:"batch-size"`
	FlushIntervalMs int64 `json:"flush-interval-ms"`
	MaxRetries    int   `json:"max-retries"`
	RetryBackoffMs int64 `json:"retry-backoff-ms"`
}

// Config tidb-sync 任务配置。
type Config struct {
	TiDB                  TiDBConfig     `json:"tidb"`
	KafkaBootstrapServers string         `json:"kafka-bootstrap-servers"`
	KafkaTopicPrefix      string         `json:"kafka-topic-prefix"`
	KafkaTopic            string         `json:"kafka-topic,omitempty"`
	CheckpointPath        string         `json:"checkpoint-path"`
	Oracle                OracleConfig   `json:"oracle"`
	TargetSchema          string         `json:"target-schema"`
	Tables                []TableEntry   `json:"tables"`
	Pipeline              PipelineConfig `json:"pipeline"`
}

// LoadConfig 读取 JSON 配置并填默认值。
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("syncer: read config: %w", err)
	}
	cfg := &Config{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("syncer: parse config: %w", err)
	}
	cfg.FillDefaults()
	return cfg, nil
}

// FillDefaults 补默认值（不覆盖显式配置）。
func (c *Config) FillDefaults() {
	if c.TiDB.Host == "" {
		c.TiDB.Host = "127.0.0.1"
	}
	if c.TiDB.Port == "" {
		c.TiDB.Port = "4000"
	}
	if c.TiDB.Username == "" {
		c.TiDB.Username = "root"
	}
	if c.KafkaBootstrapServers == "" {
		c.KafkaBootstrapServers = "127.0.0.1:9092"
	}
	if c.KafkaTopicPrefix == "" && c.KafkaTopic == "" {
		c.KafkaTopicPrefix = "tidb-data-"
	}
	if c.CheckpointPath == "" {
		c.CheckpointPath = "sync-checkpoint.db"
	}
	if c.Oracle.Host == "" {
		c.Oracle.Host = "127.0.0.1"
	}
	if c.Oracle.Port == 0 {
		c.Oracle.Port = 1521
	}
	if c.Oracle.Service == "" {
		c.Oracle.Service = "FREEPDB1"
	}
	if c.Oracle.User == "" {
		c.Oracle.User = "system"
	}
}

// Validate 校验必填项。
func (c *Config) Validate() error {
	if c.TiDB.Host == "" || c.TiDB.Port == "" || c.TiDB.Username == "" || c.TiDB.Password == "" {
		return fmt.Errorf("syncer: tidb host/port/username/password are required")
	}
	if c.KafkaBootstrapServers == "" {
		return fmt.Errorf("syncer: kafka-bootstrap-servers is required")
	}
	if c.CheckpointPath == "" {
		return fmt.Errorf("syncer: checkpoint-path is required")
	}
	if c.Oracle.Host == "" || c.Oracle.Service == "" || c.Oracle.User == "" || c.Oracle.Password == "" {
		return fmt.Errorf("syncer: oracle host/service/user/password are required")
	}
	if c.TargetSchema == "" {
		return fmt.Errorf("syncer: target-schema is required")
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("syncer: at least one table entry is required")
	}
	for i, te := range c.Tables {
		if te.Database == "" {
			return fmt.Errorf("syncer: tables[%d].database is required", i)
		}
		if len(te.Tables) == 0 {
			return fmt.Errorf("syncer: tables[%d].tables is required", i)
		}
	}
	return nil
}

// ResolveTopic 与 initrunner 相同的三级 topic 解析。
func (c *Config) ResolveTopic(te TableEntry, table string) string {
	if te.Topic != "" {
		return te.Topic
	}
	if c.KafkaTopic != "" {
		return c.KafkaTopic
	}
	return c.KafkaTopicPrefix + te.Database + "-" + table
}

func (c *Config) applierCfg() (batchSize int, flush time.Duration, retries int, backoff time.Duration) {
	return c.Pipeline.BatchSize,
		time.Duration(c.Pipeline.FlushIntervalMs) * time.Millisecond,
		c.Pipeline.MaxRetries,
		time.Duration(c.Pipeline.RetryBackoffMs) * time.Millisecond
}
