package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type TableConfig struct {
	Database    string   `json:"database"`
	Tables      []string `json:"tables"`
	Topic       string   `json:"topic,omitempty"`
	Parallelism int      `json:"parallelism"`
}

type Config struct {
	PDAddresses           string                 `json:"pd-addresses"`
	TiDBHost              string                 `json:"tidb-host"`
	TiDBPort              string                 `json:"tidb-port"`
	Username              string                 `json:"username"`
	Password              string                 `json:"password"`
	KafkaBootstrapServers string                 `json:"kafka-bootstrap-servers"`
	KafkaTopicPrefix      string                 `json:"kafka-topic-prefix"`
	KafkaTopic            string                 `json:"kafka-topic,omitempty"`
	KafkaTopicList        []string               `json:"kafka-topic-list,omitempty"`
	Tables                []TableConfig          `json:"tables"`
	KafkaProducerConfig   map[string]interface{} `json:"kafka-producer-config,omitempty"`
	SnapshotTimestamp     string                 `json:"snapshot-timestamp,omitempty"`
}

func DefaultConfig() *Config {
	return &Config{
		TiDBHost:          "127.0.0.1",
		TiDBPort:          "4000",
		KafkaTopicPrefix: "tidb-data-",
		KafkaProducerConfig: map[string]interface{}{
			"acks":             "all",
			"compression.type": "snappy",
			"batch.size":       16384,
			"linger.ms":        10,
		},
		Tables: []TableConfig{},
	}
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if len(cfg.KafkaTopicList) > 0 {
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(data, &raw); err == nil {
			var explicitPrefix string
			if v, ok := raw["kafka-topic-prefix"]; ok {
				if err := json.Unmarshal(v, &explicitPrefix); err == nil && explicitPrefix != "" {
					return nil, fmt.Errorf("kafka-topic-prefix must be empty when kafka-topic-list is set")
				}
			}
		}
		cfg.KafkaTopicPrefix = ""
	}

	for i, tc := range cfg.Tables {
		if tc.Parallelism <= 0 {
			cfg.Tables[i].Parallelism = 4
		}
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.PDAddresses == "" {
		return fmt.Errorf("pd-addresses is required")
	}
	if c.TiDBHost == "" {
		return fmt.Errorf("tidb-host is required")
	}
	if c.TiDBPort == "" {
		return fmt.Errorf("tidb-port is required")
	}
	if c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if c.Password == "" {
		return fmt.Errorf("password is required")
	}
	if c.KafkaBootstrapServers == "" {
		return fmt.Errorf("kafka-bootstrap-servers is required")
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("at least one table config is required")
	}
	for i, tc := range c.Tables {
		if tc.Database == "" {
			return fmt.Errorf("tables[%d].database is required", i)
		}
		if len(tc.Tables) == 0 {
			return fmt.Errorf("tables[%d].tables is required", i)
		}
	}

	if len(c.KafkaTopicList) > 0 {
		if c.KafkaTopic != "" {
			return fmt.Errorf("kafka-topic must be empty when kafka-topic-list is set")
		}
		if c.KafkaTopicPrefix != "" {
			return fmt.Errorf("kafka-topic-prefix must be empty when kafka-topic-list is set")
		}
		total := c.TableCount()
		if len(c.KafkaTopicList) != total {
			return fmt.Errorf("kafka-topic-list has %d topics but %d tables are configured; they must be one-to-one", len(c.KafkaTopicList), total)
		}
		for i, topic := range c.KafkaTopicList {
			if topic == "" {
				return fmt.Errorf("kafka-topic-list[%d] is empty", i)
			}
		}
	}

	return nil
}

func (c *Config) TableCount() int {
	total := 0
	for _, tc := range c.Tables {
		total += len(tc.Tables)
	}
	return total
}
