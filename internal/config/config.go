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
	return nil
}
