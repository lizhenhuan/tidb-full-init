package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/config"
)

func baseConfig() *config.Config {
	return &config.Config{
		PDAddresses:           "127.0.0.1:2379",
		TiDBHost:              "127.0.0.1",
		TiDBPort:              "4000",
		Username:              "root",
		Password:              "x",
		KafkaBootstrapServers: "127.0.0.1:9092",
		Tables: []config.TableConfig{{
			Database:    "d",
			Tables:      []string{"a", "b"},
			Parallelism: 4,
		}},
	}
}

func TestValidateTopicListOneToOne(t *testing.T) {
	cfg := baseConfig()
	cfg.KafkaTopicList = []string{"t1", "t2"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}

	cfg.KafkaTopicList = []string{"t1"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for topic list/table count mismatch")
	}
}

func TestValidateTopicListRequiresEmptyTopicAndPrefix(t *testing.T) {
	cfg := baseConfig()
	cfg.KafkaTopicList = []string{"t1", "t2"}

	cfg.KafkaTopic = "fixed"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when kafka-topic is set")
	}
	cfg.KafkaTopic = ""

	cfg.KafkaTopicPrefix = "tidb-data-"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error when kafka-topic-prefix is set")
	}
}

func TestLoadConfigClearsDefaultPrefixForTopicList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{
		"pd-addresses": "127.0.0.1:2379",
		"username": "root",
		"password": "x",
		"kafka-bootstrap-servers": "127.0.0.1:9092",
		"kafka-topic-list": ["t1", "t2"],
		"tables": [{"database": "d", "tables": ["a", "b"], "parallelism": 4}]
	}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.KafkaTopicPrefix != "" {
		t.Fatalf("expected prefix to be cleared, got %q", cfg.KafkaTopicPrefix)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
}

func TestLoadConfigRejectsExplicitPrefixWithTopicList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{
		"pd-addresses": "127.0.0.1:2379",
		"username": "root",
		"password": "x",
		"kafka-bootstrap-servers": "127.0.0.1:9092",
		"kafka-topic-prefix": "tidb-data-",
		"kafka-topic-list": ["t1", "t2"],
		"tables": [{"database": "d", "tables": ["a", "b"], "parallelism": 4}]
	}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := config.LoadConfig(path); err == nil {
		t.Fatal("expected error for explicit prefix with topic list")
	}
}
