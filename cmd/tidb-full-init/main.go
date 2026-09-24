// tidb-full-init：TiKV 快照全量扫描 → canal JSON → Kafka。
// M3 T3.1 起编排逻辑抽取到 internal/initrunner（供 JobManager 复用），
// 本文件只剩 flag 解析、config 装配与 log.Fatal 错误出口，行为不变。
package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/lizhenhuan/tidb-full-init/internal/config"
	"github.com/lizhenhuan/tidb-full-init/internal/initrunner"
)

var (
	configFile       string
	pdAddresses      string
	tidbHost         string
	tidbPort         string
	username         string
	password         string
	database         string
	tablesFlag       string
	kafkaBootstrap   string
	kafkaTopicPrefix string
	kafkaTopic       string
	parallelism      int
	startTSO         uint64
	mockMode         bool
)

func init() {
	flag.StringVar(&configFile, "config", "", "JSON configuration file path")
	flag.StringVar(&pdAddresses, "pd-addresses", "127.0.0.1:2379", "TiDB PD addresses")
	flag.StringVar(&tidbHost, "tidb-host", "127.0.0.1", "TiDB SQL connection host")
	flag.StringVar(&tidbPort, "tidb-port", "4000", "TiDB SQL connection port")
	flag.StringVar(&username, "username", "root", "TiDB username")
	flag.StringVar(&password, "password", "", "TiDB password (required)")
	flag.StringVar(&database, "database", "test", "Database name")
	flag.StringVar(&tablesFlag, "tables", "", "Tables to initialize (comma-separated)")
	flag.StringVar(&kafkaBootstrap, "kafka-bootstrap", "127.0.0.1:9092", "Kafka bootstrap servers")
	flag.StringVar(&kafkaTopicPrefix, "kafka-topic-prefix", "tidb-data-", "Kafka topic prefix (topic = prefix + db + '-' + table)")
	flag.StringVar(&kafkaTopic, "kafka-topic", "", "Kafka topic (fixed; all tables sent to this single topic, overrides -kafka-topic-prefix)")
	flag.IntVar(&parallelism, "parallelism", 4, "Parallel scan workers per table")
	flag.Uint64Var(&startTSO, "start-tso", 0, "Start TSO for snapshot scan (0=auto from PD)")
	flag.BoolVar(&mockMode, "mock", false, "Use mock mode (no real TiKV/Kafka connection)")
}

func main() {
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[tidb-full-init] ")

	var cfg *config.Config
	var err error

	if configFile != "" {
		cfg, err = config.LoadConfig(configFile)
		if err != nil {
			log.Fatalf("Load config: %v", err)
		}
	} else {
		cfg = buildConfigFromFlags()
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}

	log.Printf("Config: pd=%s tidb=%s:%s kafka=%s prefix=%s topic=%s mock=%v user=%s start-tso=%d",
		cfg.PDAddresses, cfg.TiDBHost, cfg.TiDBPort, cfg.KafkaBootstrapServers, cfg.KafkaTopicPrefix, cfg.KafkaTopic, mockMode, cfg.Username, startTSO)

	res, err := initrunner.Run(context.Background(), cfg, initrunner.Options{
		MockMode: mockMode,
		StartTSO: startTSO,
	})
	if err != nil {
		log.Fatalf("Full init failed: %v", err)
	}

	for key, rows := range res.Tables {
		log.Printf("Result: %s -> %d rows, topic=%s", key, rows, res.Topics[key])
	}
}

func buildConfigFromFlags() *config.Config {
	tables := parseTables(tablesFlag)
	return &config.Config{
		PDAddresses:           pdAddresses,
		TiDBHost:              tidbHost,
		TiDBPort:              tidbPort,
		Username:              username,
		Password:              password,
		KafkaBootstrapServers: kafkaBootstrap,
		KafkaTopicPrefix:      kafkaTopicPrefix,
		KafkaTopic:            kafkaTopic,
		Tables: []config.TableConfig{{
			Database:    database,
			Tables:      tables,
			Parallelism: parallelism,
		}},
	}
}

func parseTables(s string) []string {
	if s == "" {
		return []string{"orders"}
	}
	var result []string
	for _, t := range strings.Split(s, ",") {
		if trimmed := strings.TrimSpace(t); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
