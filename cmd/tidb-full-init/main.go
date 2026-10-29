package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/config"
	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/kafka"
	"github.com/lizhenhuan/tidb-full-init/internal/snapshot"
	"github.com/lizhenhuan/tidb-full-init/internal/tidb"
	"github.com/lizhenhuan/tidb-full-init/internal/tikv"
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
	flag.StringVar(&kafkaBootstrap, "kafka-bootstrap", "127.0.0.1:9092", "Kafka bootstrap servers (supports domain, kafka:// prefix, comma-separated list)")
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

	// --- Step 1: Verify TiDB SQL connection BEFORE creating TiKV/Kafka ---
	var dsn string
	if !mockMode {
		dsn = tidb.BuildDSN(cfg.TiDBHost, cfg.TiDBPort, cfg.Username, cfg.Password)
		log.Printf("Verifying TiDB connection at %s:%s ...", cfg.TiDBHost, cfg.TiDBPort)
		result, err := tidb.VerifyConnection(dsn)
		if err != nil {
			log.Fatalf("TiDB connection verification failed: %v", err)
		}
		log.Printf("TiDB connected: version=%s, user=%s", result.Version, result.User)
		for _, g := range result.Grants {
			log.Printf("  Grant: %s", g)
		}

		// Verify SELECT access on each target table
		for _, tc := range cfg.Tables {
			for _, tableName := range tc.Tables {
				if err := tidb.VerifyTableAccess(dsn, tc.Database, tableName); err != nil {
					log.Fatalf("Permission check failed: %v", err)
				}
				log.Printf("  SELECT permission verified: %s.%s", tc.Database, tableName)
			}
		}
		log.Println("TiDB authentication and permissions verified successfully")
	}

	// --- Step 2: Create TiKV client (after TiDB verified) ---
	var tikvClient tikv.Client
	if mockMode {
		mockClient := tikv.NewMockClient(nil)
		tikvClient = mockClient
		log.Println("Using mock TiKV client")
	} else {
		tikvClient, err = tikv.NewTiKVClient(cfg.PDAddresses)
		if err != nil {
			log.Fatalf("Create TiKV client: %v", err)
		}
		log.Println("Production TiKV client connected")
	}
	defer tikvClient.Close()

	// --- Step 3: Create SchemaFetcher using verified config ---
	var schemaFetcher *tidb.SchemaFetcher
	if !mockMode {
		schemaFetcher, err = tidb.NewSchemaFetcher(dsn)
		if err != nil {
			log.Fatalf("Create schema fetcher: %v", err)
		}
		defer schemaFetcher.Close()
	}

	reader := snapshot.NewReader(tikvClient)
	ctx := context.Background()

	// Fetch a single TSO for consistency with TiCDC
	effectiveTSO := startTSO
	if effectiveTSO == 0 && !mockMode {
		var err error
		effectiveTSO, err = tikvClient.GetTimestamp(ctx)
		if err != nil {
			log.Fatalf("Get snapshot TSO: %v", err)
		}
	}
	log.Printf("=== Snapshot TSO: %d (use --start-ts=%d with TiCDC for incremental sync) ===", effectiveTSO, effectiveTSO)

	for _, tc := range cfg.Tables {
		for _, tableName := range tc.Tables {
			topic := tc.Topic
			if topic == "" && cfg.KafkaTopic != "" {
				// Fixed Kafka topic: all tables sent to this single topic
				topic = cfg.KafkaTopic
			} else if topic == "" {
				topic = cfg.KafkaTopicPrefix + tc.Database + "-" + tableName
			}

			var kafkaProducer kafka.Producer
			if mockMode {
				kafkaProducer = kafka.NewConsoleProducer()
				log.Printf("Using console producer for topic %s", topic)
			} else {
				if err := kafka.EnsureTopic(cfg.KafkaBootstrapServers, topic, 1); err != nil {
					log.Fatalf("Ensure topic %s: %v", topic, err)
				}
				kafkaProducer, err = kafka.NewKafkaProducer(kafka.KafkaProducerConfig{
					Brokers:     cfg.KafkaBootstrapServers,
					Topic:       topic,
					Acks:        1,
					Compression: "snappy",
					BatchSize:   2000,
					MaxAttempts: 3,
					AsyncMode:   true,
				})
				if err != nil {
					log.Fatalf("Create Kafka producer: %v", err)
				}
				log.Printf("Kafka producer connected to %s, topic=%s (async batch=2000)", cfg.KafkaBootstrapServers, topic)
			}

			scanCfg := &snapshot.Config{
				Database:    tc.Database,
				TableName:   tableName,
				Parallelism: tc.Parallelism,
				BatchSize:   2000,
				StartTSO:    effectiveTSO,
			}

			var tableInfo *encoder.TableInfo
			var tableID int64

			if mockMode {
				tableInfo = buildTableInfo(tc.Database, tableName)
				tableID = tcTableID(tc.Database, tableName)

				if mc, ok := tikvClient.(*tikv.MockClient); ok {
					seedMockData(mc, tableID)
				}
		} else {
			tableInfo, tableID, err = schemaFetcher.GetTableInfo(tc.Database, tableName)
			if err != nil {
				log.Fatalf("Get table info for %s.%s: %v", tc.Database, tableName, err)
			}
		}

		// Check for partitions: partitioned tables store data under partition IDs, not the main tableID.
		var scanIDs []int64
		if !mockMode {
			partIDs, err := schemaFetcher.GetPartitionIDs(tc.Database, tableName)
			if err != nil {
				log.Fatalf("Get partition IDs for %s.%s: %v", tc.Database, tableName, err)
			}
			if len(partIDs) > 0 {
				log.Printf("[%s.%s] partitioned table with %d partitions: %v", tc.Database, tableName, len(partIDs), partIDs)
				scanIDs = partIDs
			} else {
				scanIDs = []int64{tableID}
			}
		} else {
			scanIDs = []int64{tableID}
		}

		var totalRowCount int64
		partitionStartTime := time.Now()

		// Scan all partitions concurrently
		if len(scanIDs) == 1 {
			// Single partition: scan inline
			label := fmt.Sprintf("%s.%s", tc.Database, tableName)
			log.Printf("Starting: %s (topic=%s)", label, topic)

			rowCount, err := reader.ScanTable(ctx, scanCfg, scanIDs[0], tableInfo, kafkaProducer, topic)
			if err != nil {
				log.Printf("[%s] ERROR: %v", label, err)
				kafkaProducer.Close()
				os.Exit(1)
			}

			elapsed := time.Since(partitionStartTime)
			log.Printf("[%s] Done: %d rows -> topic %s (elapsed=%s)", label, rowCount, topic, elapsed.Round(time.Second))
			totalRowCount = rowCount
		} else {
			// Multiple partitions: scan concurrently
			log.Printf("[%s.%s] Scanning %d partitions in parallel", tc.Database, tableName, len(scanIDs))
			var wg sync.WaitGroup
			errCh := make(chan error, len(scanIDs))

			for _, scanID := range scanIDs {
				wg.Add(1)
				go func(pid int64) {
					defer wg.Done()
					label := fmt.Sprintf("%s.%s [pid=%d/%d]", tc.Database, tableName, pid, len(scanIDs))
					log.Printf("Starting: %s (topic=%s)", label, topic)

					rowCount, err := reader.ScanTable(ctx, scanCfg, pid, tableInfo, kafkaProducer, topic)
					if err != nil {
						errCh <- fmt.Errorf("[%s] %w", label, err)
						return
					}

					atomic.AddInt64(&totalRowCount, rowCount)
					elapsed := time.Since(partitionStartTime)
					log.Printf("[%s] Done: %d rows -> topic %s (elapsed=%s)", label, rowCount, topic, elapsed.Round(time.Second))
				}(scanID)
			}

			wg.Wait()
			close(errCh)

			for err := range errCh {
				if err != nil {
					log.Printf("ERROR: %v", err)
					kafkaProducer.Close()
					os.Exit(1)
				}
			}
		}

		if len(scanIDs) > 1 {
			log.Printf("[%s.%s] All %d partitions done: %d total rows -> topic %s (total elapsed=%s)",
				tc.Database, tableName, len(scanIDs), totalRowCount, topic, time.Since(partitionStartTime).Round(time.Second))
		}
			kafkaProducer.Close()
		}
	}

	log.Printf("=== All tables initialized successfully ===")
	log.Printf("=== Snapshot TSO: %d ===", effectiveTSO)
	log.Printf("=== To start TiCDC incremental sync, use: --start-ts=%d ===", effectiveTSO)
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

// --- Mock mode helpers ---

func tcTableID(database, table string) int64 {
	var id int64
	for _, ch := range database + "." + table {
		id = id*31 + int64(ch)
	}
	return id % 10000
}

func buildTableInfo(database, tableName string) *encoder.TableInfo {
	return &encoder.TableInfo{
		Database: database,
		Table:    tableName,
		PKNames:  []string{"id"},
		Columns: []encoder.ColumnInfo{
			{ID: 1, Name: "id", MySQLType: "int", IsPrimaryKey: true},
			{ID: 2, Name: "name", MySQLType: "varchar"},
			{ID: 3, Name: "status", MySQLType: "tinyint"},
			{ID: 4, Name: "amount", MySQLType: "decimal"},
			{ID: 5, Name: "created_at", MySQLType: "datetime"},
		},
	}
}

func seedMockData(client *tikv.MockClient, tableID int64) {
	rows := []string{
		`{"id":"1","name":"Alice","status":"1","amount":"100.50","created_at":"2026-05-01 10:00:00"}`,
		`{"id":"2","name":"Bob","status":"0","amount":"250.00","created_at":"2026-05-02 14:30:00"}`,
		`{"id":"3","name":"Charlie","status":"1","amount":"99.99","created_at":"2026-05-03 09:15:00"}`,
	}

	for i, row := range rows {
		key := tikv.GenTableRecordKey(tableID, int64(i+1))
		value := encolizhenhuanckRow(row)
		client.SetData(string(key), value)
	}
}

func encolizhenhuanckRow(jsonRow string) []byte {
	values := strings.Split(strings.Trim(jsonRow, "{}"), ",")

	// Build a rowcodec new-format value
	// Format: VER(1B) | FLAGS(1B) | NOT_NULL_COL_CNT(2B LE) | NULL_COL_CNT(2B LE) |
	//         NOT_NULL_COL_IDS(1B each) | NOT_NULL_COL_OFFSETS(2B LE each) | COL_DATA
	ver := byte(128) // rowcodec version
	flags := byte(0)
	colCount := len(values)

	var colIDs []byte
	var colData []byte
	var offsets []uint32

	for i, entry := range values {
		kv := strings.SplitN(entry, ":", 2)
		if len(kv) != 2 {
			continue
		}
		val := strings.Trim(strings.TrimSpace(kv[1]), `"`)
		colIDs = append(colIDs, byte(i+1))
		offsets = append(offsets, uint32(len(colData)))
		colData = append(colData, []byte(val)...)
	}

	var buf []byte
	buf = append(buf, ver)
	buf = append(buf, flags)
	buf = append(buf, byte(colCount), byte(colCount>>8)) // NOT_NULL_COL_CNT LE
	buf = append(buf, 0, 0)                              // NULL_COL_CNT LE
	buf = append(buf, colIDs...)
	for _, off := range offsets {
		buf = append(buf, byte(off), byte(off>>8))
	}
	buf = append(buf, colData...)

	return buf
}
