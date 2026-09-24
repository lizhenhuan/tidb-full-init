// Package initrunner 抽取 tidb-full-init CLI 的全量初始化编排逻辑
// （M3 T3.1）：TiDB 连接验证 → TiKV 客户端 → 逐表 schema/分区解析 →
// 快照扫描 → Kafka 生产。行为与原 CLI 一致；差别仅在于错误以 error
// 返回（CLI 层转 log.Fatal），ctx 可取消（M3 JobManager 需要）。
package initrunner

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/lizhenhuan/tidb-full-init/internal/config"
	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/kafka"
	"github.com/lizhenhuan/tidb-full-init/internal/snapshot"
	"github.com/lizhenhuan/tidb-full-init/internal/tidb"
	"github.com/lizhenhuan/tidb-full-init/internal/tikv"
)

// Options 运行选项。
type Options struct {
	MockMode bool   // mock 模式：无真实 TiKV/Kafka（CLI -mock）
	StartTSO uint64 // 快照 TSO（0 = 自动从 PD 获取）
}

// Result 运行结果。
type Result struct {
	// Tables 每表总行数（含分区表全部分区），键为 "db.table"。
	Tables map[string]int64
	// Topics 每表实际写入的 topic，键为 "db.table"。
	Topics map[string]string
}

// Run 执行全量初始化。cfg 必须已通过 Validate。
func Run(ctx context.Context, cfg *config.Config, opts Options) (*Result, error) {
	res := &Result{Tables: map[string]int64{}, Topics: map[string]string{}}

	// Step 1: TiDB SQL 连接与表权限验证（在任何 TiKV/Kafka 资源创建之前）
	var dsn string
	if !opts.MockMode {
		dsn = tidb.BuildDSN(cfg.TiDBHost, cfg.TiDBPort, cfg.Username, cfg.Password)
		log.Printf("Verifying TiDB connection at %s:%s ...", cfg.TiDBHost, cfg.TiDBPort)
		result, err := tidb.VerifyConnection(dsn)
		if err != nil {
			return nil, fmt.Errorf("tidb connection verification: %w", err)
		}
		log.Printf("TiDB connected: version=%s, user=%s", result.Version, result.User)
		for _, g := range result.Grants {
			log.Printf("  Grant: %s", g)
		}
		for _, tc := range cfg.Tables {
			for _, tableName := range tc.Tables {
				if err := tidb.VerifyTableAccess(dsn, tc.Database, tableName); err != nil {
					return nil, fmt.Errorf("permission check %s.%s: %w", tc.Database, tableName, err)
				}
				log.Printf("  SELECT permission verified: %s.%s", tc.Database, tableName)
			}
		}
		log.Println("TiDB authentication and permissions verified successfully")
	}

	// Step 2: TiKV 客户端
	var tikvClient tikv.Client
	if opts.MockMode {
		tikvClient = tikv.NewMockClient(nil)
		log.Println("Using mock TiKV client")
	} else {
		var err error
		tikvClient, err = tikv.NewTiKVClient(cfg.PDAddresses)
		if err != nil {
			return nil, fmt.Errorf("create tikv client: %w", err)
		}
		log.Println("Production TiKV client connected")
	}
	defer tikvClient.Close()

	// Step 3: SchemaFetcher
	var schemaFetcher *tidb.SchemaFetcher
	if !opts.MockMode {
		var err error
		schemaFetcher, err = tidb.NewSchemaFetcher(dsn)
		if err != nil {
			return nil, fmt.Errorf("create schema fetcher: %w", err)
		}
		defer schemaFetcher.Close()
	}

	reader := snapshot.NewReader(tikvClient)

	for _, tc := range cfg.Tables {
		for _, tableName := range tc.Tables {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := runTable(ctx, reader, schemaFetcher, tikvClient, cfg, opts, tc, tableName, res); err != nil {
				return nil, err
			}
		}
	}

	log.Println("All tables initialized successfully")
	return res, nil
}

// runTable 单表：topic 解析 → producer → schema/分区 → 扫描全部 scanID。
func runTable(ctx context.Context, reader *snapshot.Reader, schemaFetcher *tidb.SchemaFetcher,
	tikvClient tikv.Client, cfg *config.Config, opts Options, tc config.TableConfig, tableName string, res *Result) error {

	topic := tc.Topic
	if topic == "" && cfg.KafkaTopic != "" {
		// 固定 Kafka topic：所有表发到同一 topic
		topic = cfg.KafkaTopic
	} else if topic == "" {
		topic = cfg.KafkaTopicPrefix + tc.Database + "-" + tableName
	}

	var producer kafka.Producer
	if opts.MockMode {
		producer = kafka.NewConsoleProducer()
		log.Printf("Using console producer for topic %s", topic)
	} else {
		if err := kafka.EnsureTopic(cfg.KafkaBootstrapServers, topic, 1); err != nil {
			return fmt.Errorf("ensure topic %s: %w", topic, err)
		}
		var err error
		producer, err = kafka.NewKafkaProducer(kafka.KafkaProducerConfig{
			Brokers:     cfg.KafkaBootstrapServers,
			Topic:       topic,
			Acks:        1,
			Compression: "snappy",
			BatchSize:   100,
			MaxAttempts: 3,
		})
		if err != nil {
			return fmt.Errorf("create kafka producer for %s: %w", topic, err)
		}
		log.Printf("Kafka producer connected to %s, topic=%s", cfg.KafkaBootstrapServers, topic)
	}
	defer producer.Close()

	scanCfg := &snapshot.Config{
		Database:    tc.Database,
		TableName:   tableName,
		Parallelism: tc.Parallelism,
		BatchSize:   500,
		StartTSO:    opts.StartTSO,
	}

	var tableInfo *encoder.TableInfo
	var tableID int64
	var scanIDs []int64

	if opts.MockMode {
		tableInfo = mockTableInfo(tc.Database, tableName)
		tableID = mockTableID(tc.Database, tableName)
		if mc, ok := tikvClient.(*tikv.MockClient); ok {
			seedMockData(mc, tableID)
		}
		scanIDs = []int64{tableID}
	} else {
		var err error
		tableInfo, tableID, err = schemaFetcher.GetTableInfo(tc.Database, tableName)
		if err != nil {
			return fmt.Errorf("get table info for %s.%s: %w", tc.Database, tableName, err)
		}
		// 分区表数据在分区 ID 下而非主 tableID
		partIDs, err := schemaFetcher.GetPartitionIDs(tc.Database, tableName)
		if err != nil {
			return fmt.Errorf("get partition ids for %s.%s: %w", tc.Database, tableName, err)
		}
		if len(partIDs) > 0 {
			log.Printf("[%s.%s] partitioned table with %d partitions: %v", tc.Database, tableName, len(partIDs), partIDs)
			scanIDs = partIDs
		} else {
			scanIDs = []int64{tableID}
		}
	}

	var totalRowCount int64
	for _, scanID := range scanIDs {
		label := fmt.Sprintf("%s.%s", tc.Database, tableName)
		if len(scanIDs) > 1 {
			label = fmt.Sprintf("%s.%s [pid=%d]", tc.Database, tableName, scanID)
		}
		log.Printf("Starting: %s (topic=%s)", label, topic)

		rowCount, err := reader.ScanTable(ctx, scanCfg, scanID, tableInfo, producer, topic)
		if err != nil {
			return fmt.Errorf("scan %s: %w", label, err)
		}

		log.Printf("[%s] Done: %d rows -> topic %s", label, rowCount, topic)
		totalRowCount += rowCount
	}

	if len(scanIDs) > 1 {
		log.Printf("[%s.%s] All partitions done: %d total rows -> topic %s", tc.Database, tableName, totalRowCount, topic)
	}
	key := tc.Database + "." + tableName
	res.Tables[key] += totalRowCount
	res.Topics[key] = topic
	return nil
}

// --- Mock mode helpers（自原 CLI cmd/tidb-full-init 原样移植，行为不变）---

func mockTableID(database, table string) int64 {
	var id int64
	for _, ch := range database + "." + table {
		id = id*31 + int64(ch)
	}
	return id % 10000
}

func mockTableInfo(database, tableName string) *encoder.TableInfo {
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
		value := encodeMockRow(row)
		client.SetData(string(key), value)
	}
}

// encodeMockRow 构造 rowcodec new-format 值（原 CLI encolizhenhuanckRow，
// 仅更名）。格式：VER(1B) | FLAGS(1B) | NOT_NULL_COL_CNT(2B LE) |
// NULL_COL_CNT(2B LE) | NOT_NULL_COL_IDS(1B each) |
// NOT_NULL_COL_OFFSETS(2B LE each) | COL_DATA
func encodeMockRow(jsonRow string) []byte {
	values := strings.Split(strings.Trim(jsonRow, "{}"), ",")

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
