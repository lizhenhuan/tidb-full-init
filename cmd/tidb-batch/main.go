// tidb-batch 批量验收编排（#t6）：对一批 db.table 跑完整闭环——
//
//	全量快照（initrunner）→ tidb-sync 消费到 Oracle → tidb-verify 比对
//	→（可选）dmlgen 增量 I/U/D → 继续消费 → 再次比对
//	→ 每轮清理（Oracle 目标表 + Kafka topic + 位点文件）
//
// 汇总打印每表结果并以退出码判定（全过 = 0）。每轮 topic 前缀带时间戳、
// 位点文件独立，天然可重复跑（验证 at-least-once + MERGE 幂等收敛）。
//
// 配置（-config batch.json）：
//
//	{
//	  "tidb-dsn": "root:pw@tcp(10.0.0.1:4000)/",
//	  "pd-addresses": "10.0.0.1:2379",
//	  "kafka-bootstrap-servers": "127.0.0.1:9092",
//	  "oracle": {"host":"127.0.0.1","port":1521,"service":"FREEPDB1",
//	             "user":"system","password":""},
//	  "target-schema": "shop",
//	  "work-dir": "batch-work",
//	  "sample": 0,
//	  "consume-timeout-sec": 300,
//	  "dml": {"inserts":50,"updates":20,"deletes":10,"seed":7},
//	  "tables": ["db1.t1","db1.t2"]
//	}
//
// Oracle 口令可用环境变量 ORACLE_FREE_PASSWORD 兜底（leader 口径）。
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	goorav2 "github.com/sijms/go-ora/v2"
	"github.com/segmentio/kafka-go"

	"github.com/lizhenhuan/tidb-full-init/internal/config"
	"github.com/lizhenhuan/tidb-full-init/internal/dmlgen"
	"github.com/lizhenhuan/tidb-full-init/internal/initrunner"
	"github.com/lizhenhuan/tidb-full-init/internal/syncer"
	"github.com/lizhenhuan/tidb-full-init/internal/verify"
)

type oracleCfg struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Service  string `json:"service"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type dmlCfg struct {
	Inserts int   `json:"inserts"`
	Updates int   `json:"updates"`
	Deletes int   `json:"deletes"`
	Seed    int64 `json:"seed"`
}

type batchCfg struct {
	TiDBDSN              string    `json:"tidb-dsn"`
	PDAddresses          string    `json:"pd-addresses"`
	KafkaBootstrapServer string    `json:"kafka-bootstrap-servers"`
	Oracle               oracleCfg `json:"oracle"`
	TargetSchema         string    `json:"target-schema"`
	WorkDir              string    `json:"work-dir"`
	Sample               int       `json:"sample"`
	ConsumeTimeoutSec    int       `json:"consume-timeout-sec"`
	DML                  dmlCfg    `json:"dml"`
	Tables               []string  `json:"tables"`
}

type tableEntry struct {
	db, table string
}

type tableResult struct {
	fullEqual, incrEqual bool
	incrSkipped          bool
	rows                 int64
	err                  error
	stage                string
}

func main() {
	cfgPath := flag.String("config", "batch.json", "批量配置文件")
	flag.Parse()

	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		log.Fatalf("tidb-batch: read config: %v", err)
	}
	bc := &batchCfg{
		TargetSchema:      "shop",
		WorkDir:           "batch-work",
		ConsumeTimeoutSec: 300,
	}
	if err := json.Unmarshal(raw, bc); err != nil {
		log.Fatalf("tidb-batch: parse config: %v", err)
	}
	if bc.TiDBDSN == "" || bc.PDAddresses == "" || bc.KafkaBootstrapServer == "" ||
		bc.Oracle.Host == "" || bc.TargetSchema == "" || len(bc.Tables) == 0 {
		log.Fatal("tidb-batch: tidb-dsn/pd-addresses/kafka-bootstrap-servers/oracle.host/target-schema/tables 必填")
	}
	if bc.Oracle.Password == "" {
		bc.Oracle.Password = os.Getenv("ORACLE_FREE_PASSWORD")
	}
	if bc.Oracle.Password == "" {
		log.Fatal("tidb-batch: oracle password 缺失（配置或 ORACLE_FREE_PASSWORD）")
	}
	if bc.ConsumeTimeoutSec <= 0 {
		bc.ConsumeTimeoutSec = 300
	}
	dsn, err := mysql.ParseDSN(bc.TiDBDSN)
	if err != nil {
		log.Fatalf("tidb-batch: parse tidb-dsn: %v", err)
	}

	var tables []tableEntry
	for _, t := range bc.Tables {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			log.Fatalf("tidb-batch: 表名 %q 应为 db.table", t)
		}
		tables = append(tables, tableEntry{db: parts[0], table: parts[1]})
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(bc.WorkDir, 0o755); err != nil {
		log.Fatalf("tidb-batch: mkdir workdir: %v", err)
	}
	round := time.Now().Unix()
	prefix := fmt.Sprintf("batch-%d-", round)

	results := map[string]*tableResult{}
	for _, te := range tables {
		results[te.db+"."+te.table] = &tableResult{stage: "pending"}
	}

	fail := runRound(ctx, bc, dsn, tables, prefix, round, results)

	// 汇总
	fmt.Printf("\n===== tidb-batch 汇总（round=%d topic-prefix=%s）=====\n", round, prefix)
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	exit := 0
	for _, k := range keys {
		r := results[k]
		status := "FAIL"
		if r.err == nil && r.fullEqual && (r.incrSkipped || r.incrEqual) {
			status = "PASS"
		} else {
			exit = 1
		}
		incr := "skip"
		if r.incrSkipped {
			incr = "skip"
		} else if r.incrEqual {
			incr = "equal"
		} else {
			incr = "DIFF"
		}
		fmt.Printf("  %-6s %-30s full=%-6s incr=%-6s rows=%d stage=%s err=%v\n",
			status, k, boolStr(r.fullEqual), incr, r.rows, r.stage, r.err)
	}
	if fail != nil {
		fmt.Printf("round error: %v\n", fail)
	}
	os.Exit(exit)
}

func boolStr(b bool) string {
	if b {
		return "equal"
	}
	return "DIFF"
}

// runRound 一轮批量：全量 → 消费 → verify →（可选增量）→ verify。
// 任一表失败不中断其余表（fail loud 记入该表结果），编排级错误才返回 fail。
func runRound(ctx context.Context, bc *batchCfg, dsn *mysql.Config, tables []tableEntry, prefix string, round int64, results map[string]*tableResult) error {
	// ---- Oracle 连接（count 轮询 + 轮末清理） ----
	oraURL := goorav2.BuildUrl(bc.Oracle.Host, bc.Oracle.Port, bc.Oracle.Service, bc.Oracle.User, bc.Oracle.Password, nil)
	ora, err := sql.Open("oracle", oraURL)
	if err != nil {
		return fmt.Errorf("open oracle: %w", err)
	}
	defer ora.Close()
	if err := ora.PingContext(ctx); err != nil {
		return fmt.Errorf("ping oracle: %w", err)
	}
	defer cleanupOracle(context.Background(), ora, bc.TargetSchema, tables)

	// ---- Step 1: 全量快照 → Kafka ----
	fullCfg := &config.Config{
		PDAddresses:           bc.PDAddresses,
		TiDBHost:              hostOf(dsn),
		TiDBPort:              portOf(dsn),
		Username:              dsn.User,
		Password:              dsn.Passwd,
		KafkaBootstrapServers: bc.KafkaBootstrapServer,
		KafkaTopicPrefix:      prefix,
		Tables:                toTableConfigs(tables),
	}
	fullRes, err := initrunner.Run(ctx, fullCfg, initrunner.Options{})
	if err != nil {
		for _, r := range results {
			r.err = fmt.Errorf("full init: %w", err)
			r.stage = "full-init"
		}
		return fmt.Errorf("full init: %w", err)
	}
	for _, te := range tables {
		results[te.db+"."+te.table].stage = "full-consume"
	}

	// ---- Step 2: 消费管道（后台常驻，整轮共用） ----
	cpPath := filepath.Join(bc.WorkDir, fmt.Sprintf("cp-%d.db", round))
	syncCfg := &syncer.Config{
		TiDB: syncer.TiDBConfig{
			Host: hostOf(dsn), Port: portOf(dsn), Username: dsn.User, Password: dsn.Passwd,
		},
		KafkaBootstrapServers: bc.KafkaBootstrapServer,
		KafkaTopicPrefix:      prefix,
		CheckpointPath:        cpPath,
		Oracle: syncer.OracleConfig{
			Host: bc.Oracle.Host, Port: bc.Oracle.Port, Service: bc.Oracle.Service,
			User: bc.Oracle.User, Password: bc.Oracle.Password,
		},
		TargetSchema: bc.TargetSchema,
		Tables:       toSyncTables(tables),
	}
	runCtx, cancelSync := context.WithCancel(ctx)
	syncDone := make(chan error, 1)
	go func() {
		_, err := syncer.Run(runCtx, syncCfg, syncer.Deps{})
		syncDone <- err
	}()

	waitConsume := func(te tableEntry, want int64) error {
		return waitForRows(ctx, ora, bc.TargetSchema, te, want, bc.ConsumeTimeoutSec)
	}

	// ---- Step 3: 全量消费到位 → 逐表 verify ----
	for _, te := range tables {
		key := te.db + "." + te.table
		want := fullRes.Tables[key]
		results[key].rows = want
		if err := waitConsume(te, want); err != nil {
			results[key].err = err
			continue
		}
		rep, err := verifyTable(ctx, bc, dsn, te)
		if err != nil {
			results[key].err = err
			continue
		}
		results[key].fullEqual = rep.Equal()
		if !rep.Equal() {
			logReport(key, rep)
		}
	}

	// ---- Step 4: 增量轮（可选）----
	if bc.DML.Inserts > 0 || bc.DML.Updates > 0 || bc.DML.Deletes > 0 {
		for _, te := range tables {
			key := te.db + "." + te.table
			results[key].stage = "incr-dml"
			if results[key].err != nil {
				results[key].incrSkipped = true
				continue // 全量已败，增量无意义
			}
			topic := prefix + te.db + "-" + te.table
			st, err := dmlgen.Run(ctx, dmlgen.Config{
				DSN: withDatabase(bc.TiDBDSN, te.db),
				Table: te.table, Brokers: []string{bc.KafkaBootstrapServer}, Topic: topic,
				NInsert: bc.DML.Inserts, NUpdate: bc.DML.Updates, NDelete: bc.DML.Deletes,
				Seed: bc.DML.Seed,
			})
			if err != nil {
				results[key].err = fmt.Errorf("dmlgen: %w", err)
				results[key].incrSkipped = true
				continue
			}
			// 终态行数 = 全量 + ins - del（UPDATE 不变）
			want := results[key].rows + int64(st.Inserted) - int64(st.Deleted)
			results[key].stage = "incr-consume"
			if err := waitConsume(te, want); err != nil {
				results[key].err = err
				continue
			}
			results[key].rows = want
			rep, err := verifyTable(ctx, bc, dsn, te)
			if err != nil {
				results[key].err = err
				continue
			}
			results[key].incrEqual = rep.Equal()
			if !rep.Equal() {
				logReport(key+" (incr)", rep)
			}
		}
	} else {
		for _, te := range tables {
			results[te.db+"."+te.table].incrSkipped = true
		}
	}

	// ---- Step 5: 停管道（优雅冲刷）----
	cancelSync()
	select {
	case err := <-syncDone:
		if err != nil && ctx.Err() == nil {
			log.Printf("[batch] syncer exit: %v", err)
			for _, r := range results {
				if r.err == nil {
					r.err = fmt.Errorf("syncer: %w", err)
				}
			}
		}
	case <-time.After(60 * time.Second):
		log.Printf("[batch] WARN syncer did not stop within 60s")
	}

	// ---- Step 6: 清理（topic 删掉；位点文件保留供排查）----
	cleanupTopics(context.Background(), bc.KafkaBootstrapServer, prefix, tables)
	return nil
}

// verifyTable TiDB vs Oracle 全量（或抽样）比对。
func verifyTable(ctx context.Context, bc *batchCfg, dsn *mysql.Config, te tableEntry) (*verify.Report, error) {
	tidbAddr := strings.TrimSuffix(hostOf(dsn)+":"+portOf(dsn), ":")
	src, err := verify.NewTiDBSource(ctx, verify.TiDBConfig{
		DSN: fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
			dsn.User, url.QueryEscape(dsn.Passwd), tidbAddr, te.db),
		Database: te.db, Table: te.table,
	})
	if err != nil {
		return nil, fmt.Errorf("tidb source: %w", err)
	}
	defer src.Close()
	dst, err := verify.NewOracleSource(ctx, verify.OracleConfig{
		Host: bc.Oracle.Host, Port: bc.Oracle.Port, Service: bc.Oracle.Service,
		User: bc.Oracle.User, Password: bc.Oracle.Password,
		Schema: bc.TargetSchema, Table: te.table,
	}, src.PKNames())
	if err != nil {
		return nil, fmt.Errorf("oracle source: %w", err)
	}
	defer dst.Close()

	if bc.Sample > 0 {
		return verify.Sample(ctx, src, dst, bc.Sample)
	}
	return verify.Compare(ctx, src, dst, verify.Options{})
}

// waitForRows 轮询 Oracle 目标表行数直到达到 want（消费完成信号）。
// 行数是最终判据（MERGE 收敛后 count 稳定即代表窗口已冲刷）。
func waitForRows(ctx context.Context, ora *sql.DB, schema string, te tableEntry, want int64, timeoutSec int) error {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, schema, te.table)
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	var last int64 = -1
	for time.Now().Before(deadline) {
		var n int64
		if err := ora.QueryRowContext(ctx, q).Scan(&n); err != nil {
			// 表可能还没 Prepare 出来（syncer 启动中），继续等
			if isOracleNotFound(err) {
				n = -2
			} else {
				return fmt.Errorf("count %s.%s: %w", schema, te.table, err)
			}
		}
		if n == want {
			return nil
		}
		last = n
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("consume timeout: %s.%s rows=%d want=%d", schema, te.table, last, want)
}

func isOracleNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "ORA-00942")
}

func logReport(key string, rep *verify.Report) {
	log.Printf("[batch] DIFF %s: src=%d dst=%d rowDiff=%d missing=%d extra=%d",
		key, rep.SourceRows, rep.TargetRows, rep.RowDiffCount, rep.MissingCount, rep.ExtraCount)
}

// cleanupOracle 轮末 drop 目标表（保证重跑不残留上次数据）。
func cleanupOracle(ctx context.Context, ora *sql.DB, schema string, tables []tableEntry) {
	for _, te := range tables {
		q := fmt.Sprintf(`DROP TABLE "%s"."%s"`, schema, te.table)
		if _, err := ora.ExecContext(ctx, q); err != nil && !isOracleNotFound(err) {
			log.Printf("[batch] cleanup oracle %s: %v", te.table, err)
		}
	}
}

// cleanupTopics 轮末删除本轮 topics（best-effort）。
func cleanupTopics(ctx context.Context, brokers, prefix string, tables []tableEntry) {
	cl := &kafka.Client{Addr: kafka.TCP(brokers), Timeout: 10 * time.Second}
	names := make([]string, 0, len(tables))
	for _, te := range tables {
		names = append(names, prefix+te.db+"-"+te.table)
	}
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := cl.DeleteTopics(dctx, &kafka.DeleteTopicsRequest{Topics: names}); err != nil {
		log.Printf("[batch] cleanup topics: %v", err)
	}
}

func toTableConfigs(tables []tableEntry) []config.TableConfig {
	byDB := map[string]*config.TableConfig{}
	var order []string
	for _, te := range tables {
		tc, ok := byDB[te.db]
		if !ok {
			// Parallelism 显式给默认：直接构造的 Config 不走
			// config.LoadConfig 的 fill（#t6 复审 P0-1 死锁诱因之一）
			tc = &config.TableConfig{Database: te.db, Parallelism: 4}
			byDB[te.db] = tc
			order = append(order, te.db)
		}
		tc.Tables = append(tc.Tables, te.table)
	}
	out := make([]config.TableConfig, 0, len(order))
	for _, db := range order {
		out = append(out, *byDB[db])
	}
	return out
}

func toSyncTables(tables []tableEntry) []syncer.TableEntry {
	byDB := map[string]*syncer.TableEntry{}
	var order []string
	for _, te := range tables {
		tc, ok := byDB[te.db]
		if !ok {
			tc = &syncer.TableEntry{Database: te.db}
			byDB[te.db] = tc
			order = append(order, te.db)
		}
		tc.Tables = append(tc.Tables, te.table)
	}
	out := make([]syncer.TableEntry, 0, len(order))
	for _, db := range order {
		out = append(out, *byDB[db])
	}
	return out
}

// hostOf/portOf 从 mysql.Config 取 host/port（Net=tcp）。
func hostOf(dsn *mysql.Config) string {
	addr := dsn.Addr
	if i := strings.LastIndex(addr, ":"); i > 0 {
		return addr[:i]
	}
	return addr
}

func portOf(dsn *mysql.Config) string {
	addr := dsn.Addr
	if i := strings.LastIndex(addr, ":"); i > 0 {
		return addr[i+1:]
	}
	return "4000"
}

// withDatabase 把 DSN 的库名替换为 db（dmlgen 连库执行 DML）。
func withDatabase(dsnStr, db string) string {
	d, err := mysql.ParseDSN(dsnStr)
	if err != nil {
		return dsnStr
	}
	d.DBName = db
	return d.FormatDSN()
}
