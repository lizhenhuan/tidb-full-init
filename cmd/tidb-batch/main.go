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
//	  "tables": ["db1.t1","db1.t2"],
//	  "expect-dlq": {"db1.t2": {"ORA-01438": 2}}
//	}
//
// expect-dlq（K3 口径）：对声明了期望死信的表，轮末按本轮 topic 机器断言
// 「有损必须可见」。全量-only 精确匹配；增量轮（dml 活跃）随机数据可
// 超发已声明码的死信，放宽为下限 + reason 白名单（见 assertDLQ L11c）。
// 用于已知有损映射（如 decimal(65,30)→NUMBER(38,30) 溢出）的验收：
// 损失行数落 DLQ 且 reason 可查，是 PASS 的前置可见性证据。
//
// dml.seed：非 0 = 固定种子（单轮复现用）；0 = 每轮按 轮次×100003+表序
// 派生（L11d：固定种子跨轮重放同一批随机值，字符串 PK 表必撞 1062）。
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

	// ExpectDLQ 表 → {ORA 码: 期望死信行数}。轮末按本轮 topic 断言
	// 每个 ORA 码的行数与总行数精确匹配（K3「有损必须可见」机器校验）。
	ExpectDLQ map[string]map[string]int `json:"expect-dlq"`
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

	// expect-dlq 断言结果独立于业务错误（L3）：预期死信会造成预期的
	// verify DIFF，不能因此掩盖「断言本身通过」这一证据。
	dlqAsserted bool
	dlqErr      error
	dlqNote     string
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
	for key := range bc.ExpectDLQ {
		if !containsTable(tables, key) {
			log.Fatalf("tidb-batch: expect-dlq 引用的表 %q 不在 tables 列表", key)
		}
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
		if r.err == nil && r.dlqErr == nil && r.fullEqual && (r.incrSkipped || r.incrEqual) {
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
		dlq := "-"
		if r.dlqAsserted {
			dlq = r.dlqNote
		}
		fmt.Printf("  %-6s %-30s full=%-6s incr=%-6s rows=%d dlq=%s stage=%s err=%v\n",
			status, k, boolStr(r.fullEqual), incr, r.rows, dlq, r.stage, r.err)
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

// expectDLQTotal 表的预期死信总数（expect-dlq 各 ORA 码之和）。
func expectDLQTotal(bc *batchCfg, key string) int {
	total := 0
	for _, n := range bc.ExpectDLQ[key] {
		total += n
	}
	return total
}

// reportOK 比对报告判定（L3/K3a）：
//   - 普通表 = Equal()；
//   - expect-dlq 表（已知有损）= 行值差异 0、目标无多余、缺失数恰为
//     预期死信数——即「全部损失可见且仅限于预期损失」。抽样模式下
//     死信行不一定被抽中，缺失数放宽为 ≤ 预期。
func reportOK(rep *verify.Report, lossy int, sampled bool) bool {
	if lossy <= 0 {
		return rep.Equal()
	}
	if rep.RowDiffCount != 0 || rep.ExtraCount != 0 {
		return false
	}
	if sampled {
		return rep.MissingCount <= int64(lossy)
	}
	return rep.MissingCount == int64(lossy)
}

// reportOKIncr 增量轮有损判定（L11c）：rowDiff+missing+extra ≤ 该表
// 累计死信总数（全量预期死信 + 增量随机超发），抽样模式天然满足。
// 未声明 expect-dlq 的表 dlqTotal=0，退化为 Equal() 强判。
func reportOKIncr(rep *verify.Report, dlqTotal int64, sampled bool) bool {
	if dlqTotal <= 0 {
		return rep.Equal()
	}
	if sampled {
		return true // 抽样不保证逐行覆盖，损失可见性由 assertDLQ 把关
	}
	affected := rep.RowDiffCount + rep.MissingCount + rep.ExtraCount
	return affected <= dlqTotal
}

func containsTable(tables []tableEntry, key string) bool {
	for _, te := range tables {
		if te.db+"."+te.table == key {
			return true
		}
	}
	return false
}

// assertDLQ 机器校验「有损必须可见」（K3）：对 expect-dlq 声明的表，
// 按本轮 topic（前缀唯一，天然限定本轮死信）断言：
//  1. 每个 ORA 码的行数（reason LIKE 匹配）：全量-only 精确相等；
//  2. 该表死信总行数 = 期望之和。
//
// L11c：增量轮（DML 活跃）dmlgen 随机值可再次触发已声明码的死信
// （如对含超大 DECIMAL 行的 UPDATE 重放 ORA-01438），精确断言必假 FAIL。
// 放宽为「下限 + 白名单」：各码 got ≥ want、total got ≥ want 之和、
// 且每条 reason 必须命中某个已声明码——随机超发的损失可见且可解释，
// 预期外的 reason（如 c_bit 误 base64 触发的 ORA-01722）仍 FAIL。
//
// 断言失败记入该表 dlqErr——独立于业务错误（L3）：预期死信本身会造成
// 预期的 verify DIFF / 行数缺口，属业务侧已知结果，不得与之混叠，否则
// 「机器断言通过」这一证据会被掩盖。汇总仍 FAIL、退出码 1，不断轮。
func assertDLQ(ctx context.Context, ora *sql.DB, bc *batchCfg, prefix string, tables []tableEntry, results map[string]*tableResult) {
	relax := dmlActive(bc)
	byKey := make(map[string]tableEntry, len(tables))
	for _, te := range tables {
		byKey[te.db+"."+te.table] = te
	}
	for key, wantByCode := range bc.ExpectDLQ {
		r := results[key]
		te := byKey[key]
		topic := prefix + te.db + "-" + te.table
		r.dlqAsserted = true

		var wantTotal int
		parts := make([]string, 0, len(wantByCode))
		for code, n := range wantByCode {
			wantTotal += n
			var got int
			if err := ora.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM "kafka_dlq" WHERE "topic" = :t AND "reason" LIKE :pat`,
				sql.Named("t", topic), sql.Named("pat", "%"+code+"%")).Scan(&got); err != nil {
				r.dlqErr = wrapDLQErr(r.dlqErr, key, "query %s: %v", code, err)
				continue
			}
			if (relax && got < n) || (!relax && got != n) {
				r.dlqErr = wrapDLQErr(r.dlqErr, key, "%s want %d rows, got %d", code, n, got)
			}
			parts = append(parts, fmt.Sprintf("%s:%d", code, got))
		}
		var gotTotal int
		if err := ora.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM "kafka_dlq" WHERE "topic" = :t`,
			sql.Named("t", topic)).Scan(&gotTotal); err != nil {
			r.dlqErr = wrapDLQErr(r.dlqErr, key, "query total: %v", err)
			continue
		}
		if (relax && gotTotal < wantTotal) || (!relax && gotTotal != wantTotal) {
			r.dlqErr = wrapDLQErr(r.dlqErr, key, "total want %d rows (sum of codes), got %d — declared loss not fully visible", wantTotal, gotTotal)
		}
		if relax && r.dlqErr == nil {
			// 白名单：随机超发的死信 reason 必须仍属已声明码
			rows, err := ora.QueryContext(ctx,
				`SELECT "reason", COUNT(*) FROM "kafka_dlq" WHERE "topic" = :t GROUP BY "reason"`,
				sql.Named("t", topic))
			if err != nil {
				r.dlqErr = wrapDLQErr(r.dlqErr, key, "query reasons: %v", err)
			} else {
				for rows.Next() {
					var reason string
					var n int
					if err := rows.Scan(&reason, &n); err != nil {
						r.dlqErr = wrapDLQErr(r.dlqErr, key, "scan reason: %v", err)
						break
					}
					if !reasonMatchesDeclared(reason, wantByCode) {
						r.dlqErr = wrapDLQErr(r.dlqErr, key, "unexpected DLQ reason %q (%d rows) outside declared codes", reason, n)
					}
				}
				rows.Close()
			}
		}
		// dlqNote 只反映断言本身（L3）：OK 与否不搭业务错误。
		if r.dlqErr == nil {
			sort.Strings(parts)
			mode := ""
			if relax {
				mode = ",lower-bound"
			}
			r.dlqNote = fmt.Sprintf("OK(%s%s)", strings.Join(parts, ","), mode)
		} else {
			r.dlqNote = fmt.Sprintf("FAIL(%v)", r.dlqErr)
		}
	}
}

// reasonMatchesDeclared reason 文本命中任一已声明码（与 per-code 查询
// 的 LIKE %code% 同口径）。
func reasonMatchesDeclared(reason string, wantByCode map[string]int) bool {
	for code := range wantByCode {
		if strings.Contains(reason, code) {
			return true
		}
	}
	return false
}

// dmlActive 本轮是否执行增量 DML。
func dmlActive(bc *batchCfg) bool {
	return bc.DML.Inserts > 0 || bc.DML.Updates > 0 || bc.DML.Deletes > 0
}

// wrapDLQErr 追加 expect-dlq 断言错误，保留该表既有失败信息。
func wrapDLQErr(prev error, key, format string, args ...interface{}) error {
	e := fmt.Errorf("expect-dlq %s: %s", key, fmt.Sprintf(format, args...))
	if prev == nil {
		return e
	}
	return fmt.Errorf("%w; %s", prev, e)
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
		lossy := expectDLQTotal(bc, key)
		want := fullRes.Tables[key]
		if lossy > 0 {
			// 预期死信行不落 Oracle（K3a）：消费到位的行数 = 快照行数
			// - 预期死信数，否则 count 轮询会空等到超时。
			want -= int64(lossy)
		}
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
		results[key].fullEqual = reportOK(rep, lossy, bc.Sample > 0)
		if !results[key].fullEqual {
			logReport(key, rep)
		}
	}

	// ---- Step 4: 增量轮（可选）----
	if dmlActive(bc) {
		for idx, te := range tables {
			key := te.db + "." + te.table
			results[key].stage = "incr-dml"
			if results[key].err != nil {
				results[key].incrSkipped = true
				continue // 全量已败，增量无意义
			}
			topic := prefix + te.db + "-" + te.table
			// L11d：seed=0 时跨轮复用同一随机序列，字符串 PK 表必然重放
			// 出重复值撞 1062；改为轮次+表序派生（单轮内仍确定可复现）。
			seed := bc.DML.Seed
			if seed == 0 {
				seed = round*100003 + int64(idx)
			}
			st, err := dmlgen.Run(ctx, dmlgen.Config{
				DSN: withDatabase(bc.TiDBDSN, te.db),
				Table: te.table, Brokers: []string{bc.KafkaBootstrapServer}, Topic: topic,
				NInsert: bc.DML.Inserts, NUpdate: bc.DML.Updates, NDelete: bc.DML.Deletes,
				Seed: seed,
			})
			if err != nil {
				results[key].err = fmt.Errorf("dmlgen: %w", err)
				results[key].incrSkipped = true
				continue
			}
			// 终态行数 = 全量 + ins - del（UPDATE 不变）
			want := results[key].rows + int64(st.Inserted) - int64(st.Deleted)
			results[key].stage = "incr-consume"
			// L11c：随机值可能新增死信（如对超大 DECIMAL 行的 UPDATE 撞
			// ORA-01438），行数永远到不了 want——收敛条件放宽为
			// 「缺口 ≤ 本轮新增死信数」（每条死信恰好解释一行缺口）。
			dlqNow, err := waitForRowsIncr(ctx, ora, bc.TargetSchema, te, topic, want, bc.ConsumeTimeoutSec)
			if err != nil {
				results[key].err = err
				continue
			}
			results[key].rows = want - dlqNow.shortfall
			rep, err := verifyTable(ctx, bc, dsn, te)
			if err != nil {
				results[key].err = err
				continue
			}
			// L11c：增量有损判定放宽为「损失全部可见且可解释」——
			// rowDiff+missing+extra ≤ 该表累计死信总数（每条死信至多
			// 损伤一行；死信 reason 白名单由轮末 assertDLQ 把关，预期外
			// 码仍 FAIL）。真 bug（如 c_bit 误 base64 的 18 处假 DIFF）
			// 远超死信数，必被拦下。
			var totalDLQ int64
			if err := ora.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM "kafka_dlq" WHERE "topic" = :t`,
				sql.Named("t", topic)).Scan(&totalDLQ); err != nil {
				results[key].err = fmt.Errorf("query dlq total: %w", err)
				continue
			}
			results[key].incrEqual = reportOKIncr(rep, totalDLQ, bc.Sample > 0)
			if !results[key].incrEqual {
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

	// ---- Step 5.5: DLQ 断言（expect-dlq，K3「有损必须可见」）----
	if len(bc.ExpectDLQ) > 0 {
		assertDLQ(ctx, ora, bc, prefix, tables, results)
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
		return verify.Sample(ctx, src, dst, bc.Sample, verify.Options{EmptyStringAsNull: true})
	}
	return verify.Compare(ctx, src, dst, verify.Options{EmptyStringAsNull: true})
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

// incrConsumeState 增量消费收敛后的状态：shortfall = want - 实际行数，
// 由本轮新增死信解释（每条死信至多造成一行缺口）。
type incrConsumeState struct {
	shortfall int64
	dlqIncr   int64
}

// waitForRowsIncr 增量轮 count 轮询（L11c）：随机死信使行数到不了
// want，收敛条件 = 行数到位，或（缺口 ≤ 新增死信数 且 状态已稳定——
// 行数与死信数连续 stablePolls 次不变，避免在批中途误判收敛）。
func waitForRowsIncr(ctx context.Context, ora *sql.DB, schema string, te tableEntry, topic string, want int64, timeoutSec int) (incrConsumeState, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, schema, te.table)
	dlqQ := `SELECT COUNT(*) FROM "kafka_dlq" WHERE "topic" = :t`
	var baseDLQ int64
	if err := ora.QueryRowContext(ctx, dlqQ, sql.Named("t", topic)).Scan(&baseDLQ); err != nil && !isOracleNotFound(err) {
		// 死信表可能尚未建（无死信路径），按 0 起步
		baseDLQ = 0
	}
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	var last, lastDLQ int64 = -1, baseDLQ
	stable := 0
	const stablePolls = 6 // 6×500ms = 3s 无变化
	for time.Now().Before(deadline) {
		var n int64
		if err := ora.QueryRowContext(ctx, q).Scan(&n); err != nil {
			if !isOracleNotFound(err) {
				return incrConsumeState{}, fmt.Errorf("count %s.%s: %w", schema, te.table, err)
			}
			n = -2
		}
		var dlq int64
		_ = ora.QueryRowContext(ctx, dlqQ, sql.Named("t", topic)).Scan(&dlq)
		if n == want {
			return incrConsumeState{shortfall: 0, dlqIncr: dlq - baseDLQ}, nil
		}
		if n == last && dlq == lastDLQ {
			stable++
		} else {
			stable = 0
		}
		if stable >= stablePolls && n >= 0 {
			shortfall := want - n
			if shortfall <= dlq-baseDLQ {
				return incrConsumeState{shortfall: shortfall, dlqIncr: dlq - baseDLQ}, nil
			}
		}
		last, lastDLQ = n, dlq
		select {
		case <-ctx.Done():
			return incrConsumeState{}, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return incrConsumeState{}, fmt.Errorf("consume timeout: %s.%s rows=%d want=%d (unexplained shortfall, dlqIncr=%d)",
		schema, te.table, last, want, lastDLQ-baseDLQ)
}

func isOracleNotFound(err error) bool {
	return err != nil && strings.Contains(strings.ToUpper(err.Error()), "ORA-00942")
}

// logReport 打印 DIFF 概要 + 列级样例差异（L8 排查体验：RowDiffs/Missing/
// Extra 的具体 PK 与列值直接进日志，不用再回数据库手工捞）。
func logReport(key string, rep *verify.Report) {
	log.Printf("[batch] DIFF %s: src=%d dst=%d rowDiff=%d missing=%d extra=%d",
		key, rep.SourceRows, rep.TargetRows, rep.RowDiffCount, rep.MissingCount, rep.ExtraCount)
	for _, d := range rep.RowDiffs {
		for _, c := range d.Columns {
			log.Printf("[batch]   %s pk=%s col=%s: source=%s target=%s",
				key, verify.FormatPK(d.PK), c.Column, shortVal(c.Source), shortVal(c.Target))
		}
	}
	for i, pk := range rep.MissingInTarget {
		if i >= 5 {
			log.Printf("[batch]   %s missing in target: ... (%d more)", key, rep.MissingCount-int64(i))
			break
		}
		log.Printf("[batch]   %s missing in target: pk=%s", key, verify.FormatPK(pk))
	}
	for i, pk := range rep.ExtraInTarget {
		if i >= 5 {
			log.Printf("[batch]   %s extra in target: ... (%d more)", key, rep.ExtraCount-int64(i))
			break
		}
		log.Printf("[batch]   %s extra in target: pk=%s", key, verify.FormatPK(pk))
	}
}

func shortVal(s string) string {
	if len(s) > 120 {
		return fmt.Sprintf("%q", s[:120]) + "..."
	}
	return fmt.Sprintf("%q", s)
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
