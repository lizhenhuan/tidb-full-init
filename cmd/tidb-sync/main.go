// tidb-sync：Kafka canal-json → Oracle 消费入口（M3 / #t6）。
// 把 M2 冻结管道（KafkaConsumer 手动分区 + SQLite 权威位点 +
// applier 窗口批 + oraclesink 幂等 MERGE + DLQ）装配为正式 CLI。
//
// 用法：
//
//	tidb-sync -config sync.json
//
// 配置见 internal/syncer.Config（JSON）。行为约定：
//   - 起始位点：SQLite 无记录的分区从 0 消费（全量重放），
//     有记录的分区续传（增量）；fail-fast 复用 applier 语义；
//   - SIGINT/SIGTERM 优雅退出：冲刷已攒窗口后落位点再退出；
//   - Oracle 口令可用 ORACLE_FREE_PASSWORD 环境变量注入（不入配置文件）。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lizhenhuan/tidb-full-init/internal/syncer"
)

func main() {
	configFile := flag.String("config", "", "JSON 同步任务配置文件（必填）")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[tidb-sync] ")

	if *configFile == "" {
		log.Fatal("-config is required")
	}
	cfg, err := syncer.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Load config: %v", err)
	}
	if cfg.Oracle.Password == "" {
		cfg.Oracle.Password = os.Getenv("ORACLE_FREE_PASSWORD")
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid config: %v", err)
	}
	log.Printf("Config: tidb=%s:%s kafka=%s checkpoint=%s oracle=%s:%d/%s target-schema=%s table-groups=%d",
		cfg.TiDB.Host, cfg.TiDB.Port, cfg.KafkaBootstrapServers, cfg.CheckpointPath,
		cfg.Oracle.Host, cfg.Oracle.Port, cfg.Oracle.Service, cfg.TargetSchema, len(cfg.Tables))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		s := <-sig
		log.Printf("Received %v; draining window and committing offsets...", s)
		cancel()
	}()

	res, err := syncer.Run(ctx, cfg, syncer.Deps{})
	m := res.Metrics
	log.Printf("Metrics: applied=%d dlq=%d skippedDDL=%d retriedBatches=%d flushedBatches=%d lastApplied=%v",
		m.AppliedEvents, m.DLQEvents, m.SkippedDDL, m.RetriedBatches, m.FlushedBatches, m.LastApplied)
	if err != nil {
		log.Fatalf("Sync stopped on error: %v", err)
	}
	log.Println("Sync stopped cleanly")
}
