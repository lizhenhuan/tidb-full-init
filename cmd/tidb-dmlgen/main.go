// tidb-dmlgen 增量 DML 驱动（#t6）：对真 TiDB 执行 INSERT/UPDATE/DELETE
// 并把每条变更编码成 canal-json 发往 Kafka（encoder 生成，TiCDC 形状）。
// 跑完由 tidb-sync 消费、tidb-verify 比对，构成增量验收闭环。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lizhenhuan/tidb-full-init/internal/dmlgen"
)

func main() {
	dsn := flag.String("dsn", "", "TiDB DSN，如 root:pw@tcp(10.0.0.1:4000)/db1")
	database := flag.String("database", "", "目标库（默认取 DSN 库名）")
	table := flag.String("table", "", "目标表（必填，需有主键）")
	brokers := flag.String("brokers", "", "Kafka brokers，逗号分隔")
	topic := flag.String("topic", "", "Kafka topic（必填，与 syncer 消费的 topic 一致）")
	nInsert := flag.Int("inserts", 0, "INSERT 条数")
	nUpdate := flag.Int("updates", 0, "UPDATE 条数")
	nDelete := flag.Int("deletes", 0, "DELETE 条数")
	seed := flag.Int64("seed", 1, "随机种子（可复现）")
	maxTry := flag.Int("kafka-try", 3, "Kafka 单条写入重试次数")
	flag.Parse()

	if *dsn == "" || *table == "" || *brokers == "" || *topic == "" {
		log.Fatal("tidb-dmlgen: -dsn、-table、-brokers、-topic 均为必填")
	}
	if *nInsert == 0 && *nUpdate == 0 && *nDelete == 0 {
		log.Fatal("tidb-dmlgen: -inserts/-updates/-deletes 至少一个大于 0")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := dmlgen.Run(ctx, dmlgen.Config{
		DSN:         *dsn,
		Database:    *database,
		Table:       *table,
		Brokers:     strings.Split(*brokers, ","),
		Topic:       *topic,
		NInsert:     *nInsert,
		NUpdate:     *nUpdate,
		NDelete:     *nDelete,
		Seed:        *seed,
		MaxKafkaTry: *maxTry,
	})
	if err != nil {
		log.Fatalf("tidb-dmlgen: %v", err)
	}
	fmt.Printf("dmlgen done: inserted=%d updated=%d deleted=%d (topic=%s)\n",
		st.Inserted, st.Updated, st.Deleted, *topic)
}
