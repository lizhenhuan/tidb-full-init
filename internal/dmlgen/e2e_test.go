// dmlgen L2 E2E（#t6）：真 TiDB（10.168.2.232）+ 真 redpanda。
// dmlgen 执行 I/U/D 并发 canal-json；测试从 topic 读回全部消息，
// 逐条过 decoder 并断言 op 计数与 Stats 一致——验证
// 「TiDB 落库 ↔ Kafka 消息」双写不丢不重、形状可解码。
//
// 跑法（需要 Docker + 内网 TiDB 同时可达）：
//
//	E2E_TIDB=1 E2E_KAFKA=1 DOCKER_HOST=unix://$HOME/.colima/default/docker.sock \
//	  TESTCONTAINERS_RYUK_DISABLED=true go test ./internal/dmlgen/ -run E2E -v -count=1
package dmlgen

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	_ "github.com/go-sql-driver/mysql"
	"github.com/lizhenhuan/tidb-full-init/internal/decoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

const e2eDSN = "root:root123@tcp(10.168.2.232:4000)/test"

func e2eGate(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_TIDB") != "1" || os.Getenv("E2E_KAFKA") != "1" {
		t.Skip("E2E_TIDK/E2E_KAFKA != 1; skipping dmlgen E2E (needs Docker + real TiDB)")
	}
}

func TestE2EDmlGenRealTiDBKafka(t *testing.T) {
	e2eGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	db, err := sql.Open("mysql", e2eDSN)
	if err != nil {
		t.Fatalf("open tidb: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping tidb: %v", err)
	}
	// 独立 schema 防污染：dmlgen_e2e_<ts>
	schema := fmt.Sprintf("dmlgen_e2e_%d", time.Now().UnixNano()%1e6)
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+schema); err != nil {
		t.Fatalf("create db: %v", err)
	}
	defer db.Exec("DROP DATABASE " + schema)
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s.t1 (id bigint PRIMARY KEY AUTO_INCREMENT, v varchar(255), score int)", schema)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v25.2.4")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	defer rp.Terminate(ctx)
	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	topic := "dmlgen-e2e"
	if _, err := (&kafka.Client{Addr: kafka.TCP(broker), Timeout: 10 * time.Second}).CreateTopics(ctx,
		&kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{
			{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}}}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	const nIns, nUpd, nDel = 20, 8, 5
	st, err := Run(ctx, Config{
		DSN: fmt.Sprintf("root:root123@tcp(10.168.2.232:4000)/%s", schema),
		Table: "t1", Brokers: []string{broker}, Topic: topic,
		NInsert: nIns, NUpdate: nUpd, NDelete: nDel, Seed: 42,
	})
	if err != nil {
		t.Fatalf("dmlgen.Run: %v", err)
	}
	if st.Inserted != nIns || st.Updated != nUpd || st.Deleted != nDel {
		t.Fatalf("stats = %+v, want %d/%d/%d", st, nIns, nUpd, nDel)
	}

	// TiDB 侧终态 = 20 - 5 = 15 行
	var rows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+schema+".t1").Scan(&rows); err != nil {
		t.Fatalf("count: %v", err)
	}
	if rows != nIns-nDel {
		t.Fatalf("tidb rows = %d, want %d", rows, nIns-nDel)
	}

	// 读回全部消息，逐条解码，op 计数必须与 stats 一致（不丢不重）。
	// 2 分区 topic：用 ListOffsets 拿各分区精确条数，读尽即止。
	cl := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 10 * time.Second}
	lo, err := cl.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: {kafka.FirstOffsetOf(0), kafka.LastOffsetOf(0), kafka.FirstOffsetOf(1), kafka.LastOffsetOf(1)},
		},
	})
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}
	ops := map[model.OpType]int{}
	for _, part := range lo.Topics[topic] {
		count := part.LastOffset - part.FirstOffset
		if count <= 0 {
			continue
		}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker}, Topic: topic, Partition: part.Partition,
			MinBytes: 1, MaxBytes: 10 << 20, MaxWait: 200 * time.Millisecond,
		})
		if err := r.SetOffset(part.FirstOffset); err != nil {
			t.Fatalf("seek p%d: %v", part.Partition, err)
		}
		for i := int64(0); i < count; i++ {
			rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
			m, ferr := r.FetchMessage(rctx)
			rcancel()
			if ferr != nil {
				t.Fatalf("fetch p%d #%d: %v", part.Partition, i, ferr)
			}
			ev, derr := decoder.CanalJSON(m.Value)
			if derr != nil {
				t.Fatalf("decode p%d offset=%d: %v\nraw: %s", part.Partition, m.Offset, derr, m.Value)
			}
			re, ok := ev.(*model.RowEvent)
			if !ok {
				t.Fatalf("want RowEvent, got %T", ev)
			}
			ops[re.Op]++
			if re.Database != schema || re.Table != "t1" {
				t.Errorf("db/table = %s.%s", re.Database, re.Table)
			}
			if _, ok := re.After["id"]; !ok {
				t.Errorf("After missing pk: %+v", re.After)
			}
		}
		r.Close()
	}
	if ops[model.OpInsert] != nIns || ops[model.OpUpdate] != nUpd || ops[model.OpDelete] != nDel {
		t.Fatalf("decoded ops = %+v, want %d/%d/%d", ops, nIns, nUpd, nDel)
	}
}
