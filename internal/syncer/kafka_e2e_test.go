// syncer L2 真 Kafka E2E（#t6）：真 redpanda + 真 KafkaConsumer 走完整
// syncer.Run 装配（分区发现 Metadata 路径 + SetStartOffsets 恢复 +
// SQLite 位点），sink 用 mocksink（Oracle 集成由 leader 在有口令环境跑）。
//
// 跑法与 applier L2 相同：
//
//	E2E_KAFKA=1 DOCKER_HOST=unix://$HOME/.colima/default/docker.sock \
//	  TESTCONTAINERS_RYUK_DISABLED=true go test ./internal/syncer/ -run E2E -v -count=1
package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/mocksink"
)

func e2eGate(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_KAFKA") != "1" {
		t.Skip("E2E_KAFKA != 1; skipping L2 Kafka E2E (needs Docker)")
	}
}

func e2eMetaOK(db, table string) (*encoder.TableInfo, int64, error) {
	return &encoder.TableInfo{
		Database: db, Table: table, PKNames: []string{"id"},
		Columns: []encoder.ColumnInfo{
			{ID: 1, Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
			{ID: 2, Name: "v", MySQLType: "varchar(255)"},
		},
	}, 1, nil
}

// TestE2ESyncerRealKafka：真 broker 上跑 syncer.Run——验证
// ListTopicPartitions（Metadata 发现多分区）+ 真消费者接管线 + 位点恢复。
func TestE2ESyncerRealKafka(t *testing.T) {
	e2eGate(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v25.2.4")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	defer rp.Terminate(ctx)
	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}

	topic := "syncer-e2e.db1-t1"
	cl := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 10 * time.Second}
	if _, err := cl.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// 先发现分区（直接验证 ListTopicPartitions 的 Metadata 路径）
	parts, err := ListTopicPartitions(ctx, []string{broker}, []string{topic})
	if err != nil {
		t.Fatalf("ListTopicPartitions: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("discovered %d partitions, want 2", len(parts))
	}

	w := kafka.NewWriter(kafka.WriterConfig{Brokers: []string{broker}, Topic: topic, Balancer: fixedBalancer{}})
	defer w.Close()
	for i := 0; i < 6; i++ {
		if err := w.WriteMessages(ctx, kafka.Message{Value: insertMsg("db1", "t1", fmt.Sprint(i+1), "x")}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	cpPath := filepath.Join(t.TempDir(), "cp.db")
	cfg := &Config{
		TiDB:                  TiDBConfig{Host: "h", Port: "4000", Username: "root", Password: "p"},
		KafkaBootstrapServers: broker,
		KafkaTopicPrefix:      "syncer-e2e.",
		CheckpointPath:        cpPath,
		Oracle:                OracleConfig{Host: "h", Port: 1521, Service: "FREEPDB1", User: "system", Password: "p"},
		TargetSchema:          "shop",
		Tables:                []TableEntry{{Database: "db1", Tables: []string{"t1"}}},
		Pipeline:              PipelineConfig{BatchSize: 10, FlushIntervalMs: 100, MaxRetries: 3, RetryBackoffMs: 10},
	}

	snk := mocksink.New()
	runCtx, runCancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Run(runCtx, cfg, Deps{Sink: snk, Fetch: e2eMetaOK, DisableDLQ: true})
		done <- err
	}()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && snk.Rows("db1", "t1") < 6 {
		time.Sleep(50 * time.Millisecond)
	}
	runCancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(15 * time.Second):
		b := make([]byte, 1<<16)
		n := runtime.Stack(b, true)
		t.Fatalf("syncer did not stop within 15s\n%s", b[:n])
	}
	if got := snk.Rows("db1", "t1"); got != 6 {
		t.Fatalf("rows = %d, want 6 (across 2 partitions)", got)
	}
	if len(snk.Prepared) != 1 || snk.Prepared[0].Schema() != "shop" {
		t.Errorf("prepared = %+v", snk.Prepared)
	}
}

// fixedBalancer 与 applier L2 同款：固定分区写入。
type fixedBalancer struct{ p int }

func (f fixedBalancer) Balance(kafka.Message, ...int) int { return f.p }
