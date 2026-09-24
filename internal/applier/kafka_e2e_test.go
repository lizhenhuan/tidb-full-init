// L2 真 Kafka E2E（M2 T2，testcontainers redpanda）：验证 consumer↔broker↔applier
// 边界——真实位点/续传、跨分区窗口、HighWatermark、以及 kafka-go Message.Value
// 生命周期（P2#7：applier 把 msg.Value 存窗口当 DLQ Raw，断言「DLQ raw == 原文」，
// 驱动若复用底层 buffer 会当场暴露）。
//
// 依赖 Docker（colima）；显式开关启动：
//
//	E2E_KAFKA=1 DOCKER_HOST=unix://$HOME/.colima/default/docker.sock \
//	  go test ./internal/applier/ -run E2EKafka -v -count=1
package applier

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/lizhenhuan/tidb-full-init/internal/checkpoint"
	"github.com/lizhenhuan/tidb-full-init/internal/consumer"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/mocksink"
)

const e2eTopic = "e2e.orders"

// fixedBalancer 强制 kafka-go Writer 落到固定分区（v0.4.51 的 balancer
// 不读 Message.Partition，低层 ProduceRequest 又只收 RecordReader）。
type fixedBalancer struct{ p int }

func (f fixedBalancer) Balance(kafka.Message, ...int) int { return f.p }

func e2eInsertValue(db, table, id string) []byte {
	return []byte(fmt.Sprintf(`{"database":%q,"table":%q,"type":"INSERT","pkNames":["id"],"data":[{"id":%q,"v":"x"}]}`, db, table, id))
}

// runUntil 轮询直到条件为真或超时。
func runUntil(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// runPipelineAsync 后台跑管道，返回取消与等待函数。
func runPipelineAsync(t *testing.T, broker, cpPath string, snk *mocksink.Sink, dlq DLQWriter) (context.CancelFunc, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cons := consumer.NewKafkaConsumer(consumer.KafkaConfig{Brokers: []string{broker}})
	store, err := checkpoint.NewSQLiteStore(ctx, cpPath)
	if err != nil {
		cancel()
		t.Fatalf("open checkpoint: %v", err)
	}
	p := New(cons, snk, store, testMeta, dlq, Config{
		BatchSize: 100, FlushInterval: 100 * time.Millisecond, MaxRetries: 3, RetryBackoff: 10 * time.Millisecond,
	})
	partitions := []model.TopicPartition{{Topic: e2eTopic, Partition: 0}, {Topic: e2eTopic, Partition: 1}}
	done := make(chan error, 1)
	go func() {
		done <- p.Run(ctx, partitions)
		cons.Close()
		store.Close()
	}()
	return cancel, func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("pipeline did not stop within 10s")
			return nil
		}
	}
}

func TestE2EKafkaPipeline(t *testing.T) {
	if os.Getenv("E2E_KAFKA") != "1" {
		t.Skip("E2E_KAFKA != 1; skipping L2 Kafka E2E (needs Docker)")
	}
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

	cl := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 10 * time.Second}
	if _, err := cl.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: e2eTopic, NumPartitions: 2, ReplicationFactor: 1}},
	}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	// fixedBalancer 强制 Writer 落到指定分区（v0.4.51 的 balancer 不读
	// Message.Partition，低层 ProduceRequest 又只收 RecordReader）。
	produce := func(partition int, values ...[]byte) {
		t.Helper()
		w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: e2eTopic, Balancer: fixedBalancer{p: partition}}
		defer w.Close()
		msgs := make([]kafka.Message, len(values))
		for i, v := range values {
			msgs[i] = kafka.Message{Value: v}
		}
		if err := w.WriteMessages(ctx, msgs...); err != nil {
			t.Fatalf("produce p%d: %v", partition, err)
		}
	}

	rawBad := []byte(`{"type":`) // 解码失败样本：DLQ raw 必须逐字节等于它（P2#7）
	produce(0, e2eInsertValue("d", "t1", "1"), rawBad)
	produce(1, e2eInsertValue("d", "t1", "2"), e2eInsertValue("d", "t2", "a"))

	cpPath := filepath.Join(t.TempDir(), "cp.db")
	ms := mocksink.New()
	dlq := &dlqRecorder{}
	cancel1, wait1 := runPipelineAsync(t, broker, cpPath, ms, dlq)

	tp0 := model.TopicPartition{Topic: e2eTopic, Partition: 0}
	tp1 := model.TopicPartition{Topic: e2eTopic, Partition: 1}
	store, err := checkpoint.NewSQLiteStore(ctx, cpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runUntil(t, 30*time.Second, func() bool {
		o0, _ := store.Load(ctx, tp0)
		o1, _ := store.Load(ctx, tp1)
		return o0 == 1 && o1 == 1
	}, "both partitions checkpointed to max offset")

	cancel1()
	if err := wait1(); err != nil {
		t.Fatalf("pipeline #1: %v", err)
	}

	// 应用结果：3 好 + 1 解码失败
	if got := ms.Rows("d", "t1"); got != 2 {
		t.Errorf("t1 rows = %d, want 2", got)
	}
	if got := ms.Rows("d", "t2"); got != 1 {
		t.Errorf("t2 rows = %d, want 1", got)
	}
	rows := dlq.snapshot()
	if len(rows) != 1 {
		t.Fatalf("DLQ rows = %d, want 1", len(rows))
	}
	if rows[0].Topic != e2eTopic || rows[0].Partition != 0 || rows[0].Offset != 1 {
		t.Errorf("DLQ coords = %+v, want %s/0@1", rows[0], e2eTopic)
	}
	if !bytes.Equal(rows[0].Raw, rawBad) {
		t.Errorf("DLQ raw = %q, want exact original bytes %q (kafka-go Message.Value lifecycle, P2#7)", rows[0].Raw, rawBad)
	}

	// HighWatermark：两分区各 2 条 → LEO=2
	cons2 := consumer.NewKafkaConsumer(consumer.KafkaConfig{Brokers: []string{broker}})
	hw0, err := cons2.HighWatermark(ctx, tp0)
	if err != nil {
		t.Fatalf("highwatermark: %v", err)
	}
	if hw0 != 2 {
		t.Errorf("HighWatermark p0 = %d, want 2", hw0)
	}
	cons2.Close()

	// 续传：追加 p0 offset=2，复用同一 SQLite 位点 → 只应用新消息
	produce(0, e2eInsertValue("d", "t1", "3"))
	ms2 := mocksink.New()
	cancel2, wait2 := runPipelineAsync(t, broker, cpPath, ms2, nil)
	runUntil(t, 30*time.Second, func() bool {
		o0, _ := store.Load(ctx, tp0)
		return o0 == 2
	}, "partition 0 checkpointed to 2 after resume")
	cancel2()
	if err := wait2(); err != nil {
		t.Fatalf("pipeline #2: %v", err)
	}
	if got := ms2.Rows("d", "t1"); got != 1 {
		t.Errorf("resumed t1 rows = %d, want 1 (only offset 2; replay would give 3)", got)
	}
}
