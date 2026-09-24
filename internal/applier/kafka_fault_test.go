// L3 故障注入 E2E（M2 T2，testcontainers redpanda；E2E_KAFKA=1 门控，
// 跑法见 kafka_e2e_test.go 文件头）。验证 at-least-once + 位点语义的三个
// 核心失败面（M2 收口复审指定）：
//
//  1. 目标端已 commit、SQLite 尚未 Save 时管道死亡 → 重启整窗重放，
//     终态幂等不丢不重；
//  2. broker 短暂不可达（容器 Stop/Start）→ 管道退出或停滞但位点不越界，
//     恢复后从 SQLite last-applied 续传，不跳不丢；
//  3. 同一 offset 重复投递（清空位点的全量重放）→ 终态幂等。
package applier

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	"github.com/lizhenhuan/tidb-full-init/internal/checkpoint"
	"github.com/lizhenhuan/tidb-full-init/internal/consumer"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/mocksink"
)

// faultGate 与 L2 E2E 同一约定：未显式设 E2E_KAFKA=1 时跳过
// （CI/无 Docker 环境默认路径必须全绿，M2 收口复审 P1）。
func faultGate(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_KAFKA") != "1" {
		t.Skip("E2E_KAFKA != 1; skipping L3 fault E2E (needs Docker)")
	}
	if testing.Short() {
		t.Skip("short mode")
	}
}

// startFaultBroker 起独立 redpanda + 双分区 topic（每个用例隔离）。
func startFaultBroker(t *testing.T) (context.Context, *redpanda.Container, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	rp, err := redpanda.Run(ctx, "docker.redpanda.com/redpandadata/redpanda:v25.2.4")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() { rp.Terminate(ctx) })
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
	return ctx, rp, broker
}

func faultProduce(t *testing.T, ctx context.Context, broker string, partition int, ids ...string) {
	t.Helper()
	w := &kafka.Writer{Addr: kafka.TCP(broker), Topic: e2eTopic, Balancer: fixedBalancer{p: partition}}
	defer w.Close()
	msgs := make([]kafka.Message, len(ids))
	for i, id := range ids {
		msgs[i] = kafka.Message{Value: e2eInsertValue("d", "t1", id)}
	}
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("produce p%d: %v", partition, err)
	}
}

// runPipelineStore 用指定 store/sink 后台跑管道（不复用 runPipelineAsync：
// 故障场景需要注入 store 失败与复用 sink 终态）。
func runPipelineStore(ctx context.Context, t *testing.T, broker string, store checkpoint.OffsetStore, snk sink.Sink, dlq DLQWriter, cfg Config) (context.CancelFunc, <-chan error) {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	cons := consumer.NewKafkaConsumer(consumer.KafkaConfig{Brokers: []string{broker}})
	p := New(cons, snk, store, testMeta, dlq, cfg)
	partitions := []model.TopicPartition{{Topic: e2eTopic, Partition: 0}, {Topic: e2eTopic, Partition: 1}}
	done := make(chan error, 1)
	go func() {
		done <- p.Run(runCtx, partitions)
		cons.Close()
	}()
	return cancel, done
}

func waitPipeline(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("pipeline did not stop within 30s")
		return nil
	}
}

// flakyStore 注入 Save 失败：模拟「目标端已 commit、位点尚未持久化」窗口。
type flakyStore struct {
	checkpoint.OffsetStore
	failSave atomic.Bool
}

func (f *flakyStore) Save(ctx context.Context, tp model.TopicPartition, offset int64) error {
	if f.failSave.Load() {
		return errors.New("injected: disk full on checkpoint save")
	}
	return f.OffsetStore.Save(ctx, tp, offset)
}

var faultCfg = Config{BatchSize: 50, FlushInterval: 100 * time.Millisecond, MaxRetries: 3, RetryBackoff: 10 * time.Millisecond}

var faultTP0 = model.TopicPartition{Topic: e2eTopic, Partition: 0}

// 场景 1：目标端已 commit、SQLite 未 Save → 管道中止（位点未前进）→
// 重启后整窗重放，终态幂等（行数不变 = 不丢不重）。
func TestE2EFaultCommitBeforeCheckpoint(t *testing.T) {
	faultGate(t)
	ctx, _, broker := startFaultBroker(t)
	faultProduce(t, ctx, broker, 0, "1", "2")

	inner, err := checkpoint.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	fs := &flakyStore{OffsetStore: inner}
	fs.failSave.Store(true)

	ms := mocksink.New()
	cancel, done := runPipelineStore(ctx, t, broker, fs, ms, nil, faultCfg)
	runErr := waitPipeline(t, done)
	cancel()
	if runErr == nil || fmt.Sprint(runErr) == "" || !contains(runErr.Error(), "save checkpoint") {
		t.Fatalf("Run must fail on checkpoint save failure, got %v", runErr)
	}
	if got := ms.Rows("d", "t1"); got != 2 {
		t.Fatalf("sink rows = %d, want 2 (target committed before Save failure)", got)
	}
	if off, _ := inner.Load(ctx, faultTP0); off != -1 {
		t.Fatalf("checkpoint = %d, want -1 (Save failed, offset not advanced)", off)
	}

	// 故障恢复：Save 恢复工作，同一 sink 终态上整窗重放 → 行数不变
	fs.failSave.Store(false)
	cancel2, done2 := runPipelineStore(ctx, t, broker, fs, ms, nil, faultCfg)
	runUntil(t, 30*time.Second, func() bool {
		off, _ := inner.Load(ctx, faultTP0)
		return off == 1
	}, "checkpoint reaches 1 after recovery")
	cancel2()
	if err := waitPipeline(t, done2); err != nil {
		t.Fatalf("recovery Run: %v", err)
	}
	if got := ms.Rows("d", "t1"); got != 2 {
		t.Fatalf("rows after full replay = %d, want 2 (idempotent, no dup/loss)", got)
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0) }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// brokerReady 轮询 broker 恢复可达（容器 Start 返回不等于监听就绪）。
func brokerReady(t *testing.T, ctx context.Context, broker string) {
	t.Helper()
	cl := &kafka.Client{Addr: kafka.TCP(broker), Timeout: 3 * time.Second}
	for i := 0; i < 60; i++ {
		if _, err := cl.Metadata(ctx, &kafka.MetadataRequest{}); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("ctx done while waiting for broker")
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatal("broker not reachable within 30s after restart")
}

// 场景 2：broker 短暂不可达（容器 Stop/Start）→ 管道 fail-loud 退出
// （分区泵连接错误上抛，收口复审 P1）但位点停在 last-applied；
// 恢复后重启管道续传，全部消息到达、无跳过、无重复。
func TestE2EFaultBrokerRestart(t *testing.T) {
	faultGate(t)
	ctx, rp, broker := startFaultBroker(t)
	ids := make([]string, 120)
	for i := range ids {
		ids[i] = fmt.Sprintf("%d", i+1)
	}
	faultProduce(t, ctx, broker, 0, ids...)

	store, err := checkpoint.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ms := mocksink.New()
	cancel, done := runPipelineStore(ctx, t, broker, store, ms, nil, faultCfg)

	// 等第一个窗口落位点后掐 broker
	runUntil(t, 30*time.Second, func() bool {
		off, _ := store.Load(ctx, faultTP0)
		return off >= 0
	}, "first window checkpointed")
	if err := rp.Stop(ctx, nil); err != nil {
		t.Fatalf("stop broker: %v", err)
	}
	time.Sleep(2 * time.Second) // 断连窗口
	if err := rp.Start(ctx); err != nil {
		t.Fatalf("start broker: %v", err)
	}

	// 断连期间管道 fail-loud 退出（预期路径）或原地存活；退出则等 broker
	// 就绪后重启续传。重启若再撞上未就绪窗口退出，继续重试。
	deadline := time.Now().Add(90 * time.Second)
	for {
		select {
		case err := <-done:
			t.Logf("pipeline exited during outage (fail-loud, acceptable): %v", err)
			brokerReady(t, ctx, broker)
			cancel, done = runPipelineStore(ctx, t, broker, store, ms, nil, faultCfg)
		default:
		}
		off, _ := store.Load(ctx, faultTP0)
		if off == 119 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint did not reach 119 within 90s (last=%d)", off)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	if err := waitPipeline(t, done); err != nil {
		// 两类可容忍的收尾错误（数据完整性由下方 rows + 循环内 off==119 把关）：
		// ① 断连期间死掉的分区泵，其错误在消费完剩余消息后才被 Fetch 取到——
		//    fail-loud 上报本身正确（专门用例 TestE2EFaultPumpDeathFailsLoud）；
		// ② 原管道实际已完成全部工作，替换管道被我们 cancel 在启动途中。
		if contains(err.Error(), "partition pump") || contains(err.Error(), "context canceled") {
			t.Logf("final Run exited with %v (tolerable post-recovery path)", err)
		} else {
			t.Fatalf("final Run: %v", err)
		}
	}
	if got := ms.Rows("d", "t1"); got != 120 {
		t.Fatalf("rows = %d, want 120 (no skip, no dup across broker restart)", got)
	}
}

// 场景 3：同一批 offset 重复投递（清空位点全量重放）→ 终态幂等。
func TestE2EFaultDuplicateDelivery(t *testing.T) {
	faultGate(t)
	ctx, _, broker := startFaultBroker(t)
	faultProduce(t, ctx, broker, 0, "1", "2", "3")

	cp1 := filepath.Join(t.TempDir(), "cp1.db")
	store1, err := checkpoint.NewSQLiteStore(ctx, cp1)
	if err != nil {
		t.Fatal(err)
	}
	ms := mocksink.New()
	cancel, done := runPipelineStore(ctx, t, broker, store1, ms, nil, faultCfg)
	runUntil(t, 30*time.Second, func() bool {
		off, _ := store1.Load(ctx, faultTP0)
		return off == 2
	}, "first pass completes")
	cancel()
	if err := waitPipeline(t, done); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if got := ms.Rows("d", "t1"); got != 3 {
		t.Fatalf("first pass rows = %d, want 3", got)
	}

	// 位点清零 = 同一 offset 全量重复投递；复用同一 sink 观察终态
	store2, err := checkpoint.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "cp2.db"))
	if err != nil {
		t.Fatal(err)
	}
	cancel2, done2 := runPipelineStore(ctx, t, broker, store2, ms, nil, faultCfg)
	runUntil(t, 30*time.Second, func() bool {
		off, _ := store2.Load(ctx, faultTP0)
		return off == 2
	}, "duplicate delivery completes")
	cancel2()
	if err := waitPipeline(t, done2); err != nil {
		t.Fatalf("replay Run: %v", err)
	}
	if got := ms.Rows("d", "t1"); got != 3 {
		t.Fatalf("rows after duplicate delivery = %d, want 3 (idempotent terminal state)", got)
	}
}

// 场景 4（收口复审 P1）：单分区泵致命错误必须 fail loud——
// 预置恢复位点 4（下一条 = 5）但分区 LEO=0（retention 截断越过
// SQLite last-applied 的丢数据场景）→ 泵收到 OffsetOutOfRange，
// Run 必须报错退出，不得静默空转、不得前进位点。
func TestE2EFaultPumpDeathFailsLoud(t *testing.T) {
	faultGate(t)
	ctx, _, broker := startFaultBroker(t) // topic 为空，LEO=0

	store, err := checkpoint.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(ctx, faultTP0, 4); err != nil {
		t.Fatal(err)
	}

	ms := mocksink.New()
	cancel, done := runPipelineStore(ctx, t, broker, store, ms, nil, faultCfg)
	defer cancel()
	runErr := waitPipeline(t, done)
	if runErr == nil || !contains(runErr.Error(), "partition pump") {
		t.Fatalf("Run must fail loud on partition pump death, got %v", runErr)
	}
	if got := ms.Rows("d", "t1"); got != 0 {
		t.Fatalf("rows = %d, want 0", got)
	}
	if off, _ := store.Load(ctx, faultTP0); off != 4 {
		t.Fatalf("checkpoint = %d, want 4 (unchanged)", off)
	}
}

// 二次 Assign 拒绝（收口复审 P2：旧 readers 泄漏 + cancel 覆盖）。
// NewReader/SetOffset 不触网，无需 Docker，不走 E2E 门控。
func TestAssignTwiceRejected(t *testing.T) {
	c := consumer.NewKafkaConsumer(consumer.KafkaConfig{Brokers: []string{"127.0.0.1:19092"}})
	defer c.Close()
	tps := []model.TopicPartition{{Topic: "t", Partition: 0}}
	if err := c.Assign(context.Background(), tps); err != nil {
		t.Fatalf("first Assign: %v", err)
	}
	if err := c.Assign(context.Background(), tps); err == nil || !contains(err.Error(), "already assigned") {
		t.Fatalf("second Assign must be rejected, got %v", err)
	}
}
