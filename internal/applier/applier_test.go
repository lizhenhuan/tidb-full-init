// Package applier 单测（M2 T2.2）：fake consumer + mocksink/stub + 内存位点，
// 覆盖管道契约的核心路径——成功应用与位点前进、DDL 跳过、解码失败 DLQ、
// 可重试整窗重试、fatal 中止不前进 offset、行级数据错误 DLQ（Raw 回填）、
// 恢复位点下发（SetStartOffsets）。
package applier

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/checkpoint"
	"github.com/lizhenhuan/tidb-full-init/internal/consumer"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/mocksink"
)

// ---- fakes ----

// fakeConsumer 顺序吐出预设消息，耗尽后阻塞到 ctx 取消。
type fakeConsumer struct {
	mu       sync.Mutex
	msgs     []consumer.Message
	i        int
	start    map[model.TopicPartition]int64
	assigned []model.TopicPartition
}

func (f *fakeConsumer) Assign(_ context.Context, partitions []model.TopicPartition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assigned = append([]model.TopicPartition{}, partitions...)
	return nil
}

// SetStartOffsets 记录 pipeline 下发的恢复位点（断言用）。
func (f *fakeConsumer) SetStartOffsets(next map[model.TopicPartition]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.start = make(map[model.TopicPartition]int64, len(next))
	for tp, off := range next {
		f.start[tp] = off
	}
}

func (f *fakeConsumer) Fetch(ctx context.Context) (consumer.Message, error) {
	f.mu.Lock()
	if f.i < len(f.msgs) {
		m := f.msgs[f.i]
		f.i++
		f.mu.Unlock()
		return m, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return consumer.Message{}, ctx.Err()
}

func (f *fakeConsumer) Commit(context.Context, consumer.Message) error          { return nil }
func (f *fakeConsumer) HighWatermark(context.Context, model.TopicPartition) (int64, error) {
	return 0, nil
}
func (f *fakeConsumer) Close() error { return nil }

// transientErr 可重试错误（满足 sink.IsRetryable 的标记接口）。
type transientErr struct{ error }

func (transientErr) IsRetryable() bool { return true }

// dlqRecorder 记录 DLQ 写入。
type dlqRecorder struct {
	mu   sync.Mutex
	rows []sink.RowFailure
}

func (d *dlqRecorder) Write(_ context.Context, fs []sink.RowFailure) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows = append(d.rows, fs...)
	return nil
}

func (d *dlqRecorder) snapshot() []sink.RowFailure {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]sink.RowFailure{}, d.rows...)
}

// rowFailSink 返回固定 RowFailure 的 stub sink（数据错误路径）。
type rowFailSink struct {
	mocksink.Sink
	failOffsets map[int64]bool // 命中的 offset 记 RowFailure
}

func (s *rowFailSink) WriteBatch(ctx context.Context, meta model.TableMeta, events []model.RowEvent) (sink.FlushResult, error) {
	res := sink.FlushResult{}
	for _, ev := range events {
		if s.failOffsets[ev.Offset] {
			res.Failed = append(res.Failed, sink.RowFailure{
				Topic: ev.Topic, Partition: ev.Partition, Offset: ev.Offset,
				Table: meta.Table, Reason: "ORA-01722: invalid number (stub)", Retryable: false,
			})
			continue
		}
		if _, err := s.Sink.WriteBatch(ctx, meta, []model.RowEvent{ev}); err != nil {
			return sink.FlushResult{}, err
		}
		res.Applied++
	}
	return res, nil
}

// ---- helpers ----

var testPartitions = []model.TopicPartition{{Topic: "t.orders", Partition: 0}}

func testMeta(db, table string) (model.TableMeta, error) {
	if db == "missing" {
		return model.TableMeta{}, fmt.Errorf("no such table %s.%s", db, table)
	}
	return model.TableMeta{
		Database: db, TargetSchema: "tgt", Table: table,
		Columns: []model.ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "v", MySQLType: "varchar(32)"},
		},
		PKNames: []string{"id"},
	}, nil
}

func insertMsg(offset int64, db, table, id string) consumer.Message {
	return consumer.Message{
		TopicPartition: model.TopicPartition{Topic: "t.orders", Partition: 0},
		Offset:         offset,
		Value:          []byte(fmt.Sprintf(`{"database":%q,"table":%q,"type":"INSERT","pkNames":["id"],"data":[{"id":%q,"v":"x"}]}`, db, table, id)),
	}
}

// runPipeline 跑完预设消息后取消 ctx，返回 Run 结果与最终指标。
func runPipeline(t *testing.T, cons consumer.Consumer, snk sink.Sink, store checkpoint.OffsetStore, dlq DLQWriter) (error, Metrics) {
	t.Helper()
	p := New(cons, snk, store, testMeta, dlq, Config{
		BatchSize: 100, FlushInterval: 50 * time.Millisecond, MaxRetries: 3, RetryBackoff: time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = p.Run(ctx, testPartitions)
	}()
	// 等 fake consumer 消息耗尽（Fetch 阻塞）后取消，触发优雅 flush
	deadline := time.After(5 * time.Second)
	for {
		f, ok := cons.(*fakeConsumer)
		if !ok {
			break
		}
		f.mu.Lock()
		exhausted := f.i >= len(f.msgs)
		f.mu.Unlock()
		if exhausted {
			break
		}
		select {
		case <-deadline:
			t.Fatal("consumer did not drain messages")
		case <-time.After(5 * time.Millisecond):
		}
	}
	time.Sleep(80 * time.Millisecond) // 留出 flush ticker 至少一轮
	cancel()
	<-done
	return runErr, p.Metrics()
}

// ---- tests ----

// 成功路径：多表混流 → 按表分组写入 → 位点前进到窗口最大 offset。
func TestPipelineHappyPath(t *testing.T) {
	cons := &fakeConsumer{msgs: []consumer.Message{
		insertMsg(0, "d", "t1", "1"),
		insertMsg(1, "d", "t2", "a"),
		insertMsg(2, "d", "t1", "2"),
		insertMsg(3, "d", "t2", "b"),
	}}
	ms := mocksink.New()
	store := checkpoint.NewMemoryStore()

	runErr, m := runPipeline(t, cons, ms, store, nil)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if got := ms.Rows("d", "t1"); got != 2 {
		t.Errorf("t1 rows = %d, want 2", got)
	}
	if got := ms.Rows("d", "t2"); got != 2 {
		t.Errorf("t2 rows = %d, want 2", got)
	}
	if m.AppliedEvents != 4 {
		t.Errorf("AppliedEvents = %d, want 4", m.AppliedEvents)
	}
	// 位点 = 窗口内最大 offset（3），且 LastApplied 同步
	off, _ := store.Load(context.Background(), testPartitions[0])
	if off != 3 {
		t.Errorf("checkpoint = %d, want 3", off)
	}
	if m.LastApplied[testPartitions[0]] != 3 {
		t.Errorf("LastApplied = %d, want 3", m.LastApplied[testPartitions[0]])
	}
	// 恢复位点：无历史 → 从 0 开始
	if got := cons.start[testPartitions[0]]; got != 0 {
		t.Errorf("start offset = %d, want 0", got)
	}
}

// DDL 跳过计数；解码失败 → 原始字节进 DLQ、窗口照常前进。
func TestPipelineDDLAndDecodeFailure(t *testing.T) {
	bad := consumer.Message{
		TopicPartition: testPartitions[0], Offset: 1,
		Value: []byte(`{"type":`), // malformed json
	}
	cons := &fakeConsumer{msgs: []consumer.Message{
		insertMsg(0, "d", "t1", "1"),
		bad,
		{
			TopicPartition: testPartitions[0], Offset: 2,
			Value: []byte(`{"database":"d","table":"t1","isDdl":true,"type":"QUERY","sql":"ALTER TABLE t1 ADD c INT"}`),
		},
		insertMsg(3, "d", "t1", "2"),
	}}
	ms := mocksink.New()
	store := checkpoint.NewMemoryStore()
	dlq := &dlqRecorder{}

	runErr, m := runPipeline(t, cons, ms, store, dlq)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if m.SkippedDDL != 1 {
		t.Errorf("SkippedDDL = %d, want 1", m.SkippedDDL)
	}
	rows := dlq.snapshot()
	if len(rows) != 1 {
		t.Fatalf("DLQ rows = %d, want 1", len(rows))
	}
	if string(rows[0].Raw) != string(bad.Value) {
		t.Errorf("DLQ raw = %q, want original message", rows[0].Raw)
	}
	if !strings.Contains(rows[0].Reason, "decode failed") {
		t.Errorf("DLQ reason = %q, want decode-failure marker", rows[0].Reason)
	}
	// 好行照常应用；位点前进到 3（数据错误不阻塞窗口）
	if m.AppliedEvents != 2 || ms.Rows("d", "t1") != 2 {
		t.Errorf("applied=%d rows=%d, want 2/2", m.AppliedEvents, ms.Rows("d", "t1"))
	}
	off, _ := store.Load(context.Background(), testPartitions[0])
	if off != 3 {
		t.Errorf("checkpoint = %d, want 3", off)
	}
}

// 可重试错误：整窗退避重试后成功，位点正常前进。
func TestPipelineRetryThenSuccess(t *testing.T) {
	cons := &fakeConsumer{msgs: []consumer.Message{insertMsg(0, "d", "t1", "1")}}
	ms := mocksink.New()
	ms.FailNext = transientErr{errors.New("ORA-12170: TNS:Connect timeout occurred")}
	store := checkpoint.NewMemoryStore()

	runErr, m := runPipeline(t, cons, ms, store, nil)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if m.RetriedBatches != 1 {
		t.Errorf("RetriedBatches = %d, want 1", m.RetriedBatches)
	}
	if ms.Rows("d", "t1") != 1 || m.AppliedEvents != 1 {
		t.Errorf("rows=%d applied=%d, want 1/1", ms.Rows("d", "t1"), m.AppliedEvents)
	}
	off, _ := store.Load(context.Background(), testPartitions[0])
	if off != 0 {
		t.Errorf("checkpoint = %d, want 0", off)
	}
}

// fatal：中止任务、不前进 offset（重试到上限同样 fatal）。
func TestPipelineFatalNoOffsetAdvance(t *testing.T) {
	cons := &fakeConsumer{msgs: []consumer.Message{insertMsg(0, "d", "t1", "1")}}
	ms := mocksink.New()
	ms.FailNext = errors.New("ORA-00942: table or view does not exist") // fatal（非白名单、无重试标记）
	store := checkpoint.NewMemoryStore()

	runErr, m := runPipeline(t, cons, ms, store, nil)
	if runErr == nil {
		t.Fatal("Run must fail on fatal error")
	}
	if !strings.Contains(runErr.Error(), "ORA-00942") {
		t.Errorf("Run error = %v, want ORA-00942 wrapped", runErr)
	}
	off, _ := store.Load(context.Background(), testPartitions[0])
	if off != -1 {
		t.Errorf("checkpoint = %d, want -1 (no advance on fatal)", off)
	}
	if m.AppliedEvents != 0 {
		t.Errorf("AppliedEvents = %d, want 0", m.AppliedEvents)
	}
}

// 行级数据错误：RowFailure 回填原始消息体后写 DLQ，其余行提交、窗口前进。
func TestPipelineRowDataDLQWithRaw(t *testing.T) {
	good := insertMsg(0, "d", "t1", "1")
	bad := insertMsg(1, "d", "t1", "2")
	cons := &fakeConsumer{msgs: []consumer.Message{good, bad}}
	rfs := &rowFailSink{Sink: *mocksink.New(), failOffsets: map[int64]bool{1: true}}
	store := checkpoint.NewMemoryStore()
	dlq := &dlqRecorder{}

	runErr, m := runPipeline(t, cons, rfs, store, dlq)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	rows := dlq.snapshot()
	if len(rows) != 1 {
		t.Fatalf("DLQ rows = %d, want 1", len(rows))
	}
	if rows[0].Offset != 1 || rows[0].Table != "t1" || rows[0].Retryable {
		t.Errorf("DLQ row = %+v", rows[0])
	}
	if string(rows[0].Raw) != string(bad.Value) {
		t.Errorf("DLQ raw = %q, want original bad message", rows[0].Raw)
	}
	if m.AppliedEvents != 1 || rfs.Rows("d", "t1") != 1 {
		t.Errorf("applied=%d rows=%d, want 1/1", m.AppliedEvents, rfs.Rows("d", "t1"))
	}
	if m.DLQEvents != 1 {
		t.Errorf("DLQEvents = %d, want 1", m.DLQEvents)
	}
	off, _ := store.Load(context.Background(), testPartitions[0])
	if off != 1 {
		t.Errorf("checkpoint = %d, want 1 (advance past DLQ'd row)", off)
	}
}

// 恢复：历史位点 5 → SetStartOffsets(6) 下发给消费者。
func TestPipelineResumeFromCheckpoint(t *testing.T) {
	cons := &fakeConsumer{msgs: []consumer.Message{insertMsg(6, "d", "t1", "9")}}
	store := checkpoint.NewMemoryStore()
	if err := store.Save(context.Background(), testPartitions[0], 5); err != nil {
		t.Fatal(err)
	}

	runErr, _ := runPipeline(t, cons, mocksink.New(), store, nil)
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if got := cons.start[testPartitions[0]]; got != 6 {
		t.Errorf("start offset = %d, want 6", got)
	}
}

// 元数据缺失（schema drift 疑似）：整组进 DLQ，任务不中止、窗口前进。
func TestPipelineMetaMissingGroupDLQ(t *testing.T) {
	cons := &fakeConsumer{msgs: []consumer.Message{insertMsg(0, "missing", "t1", "1")}}
	store := checkpoint.NewMemoryStore()
	dlq := &dlqRecorder{}

	runErr, m := runPipeline(t, cons, mocksink.New(), store, dlq)
	if runErr != nil {
		t.Fatalf("Run: %v (meta missing must not abort)", runErr)
	}
	rows := dlq.snapshot()
	if len(rows) != 1 || !strings.Contains(rows[0].Reason, "meta unavailable") {
		t.Fatalf("DLQ rows = %+v, want meta-unavailable row", rows)
	}
	off, _ := store.Load(context.Background(), testPartitions[0])
	if off != 0 {
		t.Errorf("checkpoint = %d, want 0", off)
	}
	if m.AppliedEvents != 0 || m.DLQEvents != 1 {
		t.Errorf("applied=%d dlq=%d, want 0/1", m.AppliedEvents, m.DLQEvents)
	}
}
