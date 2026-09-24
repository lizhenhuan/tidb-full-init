package syncer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/consumer"
	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/mocksink"
)

func testConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{
		TiDB:                  TiDBConfig{Host: "h", Port: "4000", Username: "root", Password: "p"},
		KafkaBootstrapServers: "127.0.0.1:9092",
		KafkaTopicPrefix:      "tidb-data-",
		CheckpointPath:        filepath.Join(t.TempDir(), "cp.db"),
		Oracle:                OracleConfig{Host: "h", Port: 1521, Service: "FREEPDB1", User: "system", Password: "p"},
		TargetSchema:          "shop",
		Tables:                []TableEntry{{Database: "db1", Tables: []string{"t1"}}},
	}
}

func TestConfigLoadValidateResolve(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sync.json")
	json := `{
	  "tidb": {"host":"10.0.0.1","port":"4000","username":"root","password":"x"},
	  "kafka-bootstrap-servers": "k1:9092",
	  "checkpoint-path": "cp.db",
	  "oracle": {"host":"127.0.0.1","port":1521,"service":"FREEPDB1","user":"system","password":"y"},
	  "target-schema": "shop",
	  "tables": [{"database":"db1","tables":["a","b"]}]
	}`
	if err := os.WriteFile(path, []byte(json), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// 三级 topic 解析与 initrunner 对齐
	if got := cfg.ResolveTopic(cfg.Tables[0], "a"); got != "tidb-data-db1-a" && got != "k1-prefix" {
		// prefix 为空时 FillDefaults 填 tidb-data-
		if got != "tidb-data-db1-a" {
			t.Errorf("prefix resolve = %q, want tidb-data-db1-a", got)
		}
	}
	cfg.Tables[0].Topic = "dedicated"
	if got := cfg.ResolveTopic(cfg.Tables[0], "a"); got != "dedicated" {
		t.Errorf("per-table topic = %q, want dedicated", got)
	}
	cfg.Tables[0].Topic = ""
	cfg.KafkaTopic = "fixed-all"
	if got := cfg.ResolveTopic(cfg.Tables[0], "b"); got != "fixed-all" {
		t.Errorf("fixed topic = %q, want fixed-all", got)
	}

	// 缺必填项报错
	bad := *cfg
	bad.Oracle.Password = ""
	if err := bad.Validate(); err == nil {
		t.Error("Validate should fail without oracle password")
	}
	bad2 := *cfg
	bad2.TargetSchema = ""
	if err := bad2.Validate(); err == nil {
		t.Error("Validate should fail without target-schema")
	}
}

func TestTableMetaFrom(t *testing.T) {
	info := &encoder.TableInfo{
		Database: "db1", Table: "t1", PKNames: []string{"id"},
		Columns: []encoder.ColumnInfo{
			{ID: 1, Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
			{ID: 2, Name: "v", MySQLType: "varchar(255)"},
		},
	}
	m := TableMetaFrom(info, "shop")
	if m.Schema() != "shop" || m.Database != "db1" || m.Table != "t1" {
		t.Errorf("meta identity = %+v", m)
	}
	if len(m.Columns) != 2 || m.Columns[0].Name != "id" || !m.Columns[0].IsPK || m.Columns[1].MySQLType != "varchar(255)" {
		t.Errorf("columns = %+v", m.Columns)
	}
	// TargetSchema 空 → 回落 Database（P0#4 语义）
	m2 := TableMetaFrom(info, "")
	if m2.Schema() != "db1" {
		t.Errorf("fallback schema = %q, want db1", m2.Schema())
	}
}

func TestMetaProviderPassthrough(t *testing.T) {
	calls := 0
	fetch := func(db, table string) (*encoder.TableInfo, int64, error) {
		calls++
		if db != "db1" || table != "t1" {
			return nil, 0, fmt.Errorf("unexpected %s.%s", db, table)
		}
		return &encoder.TableInfo{Database: db, Table: table}, 77, nil
	}
	mp := MetaProvider(fetch, "shop")
	m, err := mp("db1", "t1")
	if err != nil || m.TargetSchema != "shop" || calls != 1 {
		t.Fatalf("mp = %+v err=%v calls=%d", m, err, calls)
	}
	if _, err := mp("x", "y"); err == nil {
		t.Error("error should pass through")
	}
}

// --- 离线全链路接线测试（fake consumer + mocksink + SQLite） ---

// fakeConsumer 内存消费者：实现 consumer.Consumer + SetStartOffsets。
// 字段全部加锁：pipeline goroutine 读，测试 goroutine 轮询/断言。
type fakeConsumer struct {
	mu       sync.Mutex
	queued   []fakeMsg
	started  map[model.TopicPartition]int64
	assigned []model.TopicPartition
	offsets  int64 // 逐条递增的 offset 源
}

type fakeMsg struct {
	tp    model.TopicPartition
	value []byte
}

func newFakeConsumer() *fakeConsumer {
	return &fakeConsumer{started: map[model.TopicPartition]int64{}}
}

func (f *fakeConsumer) push(tp model.TopicPartition, value []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued = append(f.queued, fakeMsg{tp: tp, value: value})
}

func (f *fakeConsumer) remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.queued)
}

func (f *fakeConsumer) startOffset(tp model.TopicPartition) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started[tp]
}

func (f *fakeConsumer) Assign(_ context.Context, partitions []model.TopicPartition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assigned = append(f.assigned, partitions...)
	return nil
}
func (f *fakeConsumer) SetStartOffsets(next map[model.TopicPartition]int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for tp, off := range next {
		f.started[tp] = off
	}
}
func (f *fakeConsumer) Fetch(ctx context.Context) (consumer.Message, error) {
	f.mu.Lock()
	if len(f.queued) > 0 {
		q := f.queued[0]
		f.queued = f.queued[1:]
		f.offsets++
		off := f.offsets
		f.mu.Unlock()
		return consumer.Message{TopicPartition: q.tp, Value: q.value, Offset: off}, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return consumer.Message{}, ctx.Err()
}
func (f *fakeConsumer) Commit(context.Context, consumer.Message) error { return nil }
func (f *fakeConsumer) HighWatermark(context.Context, model.TopicPartition) (int64, error) {
	return 0, nil
}
func (f *fakeConsumer) Close() error { return nil }

func insertMsg(db, table, id, v string) []byte {
	return []byte(fmt.Sprintf(`{"database":%q,"table":%q,"type":"INSERT","pkNames":["id"],"data":[{"id":%q,"v":%q}]}`, db, table, id, v))
}

func TestRunOfflineWiring(t *testing.T) {
	cfg := testConfig(t)
	snk := mocksink.New()
	topic := model.TopicPartition{Topic: "tidb-data-db1-t1", Partition: 0}
	cons := newFakeConsumer()
	cons.push(topic, insertMsg("db1", "t1", "1", "a"))
	cons.push(topic, insertMsg("db1", "t1", "2", "b"))
	cons.push(topic, insertMsg("db1", "t1", "3", "c"))

	metaOK := func(db, table string) (*encoder.TableInfo, int64, error) {
		return &encoder.TableInfo{
			Database: db, Table: table, PKNames: []string{"id"},
			Columns: []encoder.ColumnInfo{
				{ID: 1, Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
				{ID: 2, Name: "v", MySQLType: "varchar(255)"},
			},
		}, 1, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res *Result
	var runErr error
	go func() {
		res, runErr = Run(ctx, cfg, Deps{
			Sink: snk, Fetch: metaOK, Consumer: cons, DisableDLQ: true,
			ListPartitions: func(context.Context, []string, []string) ([]model.TopicPartition, error) {
				return []model.TopicPartition{topic}, nil
			},
		})
		close(done)
	}()

	// 等三行落地后优雅取消
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if snk.Rows("db1", "t1") >= 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if got := snk.Rows("db1", "t1"); got != 3 {
		t.Fatalf("rows = %d, want 3", got)
	}
	// 目标表 Prepare 被调用（幂等建表路径）
	if len(snk.Prepared) != 1 || snk.Prepared[0].Schema() != "shop" {
		t.Errorf("prepared = %+v, want 1 table under shop", snk.Prepared)
	}
	if res.Metrics.AppliedEvents != 3 {
		t.Errorf("applied = %d, want 3", res.Metrics.AppliedEvents)
	}
}

// TestRunResumeFromCheckpoint：第二次运行（同一 SQLite 位点文件）时，
// Pipeline 把「已应用 offset+1」经 SetStartOffsets 下发给消费者——
// 即 #t6 约定的「全量从 0、增量续传」接线证明。
func TestRunResumeFromCheckpoint(t *testing.T) {
	cfg := testConfig(t)
	topic := model.TopicPartition{Topic: "tidb-data-db1-t1", Partition: 0}
	metaOK := func(db, table string) (*encoder.TableInfo, int64, error) {
		return &encoder.TableInfo{
			Database: db, Table: table, PKNames: []string{"id"},
			Columns: []encoder.ColumnInfo{
				{ID: 1, Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
				{ID: 2, Name: "v", MySQLType: "varchar(255)"},
			},
		}, 1, nil
	}
	runOnce := func(cons *fakeConsumer) *Result {
		t.Helper()
		snk := mocksink.New()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var res *Result
		var err error
		go func() {
			res, err = Run(ctx, cfg, Deps{
				Sink: snk, Fetch: metaOK, Consumer: cons, DisableDLQ: true,
				ListPartitions: func(context.Context, []string, []string) ([]model.TopicPartition, error) {
					return []model.TopicPartition{topic}, nil
				},
			})
			close(done)
		}()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && cons.remaining() > 0 {
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond) // 让 flush ticker 落位点
		cancel()
		<-done
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		return res
	}

	// 第一轮：3 条 offset 1..3，位点应到 3
	c1 := newFakeConsumer()
	c1.push(topic, insertMsg("db1", "t1", "1", "a"))
	c1.push(topic, insertMsg("db1", "t1", "2", "b"))
	c1.push(topic, insertMsg("db1", "t1", "3", "c"))
	runOnce(c1)
	if got := c1.startOffset(topic); got != 0 {
		t.Errorf("round1 start = %d, want 0（无记录分区从 0 消费）", got)
	}
	// 第二轮（同一 checkpoint）：SetStartOffsets 必须收到 4（=3+1）
	c2 := newFakeConsumer()
	runOnce(c2)
	if got := c2.startOffset(topic); got != 4 {
		t.Errorf("round2 start offset = %d, want 4（已应用 3 + 1）", got)
	}
}
