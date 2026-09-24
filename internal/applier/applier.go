// Package applier 实现消费→批聚合→幂等应用→位点前进的管道
// （M2 T2.2，设计文档 D2/D3/D10/D11）。
//
// 批语义（窗口模型）：以 N 条消息为一个窗口（跨表混流），窗口满或
// 到达 flush 间隔时按 (database, table) 分组、逐组调 Sink.WriteBatch；
// 全部组成功后才把窗口内各分区的最大 offset 写入 OffsetStore。
// 这保证了位点前进只发生在"该 offset 及之前的所有事件都已处理"之后
// ——按表分批与按分区前进的位点因此不会错位。
//
// 失败契约（M1.5 P0#1 定稿 + 真库实测补充）：
//   - 可重试错误：整窗退避重试（绝不部分续传；ctx 取消下驱动可能已
//     提交部分行，整窗重放由幂等 MERGE 保证正确）；
//   - 数据错误：RowFailure 补 Raw 后写 DLQ，窗口照常前进；
//   - 全窗口都是数据错误 → 升级为任务级告警（大概率 schema drift，复审 P2#6）。
package applier

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/checkpoint"
	"github.com/lizhenhuan/tidb-full-init/internal/consumer"
	"github.com/lizhenhuan/tidb-full-init/internal/decoder"
	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink"
)

// Config 管道参数。
type Config struct {
	BatchSize     int           // 窗口消息数（默认 500）
	FlushInterval time.Duration // 窗口最长攒批时间（默认 1s）
	MaxRetries    int           // 可重试错误整窗重试上限（默认 5，指数退避）
	RetryBackoff  time.Duration // 基础退避（默认 500ms，逐次翻倍）
}

func (c *Config) fill() {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 5
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 500 * time.Millisecond
	}
}

// MetaProvider 返回目标表元数据（TiDB SchemaFetcher 为权威来源，D5）。
// 错误语义（M2 复审 P1#1）：SchemaFetcher 打 TiDB 是运行期调用，瞬时
// 不可用属常态，默认一律 fatal（中止、不前进 offset）；只有返回
// *model.TableNotFoundError（表已从源删除）才允许整组 DLQ 并前进 offset。
// 该类型定义在 model 层，SchemaFetcher 与 applier 共同引用（不倒置依赖）。
type MetaProvider func(db, table string) (model.TableMeta, error)

// DLQWriter 死信落库（oraclesink.DLQ 实现；测试用内存实现）。
type DLQWriter interface {
	Write(ctx context.Context, failures []sink.RowFailure) error
}

// Metrics 运行指标（M3 JobManager 采集快照用）。
type Metrics struct {
	AppliedEvents   int64
	DLQEvents       int64
	SkippedDDL      int64
	RetriedBatches  int64
	FlushedBatches  int64
	LastApplied     map[model.TopicPartition]int64
}

// Pipeline 消费管道。
type Pipeline struct {
	cons  consumer.Consumer
	snk   sink.Sink
	store checkpoint.OffsetStore
	meta  MetaProvider
	dlq   DLQWriter
	cfg   Config

	// mu 保护 m：Run goroutine 写、M3 JobManager 轮询 Metrics() 并发读
	// （M2 复审 P2#3），尤其 LastApplied map 不能裸并发读写。
	mu sync.Mutex
	m  Metrics
}

// New 构造管道。store 为权威位点（SQLite），dlq 可为 nil（数据错误仅计数）。
func New(cons consumer.Consumer, snk sink.Sink, store checkpoint.OffsetStore, meta MetaProvider, dlq DLQWriter, cfg Config) *Pipeline {
	cfg.fill()
	return &Pipeline{
		cons: cons, snk: snk, store: store, meta: meta, dlq: dlq,
		cfg:   cfg,
		m:     Metrics{LastApplied: make(map[model.TopicPartition]int64)},
	}
}

// rawEvent 窗口内条目：事件 + 原始消息体（RowFailure.Raw 的来源，复审 P2#7）。
type rawEvent struct {
	ev  model.RowEvent
	raw []byte
}

// Run 主循环；ctx 取消或不可恢复错误时返回。
func (p *Pipeline) Run(ctx context.Context, partitions []model.TopicPartition) error {
	start := make(map[model.TopicPartition]int64, len(partitions))
	for _, tp := range partitions {
		off, err := p.store.Load(ctx, tp)
		if err != nil {
			return fmt.Errorf("applier: load checkpoint %s/%d: %w", tp.Topic, tp.Partition, err)
		}
		start[tp] = off + 1 // -1（无记录）→ 0 从头消费（P1#9 语义）
	}
	// 把恢复位点交给消费者（KafkaConsumer.SetStartOffsets；Assign 之前调用）。
	// Consumer 接口保持冻结（D8），因此走类型断言。fail-fast（M2 复审 P2#5）：
	// 存在非 0 恢复位点却断言失败 = 该消费者拿不到位点，若其默认 latest 会
	// 跳消息、默认 earliest 会重放 —— 都不如启动即报错让人排查。
	so, canSet := p.cons.(interface {
		SetStartOffsets(next map[model.TopicPartition]int64)
	})
	hasResume := false
	for _, off := range start {
		if off > 0 {
			hasResume = true
			break
		}
	}
	if hasResume && !canSet {
		return fmt.Errorf("applier: consumer %T does not implement SetStartOffsets but resume offsets exist; refusing to start blind", p.cons)
	}
	if canSet {
		so.SetStartOffsets(start)
	}
	if err := p.cons.Assign(ctx, partitions); err != nil {
		return fmt.Errorf("applier: assign: %w", err)
	}

	window := make([]rawEvent, 0, p.cfg.BatchSize)
	flushTicker := time.NewTicker(p.cfg.FlushInterval)
	defer flushTicker.Stop()

	windowMax := make(map[model.TopicPartition]int64)

	// saveWindowMax 全组成功 → 位点前进到窗口内各分区最大 offset
	// （flush-后-commit，D2），并同步 LastApplied 指标。
	saveWindowMax := func(fctx context.Context) error {
		for tp, off := range windowMax {
			if err := p.store.Save(fctx, tp, off); err != nil {
				return fmt.Errorf("applier: save checkpoint %s/%d@%d: %w", tp.Topic, tp.Partition, off, err)
			}
			p.mu.Lock()
			p.m.LastApplied[tp] = off
			p.mu.Unlock()
		}
		windowMax = make(map[model.TopicPartition]int64)
		return nil
	}

	// flushWith 用指定 ctx 冲刷窗口。运行期用活 ctx；优雅退出时 run ctx
	// 已取消，必须换一个未取消的 ctx（否则目标端写入与位点 Save 会立刻
	// 失败，攒了一半的窗口丢失），只给一个有界超时防止关停挂死。
	flushWith := func(fctx context.Context) error {
		if len(window) == 0 {
			// 纯 DDL/解码失败窗口：无行要写，但窗口位点仍需前进
			// （M2 复审 P2#4：否则该分区位点长期不动，lag 展示假高）。
			return saveWindowMax(fctx)
		}
		if err := p.flushWindow(fctx, window); err != nil {
			return err
		}
		if err := saveWindowMax(fctx); err != nil {
			return err
		}
		window = window[:0]
		return nil
	}
	flush := func() error { return flushWith(ctx) }

	// gracefulFlush 优雅退出路径的冲刷：脱离已取消的 run ctx，限时完成。
	gracefulFlush := func() error {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return flushWith(fctx)
	}

	// flushOrGraceful 运行期冲刷；若恰在退出瞬间命中已取消的 run ctx
	// （ticker 与 cancel 竞态），改走优雅冲刷而不是把 context canceled
	// 当致命错误上抛——位点语义不变：仍只在写入成功后前进。
	flushOrGraceful := func() error {
		if err := flush(); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return gracefulFlush()
			}
			return err
		}
		return nil
	}

	// fetchPoll 限制单次 Fetch 阻塞时长：Consumer.Fetch 无法感知 flush
	// ticker，若无限阻塞会在低流量时饿死攒批窗口的定时冲刷。
	fetchPoll := p.cfg.FlushInterval / 4
	if fetchPoll < 10*time.Millisecond {
		fetchPoll = 10 * time.Millisecond
	}

	for {
		var msg consumer.Message
		select {
		case <-ctx.Done():
			return gracefulFlush() // 优雅退出：先冲刷已攒窗口
		case <-flushTicker.C:
			if err := flushOrGraceful(); err != nil {
				return err
			}
			continue
		default:
		}
		fetchCtx, fetchCancel := context.WithTimeout(ctx, fetchPoll)
		m, err := p.cons.Fetch(fetchCtx)
		fetchCancel()
		if err != nil {
			if ctx.Err() != nil {
				return gracefulFlush()
			}
			// 轮询超时（低流量 idle）→ 回到循环顶处理 ticker，不是错误
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("applier: fetch: %w", err)
		}
		msg = m

		tp := msg.TopicPartition
		if _, ok := windowMax[tp]; !ok || msg.Offset > windowMax[tp] {
			windowMax[tp] = msg.Offset // 注意 offset=0：用存在性判断，不能用 >0 默认值
		}

		dec, derr := decoder.CanalJSON(msg.Value)
		if derr != nil {
			// 解码失败：数据错误 → DLQ（原始字节即证据），窗口照常前进
			log.Printf("[applier] decode failed at %s/%d@%d (%d bytes): %v",
				tp.Topic, tp.Partition, msg.Offset, len(msg.Value), derr)
			p.writeDLQ(ctx, []sink.RowFailure{{
				Topic: tp.Topic, Partition: tp.Partition, Offset: msg.Offset,
				Reason: "decode failed: " + derr.Error(), Raw: msg.Value, Retryable: false,
			}})
			if len(window) >= p.cfg.BatchSize {
				if err := flushOrGraceful(); err != nil {
					return err
				}
			}
			continue
		}
		switch e := dec.(type) {
		case *model.DDLEvent:
			p.mu.Lock()
			p.m.SkippedDDL++ // 一期只识别与计数（D5）
			p.mu.Unlock()
			log.Printf("[applier] skip DDL %s.%s offset=%d", e.Database, e.Table, msg.Offset)
		case *model.RowEvent:
			e.Topic, e.Partition, e.Offset = tp.Topic, tp.Partition, msg.Offset
			window = append(window, rawEvent{ev: *e, raw: msg.Value})
		}

		if len(window) >= p.cfg.BatchSize {
			if err := flushOrGraceful(); err != nil {
				return err
			}
		}
	}
}

// flushWindow 按表分组逐组写入；可重试错误整窗退避重试。
func (p *Pipeline) flushWindow(ctx context.Context, window []rawEvent) error {
	groups := make(map[string][]rawEvent)
	for _, re := range window {
		key := re.ev.Database + "." + re.ev.Table
		groups[key] = append(groups[key], re)
	}

	for key, group := range groups {
		if err := p.flushGroup(ctx, key, group); err != nil {
			return err
		}
	}
	p.mu.Lock()
	p.m.FlushedBatches++
	p.mu.Unlock()
	return nil
}

func (p *Pipeline) flushGroup(ctx context.Context, key string, group []rawEvent) error {
	db, table := group[0].ev.Database, group[0].ev.Table
	meta, err := p.meta(db, table)
	if err != nil {
		// M2 复审 P1#1：默认 fatal —— SchemaFetcher 打 TiDB 是运行期调用，
		// 瞬时不可用就把整表事件 DLQ 并越过位点 = 静默丢数据。只有确认
		// "表已从源删除"（*TableNotFoundError，schema drift 终态）才 DLQ。
		var nf *model.TableNotFoundError
		if errors.As(err, &nf) {
			log.Printf("[applier] WARN table %s dropped from source (%v); %d events to DLQ", key, err, len(group))
			p.toDLQ(ctx, group, fmt.Sprintf("table dropped from source: %v", err))
			return nil
		}
		return fmt.Errorf("applier: meta %s unavailable (transient or unknown, not advancing offset): %w", key, err)
	}

	events := make([]model.RowEvent, len(group))
	for i, re := range group {
		events[i] = re.ev
	}

	backoff := p.cfg.RetryBackoff
	for attempt := 0; ; attempt++ {
		res, err := p.snk.WriteBatch(ctx, meta, events)
		if err == nil {
			p.mu.Lock()
			p.m.AppliedEvents += int64(res.Applied)
			p.mu.Unlock()
			if len(res.Failed) > 0 {
				p.fillRawAndDLQ(ctx, group, res.Failed)
				if res.Applied == 0 {
					log.Printf("[applier] WARN batch for %s entirely data-failed (%d rows) — "+
						"possible schema drift, check DLQ", key, len(res.Failed))
				}
			}
			return nil
		}
		if !sink.IsRetryable(err) || attempt >= p.cfg.MaxRetries {
			// fatal（或重试耗尽）：中止任务，不前进 offset，由 JobManager 告警
			return fmt.Errorf("applier: write %s (attempt %d): %w", key, attempt+1, err)
		}
		// 可重试：整组（=整窗的一部分，窗内其余组同样会重走）退避重试。
		// 真库实测：ctx 取消/阻塞场景下事务可能未完全回滚，整批重放幂等安全。
		p.mu.Lock()
		p.m.RetriedBatches++
		p.mu.Unlock()
		log.Printf("[applier] retryable error on %s (attempt %d/%d): %v; backoff %s",
			key, attempt+1, p.cfg.MaxRetries, err, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
}

// fillRawAndDLQ 给 RowFailure 回填原始消息体后写 DLQ。
// 匹配键 = (topic, partition, offset)，Kafka 坐标内唯一。
func (p *Pipeline) fillRawAndDLQ(ctx context.Context, group []rawEvent, failures []sink.RowFailure) {
	rawByKey := make(map[string][]byte, len(group))
	for _, re := range group {
		rawByKey[fmt.Sprintf("%s/%d/%d", re.ev.Topic, re.ev.Partition, re.ev.Offset)] = re.raw
	}
	for i := range failures {
		if failures[i].Raw == nil {
			failures[i].Raw = rawByKey[fmt.Sprintf("%s/%d/%d",
				failures[i].Topic, failures[i].Partition, failures[i].Offset)]
		}
	}
	p.writeDLQ(ctx, failures)
}

func (p *Pipeline) toDLQ(ctx context.Context, group []rawEvent, reason string) {
	fs := make([]sink.RowFailure, 0, len(group))
	for _, re := range group {
		fs = append(fs, sink.RowFailure{
			Topic: re.ev.Topic, Partition: re.ev.Partition, Offset: re.ev.Offset,
			Table: re.ev.Table, Reason: reason, Raw: re.raw,
		})
	}
	p.writeDLQ(ctx, fs)
}

func (p *Pipeline) writeDLQ(ctx context.Context, fs []sink.RowFailure) {
	if p.dlq == nil || len(fs) == 0 {
		return
	}
	// 计数在写入前：即便 DLQ 落库失败，数据错误事件数也要如实反映
	p.mu.Lock()
	p.m.DLQEvents += int64(len(fs))
	p.mu.Unlock()
	if err := p.dlq.Write(ctx, fs); err != nil {
		log.Printf("[applier] DLQ write failed (%d rows): %v", len(fs), err)
	}
}

// Metrics 返回指标快照（深拷贝，调用方可安全持有/并发轮询）。
func (p *Pipeline) Metrics() Metrics {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.m
	out.LastApplied = make(map[model.TopicPartition]int64, len(p.m.LastApplied))
	for tp, off := range p.m.LastApplied {
		out.LastApplied[tp] = off
	}
	return out
}
