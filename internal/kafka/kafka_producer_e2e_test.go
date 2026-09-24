// KafkaProducer 吞吐 E2E（#t6 复审 P0-2 修正版）：逐条同步 Send 在
// BatchSize=1 下不得逐条等待 BatchTimeout（修复前默认 BatchSize=100 +
// BatchTimeout≈1s ≈ 0.55s/条，100 条要 ~1 分钟，形似挂死）。
//
// 跑法同其他 L2：
//
//	E2E_KAFKA=1 DOCKER_HOST=unix://$HOME/.colima/default/docker.sock \
//	  TESTCONTAINERS_RYUK_DISABLED=true go test ./internal/kafka/ -run E2E -v -count=1
package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

func TestE2EProducerThroughput(t *testing.T) {
	if os.Getenv("E2E_KAFKA") != "1" {
		t.Skip("E2E_KAFKA != 1; skipping producer E2E (needs Docker)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	if err := EnsureTopic(broker, "throughput-e2e", 1); err != nil {
		t.Fatalf("ensure topic: %v", err)
	}

	p, err := NewKafkaProducer(KafkaProducerConfig{Brokers: broker, Topic: "throughput-e2e", Acks: 1})
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer p.Close()

	const n = 100
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := p.Send(ctx, &Message{Key: []byte("k"), Value: []byte("v")}); err != nil {
			t.Fatalf("send #%d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d msgs in %s (%.1f ms/msg)", n, elapsed, float64(elapsed.Milliseconds())/n)
	// 修复前 ≈0.55s/条（100 条 ~55s）。阈值 50ms/条：只抓「每条等一个
	// BatchTimeout」的病态回归，不卡正常网络抖动。
	if per := elapsed / n; per > 50*time.Millisecond {
		t.Fatalf("throughput regression: %v/msg (want <50ms; BatchTimeout wait leaked back?)", per)
	}

	// 消息确实写进去了
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, Topic: "throughput-e2e", Partition: 0,
		MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond,
	})
	defer r.Close()
	if err := r.SetOffset(kafka.FirstOffset); err != nil {
		t.Fatalf("seek: %v", err)
	}
	got := 0
	deadline := time.Now().Add(30 * time.Second)
	for got < n && time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := r.FetchMessage(rctx)
		rcancel()
		if err == nil {
			got++
		}
	}
	if got != n {
		t.Fatalf("read back %d/%d", got, n)
	}
}

// TestE2EFreshTopicProduceRace：#t6 复审 F1 回归——多表批量里第 2 个
// 新建 topic 的首条 produce 稳定报 [3] UnknownTopicOrPartition（创建→
// 元数据可见性竞态）。EnsureTopic 现在轮询到分区有 leader 才返回，
// Send 对码 3 退避重试兜底。
func TestE2EFreshTopicProduceRace(t *testing.T) {
	if os.Getenv("E2E_KAFKA") != "1" {
		t.Skip("E2E_KAFKA != 1; skipping producer E2E (needs Docker)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	// 复现形态：连续建两个 fresh topic，各自立即生产首条（修复前
	// 第 2 个稳定 [3]）。为放大竞态跑 3 组。
	for g := 0; g < 3; g++ {
		for i := 0; i < 2; i++ {
			topic := fmt.Sprintf("fresh-race-%d-%d-%d", g, i, time.Now().UnixNano())
			if err := EnsureTopic(broker, topic, 1); err != nil {
				t.Fatalf("EnsureTopic %s: %v", topic, err)
			}
			p, err := NewKafkaProducer(KafkaProducerConfig{Brokers: broker, Topic: topic, Acks: 1})
			if err != nil {
				t.Fatalf("producer %s: %v", topic, err)
			}
			if err := p.Send(ctx, &Message{Value: []byte("first")}); err != nil {
				t.Fatalf("first produce on %s (F1 race): %v", topic, err)
			}
			p.Close()
		}
	}
}
