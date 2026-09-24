package initrunner

import (
	"context"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/config"
)

func mockCfg() *config.Config {
	return &config.Config{
		PDAddresses:           "127.0.0.1:2379",
		TiDBHost:              "127.0.0.1",
		TiDBPort:              "4000",
		Username:              "root",
		Password:              "root",
		KafkaBootstrapServers: "127.0.0.1:9092",
		KafkaTopicPrefix:      "tidb-data-",
		Tables: []config.TableConfig{{
			Database:    "test",
			Tables:      []string{"orders", "users"},
			Parallelism: 2,
		}},
	}
}

// TestRunMockMode smoke：mock 模式跑通两张表，行数与 topic 解析符合原 CLI 行为
// （每表 seed 3 行；topic = prefix + db + "-" + table）。
func TestRunMockMode(t *testing.T) {
	cfg := mockCfg()
	res, err := Run(context.Background(), cfg, Options{MockMode: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Tables) != 2 {
		t.Fatalf("tables = %d, want 2 (%v)", len(res.Tables), res.Tables)
	}
	for _, key := range []string{"test.orders", "test.users"} {
		if res.Tables[key] != 3 {
			t.Errorf("rows[%s] = %d, want 3", key, res.Tables[key])
		}
		want := "tidb-data-test-" + key[len("test."):]
		if res.Topics[key] != want {
			t.Errorf("topic[%s] = %q, want %q", key, res.Topics[key], want)
		}
	}
}

// TestRunMockModeFixedTopic：固定 KafkaTopic 时所有表写同一 topic。
func TestRunMockModeFixedTopic(t *testing.T) {
	cfg := mockCfg()
	cfg.KafkaTopic = "all-tables"
	res, err := Run(context.Background(), cfg, Options{MockMode: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for key, topic := range res.Topics {
		if topic != "all-tables" {
			t.Errorf("topic[%s] = %q, want all-tables", key, topic)
		}
	}
}

// TestRunMockModePerTableTopic：TableConfig.Topic 优先于固定 topic 与前缀。
func TestRunMockModePerTableTopic(t *testing.T) {
	cfg := mockCfg()
	cfg.Tables[0].Topic = "orders-dedicated"
	res, err := Run(context.Background(), cfg, Options{MockMode: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Topics["test.orders"] != "orders-dedicated" {
		t.Errorf("topic[test.orders] = %q, want orders-dedicated", res.Topics["test.orders"])
	}
}

// TestRunCtxCancelled：ctx 已取消时 Run 立即返回 ctx 错误，不产生副作用。
func TestRunCtxCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := Run(ctx, mockCfg(), Options{MockMode: true})
	if err == nil {
		t.Fatalf("expected ctx error, got res=%v", res)
	}
	if ctx.Err() == nil || err.Error() != context.Canceled.Error() {
		t.Errorf("err = %v, want context canceled", err)
	}
}
