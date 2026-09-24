// Package checkpoint 定义消费位点持久化（设计文档 D10）。
// SQLite 为唯一权威位点：flush 成功 → Save → best-effort commit Kafka。
package checkpoint

import (
	"context"
	"sync"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// OffsetStore 权威位点存储。Save 必须在目标端事务提交成功之后调用。
type OffsetStore interface {
	// Load 返回分区已应用的 offset（下一条待消费消息位点）；无记录返回 -1。
	Load(tp model.TopicPartition) (int64, error)
	Save(ctx context.Context, tp model.TopicPartition, offset int64) error
}

// MemoryStore 内存实现（单测用；权威实现为 SQLite，见 M2）。
type MemoryStore struct {
	mu   sync.Mutex
	offs map[model.TopicPartition]int64
}

// NewMemoryStore 构造内存位点存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{offs: make(map[model.TopicPartition]int64)}
}

func (s *MemoryStore) Load(tp model.TopicPartition) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if off, ok := s.offs[tp]; ok {
		return off, nil
	}
	return -1, nil
}

func (s *MemoryStore) Save(_ context.Context, tp model.TopicPartition, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offs[tp] = offset
	return nil
}
