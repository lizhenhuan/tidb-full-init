package tikv

import (
	"context"
	"sort"
	"sync/atomic"
)

type MockClient struct {
	data      map[string][]byte
	timestamp uint64
}

func NewMockClient(data map[string][]byte) *MockClient {
	if data == nil {
		data = make(map[string][]byte)
	}
	return &MockClient{data: data}
}

func (m *MockClient) SetData(key string, value []byte) {
	m.data[key] = value
}

func (m *MockClient) GetTimestamp(_ context.Context) (uint64, error) {
	return atomic.AddUint64(&m.timestamp, 1), nil
}

func (m *MockClient) ScanWithTS(ctx context.Context, startKey, endKey []byte, limit int, _ uint64) ([]KvPair, error) {
	return m.Scan(ctx, startKey, endKey, limit)
}

func (m *MockClient) Scan(_ context.Context, startKey, endKey []byte, limit int) ([]KvPair, error) {
	// 必须：① 按 key 升序返回；② 区间语义 [startKey, endKey)——与真实 TiKV
	// 一致是范围扫描而非前缀匹配。snapshot.Reader 用 incrementKey(最后一条)
	// 做分页游标：顺序随机 → 游标撞已返回 key 重复计数（曾致 mock 行数
	// 3→4/5 抖动）；前缀匹配 → 稀疏 handle 下游标翻页拿不到后续 key
	// （只有连续 handle 时 increment 恰好等于下一个 key 才碰巧可用）。
	keys := make([]string, 0, len(m.data))
	start, end := string(startKey), string(endKey)
	for k := range m.data {
		if k >= start && k < end {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	pairs := make([]KvPair, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, KvPair{Key: []byte(k), Value: m.data[k]})
	}
	return pairs, nil
}

func (m *MockClient) Close() error {
	return nil
}
