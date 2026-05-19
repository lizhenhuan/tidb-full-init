package tikv

import (
	"context"
	"fmt"
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
	var pairs []KvPair
	for k, v := range m.data {
		if limit > 0 && len(pairs) >= limit {
			break
		}
		if startsWith(k, string(startKey)) && lessThan(k, string(endKey)) {
			pairs = append(pairs, KvPair{Key: []byte(k), Value: v})
		}
	}
	return pairs, nil
}

func (m *MockClient) Close() error {
	return nil
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func lessThan(a, b string) bool {
	return a < b
}

var _ = fmt.Sprintf
