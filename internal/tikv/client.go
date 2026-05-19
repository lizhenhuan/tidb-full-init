package tikv

import "context"

type KvPair struct {
	Key   []byte
	Value []byte
}

type Client interface {
	GetTimestamp(ctx context.Context) (uint64, error)
	Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]KvPair, error)
	ScanWithTS(ctx context.Context, startKey, endKey []byte, limit int, ts uint64) ([]KvPair, error)
	Close() error
}
