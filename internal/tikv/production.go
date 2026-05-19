package tikv

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"strings"

	"github.com/tikv/client-go/v2/txnkv"
)

const signMask uint64 = 0x8000000000000000

// EncodeIntToCmpUint converts a signed int64 to a memcomparable uint64
// by XORing with the sign bit, matching TiDB's codec.EncodeInt.
func EncodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

// EncodeInt encodes a signed int64 in memcomparable format (big-endian with sign flip).
func EncodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

// DecodeInt decodes a memcomparable int64 back to a signed int64.
func DecodeInt(b []byte) int64 {
	u := binary.BigEndian.Uint64(b[:8])
	return int64(u ^ signMask)
}

// GenTableRecordPrefix generates the key prefix for all rows of a table.
// Format: 't' + EncodeInt(tableID) + "_r"
func GenTableRecordPrefix(tableID int64) []byte {
	var buf []byte
	buf = append(buf, 't')
	buf = EncodeInt(buf, tableID)
	buf = append(buf, '_', 'r')
	return buf
}

// GenTableRecordKey generates a specific row key.
// Format: 't' + EncodeInt(tableID) + "_r" + EncodeInt(handle)
func GenTableRecordKey(tableID, handle int64) []byte {
	buf := GenTableRecordPrefix(tableID)
	buf = EncodeInt(buf, handle)
	return buf
}

// GetTableKeyRange returns the start and end keys for scanning all rows of a table.
func GetTableKeyRange(tableID int64) (startKey, endKey []byte) {
	startKey = GenTableRecordPrefix(tableID)
	endKey = GenTableRecordPrefix(tableID + 1)
	return
}

type TiKVClient struct {
	client *txnkv.Client
}

func NewTiKVClient(pdAddresses string) (*TiKVClient, error) {
	addrs := strings.Split(pdAddresses, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	log.Printf("[tikv] connecting to PD (txnkv): %v", addrs)

	client, err := txnkv.NewClient(addrs)
	if err != nil {
		return nil, fmt.Errorf("create txnkv client: %w", err)
	}

	log.Printf("[tikv] connected, clusterID=%d", client.GetClusterID())
	return &TiKVClient{client: client}, nil
}

func (c *TiKVClient) GetTimestamp(ctx context.Context) (uint64, error) {
	ts, err := c.client.GetTimestamp(ctx)
	if err != nil {
		return 0, fmt.Errorf("get timestamp: %w", err)
	}
	return ts, nil
}

func (c *TiKVClient) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]KvPair, error) {
	ts, err := c.GetTimestamp(ctx)
	if err != nil {
		return nil, fmt.Errorf("get snapshot ts for scan: %w", err)
	}
	return c.ScanWithTS(ctx, startKey, endKey, limit, ts)
}

func (c *TiKVClient) ScanWithTS(_ context.Context, startKey, endKey []byte, limit int, ts uint64) ([]KvPair, error) {
	snapshot := c.client.GetSnapshot(ts)
	snapshot.SetScanBatchSize(limit)

	iter, err := snapshot.Iter(startKey, endKey)
	if err != nil {
		return nil, fmt.Errorf("snapshot iter: %w", err)
	}
	defer iter.Close()

	var pairs []KvPair
	for iter.Valid() {
		if len(pairs) >= limit {
			break
		}
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		pairs = append(pairs, KvPair{Key: key, Value: value})

		if err := iter.Next(); err != nil {
			return pairs, fmt.Errorf("iterator next: %w", err)
		}
	}

	return pairs, nil
}

func (c *TiKVClient) Close() error {
	return c.client.Close()
}
