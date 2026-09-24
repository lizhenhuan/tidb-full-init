package snapshot

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/kafka"
	"github.com/lizhenhuan/tidb-full-init/internal/tikv"
)

type Reader struct {
	client tikv.Client
}

type Config struct {
	Database    string
	TableName   string
	Parallelism int
	BatchSize   int
	StartTSO    uint64 // 0 means auto-fetch from PD
}

func NewReader(client tikv.Client) *Reader {
	return &Reader{client: client}
}

func (r *Reader) ScanTable(
	ctx context.Context,
	cfg *Config,
	tableID int64,
	tableInfo *encoder.TableInfo,
	kafkaProducer kafka.Producer,
	topic string,
) (int64, error) {
	startKey, endKey := tikv.GetTableKeyRange(tableID)
	log.Printf("[%s.%s] key range: start=%x end=%x (tableID=%d)", cfg.Database, cfg.TableName, startKey, endKey, tableID)

	var startTs uint64
	var err error
	if cfg.StartTSO > 0 {
		startTs = cfg.StartTSO
		log.Printf("[%s.%s] using provided startTs=%d", cfg.Database, cfg.TableName, startTs)
	} else {
		startTs, err = r.client.GetTimestamp(ctx)
		if err != nil {
			return 0, fmt.Errorf("get snapshot timestamp: %w", err)
		}
		log.Printf("[%s.%s] snapshot startTs=%d", cfg.Database, cfg.TableName, startTs)
	}

	ranges := splitKeyRange(startKey, endKey, cfg.Parallelism)
	log.Printf("[%s.%s] split into %d ranges (parallelism=%d)", cfg.Database, cfg.TableName, len(ranges), cfg.Parallelism)

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}

	var totalRows int64
	var wg sync.WaitGroup
	errCh := make(chan error, cfg.Parallelism)

	for i, kr := range ranges {
		wg.Add(1)
		go func(partition int, rangeStart, rangeEnd []byte) {
			defer wg.Done()
			r.scanPartition(ctx, partition, rangeStart, rangeEnd, batchSize, startTs,
				tableInfo, kafkaProducer, topic, &totalRows, errCh)
		}(i, kr[0], kr[1])
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return totalRows, err
		}
	}

	log.Printf("[%s.%s] snapshot done: rows=%d", cfg.Database, cfg.TableName, totalRows)

	return totalRows, nil
}

func (r *Reader) scanPartition(
	ctx context.Context,
	partition int,
	start, end []byte,
	batchSize int,
	startTs uint64,
	tableInfo *encoder.TableInfo,
	kafkaProducer kafka.Producer,
	topic string,
	totalRows *int64,
	errCh chan<- error,
) {
	var partitionRows int64
	currentKey := make([]byte, len(start))
	copy(currentKey, start)
	validated := false

	for {
		pairs, err := r.client.ScanWithTS(ctx, currentKey, end, batchSize, startTs)
		if err != nil {
			errCh <- fmt.Errorf("partition %d scan error: %w", partition, err)
			return
		}
		if len(pairs) == 0 {
			break
		}

		// D13 启动校验：首批含 record 的数据先验证列 ID 与 schema 一致，
		// 防止 DROP COLUMN 后 ORDINAL_POSITION 与真实列 ID 错位导致静默丢列。
		// 首批可能全是非 record key（索引等）：遇到首个非空 sample 才置位，
		// 否则会跳过校验（复审 P2#4）。
		if !validated {
			sample := make([][]byte, 0, len(pairs))
			for _, pair := range pairs {
				if isRecordKey(pair.Key) {
					sample = append(sample, pair.Value)
				}
			}
			if len(sample) > 0 {
				if err := ValidateColumnIDs(sample, tableInfo.Columns); err != nil {
					errCh <- fmt.Errorf("partition %d: %w", partition, err)
					return
				}
				validated = true
			}
		}

		for _, pair := range pairs {
			if !isRecordKey(pair.Key) {
				continue
			}

			handle := extractHandleFromKey(pair.Key)
			rowData, err := decodeRowValue(pair.Value, tableInfo)
			if err != nil {
				log.Printf("[part-%d] decode error at key=%x: %v", partition, pair.Key, err)
				continue
			}

			// Set primary key value from handle (stored in key, not in value)
			if handle > 0 {
				for _, pk := range tableInfo.PKNames {
					if _, exists := rowData[pk]; !exists {
						rowData[pk] = fmt.Sprintf("%d", handle)
					}
				}
			}

			msg := encoder.NewInsertMessage(tableInfo)
			msg.BuildTypeMapping(tableInfo.Columns)
			msg.SetRowData(rowData)

			msgBytes, err := msg.ToJSON()
			if err != nil {
				log.Printf("[part-%d] encode error: %v", partition, err)
				continue
			}

			pkKey := buildPKKey(tableInfo, rowData)
			if err := kafkaProducer.Send(ctx, &kafka.Message{
				Key:   pkKey,
				Value: msgBytes,
				Topic: topic,
			}); err != nil {
				errCh <- fmt.Errorf("partition %d send error: %w", partition, err)
				return
			}
			partitionRows++
		}

		atomic.AddInt64(totalRows, partitionRows)
		partitionRows = 0

		currentKey = incrementKey(pairs[len(pairs)-1].Key)
	}
	atomic.AddInt64(totalRows, partitionRows)
}

// splitKeyRange divides [start, end) into n partitions for parallel scanning.
// If the key range is too narrow for effective splitting, returns a single partition.
func splitKeyRange(start, end []byte, partitions int) [][2][]byte {
	if partitions <= 1 {
		return [][2][]byte{{start, end}}
	}

	// Check if the key range is wide enough to split meaningfully.
	// If start and end differ by too little (e.g., only tableID boundary),
	// splitting produces many zero-width partitions.
	if !canSplitRange(start, end, partitions) {
		return [][2][]byte{{start, end}}
	}

	ranges := make([][2][]byte, partitions)
	for i := 0; i < partitions; i++ {
		if i == 0 {
			ranges[i][0] = start
		} else {
			ranges[i][0] = calculateSplitPoint(start, end, partitions, i)
		}
		if i == partitions-1 {
			ranges[i][1] = end
		} else {
			ranges[i][1] = calculateSplitPoint(start, end, partitions, i+1)
		}
	}
	return ranges
}

// canSplitRange checks if the key range has enough spread for splitting.
// For table-level key ranges (11 bytes with only tableID differing),
// each partition needs at least 1 distinct byte value.
func canSplitRange(start, end []byte, partitions int) bool {
	maxLen := len(start)
	if len(end) > maxLen {
		maxLen = len(end)
	}
	s := make([]byte, maxLen)
	e := make([]byte, maxLen)
	copy(s[maxLen-len(start):], start)
	copy(e[maxLen-len(end):], end)

	// Calculate total range width as a big integer
	var diff uint64
	for i := 0; i < maxLen; i++ {
		diff = diff*256 + uint64(e[i]) - uint64(s[i])
	}
	return diff >= uint64(partitions)
}

// calculateSplitPoint evenly divides the key space.
// Since TiDB keys are memcomparable (unsigned big-endian), we can treat them as
// big numbers and interpolate split points.
func calculateSplitPoint(start, end []byte, partitions, idx int) []byte {
	// Normalize to same length
	maxLen := len(start)
	if len(end) > maxLen {
		maxLen = len(end)
	}

	s := make([]byte, maxLen)
	e := make([]byte, maxLen)
	copy(s[maxLen-len(start):], start)
	copy(e[maxLen-len(end):], end)

	// Calculate offset: for idx out of partitions,
	// split = start + (end-start)*idx/partitions
	result := make([]byte, maxLen)
	carry := 0
	for i := maxLen - 1; i >= 0; i-- {
		diff := int(e[i]) - int(s[i])
		scaled := diff * idx / partitions
		val := int(s[i]) + scaled + carry
		if val > 255 {
			carry = 1
			val -= 256
		} else {
			carry = 0
		}
		result[i] = byte(val)
	}

	// Trim leading zeros to normalize
	leading := 0
	for leading < len(result)-1 && result[leading] == 0 && s[leading] == 0 {
		leading++
	}

	return result[leading:]
}

func incrementKey(k []byte) []byte {
	result := make([]byte, len(k))
	copy(result, k)
	for i := len(result) - 1; i >= 0; i-- {
		result[i]++
		if result[i] != 0 {
			break
		}
	}
	return result
}

func isRecordKey(k []byte) bool {
	// TiDB record keys start with 't' + EncodeInt(tableID) + "_r" + EncodeInt(handle)
	// The prefix is 't' (0x74) followed by encoded table ID.
	// We check for 't' prefix - this is sufficient since we only scan within the correct table range.
	if len(k) < 11 { // min: 't'(1) + tableID(8) + "_r"(2)
		return false
	}
	return k[0] == 't' && k[9] == '_' && k[10] == 'r'
}

// extractHandleFromKey extracts the int64 handle (row ID) from a TiDB record key.
// Key format: 't' + EncodeInt(tableID) + "_r" + EncodeInt(handle)
func extractHandleFromKey(k []byte) int64 {
	if len(k) < 19 { // 1 + 8 + 2 + 8
		return 0
	}
	return tikv.DecodeInt(k[11:19])
}

func decodeRowValue(value []byte, tableInfo *encoder.TableInfo) (map[string]string, error) {
	if len(value) == 0 {
		return nil, fmt.Errorf("empty value")
	}

	if IsRowcodecNewFormat(value) {
		return DecodeRowNewFormat(value, tableInfo.Columns)
	}
	return DecodeRowOldFormat(value, tableInfo.Columns)
}

func buildPKKey(tableInfo *encoder.TableInfo, row map[string]string) []byte {
	for _, pk := range tableInfo.PKNames {
		if v, ok := row[pk]; ok {
			return []byte(v)
		}
	}
	return []byte(tableInfo.Database + "." + tableInfo.Table)
}

func sendMessage(prod kafka.Producer, topic string, key []byte, value string) error {
	return prod.Send(context.Background(), &kafka.Message{
		Key:   key,
		Value: []byte(value),
		Topic: topic,
	})
}

var _ = math.MaxInt64
