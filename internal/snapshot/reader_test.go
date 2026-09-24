// ScanTable 死锁回归（#t6 复审 P0-1）：errCh 容量曾取 cfg.Parallelism，
// 直接构造 Config 漏填（=0）时无缓冲——worker 发致命错误阻塞、主协程
// wg.Wait 永久死锁。修复后容量=len(ranges) 与 parallelism 解耦，
// Parallelism<=0 也有默认值。
package snapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/kafka"
	"github.com/lizhenhuan/tidb-full-init/internal/tikv"
)

// failScanClient 任意 Scan 立即失败（触发 worker 的 errCh 发送路径）。
type failScanClient struct{}

func (failScanClient) GetTimestamp(context.Context) (uint64, error) { return 1, nil }
func (failScanClient) Scan(context.Context, []byte, []byte, int) ([]tikv.KvPair, error) {
	return nil, errors.New("scan boom")
}
func (failScanClient) ScanWithTS(context.Context, []byte, []byte, int, uint64) ([]tikv.KvPair, error) {
	return nil, errors.New("scan boom")
}
func (failScanClient) Close() error { return nil }

// emptyScanClient 空表（每次 Scan 返回空页，worker 正常退出）。
type emptyScanClient struct{}

func (emptyScanClient) GetTimestamp(context.Context) (uint64, error) { return 1, nil }
func (emptyScanClient) Scan(context.Context, []byte, []byte, int) ([]tikv.KvPair, error) {
	return nil, nil
}
func (emptyScanClient) ScanWithTS(context.Context, []byte, []byte, int, uint64) ([]tikv.KvPair, error) {
	return nil, nil
}
func (emptyScanClient) Close() error { return nil }

type nopProducer struct{}

func (nopProducer) Send(context.Context, *kafka.Message) error { return nil }
func (nopProducer) Close() error                                { return nil }

func scanTableWithTimeout(t *testing.T, r *Reader, cfg *Config) (int64, error) {
	t.Helper()
	type out struct {
		rows int64
		err  error
	}
	done := make(chan out, 1)
	go func() {
		rows, err := r.ScanTable(context.Background(), cfg, 42,
			&encoder.TableInfo{Database: cfg.Database, Table: cfg.TableName,
				PKNames: []string{"id"},
				Columns: []encoder.ColumnInfo{{ID: 1, Name: "id", IsPrimaryKey: true}}},
			nopProducer{}, "t")
		done <- out{rows, err}
	}()
	select {
	case o := <-done:
		return o.rows, o.err
	case <-time.After(10 * time.Second):
		t.Fatal("ScanTable deadlocked (errCh unbuffered / wg.Wait blocked)")
		return 0, nil
	}
}

// TestScanTableErrorNoDeadlock Parallelism=0 + Scan 失败必须报错返回，
// 不能死锁（修复前的挂死路径）。
func TestScanTableErrorNoDeadlock(t *testing.T) {
	r := NewReader(failScanClient{})
	_, err := scanTableWithTimeout(t, r, &Config{Database: "db1", TableName: "t1"}) // Parallelism=0
	if err == nil {
		t.Fatal("want scan error, got nil")
	}
}

// TestScanTableParallelismZeroCompletes Parallelism=0 空表正常完成
//（默认 parallelism 兜底 + errCh 解耦后的健康路径）。
func TestScanTableParallelismZeroCompletes(t *testing.T) {
	r := NewReader(emptyScanClient{})
	rows, err := scanTableWithTimeout(t, r, &Config{Database: "db1", TableName: "t1"})
	if err != nil {
		t.Fatalf("ScanTable: %v", err)
	}
	if rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
}
