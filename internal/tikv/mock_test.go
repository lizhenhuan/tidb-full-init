package tikv

import (
	"bytes"
	"context"
	"testing"
)

// TestMockScanOrdered（T3.1 复审回归）：Scan 必须按 key 升序返回。
// snapshot.Reader 用 incrementKey(最后一条) 做分页游标；若 Scan 顺序随机，
// 游标会落在已返回的 key 上导致重复计数（曾致 initrunner mock 行数
// 3→4/5 抖动，-count=20 9/20 FAIL）。
func TestMockScanOrdered(t *testing.T) {
	// 非连续 handle：1,2,3,5,8,13,21,34（刻意留空洞，游标 increment 不得命中既有 key）
	handles := []int64{1, 2, 3, 5, 8, 13, 21, 34}
	c := NewMockClient(nil)
	for _, h := range handles {
		key := GenTableRecordKey(42, h)
		c.SetData(string(key), []byte{byte(h)})
	}
	start := GenTableRecordPrefix(42)
	end := GenTableRecordPrefix(43)

	pairs, err := c.Scan(context.Background(), start, end, 0)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(pairs) != len(handles) {
		t.Fatalf("Scan returned %d pairs, want %d", len(pairs), len(handles))
	}
	for i := 1; i < len(pairs); i++ {
		if bytes.Compare(pairs[i-1].Key, pairs[i].Key) >= 0 {
			t.Fatalf("Scan not strictly ascending at %d: %x >= %x", i, pairs[i-1].Key, pairs[i].Key)
		}
	}
}

// TestMockScanPagedStable：模拟 Reader 的分页循环（limit 截断 +
// incrementKey 游标推进），重复多轮行数必须稳定——回归「随机顺序下
// 游标撞已返回 key → 行数抖动」。
func TestMockScanPagedStable(t *testing.T) {
	handles := []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89}
	c := NewMockClient(nil)
	for _, h := range handles {
		c.SetData(string(GenTableRecordKey(7, h)), []byte("v"))
	}
	start := GenTableRecordPrefix(7)
	end := GenTableRecordPrefix(8)

	// incrementKey 与 snapshot.Reader 同实现：末字节 +1 进位
	incrementKey := func(k []byte) []byte {
		out := make([]byte, len(k))
		copy(out, k)
		for i := len(out) - 1; i >= 0; i-- {
			out[i]++
			if out[i] != 0 {
				break
			}
		}
		return out
	}

	scanAll := func(limit int) int {
		total := 0
		cursor := append([]byte(nil), start...)
		for {
			pairs, err := c.Scan(context.Background(), cursor, end, limit)
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(pairs) == 0 {
				return total
			}
			total += len(pairs)
			cursor = incrementKey(pairs[len(pairs)-1].Key)
			if bytes.Compare(cursor, end) >= 0 {
				return total
			}
		}
	}

	for round := 0; round < 20; round++ {
		for _, limit := range []int{1, 2, 3, len(handles) - 1, 100} {
			if got := scanAll(limit); got != len(handles) {
				t.Fatalf("round %d limit %d: paged scan total = %d, want %d (cursor regression)", round, limit, got, len(handles))
			}
		}
	}
}
