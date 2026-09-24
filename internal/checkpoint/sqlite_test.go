package checkpoint

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

func newSQLite(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// Load 无记录返回 -1（P1#9：从头消费）；Save 后可读回；
// 写更小 offset 被忽略（单调前进，防乱序回退）。
func TestSQLiteStoreSemantics(t *testing.T) {
	s := newSQLite(t)
	ctx := context.Background()
	tp := model.TopicPartition{Topic: "t.orders", Partition: 2}

	if off, err := s.Load(ctx, tp); err != nil || off != -1 {
		t.Fatalf("empty Load = (%d, %v), want (-1, nil)", off, err)
	}
	if err := s.Save(ctx, tp, 41); err != nil {
		t.Fatal(err)
	}
	if off, err := s.Load(ctx, tp); err != nil || off != 41 {
		t.Fatalf("Load = (%d, %v), want (41, nil)", off, err)
	}
	// 回退写被 WHERE 挡掉，幂等成功
	if err := s.Save(ctx, tp, 7); err != nil {
		t.Fatal(err)
	}
	if off, _ := s.Load(ctx, tp); off != 41 {
		t.Fatalf("regressive Save applied: %d, want 41", off)
	}
	// 前进正常 + All 可见
	if err := s.Save(ctx, tp, 42); err != nil {
		t.Fatal(err)
	}
	all, err := s.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if all[tp] != 42 {
		t.Fatalf("All = %v, want 42 for %v", all, tp)
	}
}
