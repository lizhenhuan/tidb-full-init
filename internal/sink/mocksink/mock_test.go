package mocksink_test

import (
	"context"
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink/mocksink"
)

func meta() model.TableMeta {
	return model.TableMeta{
		Database: "shop", Table: "orders", PKNames: []string{"id"},
		Columns: []model.ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "memo", MySQLType: "varchar(32)"},
		},
	}
}

func ev(op model.OpType, after, before map[string]string) model.RowEvent {
	return model.RowEvent{
		Op: op, Database: "shop", Table: "orders",
		PKNames: []string{"id"}, After: after, Before: before,
		Topic: "tidb-data-shop-orders", Partition: 0, Offset: 1,
	}
}

// TestIdempotentReplay 幂等重放：同一批事件再写一遍，终态不变（D2 核心断言）。
func TestIdempotentReplay(t *testing.T) {
	s := mocksink.New()
	ctx := context.Background()
	batch := []model.RowEvent{
		ev(model.OpInsert, map[string]string{"id": "1", "memo": "a"}, nil),
		ev(model.OpInsert, map[string]string{"id": "2", "memo": "b"}, nil),
		ev(model.OpUpdate, map[string]string{"id": "1", "memo": "a2"}, map[string]string{"memo": "a"}),
		ev(model.OpDelete, map[string]string{"id": "2", "memo": "b"}, nil),
	}
	if _, err := s.WriteBatch(ctx, meta(), batch); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := s.Rows("shop", "orders"); got != 1 {
		t.Fatalf("rows = %d, want 1", got)
	}
	row := s.State["shop.orders"]["1"]
	if row["memo"] != "a2" {
		t.Errorf("memo = %q, want a2", row["memo"])
	}

	// 重放整批
	if _, err := s.WriteBatch(ctx, meta(), batch); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got := s.Rows("shop", "orders"); got != 1 {
		t.Errorf("after replay rows = %d, want 1", got)
	}
	if row := s.State["shop.orders"]["1"]; row["memo"] != "a2" {
		t.Errorf("after replay memo = %q, want a2", row["memo"])
	}
}

// TestPKChangeAppliesDeleteThenUpsert D2：PK 变更 = 删旧主键行 + 覆盖新主键行。
func TestPKChangeAppliesDeleteThenUpsert(t *testing.T) {
	s := mocksink.New()
	ctx := context.Background()
	seed := []model.RowEvent{
		ev(model.OpInsert, map[string]string{"id": "1", "memo": "a"}, nil),
		ev(model.OpInsert, map[string]string{"id": "2", "memo": "keep"}, nil),
	}
	if _, err := s.WriteBatch(ctx, meta(), seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	change := ev(model.OpUpdate,
		map[string]string{"id": "9", "memo": "moved"},
		map[string]string{"id": "1"})
	if !change.PKChanged() {
		t.Fatal("PKChanged should be true")
	}
	if _, err := s.WriteBatch(ctx, meta(), []model.RowEvent{change}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := s.Rows("shop", "orders"); got != 2 {
		t.Errorf("rows = %d, want 2 (旧 PK 已删、新 PK 已写)", got)
	}
	if _, exists := s.State["shop.orders"]["1"]; exists {
		t.Error("old PK row should be deleted")
	}
	if s.State["shop.orders"]["9"]["memo"] != "moved" {
		t.Error("new PK row missing")
	}
	if s.State["shop.orders"]["2"]["memo"] != "keep" {
		t.Error("unrelated row should be untouched")
	}
}
