package dmlgen

// #t15：UPDATE 变更列选择优先级钉子——UNIQUE 约束列随机新值撞既有行
// 即 1062（employees.store idx_unique_manager 现场），选列必须避开；
// 无普通列时按序退避，全 PK 表退首 PK 列（改回同值也算 UPDATE 事件）。

import (
	"testing"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

func mkInfo(cols []encoder.ColumnInfo, pkNames ...string) (*encoder.TableInfo, *tiDBInfo) {
	info := &encoder.TableInfo{Database: "d", Table: "t", PKNames: pkNames}
	for _, c := range cols {
		info.Columns = append(info.Columns, c)
	}
	t := &tiDBInfo{pkCols: pkNames}
	return info, t
}

func TestPickUpdateTargetAvoidsUnique(t *testing.T) {
	// id PK + manager UNIQUE + name/score 普通：必须选普通列。
	info, tdb := mkInfo([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
		{Name: "manager", MySQLType: "int"},
		{Name: "name", MySQLType: "varchar(32)"},
		{Name: "score", MySQLType: "int"},
	}, "id")
	tdb.uniqCols = map[string]bool{"manager": true}
	got := pickUpdateTarget(info, tdb)
	if got == nil || got.Name != "name" {
		t.Fatalf("pickUpdateTarget = %v, want first non-PK non-UNIQUE column \"name\"", got)
	}
}

func TestPickUpdateTargetCompositeUniqueMembersSkipped(t *testing.T) {
	// 复合 UNIQUE(a,b)：任一成员都不能改（只改一半仍可能组合撞键）。
	info, tdb := mkInfo([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
		{Name: "a", MySQLType: "int"},
		{Name: "b", MySQLType: "int"},
		{Name: "note", MySQLType: "varchar(64)"},
	}, "id")
	tdb.uniqCols = map[string]bool{"a": true, "b": true}
	got := pickUpdateTarget(info, tdb)
	if got == nil || got.Name != "note" {
		t.Fatalf("pickUpdateTarget = %v, want \"note\" (both composite-unique members skipped)", got)
	}
}

func TestPickUpdateTargetFallbacks(t *testing.T) {
	// 只有 PK + UNIQUE 列：退到 UNIQUE 列（残余碰撞由 1062 重试兜底）。
	info, tdb := mkInfo([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
		{Name: "manager", MySQLType: "int"},
	}, "id")
	tdb.uniqCols = map[string]bool{"manager": true}
	if got := pickUpdateTarget(info, tdb); got == nil || got.Name != "manager" {
		t.Fatalf("pickUpdateTarget = %v, want unique-col fallback \"manager\"", got)
	}

	// 全 PK 表：退首 PK 列。
	info2, tdb2 := mkInfo([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
	}, "id")
	if got := pickUpdateTarget(info2, tdb2); got == nil || got.Name != "id" {
		t.Fatalf("pickUpdateTarget = %v, want PK fallback \"id\"", got)
	}
}

// nil uniqCols（无 UNIQUE 索引的表）行为与旧口径一致：首个非 PK 列。
func TestPickUpdateTargetNoUniqueIndex(t *testing.T) {
	info, tdb := mkInfo([]encoder.ColumnInfo{
		{Name: "id", MySQLType: "bigint", IsPrimaryKey: true},
		{Name: "a", MySQLType: "int"},
		{Name: "b", MySQLType: "int"},
	}, "id")
	if got := pickUpdateTarget(info, tdb); got == nil || got.Name != "a" {
		t.Fatalf("pickUpdateTarget = %v, want \"a\"", got)
	}
}
