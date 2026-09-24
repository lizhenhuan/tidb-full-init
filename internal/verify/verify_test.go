package verify

import (
	"context"
	"testing"
)

func testCols() []Column {
	return []Column{
		{Name: "id", Kind: KindInt},
		{Name: "amount", Kind: KindDecimal},
		{Name: "memo", Kind: KindString},
	}
}

func rows1() [][]string {
	return [][]string{
		{"1", "10.5", "a"},
		{"2", "20", "b"},
		{"3", "30.25", "c"},
	}
}

func TestCompareEqual(t *testing.T) {
	src := NewSliceSource(testCols(), []string{"id"}, rows1())
	dst := NewSliceSource(testCols(), []string{"id"}, rows1())
	rep, err := Compare(context.Background(), src, dst, Options{ChunkSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Equal() {
		t.Fatalf("expected equal, got %+v", rep)
	}
	if len(rep.Chunks) != 2 { // 3 行 / chunk=2 → 2 块
		t.Fatalf("chunks = %d, want 2", len(rep.Chunks))
	}
	for _, c := range rep.Chunks {
		if !c.ChecksumMatch {
			t.Errorf("chunk %d checksum mismatch on equal data", c.Index)
		}
	}
}

func TestCompareDetectsAllDiffKinds(t *testing.T) {
	mod := rows1()
	mod[1][2] = "changed"           // 行差异
	missing := [][]string{mod[0], mod[2]} // 缺 pk=2
	dst := NewSliceSource(testCols(), []string{"id"}, append(missing, []string{"9", "9", "extra"}))
	src := NewSliceSource(testCols(), []string{"id"}, rows1())
	rep, err := Compare(context.Background(), src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Equal() {
		t.Fatal("expected mismatch")
	}
	if rep.RowDiffCount != 0 {
		t.Errorf("RowDiffCount = %d, want 0 (pk=2 是缺失不是差异)", rep.RowDiffCount)
	}
	if rep.MissingCount != 1 || len(rep.MissingInTarget) != 1 || rep.MissingInTarget[0] != "2" {
		t.Errorf("missing = %v (count %d), want [2]", rep.MissingInTarget, rep.MissingCount)
	}
	if rep.ExtraCount != 1 || rep.ExtraInTarget[0] != "9" {
		t.Errorf("extra = %v, want [9]", rep.ExtraInTarget)
	}
	if rep.ChunksMismatched == 0 {
		t.Error("chunk checksums should flag the mismatch")
	}
}

func TestCompareRowDiffColumns(t *testing.T) {
	mod := rows1()
	mod[0][1] = "99" // amount 差异
	dst := NewSliceSource(testCols(), []string{"id"}, mod)
	src := NewSliceSource(testCols(), []string{"id"}, rows1())
	rep, err := Compare(context.Background(), src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowDiffCount != 1 || len(rep.RowDiffs) != 1 {
		t.Fatalf("RowDiffs = %+v (count %d)", rep.RowDiffs, rep.RowDiffCount)
	}
	d := rep.RowDiffs[0]
	if d.PK != "1" || len(d.Columns) != 1 || d.Columns[0].Column != "amount" ||
		d.Columns[0].Source != "10.5" || d.Columns[0].Target != "99" {
		t.Fatalf("diff = %+v", d)
	}
}

func TestCompareDiffCaps(t *testing.T) {
	var big [][]string
	for i := 0; i < 30; i++ {
		big = append(big, []string{itoa(i), "1", "x"})
	}
	mod := make([][]string, len(big))
	for i, r := range big {
		mod[i] = append([]string(nil), r...)
		mod[i][2] = "y"
	}
	src := NewSliceSource(testCols(), []string{"id"}, big)
	dst := NewSliceSource(testCols(), []string{"id"}, mod)
	rep, err := Compare(context.Background(), src, dst, Options{MaxRowDiffs: 5, MaxPKs: 5})
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowDiffCount != 30 || len(rep.RowDiffs) != 5 {
		t.Fatalf("RowDiffCount=%d expanded=%d, want 30/5", rep.RowDiffCount, len(rep.RowDiffs))
	}
}

func TestCompareCompositePK(t *testing.T) {
	cols := []Column{{Name: "a", Kind: KindInt}, {Name: "b", Kind: KindInt}, {Name: "v", Kind: KindString}}
	rows := [][]string{{"1", "1", "x"}, {"1", "2", "y"}, {"2", "1", "z"}}
	src := NewSliceSource(cols, []string{"a", "b"}, rows)
	dst := NewSliceSource(cols, []string{"a", "b"}, rows)
	rep, err := Compare(context.Background(), src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Equal() {
		t.Fatalf("composite pk equal failed: %+v", rep)
	}
	if rep.MissingInTarget != nil {
		t.Fatalf("missing = %v", rep.MissingInTarget)
	}
}

// K2：Oracle 空串即 NULL——EmptyStringAsNull 开启时源端 '' 与目标端 NULL
// 判等（wform 2000 行 F_Description='' 假 DIFF 回归）；关闭时镜像比对
// 仍严格区分（'' ≠ NULL）。
func TestCompareEmptyStringAsNull(t *testing.T) {
	cols := []Column{{Name: "id", Kind: KindInt}, {Name: "memo", Kind: KindString}}
	src := NewSliceSource(cols, []string{"id"}, [][]string{
		{"1", ""}, // TiDB ''
		{"2", nullSentinel},
	})
	dst := NewSliceSource(cols, []string{"id"}, [][]string{
		{"1", nullSentinel}, // Oracle：'' 落库即 NULL
		{"2", nullSentinel},
	})

	// 默认（TiDB↔TiDB 镜像口径）：'' ≠ NULL → 1 行 DIFF
	rep, err := Compare(context.Background(), src, dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowDiffCount != 1 {
		t.Fatalf("default mode RowDiffCount = %d, want 1 ('' must differ from NULL)", rep.RowDiffCount)
	}

	// EmptyStringAsNull（TiDB↔Oracle 口径）：'' ≡ NULL → equal
	rep, err = Compare(context.Background(), src, dst, Options{EmptyStringAsNull: true})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Equal() {
		t.Fatalf("EmptyStringAsNull compare not equal: %+v", rep)
	}

	// 非空值不受影响：'' vs "x" 仍必须 DIFF
	dst2 := NewSliceSource(cols, []string{"id"}, [][]string{{"1", "x"}, {"2", nullSentinel}})
	rep, err = Compare(context.Background(), src, dst2, Options{EmptyStringAsNull: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.RowDiffCount != 1 {
		t.Fatalf("'' vs non-empty must still differ, RowDiffCount = %d", rep.RowDiffCount)
	}

	// Sample 路径同口径
	srcS := NewSliceSource(cols, []string{"id"}, [][]string{{"1", ""}})
	dstS := NewSliceSource(cols, []string{"id"}, [][]string{{"1", nullSentinel}})
	repS, err := Sample(context.Background(), srcS, dstS, 1, Options{EmptyStringAsNull: true})
	if err != nil {
		t.Fatal(err)
	}
	if !repS.Equal() {
		t.Fatalf("sample EmptyStringAsNull not equal: %+v", repS)
	}
}

func TestCompareIncompatibleSchema(t *testing.T) {
	src := NewSliceSource(testCols(), []string{"id"}, rows1())
	otherCols := []Column{{Name: "id", Kind: KindInt}, {Name: "amount", Kind: KindDecimal}}
	dst := NewSliceSource(otherCols, []string{"id"}, [][]string{{"1", "1"}})
	if _, err := Compare(context.Background(), src, dst, Options{}); err == nil {
		t.Fatal("missing column must fail compatibility check")
	}
	badPK := NewSliceSource(testCols(), []string{"memo"}, rows1())
	if _, err := Compare(context.Background(), src, badPK, Options{}); err == nil {
		t.Fatal("pk mismatch must fail compatibility check")
	}
}

func TestSample(t *testing.T) {
	var big [][]string
	for i := 0; i < 100; i++ {
		big = append(big, []string{itoa(i), "1", "x"})
	}
	src := NewSliceSource(testCols(), []string{"id"}, big)
	dst := NewSliceSource(testCols(), []string{"id"}, big)
	rep, err := Sample(context.Background(), src, dst, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Equal() {
		t.Fatalf("sample equal failed: %+v", rep)
	}
	// 挖掉一行制造缺失
	var hole [][]string
	for i, r := range big {
		if i != 50 {
			hole = append(hole, r)
		}
	}
	dst2 := NewSliceSource(testCols(), []string{"id"}, hole)
	rep2, err := Sample(context.Background(), src, dst2, 100) // 全量抽 → 必命中
	if err != nil {
		t.Fatal(err)
	}
	if rep2.MissingCount != 1 || rep2.MissingInTarget[0] != "50" {
		t.Fatalf("sample missing = %+v", rep2)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}
