package verify

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
)

// RowSource 比对数据源（TiDB / Oracle / 测试内存实现）。
// 行值一律以 canonical 字符串给出，与 Columns() 顺序对齐。
type RowSource interface {
	// Columns 有序列（含主键列），顺序与 Stream/Get 返回的行值一致。
	Columns() []Column
	// PKNames 主键列名（比对前校验两端同名同数）。
	PKNames() []string
	Count(ctx context.Context) (int64, error)
	// Stream 按 PK 升序流式输出全部行。
	Stream(ctx context.Context, fn func(pk string, row []string) error) error
	// Get 按 PK 点查；行不存在返回 nil。pk 为复合主键的 \x01 连接串。
	Get(ctx context.Context, pk string) ([]string, error)
	// SamplePKs 随机抽 n 个主键（大表抽样比对用）。
	SamplePKs(ctx context.Context, n int) ([]string, error)
}

// Column 比对列。
type Column struct {
	Name string
	Kind Kind
}

// Options 比对参数。
type Options struct {
	ChunkSize   int // 分块大小（分块 checksum 的报告粒度，默认 1000）
	MaxRowDiffs int // 行级差异最多展开数（默认 50；其余只计数）
	MaxPKs      int // 缺失/多余主键最多展开数（默认 50）

	// EmptyStringAsNull 把 canonical "" 与 NULL 判等（K2，TiDB↔Oracle
	// 比对必开）：Oracle 空串即 NULL，源端 '' 写入目标后物理不可区分
	// （管道 convertValue ""→nil 与 Oracle 语义双重决定）。双向归一后
	// 不可能产生假 PASS——目标端不存在能区分 '' 与 NULL 的状态。
	// TiDB↔TiDB 镜像比对保持默认关闭（'' 与 NULL 仍区分）。
	EmptyStringAsNull bool
}

func (o *Options) fill() {
	if o.ChunkSize <= 0 {
		o.ChunkSize = 1000
	}
	if o.MaxRowDiffs <= 0 {
		o.MaxRowDiffs = 50
	}
	if o.MaxPKs <= 0 {
		o.MaxPKs = 50
	}
}

// normalizeRow 按 EmptyStringAsNull 口径把 canonical "" 折叠为 NULL 哨兵。
// 数值/时间列的 canonical 永不为 ""，规则天然只作用于字符串/二进制空值。
// 必须对源/目标两侧同时应用（分块 checksum 与 diffRow 才能口径一致）。
func (o Options) normalizeRow(row []string) {
	if !o.EmptyStringAsNull {
		return
	}
	for i, v := range row {
		if v == "" {
			row[i] = nullSentinel
		}
	}
}

// ColumnDiff 单列差异。
type ColumnDiff struct {
	Column string
	Source string
	Target string
}

// RowDiff 单行差异（源与目标都存在但列值不同）。
type RowDiff struct {
	PK      string
	Columns []ColumnDiff
}

// ChunkResult 分块比对结果（按源端流序切 ChunkSize 行一块）。
// Checksum 为顺序无关聚合（行哈希 XOR + 行数），缺失/多余/差异都会失配。
type ChunkResult struct {
	Index          int
	Rows           int64
	ChecksumMatch  bool
	SourceChecksum uint64
}

// Report 比对报告。
type Report struct {
	SourceRows int64
	TargetRows int64

	Chunks           []ChunkResult
	ChunksMismatched int

	RowDiffs     []RowDiff
	RowDiffCount int64 // 全部差异行数（含超出 MaxRowDiffs 未展开的）
	MissingInTarget []string
	MissingCount    int64
	ExtraInTarget   []string
	ExtraCount      int64

	Duration time.Duration
}

// Equal 数据完全一致（行数、内容；与报告展开上限无关）。
func (r *Report) Equal() bool {
	return r.SourceRows == r.TargetRows &&
		r.RowDiffCount == 0 && r.MissingCount == 0 && r.ExtraCount == 0
}

// Verifier 二期 Verifier 雏形（设计文档 internal/verify 接口）。
type Verifier interface {
	Verify(ctx context.Context, src, dst RowSource, opts Options) (*Report, error)
}

// VerifierFunc 函数适配。
type VerifierFunc func(ctx context.Context, src, dst RowSource, opts Options) (*Report, error)

func (f VerifierFunc) Verify(ctx context.Context, src, dst RowSource, opts Options) (*Report, error) {
	return f(ctx, src, dst, opts)
}

// Compare 全量比对：两端 count + 全量读入逐行逐列比较 + 分块 checksum。
// 内存 O(源端行数 × 行宽)，面向集成/长稳测试断言与中小表；超大表用 Sample。
func Compare(ctx context.Context, src, dst RowSource, opts Options) (*Report, error) {
	opts.fill()
	start := time.Now()
	if err := checkCompatible(src, dst); err != nil {
		return nil, err
	}

	rep := &Report{}
	srcRows, err := src.Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: source count: %w", err)
	}
	dstRows, err := dst.Count(ctx)
	if err != nil {
		return nil, fmt.Errorf("verify: target count: %w", err)
	}
	rep.SourceRows, rep.TargetRows = srcRows, dstRows

	type chunkAcc struct {
		rows    int64
		xorHash uint64
}
	srcMap := make(map[string][]string, 64)
	pkChunk := make(map[string]int, 64)
	var chunks []*chunkAcc
	cur := &chunkAcc{}
	err = src.Stream(ctx, func(pk string, row []string) error {
		opts.normalizeRow(row)
		srcMap[pk] = row
		pkChunk[pk] = len(chunks)
		cur.rows++
		cur.xorHash ^= rowHash(row)
		if cur.rows >= int64(opts.ChunkSize) {
			chunks = append(chunks, cur)
			cur = &chunkAcc{}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("verify: source stream: %w", err)
	}
	if cur.rows > 0 {
		chunks = append(chunks, cur)
	}

	dstChunk := make([]*chunkAcc, len(chunks))
	for i := range dstChunk {
		dstChunk[i] = &chunkAcc{}
	}
	err = dst.Stream(ctx, func(pk string, row []string) error {
		srow, ok := srcMap[pk]
		if !ok {
			rep.ExtraCount++
			if len(rep.ExtraInTarget) < opts.MaxPKs {
				rep.ExtraInTarget = append(rep.ExtraInTarget, pk)
			}
			return nil
		}
		delete(srcMap, pk)
		// 目标行列序/大小写可能与源端不同：先对齐到源列序再哈希/比对
		aligned, err := alignRow(dst.Columns(), row, src.Columns())
		if err != nil {
			return err
		}
		opts.normalizeRow(aligned)
		if ci, ok := pkChunk[pk]; ok && ci < len(dstChunk) {
			dstChunk[ci].rows++
			dstChunk[ci].xorHash ^= rowHash(aligned)
		}
		if diffs := diffRow(src.Columns(), srow, aligned); len(diffs) > 0 {
			rep.RowDiffCount++
			if len(rep.RowDiffs) < opts.MaxRowDiffs {
				rep.RowDiffs = append(rep.RowDiffs, RowDiff{PK: pk, Columns: diffs})
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("verify: target stream: %w", err)
	}

	rep.MissingCount = int64(len(srcMap))
	for pk := range srcMap {
		if len(rep.MissingInTarget) < opts.MaxPKs {
			rep.MissingInTarget = append(rep.MissingInTarget, pk)
		}
	}
	sort.Strings(rep.MissingInTarget)

	for i, c := range chunks {
		cr := ChunkResult{Index: i, Rows: c.rows, SourceChecksum: c.xorHash}
		cr.ChecksumMatch = dstChunk[i].rows == c.rows && dstChunk[i].xorHash == c.xorHash
		if !cr.ChecksumMatch {
			rep.ChunksMismatched++
		}
		rep.Chunks = append(rep.Chunks, cr)
	}

	rep.Duration = time.Since(start)
	return rep, nil
}

// Sample 抽样比对：源端随机抽 n 个主键，两端点查逐行比较。
// 内存 O(n)，面向超大表。可选 Options（EmptyStringAsNull 等，K2）。
func Sample(ctx context.Context, src, dst RowSource, n int, optsList ...Options) (*Report, error) {
	opts := Options{}
	if len(optsList) > 0 {
		opts = optsList[0]
	}
	if n <= 0 {
		n = 100
	}
	start := time.Now()
	if err := checkCompatible(src, dst); err != nil {
		return nil, err
	}
	pks, err := src.SamplePKs(ctx, n)
	if err != nil {
		return nil, fmt.Errorf("verify: sample pks: %w", err)
	}
	rep := &Report{SourceRows: int64(len(pks)), TargetRows: int64(len(pks))}
	for _, pk := range pks {
		srow, err := src.Get(ctx, pk)
		if err != nil {
			return nil, fmt.Errorf("verify: source get %q: %w", pk, err)
		}
		trow, err := dst.Get(ctx, pk)
		if err != nil {
			return nil, fmt.Errorf("verify: target get %q: %w", pk, err)
		}
		switch {
		case srow == nil && trow == nil:
			// 两端都无，跳过
		case srow == nil:
			rep.ExtraCount++
			if len(rep.ExtraInTarget) < 50 {
				rep.ExtraInTarget = append(rep.ExtraInTarget, pk)
			}
		case trow == nil:
			rep.MissingCount++
			if len(rep.MissingInTarget) < 50 {
				rep.MissingInTarget = append(rep.MissingInTarget, pk)
			}
		default:
			aligned, err := alignRow(dst.Columns(), trow, src.Columns())
			if err != nil {
				return nil, err
			}
			opts.normalizeRow(srow)
			opts.normalizeRow(aligned)
			if diffs := diffRow(src.Columns(), srow, aligned); len(diffs) > 0 {
				rep.RowDiffCount++
				if len(rep.RowDiffs) < 50 {
					rep.RowDiffs = append(rep.RowDiffs, RowDiff{PK: pk, Columns: diffs})
				}
			}
		}
	}
	rep.Duration = time.Since(start)
	return rep, nil
}

func checkCompatible(src, dst RowSource) error {
	s, d := src.PKNames(), dst.PKNames()
	if len(s) != len(d) {
		return fmt.Errorf("verify: pk mismatch: source %v vs target %v", s, d)
	}
	sm := map[string]bool{}
	for _, p := range s {
		sm[strings.ToLower(p)] = true
	}
	for _, p := range d {
		if !sm[strings.ToLower(p)] {
			return fmt.Errorf("verify: pk %q missing on source", p)
		}
	}
	dcols := map[string]bool{}
	for _, c := range dst.Columns() {
		dcols[strings.ToLower(c.Name)] = true
	}
	for _, c := range src.Columns() {
		if !dcols[strings.ToLower(c.Name)] {
			return fmt.Errorf("verify: column %q missing on target", c.Name)
		}
	}
	return nil
}

// alignRow 把目标行重排到源列序（列名大小写不敏感）。
func alignRow(dstCols []Column, dstRow []string, srcCols []Column) ([]string, error) {
	idx := make([]int, len(srcCols))
	for i, sc := range srcCols {
		idx[i] = -1
		for j, dc := range dstCols {
			if strings.EqualFold(dc.Name, sc.Name) {
				idx[i] = j
				break
			}
		}
		if idx[i] < 0 {
			return nil, fmt.Errorf("verify: target column %q not found", sc.Name)
		}
	}
	out := make([]string, len(srcCols))
	for i, j := range idx {
		out[i] = dstRow[j]
	}
	return out, nil
}

func rowHash(row []string) uint64 {
	h := fnv.New64a()
	for _, v := range row {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

func diffRow(cols []Column, srow, trow []string) []ColumnDiff {
	var diffs []ColumnDiff
	n := len(cols)
	if len(srow) < n {
		n = len(srow)
	}
	if len(trow) < n {
		n = len(trow)
	}
	for i := 0; i < n; i++ {
		if srow[i] != trow[i] {
			diffs = append(diffs, ColumnDiff{Column: cols[i].Name, Source: srow[i], Target: trow[i]})
		}
	}
	return diffs
}

// FormatPK 复合主键规范键转可读形式。
func FormatPK(pk string) string {
	return strings.ReplaceAll(pk, pkSep, ", ")
}
