package verify

import (
	"context"
	"sort"
	"strings"
)

// SliceSource 内存比对源（测试与 mocksink 断言用）。
// Rows 的行值应为已规范化字符串（或与 Kind 兼容的原始值经 canonical 处理）。
type SliceSource struct {
	Cols    []Column
	Pks     []string
	Rows    [][]string // 与 Cols 对齐；PK 列值须在对应位置
	rows    map[string][]string
	sorted  []string
}

// NewSliceSource 构造并建立索引。pks 为每行的 \x01 连接主键串。
func NewSliceSource(cols []Column, pkNames []string, rows [][]string) *SliceSource {
	s := &SliceSource{Cols: cols, Pks: pkNames}
	s.rows = make(map[string][]string, len(rows))
	idx := pkIndexesOf(cols, pkNames)
	for _, r := range rows {
		parts := make([]string, len(idx))
		for i, j := range idx {
			parts[i] = r[j]
		}
		pk := strings.Join(parts, pkSep)
		s.rows[pk] = r
		s.sorted = append(s.sorted, pk)
	}
	sort.Strings(s.sorted)
	return s
}

func (s *SliceSource) Columns() []Column { return s.Cols }
func (s *SliceSource) PKNames() []string { return s.Pks }

func (s *SliceSource) Count(context.Context) (int64, error) {
	return int64(len(s.rows)), nil
}

func (s *SliceSource) Stream(_ context.Context, fn func(pk string, row []string) error) error {
	for _, pk := range s.sorted {
		if err := fn(pk, s.rows[pk]); err != nil {
			return err
		}
	}
	return nil
}

func (s *SliceSource) Get(_ context.Context, pk string) ([]string, error) {
	if r, ok := s.rows[pk]; ok {
		return r, nil
	}
	return nil, nil
}

// SamplePKs 确定性抽样（排序后等距取 n 个），便于测试断言。
func (s *SliceSource) SamplePKs(_ context.Context, n int) ([]string, error) {
	if n >= len(s.sorted) {
		out := make([]string, len(s.sorted))
		copy(out, s.sorted)
		return out, nil
	}
	var out []string
	step := len(s.sorted) / n
	for i := 0; i < n; i++ {
		out = append(out, s.sorted[i*step])
	}
	return out, nil
}

var _ RowSource = (*SliceSource)(nil)
var _ RowSource = (*TiDBSource)(nil)
var _ RowSource = (*OracleSource)(nil)
