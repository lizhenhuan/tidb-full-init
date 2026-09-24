// Package mocksink 是 Sink 的内存 mock 实现（单测用，沿用仓库 interface+mock 习惯）。
package mocksink

import (
	"context"
	"sync"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
	"github.com/lizhenhuan/tidb-full-init/internal/sink"
)

// Sink 内存 mock：记录所有 Prepare/WriteBatch 调用与终态行集，
// 可注入失败。State 按 db.table → pk → after 行组织，用于幂等断言。
type Sink struct {
	mu       sync.Mutex
	Prepared []model.TableMeta
	Batches  [][]model.RowEvent
	State    map[string]map[string]map[string]string // db.table → pk → row
	FailNext error
}

// New 构造 mock sink。
func New() *Sink {
	return &Sink{State: make(map[string]map[string]map[string]string)}
}

func (s *Sink) Prepare(_ context.Context, meta model.TableMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.FailNext != nil {
		return s.FailNext
	}
	s.Prepared = append(s.Prepared, meta)
	return nil
}

func (s *Sink) WriteBatch(_ context.Context, meta model.TableMeta, events []model.RowEvent) (sink.FlushResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.FailNext != nil {
		err := s.FailNext
		s.FailNext = nil // 一次性失败，便于重试测试
		return sink.FlushResult{}, err
	}
	s.Batches = append(s.Batches, events)

	key := meta.Database + "." + meta.Table
	table, ok := s.State[key]
	if !ok {
		table = make(map[string]map[string]string)
		s.State[key] = table
	}

	res := sink.FlushResult{Applied: 0}
	for _, ev := range events {
		pk := pkValue(&ev)
		switch ev.Op {
		case model.OpDelete, model.OpUpdate:
			if old := oldPK(&ev); old != "" && old != pk {
				delete(table, old) // PK 变更：先删旧主键行（D2）
			}
		}
		switch ev.Op {
		case model.OpDelete:
			delete(table, pk)
		default: // INSERT/UPDATE 幂等覆盖
			table[pk] = ev.After
		}
		res.Applied++
	}
	return res, nil
}

func (s *Sink) Close() error { return nil }

// Rows 返回某表终态行数。
func (s *Sink) Rows(db, table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.State[db+"."+table])
}

func pkValue(ev *model.RowEvent) string {
	v := ""
	for i, pk := range ev.PKNames {
		if i > 0 {
			v += "|"
		}
		v += ev.After[pk]
	}
	return v
}

func oldPK(ev *model.RowEvent) string {
	v := ""
	for i, pk := range ev.PKNames {
		old, ok := ev.Before[pk]
		if !ok {
			old = ev.After[pk] // old 未含该列 = 未变更
		}
		if i > 0 {
			v += "|"
		}
		v += old
	}
	return v
}

var _ sink.Sink = (*Sink)(nil)
