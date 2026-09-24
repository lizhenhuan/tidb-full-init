package verify

// #t17：Oracle SELECT 列表必须带唯一别名——池内真实表存在大小写
// 不敏感重复列名（awards_coaches/PEOPLE：COACHID 与 coachid 并存），
// 裸列名 SELECT 选择列表重名 → ORA-00960，整表无法比对。

import (
	"strconv"
	"strings"
	"testing"
)

func TestColListUniqueAliases(t *testing.T) {
	s := &OracleSource{cols: []Column{
		{Name: "COACHID"},
		{Name: "coachid"}, // 大小写不敏感重复（#t17 现场形态）
		{Name: "award"},
	}}
	got := s.colList()
	parts := strings.Split(got, ",")
	if len(parts) != len(s.cols) {
		t.Fatalf("colList = %s, want %d items", got, len(s.cols))
	}
	seen := map[string]bool{}
	for i, p := range parts {
		want := `"` + s.cols[i].Name + `" AS c` + strconv.Itoa(i)
		if p != want {
			t.Errorf("item %d = %q, want %q", i, p, want)
		}
		alias := p[strings.LastIndex(p, "AS ")+3:]
		if seen[alias] {
			t.Errorf("duplicate alias %q in %q（ORA-00960 形态）", alias, got)
		}
		seen[alias] = true
	}
}

// 防御：普通表（无重名）也统一带别名，口径单一。
func TestColListNormalTable(t *testing.T) {
	s := &OracleSource{cols: []Column{{Name: "id"}, {Name: "name"}}}
	got := s.colList()
	want := `"id" AS c0,"name" AS c1`
	if got != want {
		t.Fatalf("colList = %s, want %s", got, want)
	}
}
