package sink

import (
	"errors"
	"fmt"
	"testing"
)

// 三态标记实现样例（与 oraclesink.WriteError 同构的最小实现）。
type markedErr struct {
	error
	retryable bool
	rowData   bool
}

func (m *markedErr) IsRetryable() bool { return m.retryable }
func (m *markedErr) IsRowData() bool   { return m.rowData }

// TestIsRetryable / TestIsRowData：applier 的重试与 DLQ 判据走
// errors.As 链式查找 —— 实现方错误被 fmt.Errorf("%w") 包装后仍可识别；
// 普通错误一律 false（fatal），两个标记可独立为真。
func TestErrorMarkers(t *testing.T) {
	cases := []struct {
		name              string
		err               error
		wantRetryable     bool
		wantRowData       bool
	}{
		{"retryable", &markedErr{error: errors.New("ORA-12170"), retryable: true}, true, false},
		{"row-data", &markedErr{error: errors.New("ORA-01722"), rowData: true}, false, true},
		{"both-false fatal", &markedErr{error: errors.New("ORA-00942")}, false, false},
		{"plain error", errors.New("boom"), false, false},
		{"wrapped retryable", fmt.Errorf("write t1: %w", &markedErr{error: errors.New("net"), retryable: true}), true, false},
		{"wrapped row-data", fmt.Errorf("write t1: %w", &markedErr{error: errors.New("conv"), rowData: true}), false, true},
		{"wrapped plain", fmt.Errorf("write t1: %w", errors.New("boom")), false, false},
		{"nil", nil, false, false},
	}
	for _, tc := range cases {
		if got := IsRetryable(tc.err); got != tc.wantRetryable {
			t.Errorf("%s: IsRetryable = %v, want %v", tc.name, got, tc.wantRetryable)
		}
		if got := IsRowData(tc.err); got != tc.wantRowData {
			t.Errorf("%s: IsRowData = %v, want %v", tc.name, got, tc.wantRowData)
		}
	}
}
