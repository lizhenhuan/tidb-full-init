package oraclesink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// TestConvertValue 表驱动覆盖 P0#2（时间显式绑定）/ P0#3（NULL→nil）/
// 二进制 base64 / year 数值化（M1.5 review P1#12）。
func TestConvertValue(t *testing.T) {
	cases := []struct {
		name      string
		mysqlType string
		raw       string
		want      any
		wantErr   bool
	}{
		// 时间类型：显式 time.Time，不依赖 NLS（P0#2）
		{name: "datetime", mysqlType: "datetime", raw: "2024-01-02 03:04:05",
			want: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		{name: "datetime micros", mysqlType: "datetime(6)", raw: "2024-01-02 03:04:05.123456",
			want: time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		{name: "timestamp", mysqlType: "timestamp", raw: "2024-12-31 23:59:59.999999",
			want: time.Date(2024, 12, 31, 23, 59, 59, 999999000, time.UTC)},
		{name: "date", mysqlType: "date", raw: "2024-01-02",
			want: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{name: "iso8601", mysqlType: "datetime",
			raw:  "2024-01-02T03:04:05Z",
			want: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)},
		{name: "bad datetime", mysqlType: "datetime", raw: "not-a-date", wantErr: true},
		{name: "empty date", mysqlType: "date", raw: "", want: nil},

		// NULL：空串统一绑 nil（P0#3），含二进制与数值
		{name: "null blob", mysqlType: "mediumblob", raw: "", want: nil},
		{name: "null varbinary", mysqlType: "varbinary(16)", raw: "", want: nil},
		{name: "null bigint", mysqlType: "bigint unsigned", raw: "", want: nil},
		{name: "null varchar", mysqlType: "varchar(255)", raw: "", want: nil},

		// 二进制 base64 解码
		{name: "blob base64", mysqlType: "blob", raw: "aGVsbG8=", want: []byte("hello")},
		{name: "varbinary base64", mysqlType: "varbinary(4)", raw: "AQIDBA==", want: []byte{1, 2, 3, 4}},
		{name: "bad base64", mysqlType: "blob", raw: "!!!not-base64!!!", wantErr: true},

		// year → 数值
		{name: "year", mysqlType: "year", raw: "2024", want: 2024},
		{name: "bad year", mysqlType: "year", raw: "20x4", wantErr: true},

		// 透传：time 规范串 / 数值规范串 / json 原文
		{name: "time string", mysqlType: "time", raw: "-838:59:59", want: "-838:59:59"},
		{name: "bigint", mysqlType: "bigint", raw: "-9223372036854775808", want: "-9223372036854775808"},
		{name: "decimal", mysqlType: "decimal(38,10)", raw: "12345678901234567890.0123456789",
			want: "12345678901234567890.0123456789"},
		{name: "json", mysqlType: "json", raw: `{"a":1}`, want: `{"a":1}`},
		{name: "varchar", mysqlType: "varchar(10)", raw: "abc", want: "abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := convertValue(tc.mysqlType, tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("convertValue(%q, %q) = %v, want error", tc.mysqlType, tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("convertValue(%q, %q) unexpected error: %v", tc.mysqlType, tc.raw, err)
			}
			switch want := tc.want.(type) {
			case time.Time:
				gotT, ok := got.(time.Time)
				if !ok {
					t.Fatalf("want time.Time, got %T (%v)", got, got)
				}
				if !gotT.Equal(want) {
					t.Fatalf("want %v, got %v", want, gotT)
				}
			case []byte:
				gotB, ok := got.([]byte)
				if !ok {
					t.Fatalf("want []byte, got %T (%v)", got, got)
				}
				if string(gotB) != string(want) {
					t.Fatalf("want %q, got %q", want, gotB)
				}
			default:
				if got != want {
					t.Fatalf("want %#v, got %#v", want, got)
				}
			}
		})
	}
}

// TestConvertValueNoNLSDependency 防回归：绑定值必须是 time.Time 而非字符串，
// 否则真实 Oracle 上按 NLS_DATE_FORMAT 隐式转换会抛 ORA-01861/01858。
func TestConvertValueNoNLSDependency(t *testing.T) {
	v, err := convertValue("datetime", "2024-01-02 03:04:05")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := v.(time.Time); !ok {
		t.Fatalf("datetime must bind as time.Time, got %T", v)
	}
}

// TestOraErrorCode 各形态错误消息的 ORA 码提取。
func TestOraErrorCode(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{errors.New("ORA-00001: unique constraint (X.PK_T) violated"), 1},
		{errors.New("ORA-01722: invalid number"), 1722},
		{errors.New("ORA-12170: TNS:Connect timeout occurred"), 12170},
		{errors.New("ORA-00972: identifier is too long"), 972},
		{errors.New("dial tcp 10.0.0.1:1521: i/o timeout"), 0},
		{errors.New("sql: no rows in result set"), 0},
	}
	for _, tc := range cases {
		if got := oraErrorCode(tc.err); got != tc.want {
			t.Errorf("oraErrorCode(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// TestClassify 三态失败分类契约（M1.5 跟进复审 P1#1）：
// retryable（网络/锁/ctx 取消）/ row-data（白名单 ORA 码）/ fatal（默认）。
func TestClassify(t *testing.T) {
	// retryable：网络/锁类 ORA 码 + ctx 取消/超时（P1#2 修正后的码表）
	retryable := []error{
		errors.New("ORA-12170: TNS:Connect timeout occurred"),
		errors.New("ORA-00054: resource busy and acquire with NOWAIT specified"),
		errors.New("ORA-00060: deadlock detected while waiting for resource"),
		errors.New("ORA-01013: user requested cancel of current operation"),
		errors.New("ORA-04021: timeout occurred while waiting to lock object"),
		errors.New("ORA-03113: end-of-file on communication channel"),
		errors.New("ORA-03135: connection lost contact"),
		errors.New("ORA-12543: TNS:destination host unreachable"),
		errors.New("ORA-12570: TNS:packet reader failure"),
		context.Canceled,
		context.DeadlineExceeded,
		fmt.Errorf("write batch: %w", context.Canceled), // 包装后仍可识别
		sql.ErrTxDone,     // ctx 取消与自动回滚的竞态残留（真库实测）
		driver.ErrBadConn, // 一次网络抖动不应升级为任务中止（M2 复审 P1#2）
		io.EOF,
		io.ErrUnexpectedEOF,
		syscall.ECONNRESET,
		syscall.EPIPE,
		syscall.ECONNREFUSED,
		&net.OpError{Op: "read", Net: "tcp", Err: errors.New("i/o timeout")}, // net.Error
		fmt.Errorf("wrapped: %w", syscall.ECONNRESET),                       // 包装后仍可识别
	}
	for _, err := range retryable {
		got := classify(err)
		var we *WriteError
		if !errors.As(got, &we) {
			t.Fatalf("classify must return *WriteError, got %T", got)
		}
		if !we.Retryable {
			t.Errorf("classify(%v).Retryable = false, want true", err)
		}
		if we.RowData {
			t.Errorf("classify(%v).RowData = true, want false", err)
		}
		if !errors.Is(got, err) {
			t.Errorf("classify(%v) must unwrap to original", err)
		}
	}
	// row-data：白名单 ORA 码 → 跳行 + DLQ + 前进 offset
	dataErrors := []error{
		errors.New("ORA-00001: unique constraint violated"),
		errors.New("ORA-01722: invalid number"),
		errors.New("ORA-01400: cannot insert NULL into mandatory column"),
		errors.New("ORA-01861: literal does not match format string"),
		errors.New("ORA-12899: value too large for column"),
		errors.New("ORA-01438: value larger than specified precision allows"), // H2：精度映射溢出→DLQ 不中止
	}
	for _, err := range dataErrors {
		got := classify(err)
		var we *WriteError
		if !errors.As(got, &we) {
			t.Fatalf("classify must return *WriteError, got %T", got)
		}
		if we.Retryable || !we.RowData {
			t.Errorf("classify(%v) = retryable=%v rowdata=%v, want row-data", err, we.Retryable, we.RowData)
		}
	}
	// fatal：配置/权限/未知（表不存在、非法标识符、权限不足、旧码表误纳的
	// ORA-00031、无 ORA 码的驱动错误）→ 中止任务、不前进 offset
	fatal := []error{
		errors.New("ORA-00942: table or view does not exist"),
		errors.New("ORA-00904: invalid identifier"),
		errors.New("ORA-01031: insufficient privileges"),
		errors.New("ORA-00031: session marked for kill"), // P1#2：不在重试码表
		errors.New("dial tcp: connection refused"),        // 无 ORA 码且非 ctx → fatal
	}
	for _, err := range fatal {
		got := classify(err)
		var we *WriteError
		if !errors.As(got, &we) {
			t.Fatalf("classify must return *WriteError, got %T", got)
		}
		if we.Retryable || we.RowData {
			t.Errorf("classify(%v) = retryable=%v rowdata=%v, want fatal", err, we.Retryable, we.RowData)
		}
	}

	// 本地绑定/转换错误走 dataError：行级数据错误
	localErrors := []error{
		fmt.Errorf("cannot parse datetime value %q", "bad"),
		fmt.Errorf("decode base64 binary value: illegal base64 data"),
		fmt.Errorf("event missing column %q", "c1"),
	}
	for _, err := range localErrors {
		got := dataError(err)
		var we *WriteError
		if !errors.As(got, &we) {
			t.Fatalf("dataError must return *WriteError, got %T", got)
		}
		if we.Retryable || !we.RowData {
			t.Errorf("dataError(%v) = retryable=%v rowdata=%v, want row-data", err, we.Retryable, we.RowData)
		}
		if !errors.Is(got, err) {
			t.Errorf("dataError(%v) must unwrap to original", err)
		}
	}
}

// TestBindPKArgsConversion PK 绑定同样走显式转换（日期 PK 不能依赖隐式转换）。
func TestBindPKArgsConversion(t *testing.T) {
	s := &Sink{}
	meta := model.TableMeta{
		Database: "db1", Table: "t1",
		Columns: []model.ColumnMeta{
			{Name: "id", MySQLType: "bigint", IsPK: true},
			{Name: "created_at", MySQLType: "datetime", IsPK: true},
			{Name: "val", MySQLType: "varchar(10)"},
		},
		PKNames: []string{"id", "created_at"},
	}
	ev := model.RowEvent{
		Op: model.OpUpdate, Database: "db1", Table: "t1",
		PKNames: []string{"id", "created_at"},
		Before:  map[string]string{"id": "7", "created_at": "2023-12-31 23:59:59"},
		After:   map[string]string{"id": "8", "created_at": "2024-01-02 03:04:05", "val": "x"},
	}

	args, err := s.bindPKArgs(&meta, &ev, true)
	if err != nil {
		t.Fatal(err)
	}
	named := map[string]any{}
	for _, a := range args {
		na, ok := a.(sql.NamedArg)
		if !ok {
			t.Fatalf("want sql.NamedArg, got %T", a)
		}
		named[na.Name] = na.Value
	}
	if v, ok := named["id"].(string); !ok || v != "7" {
		t.Errorf("old PK id: want string 7, got %#v", named["id"])
	}
	wantT := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)
	if v, ok := named["created_at"].(time.Time); !ok || !v.Equal(wantT) {
		t.Errorf("old PK created_at: want %v as time.Time, got %#v", wantT, named["created_at"])
	}

	// useOld=false → 取 After 镜像
	args, err = s.bindPKArgs(&meta, &ev, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range args {
		na := a.(sql.NamedArg)
		if na.Name == "created_at" {
			if v, ok := na.Value.(time.Time); !ok || !v.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
				t.Errorf("new PK created_at: got %#v", na.Value)
			}
		}
	}

	// 缺失 PK 列 → 数据错误
	delete(ev.After, "id")
	if _, err := s.bindPKArgs(&meta, &ev, false); err == nil {
		t.Error("missing PK column must error")
	}
}
