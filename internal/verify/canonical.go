// Package verify 实现 TiDB↔Oracle 一致性比对（M2 T2.4，设计文档 L5）：
// 全量 count + 分块 checksum + 抽样比对，是所有集成/长稳测试的断言基础，
// 也是二期 Verifier 的雏形。
//
// 核心思路：两端各抽象为 RowSource，行值按「逻辑类型」规范化为 canonical
// 字符串后比较——TiDB 侧按 MySQL 列类型、Oracle 侧按 DATA_TYPE/SCALE 推断
// 逻辑类型，规范化规则对同一逻辑类型必须产出相同字符串（跨库数值/时间
// 表示差异在此抹平：DECIMAL "123.40" ≡ NUMBER 123.4，datetime 秒后尾零
// 截断等）。规则见 canonical()。
package verify

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

// Kind 列的逻辑类型（规范化契约的键）。
type Kind int

const (
	KindString Kind = iota
	KindInt
	KindDecimal // 定点数（MySQL DECIMAL / Oracle NUMBER 带小数）
	KindFloat   // 浮点（FLOAT/BINARY_DOUBLE；按最短十进制表示比较）
	KindTime    // datetime/timestamp（秒级以下尾零截断）
	KindDate    // 纯日期（Oracle DATE 仅承载 MySQL date，时间部分丢弃）
	KindBytes   // 二进制（base64）
)

// nullSentinel NULL 的规范化表示（与空串区分）。
const nullSentinel = "\x00N"

// pkSep 复合主键规范化值的连接符。
const pkSep = "\x01"

// mysqlKind 按 MySQL COLUMN_TYPE 推断逻辑类型。
func mysqlKind(mysqlType string) Kind {
	t := strings.ToLower(mysqlType)
	switch {
	case strings.Contains(t, "unsigned") || strings.HasPrefix(t, "bigint") ||
		strings.HasPrefix(t, "int") || strings.HasPrefix(t, "smallint") ||
		strings.HasPrefix(t, "tinyint") || strings.HasPrefix(t, "mediumint"):
		return KindInt
	case strings.Contains(t, "decimal") || strings.Contains(t, "numeric"):
		return KindDecimal
	case strings.Contains(t, "float") || strings.Contains(t, "double"):
		return KindFloat
	case strings.HasPrefix(t, "datetime") || strings.HasPrefix(t, "timestamp"):
		return KindTime
	case strings.HasPrefix(t, "date"):
		return KindDate
	case encoder.IsBinaryType(t):
		return KindBytes
	default:
		return KindString
	}
}

// oracleKind 按 ALL_TAB_COLUMNS 的 DATA_TYPE / DATA_SCALE 推断逻辑类型。
// 与 dialect/oracle 的类型映射互为逆（date→DATE、datetime→TIMESTAMP）。
func oracleKind(dataType string, scale *int) Kind {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	switch {
	case t == "NUMBER":
		if scale == nil || *scale > 0 {
			// 无精度刻度的 NUMBER 或带小数刻度 → 定点语义
			return KindDecimal
		}
		return KindInt
	case strings.HasPrefix(t, "FLOAT") || strings.HasPrefix(t, "BINARY_DOUBLE") || strings.HasPrefix(t, "BINARY_FLOAT"):
		return KindFloat
	case strings.HasPrefix(t, "TIMESTAMP"):
		return KindTime
	case t == "DATE":
		return KindDate
	case strings.HasPrefix(t, "BLOB") || strings.HasPrefix(t, "RAW") || strings.HasPrefix(t, "BFILE"):
		return KindBytes
	default: // VARCHAR2/CHAR/NVARCHAR2/NCHAR/CLOB/NCLOB/XMLTYPE...
		return KindString
	}
}

// canonical 把 driver 扫描值规范化为比较用字符串。
// 同一逻辑类型下 MySQL 与 Oracle 的表示差异（尾零、指数、时间格式、
// 隐式类型 int64/float64/string）必须产出相同结果。
func canonical(v any, k Kind) (string, error) {
	if v == nil {
		return nullSentinel, nil
	}
	switch k {
	case KindInt, KindDecimal, KindFloat:
		return canonicalNumber(v, k)
	case KindTime:
		return canonicalTime(v, time.DateTime+".999999999")
	case KindDate:
		return canonicalTime(v, "2006-01-02")
	case KindBytes:
		return canonicalBytes(v), nil
	default:
		return canonicalString(v), nil
	}
}

func canonicalNumber(v any, k Kind) (string, error) {
	var s string
	switch n := v.(type) {
	case int64:
		s = strconv.FormatInt(n, 10)
	case int:
		s = strconv.Itoa(n)
	case uint64:
		s = strconv.FormatUint(n, 10)
	case float64:
		// 最短往返表示：float64(123.45) → "123.45"，与 MySQL DECIMAL 文本一致
		s = strconv.FormatFloat(n, 'f', -1, 64)
	case []byte:
		s = string(n)
	case string:
		s = n
	default:
		return "", fmt.Errorf("verify: cannot canonicalize %T as number", v)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("verify: empty number string")
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return "", fmt.Errorf("verify: %q is not a number", s)
	}
	// 负零归一（"-0" 与 "0" 相等）
	if r.Sign() == 0 {
		return "0", nil
	}
	return r.RatString(), nil
}

var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	time.RFC3339Nano,
	time.RFC3339,
}

func canonicalTime(v any, layout string) (string, error) {
	var t time.Time
	switch x := v.(type) {
	case time.Time:
		t = x
	default:
		s := canonicalString(v)
		var err error
		for _, l := range timeLayouts {
			if t, err = time.Parse(l, s); err == nil {
				break
			}
		}
		if err != nil {
			return "", fmt.Errorf("verify: %q is not a time", s)
		}
	}
	// 布局中的 .999999999 会自动省略尾零/小数点，抹平秒级以下精度差
	s := t.Format(layout)
	// Oracle DATE 承载 MySQL date：时间部分必须恒为 0（映射约定）
	return s, nil
}

func canonicalBytes(v any) string {
	switch b := v.(type) {
	case []byte:
		return encoder.EncodeBinaryValue(b)
	case string:
		return encoder.EncodeBinaryValue([]byte(b))
	default:
		return encoder.EncodeBinaryValue([]byte(fmt.Sprintf("%v", b)))
	}
}

func canonicalString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprintf("%v", s)
	}
}
