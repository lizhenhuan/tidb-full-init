// 值生成与 SQL 拼装辅助（#t6 dmlgen）。
// 类型判别基于 MySQLType 前缀（information_schema column_type 带修饰），
// 与 encoder/sink 的方言判别口径一致。
package dmlgen

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

// genRowValues 为整行生成随机值。auto_increment PK 返回 nil（INSERT NULL
// 让 TiDB 分配，doInsert 用 LastInsertId 回填消息）。
func genRowValues(t *tiDBInfo, info *encoder.TableInfo, rng *rand.Rand) map[string]interface{} {
	vals := make(map[string]interface{}, len(info.Columns))
	for _, c := range info.Columns {
		if c.IsPrimaryKey && t.autoInc[c.Name] {
			vals[c.Name] = nil
			continue
		}
		vals[c.Name] = genValue(c.MySQLType, rng)
	}
	return vals
}

// genValue 按类型前缀生成值。宽度裁剪防超长（变宽列 varchar(N)）。
func genValue(mysqlType string, rng *rand.Rand) interface{} {
	mt := strings.ToLower(mysqlType)
	// 去掉带符号/枚举字面量等后缀修饰，取基础类型
	base := mt
	if i := strings.IndexAny(base, "( "); i > 0 {
		base = base[:i]
	}

	switch base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return rng.Int63n(intMaxOf(base, strings.Contains(mt, "unsigned"))) + 1
	case "float", "double", "real":
		return float64(rng.Intn(1_000_000)) / 100
	case "decimal", "numeric":
		return fmt.Sprintf("%d.%02d", rng.Intn(100000), rng.Intn(100))
	case "date":
		return fmt.Sprintf("20%02d-%02d-%02d", rng.Intn(25), rng.Intn(12)+1, rng.Intn(28)+1)
	case "datetime", "timestamp":
		return fmt.Sprintf("20%02d-%02d-%02d %02d:%02d:%02d",
			rng.Intn(25), rng.Intn(12)+1, rng.Intn(28)+1,
			rng.Intn(24), rng.Intn(60), rng.Intn(60))
	case "time":
		return fmt.Sprintf("%02d:%02d:%02d", rng.Intn(24), rng.Intn(60), rng.Intn(60))
	case "year":
		return 1990 + rng.Intn(50)
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext":
		n := widthOf(mt)
		s := randString(rng, 32)
		if n > 0 && len(s) > n {
			s = s[:n]
		}
		return s
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		n := widthOf(mt)
		b := make([]byte, 16)
		rng.Read(b)
		if n >= 0 && len(b) > n {
			b = b[:n]
		}
		return b
	case "enum", "set":
		// 值集在 column_type 字面量里（enum('a','b')）；取第一个成员
		return firstEnumLiteral(mt)
	case "bit", "boolean", "bool":
		return rng.Intn(2)
	case "json":
		return fmt.Sprintf(`{"k":%d,"s":"%s"}`, rng.Intn(1000), randString(rng, 8))
	default:
		// 未知类型：给短字符串（宁可在 TiDB 报错暴露，不静默跳过）
		return randString(rng, 8)
	}
}

// mutateValue 对已存在值生成一个"不同"的新值（UPDATE 用）。
// 比较走 encoder.FormatValue，与消息编码口径一致。
func mutateValue(c encoder.ColumnInfo, old interface{}, rng *rand.Rand) interface{} {
	for i := 0; i < 8; i++ {
		nv := genValue(c.MySQLType, rng)
		if encoder.FormatValue(c.MySQLType, nv) != encoder.FormatValue(c.MySQLType, old) {
			return nv
		}
	}
	// 兜底：追加确定性后缀保证变化
	return encoder.FormatValue(c.MySQLType, old) + "_x"
}

// --- SQL 拼装 ---

func quoteIdent(s string) string { return "`" + s + "`" }

func joinQuoted(parts []string) string { return strings.Join(parts, ", ") }

func joinCSV(parts []string) string { return strings.Join(parts, ", ") }

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// wherePK "pk1 = ? AND pk2 = ?"；pkArgs 按声明序取值。
func wherePK(t *tiDBInfo) string {
	parts := make([]string, len(t.pkCols))
	for i, pk := range t.pkCols {
		parts[i] = quoteIdent(pk) + " = ?"
	}
	return strings.Join(parts, " AND ")
}

func pkArgs(t *tiDBInfo, pkVals []interface{}) []interface{} { return pkVals }

func cloneMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// widthOf 从 "varchar(255)"/"decimal(10,2)" 取第一个宽度参数。
// 注意仅对字符串/二进制类型有意义（数值类型的 (N) 是显示宽度）。
func widthOf(mysqlType string) int {
	i := strings.Index(mysqlType, "(")
	if i < 0 {
		return -1
	}
	rest := mysqlType[i+1:]
	if j := strings.IndexAny(rest, ",)"); j > 0 {
		if n, err := strconv.Atoi(rest[:j]); err == nil {
			return n
		}
	}
	return -1
}

// intMaxOf 数值类型值域上限（unsigned 翻倍）。
func intMaxOf(base string, unsigned bool) int64 {
	var max int64
	switch base {
	case "tinyint":
		max = 127
	case "smallint":
		max = 32767
	case "mediumint":
		max = 8388607
	case "int", "integer":
		max = 2147483647
	default: // bigint
		max = 1 << 52 // json 数值精度内足够
	}
	if unsigned {
		max = max*2 + 1
	}
	return max
}

// firstEnumLiteral 取 enum('a','b') 的第一个成员 'a'。
func firstEnumLiteral(mysqlType string) string {
	i := strings.Index(mysqlType, "'")
	if i < 0 {
		return ""
	}
	rest := mysqlType[i+1:]
	if j := strings.Index(rest, "'"); j >= 0 {
		return rest[:j]
	}
	return ""
}

func randString(rng *rand.Rand, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

// isDupEntry 主键/唯一键冲突（MySQL 1062）。
func isDupEntry(err error) bool {
	return err != nil && strings.Contains(err.Error(), "1062")
}
