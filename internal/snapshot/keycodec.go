package snapshot

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
	"github.com/lizhenhuan/tidb-full-init/internal/tikv"
)

// TiDB 行键中主键值的解码（#t6 复审 H1）。
//
// 行键格式：'t' + EncodeInt(tableID) + "_r" + handle 编码，handle 有两种：
//   - int handle：非聚簇表（handle=_tidb_rowid）或单列整数主键聚簇表，
//     8 字节裸 memcomparable 整数（无 flag 字节），后缀长度恒为 8；
//   - CommonHandle：聚簇复合主键 / 单列非整数主键，按 PK 声明顺序对每个
//     主键列用 TiDB util/codec 键格式编码（flag 字节 + memcomparable 载荷），
//     最短 9 字节（flagInt + 8B），与 int handle 的 8 字节后缀按长度区分。
//
// 聚簇表的主键列只存于 key（row value 不含），必须解码回填，否则事件缺
// 主键列（H1 现场：part_range(id,region_id) 4/4 行 DLQ
// "event missing column id"）。
//
// 已知边界：
//   - varchar/char 主键在二进制排序规则（utf8mb4_bin，TiDB 默认）下可精确
//     还原；非二进制排序规则的 key 是排序键（大小写折叠等），还原值可能与
//     原值在大小写上有差；
//   - decimal/datetime/time 等类型主键暂不支持：返回错误让扫描失败暴露，
//     不静默跳行。需要时按 flag 扩展 decodeHandleValues。
const (
	flagBytes byte = 1 // TiDB util/codec: memcomparable padded bytes
	flagInt   byte = 3 // memcomparable int64（符号位取反 + 大端）
	flagUint  byte = 4 // uint64（仅 >MaxInt64 的无符号值用）
	flagFloat byte = 5 // memcomparable float64
)

// fillPKFromKey 把行键中的主键值回填进 rowData 缺失的主键列。
// 非聚簇表的主键列在 value 里已解码，无事可做。
// 失败返回错误：全量快照阶段没有 DLQ 旁路，静默跳行等于悄悄丢整行数据，
// 调用方应中止扫描让人工介入。
func fillPKFromKey(key []byte, tableInfo *encoder.TableInfo, rowData map[string]string) error {
	missing := false
	for _, pk := range tableInfo.PKNames {
		if _, ok := rowData[pk]; !ok {
			missing = true
			break
		}
	}
	if !missing {
		return nil
	}
	if len(key) < 11 {
		return fmt.Errorf("malformed record key %x", key)
	}

	// 按声明顺序取主键列元信息（列序 != PK 声明序，按 PKNames 对齐）
	pkCols := make([]encoder.ColumnInfo, 0, len(tableInfo.PKNames))
	for _, pk := range tableInfo.PKNames {
		var found *encoder.ColumnInfo
		for i := range tableInfo.Columns {
			if tableInfo.Columns[i].Name == pk {
				found = &tableInfo.Columns[i]
				break
			}
		}
		if found == nil {
			return fmt.Errorf("pk column %q not found in table schema", pk)
		}
		pkCols = append(pkCols, *found)
	}

	vals, err := decodeHandleValues(key[11:], pkCols)
	if err != nil {
		return fmt.Errorf("decode pk from row key %x for %s.%s: %w", key, tableInfo.Database, tableInfo.Table, err)
	}
	for i, pk := range tableInfo.PKNames {
		if _, ok := rowData[pk]; !ok {
			rowData[pk] = vals[i]
		}
	}
	return nil
}

// decodeHandleValues 解码行键 _r 后缀为主键列的字符串值（顺序对齐 pkCols）。
func decodeHandleValues(suffix []byte, pkCols []encoder.ColumnInfo) ([]string, error) {
	if len(suffix) == 8 {
		// int handle：单列整数主键聚簇表的主键值，或非聚簇表的 rowid。
		if len(pkCols) != 1 {
			return nil, fmt.Errorf("int handle (8B suffix) but table has %d pk columns", len(pkCols))
		}
		return []string{strconv.FormatInt(tikv.DecodeInt(suffix), 10)}, nil
	}

	vals := make([]string, 0, len(pkCols))
	rest := suffix
	for i, col := range pkCols {
		if len(rest) == 0 {
			return nil, fmt.Errorf("truncated handle: need %d pk values, got %d", len(pkCols), i)
		}
		var s string
		flag := rest[0]
		rest = rest[1:]
		switch flag {
		case flagInt:
			if len(rest) < 8 {
				return nil, fmt.Errorf("pk column %q: truncated int payload", col.Name)
			}
			s = strconv.FormatInt(tikv.DecodeInt(rest[:8]), 10)
			rest = rest[8:]
		case flagUint:
			if len(rest) < 8 {
				return nil, fmt.Errorf("pk column %q: truncated uint payload", col.Name)
			}
			s = strconv.FormatUint(binary.BigEndian.Uint64(rest[:8]), 10)
			rest = rest[8:]
		case flagFloat:
			if len(rest) < 8 {
				return nil, fmt.Errorf("pk column %q: truncated float payload", col.Name)
			}
			s = strconv.FormatFloat(decodeCmpFloat(binary.BigEndian.Uint64(rest[:8])), 'g', -1, 64)
			rest = rest[8:]
		case flagBytes:
			b, r, err := decodePaddedBytes(rest)
			if err != nil {
				return nil, fmt.Errorf("pk column %q: %w", col.Name, err)
			}
			rest = r
			// 与 rowcodec 值路径同口径：二进制列 base64，文本列原样
			if isBinaryType(strings.ToLower(col.MySQLType)) {
				s = base64.StdEncoding.EncodeToString(b)
			} else {
				s = string(b)
			}
		default:
			return nil, fmt.Errorf("pk column %q (%s): unsupported key codec flag 0x%02x; "+
				"decimal/datetime 等主键类型需扩展 decodeHandleValues", col.Name, col.MySQLType, flag)
		}
		vals = append(vals, s)
	}
	return vals, nil
}

// decodeCmpFloat 还原 TiDB codec.encodeFloat 的 memcomparable 编码：
// 非负数置最高位，负数按位取反。
func decodeCmpFloat(u uint64) float64 {
	if u&(1<<63) != 0 {
		u &^= 1 << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

// decodePaddedBytes 解码 TiDB codec.EncodeBytes 的分组填充格式：
// 每 9 字节一组 = 8 字节载荷 + 尾随 marker（= 9-有效字节数，恒 >= 1），
// 最后一个组必为部分组（marker > 1）——恰为 8 字节整倍数的值会追加一个
// 空组收尾，保证多值连排（复合主键）时自终结。
func decodePaddedBytes(b []byte) (val []byte, rest []byte, err error) {
	for {
		if len(b) < 9 {
			return nil, nil, fmt.Errorf("truncated padded bytes group")
		}
		group := b[:8]
		marker := int(b[8])
		b = b[9:]
		n := 9 - marker
		if n < 0 || n > 8 {
			return nil, nil, fmt.Errorf("invalid pad marker %d", marker)
		}
		val = append(val, group[:n]...)
		if n < 8 {
			return val, b, nil
		}
	}
}
