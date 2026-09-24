// compat.go：兼容项/不支持项的显式记录（设计 §3）。
//
// 从 t2o 的 PreCheckCompatibility/errCompatibility* 结构移植（Apache-2.0），
// 字段口径与前端 DdlCompatIssue 契约一致；FK/分区一期只记录不执行
// （preCheckType=FK/PARTITION，suggest 明确「目标未创建该对象」）。
package ddlsync

import (
	"fmt"

	"github.com/lizhenhuan/tidb-full-init/internal/dialect/oracle"
)

// 兼容项 preCheckType 取值（与契约 DdlCompatIssue.preCheckType 同源）。
const (
	PreCheckDataType     = "DATA_TYPE"
	PreCheckCharset      = "CHARSET"
	PreCheckCollation    = "COLLATION"
	PreCheckFK           = "FK"
	PreCheckPartition    = "PARTITION"
)

// CompatItem 单条兼容项（表级或列级）。
type CompatItem struct {
	Level        string // "table" | "column"
	Column       string // Level=column 时非空
	PreCheckType string // DATA_TYPE | CHARSET | COLLATION | FK | PARTITION
	SourceValue  string
	TargetValue  string
	Suggest      string
}

// targetCharsetSupported 源表字符集 → Oracle 目标字符集（t2o
// MigrateTableStructureDatabaseCharsetMap 的 TiDB 子集，设计 §0 移植）。
var targetCharsetSupported = map[string]string{
	"utf8":    "AL32UTF8",
	"utf8mb4": "AL32UTF8",
	"gbk":     "ZHS16GBK",
}

// CheckCompat 逐项检查，返回表的全部兼容项。
// 类型不兼容（ENUM/SET/未知类型）同时决定该表 convert 是否可行：
// 存在 DATA_TYPE 项时调用方应将表标记 failed（仍输出 compat[]，
// FixedDDL 重跑是恢复路径，设计 F2 定性）。
func CheckCompat(meta *TableMetaFull, d *oracle.Dialect) []CompatItem {
	var items []CompatItem

	// 表级字符集
	target, ok := targetCharsetSupported[meta.Charset]
	if !ok {
		items = append(items, CompatItem{
			Level:        "table",
			PreCheckType: PreCheckCharset,
			SourceValue:  meta.Charset,
			TargetValue:  "",
			Suggest:      fmt.Sprintf("源库字符集 %s 无 Oracle 映射（支持 utf8/utf8mb4/gbk），表级跳过", meta.Charset),
		})
		return items // 表级不兼容：其余检查无意义
	}

	for _, c := range meta.Columns {
		// 列级类型（MapType 唯一权威；ENUM/SET/未知类型 → DATA_TYPE 项）
		if _, err := d.MapType(meta.Table, c.Name, c.MySQLType); err != nil {
			reason := err.Error()
			var unsupported *oracle.ErrUnsupportedType
			if asUnsupported(err, &unsupported) {
				reason = unsupported.Reason
			}
			items = append(items, CompatItem{
				Level:        "column",
				Column:       c.Name,
				PreCheckType: PreCheckDataType,
				SourceValue:  c.MySQLType,
				TargetValue:  "",
				Suggest:      reason,
			})
			continue
		}
		// 列级字符集与表默认不一致（二进制串列无字符集，跳过）
		if c.Charset != "" && c.Charset != meta.Charset {
			if _, ok := targetCharsetSupported[c.Charset]; !ok {
				items = append(items, CompatItem{
					Level:        "column",
					Column:       c.Name,
					PreCheckType: PreCheckCharset,
					SourceValue:  c.Charset,
					TargetValue:  target,
					Suggest:      fmt.Sprintf("列字符集 %s 与表默认 %s 不一致且无 Oracle 映射", c.Charset, meta.Charset),
				})
			}
		}
	}

	// FK：一期只记录不执行（已知差异，报告显式列出）
	for _, fk := range meta.ForeignKeys {
		items = append(items, CompatItem{
			Level:        "table",
			PreCheckType: PreCheckFK,
			SourceValue:  fmt.Sprintf("%s(%s) → %s.%s(%s)", fk.Name, joinNames(fk.Columns), fk.RefDatabase, fk.RefTable, joinNames(fk.RefColumns)),
			TargetValue:  "",
			Suggest:      "目标未创建该外键约束（一期不执行 FK；目标表无外键为已知差异）",
		})
	}

	// 分区：一期只记录（P2.5 落地转换，绑定 #t12 验收时间盒）
	if meta.PartitionExpr != "" {
		items = append(items, CompatItem{
			Level:        "table",
			PreCheckType: PreCheckPartition,
			SourceValue:  meta.PartitionExpr,
			TargetValue:  "",
			Suggest:      "目标未创建分区（一期不转换分区 DDL；产物与源不等价为已知差异，P2.5 落地）",
		})
	}
	return items
}

// HasDataTypeIssue 是否存在阻断 convert 的列级类型项（→ failed）。
func HasDataTypeIssue(items []CompatItem) bool {
	for _, it := range items {
		if it.PreCheckType == PreCheckDataType {
			return true
		}
	}
	return false
}

// HasTableCharsetIssue 是否表级字符集不兼容（→ skipped）。
func HasTableCharsetIssue(items []CompatItem) bool {
	for _, it := range items {
		if it.Level == "table" && it.PreCheckType == PreCheckCharset {
			return true
		}
	}
	return false
}

func asUnsupported(err error, target **oracle.ErrUnsupportedType) bool {
	if e, ok := err.(*oracle.ErrUnsupportedType); ok {
		*target = e
		return true
	}
	return false
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ","
		}
		out += n
	}
	return out
}
