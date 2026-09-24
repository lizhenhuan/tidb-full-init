// Package ddlsync 实现 TiDB→Oracle DDL 表结构同步（#t8，设计文档
// docs/design/ddl-sync-design.md）。
//
// 定位（设计 §0）：tms 的 t2o 引擎**移植不 import**（私有 gitee 模块 +
// CGO godror；且其类型映射与数据管道不兼容）。类型映射唯一权威 =
// internal/dialect/oracle.MapType（经 #t6 R1 十一轮数据级验收）——
// ddl-sync 生成的表就是数据管道随后要写的表，两条路径必须逐列一致
// （由一致性守卫测试钉住，#t9 首条）。
//
// 从 t2o 吸收（Apache-2.0，github.com/Marvinemen/transferdb 思路）：
// charset/collation 兼容预检 + errCompatibility* 字段结构、默认值
// 引用转换、COMMENT ON DDL、标识符大小写三态、FixedDDL(base64) 重跑语义。
package ddlsync

import "strings"

// ColumnMeta 列元数据（比管道 model.ColumnMeta 多默认值/注释/字符集，
// 供 DDL 生成与兼容预检）。
type ColumnMeta struct {
	Name       string
	MySQLType  string // column_type 全形态（含 unsigned/宽度/字面量）
	Nullable   bool
	HasDefault bool
	Default    string // information_schema COLUMN_DEFAULT 原文（字符串字面量/函数/NULL）
	Comment    string
	Charset    string // 字符列非空
	Collation  string
	IsPK       bool
	Extra      string // auto_increment 等
}

// IndexMeta 索引元数据（UK 与二级索引；PK 走 TableMeta.PKNames）。
type IndexMeta struct {
	Name    string
	Columns []string
	Unique  bool
}

// FKMeta 外键元数据（一期只记录为兼容项不执行，设计 §7）。
type FKMeta struct {
	Name             string
	Columns          []string
	RefDatabase      string
	RefTable         string
	RefColumns       []string
	OnDeleteCascade  bool
	OnUpdateCascade  bool
}

// TableMetaFull 单表完整元数据（introspect 产物，convert 输入）。
type TableMetaFull struct {
	Database   string
	Table      string
	Comment    string
	Charset    string // 表默认字符集
	Collation  string
	Columns    []ColumnMeta
	PKNames    []string
	Uniques    []IndexMeta // 唯一键（非 PK）
	Indexes    []IndexMeta // 二级索引
	ForeignKeys []FKMeta
	// PartitionExpr 非空 = 分区表（MySQL 语法原文，一期不转换，只记录
	// 兼容项「目标未创建分区」；P2.5 落地 RANGE/HASH 转换）。
	PartitionExpr string
}

// IsEnumOrSet 列是否 ENUM/SET（列级置灰/兼容项判定，前端
// enumOrSetColumns 契约同源）。
func (c ColumnMeta) IsEnumOrSet() bool {
	return enumOrSet(c.MySQLType)
}

func enumOrSet(mysqlType string) bool {
	b := baseName(mysqlType)
	return b == "enum" || b == "set"
}

// baseName 类型基名：去修饰取小写（enum('a','b') → enum，
// int unsigned → int）。
func baseName(mysqlType string) string {
	s := mysqlType
	if i := strings.IndexAny(s, "( "); i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(strings.TrimSpace(s))
}
