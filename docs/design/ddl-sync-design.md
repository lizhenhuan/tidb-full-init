# TiDB→Oracle DDL 表结构同步（ddl-sync）设计说明

状态：待 leader review（#t8）｜2026-09-24｜作者：cc_glm-5.3-pro

## 0. 前置决策：t2o 复用方式 = **移植（port），不 import**

1. **依赖不可行**：tms 是私有 gitee 模块（`gitee.com/pingcap_enterprise/tms`），import 会拖进 gorm/zap/go-pretty + vendored **godror（CGO + ODPI-C）**。本项目全线纯 Go go-ora 静态构建，引入 CGO 破坏发布形态与交叉编译。
2. **类型映射权威只能有一个**：数据管道的 Oracle 表由 `internal/dialect/oracle` 生成并经 #t6 R1 十一轮验收（full+incr 全等）。t2o 映射与数据管道**不兼容**：`datetime→DATE`（丢时间部分，sink 绑 time.Time 必坏）、`time→DATE`（规范串无法写入）、`bit→RAW`（L10/L11 已验证 NUMBER 才能往返）、`json` 无映射（L8c 已验证 CLOB）。**ddl-sync 生成的表就是数据管道随后要写的表**——映射必须以本项目 dialect 为唯一权威，否则 DDL 同步「成功」之日就是全量加载失败之时。
3. **t2o 自身缺陷**（侦察核实）：FK/CHECK ALTER 带 MySQL 反引号（ddl.go:216/229）、`GenSchemaName/GenTableName` 大小写模式 copy-paste bug（rule.go:605/630）、JSON 无映射、分区 DDL 原样透传 MySQL 语法（Oracle 非法）、表达式索引不渲染；且全目录 0 测试。

**移植内容**（t2o 的工程价值，吸收进本项目）：
- charset/collation 兼容预检框架（`PreCheckCompatibility`）+ 兼容项记录结构（表级/列级、`PreCheckType=DATA_TYPE|CHARSET|COLLATION`、字段对齐 `errCompatibilityTable/Column`）
- 默认值转换：字符串 `''` doubling、`CURRENT_TIMESTAMP`/函数默认直通、`NULLSTRING` 哨兵→NULL
- `COMMENT ON TABLE/COLUMN` DDL；标识符大小写三态（origin/lower/upper，修掉 copy-paste bug）
- 报告形态：兼容项与失败分离、per-table 结果、FixedDDL 重跑（线上 base64 语义）

License：transferdb 为 Apache-2.0，移植处注明出处。

## 1. 模块边界（tidb-full-init）

```
internal/ddlsync/
  introspect.go  # TiDB information_schema：列（含默认值/注释/charset）、PK/UK/FK/二级索引、表注释；
                 #  复用 internal/tidb SchemaFetcher（status API 真实列 ID 经验直接受益）
  convert.go     # 扩展 TableMeta → DDL 集：CREATE TABLE（类型走 dialect.MapType + NOT NULL/DEFAULT）
                 #  + PK + UK(约束) + 二级索引 + COMMENT ON；大小写三态
  compat.go      # 显式兼容项：ENUM/SET 列级、charset/collation 不兼容表级、FK/分区（一期只记录不执行）
  apply.go       # 逐表执行：默认 CREATE（ORA-00955 且 overwrite→DROP 重建）/ overwrite=强制 DROP+CREATE；
                 #  FixedDDL(base64) 覆盖生成结果；per-table 事务
  report.go      # 每表结果（status 三态 + errorCode + compat[]）
cmd/tidb-ddlsync/ # CLI：-dry-run 输出 SQL + 兼容项报告 / 直写模式（对齐 t2o 文件/直写双模式）
```

dialect 扩展：`RenderCreateTableFull`（列默认值/NULL/注释/UK/索引/表注释；分区 P2.5）。现 `RenderCreateTable` 保持为数据管道路径不动（行为兼容）。

## 2. 任务类型与编排

- console `JobType=ddl-sync`；作为 M3 JobManager 骨架落地的**第一个真实任务类型**
- payload：`{tidbDatasourceId, oracleDatasourceId, targetSchema, tables[], overwrite}`；**overwrite 默认 false**，且只作用于 failed/skipped 表的重跑路径（DROP+CREATE 会丢数据，报告显式标注）
- 目标 Oracle 版本/extended（12.2）探测并存入 job
- **标识符上限（review 定稿）**：dialect 参数化 `maxIdentLen`——`New()` 默认 30（现网行为不动），ddlsync 按探测版本 ≥12.2 传 128；`validateIdentifier`/`pkConstraintName`/列名同改。数据管道 P2 接同一探测（#t12 语料含大量 >30 标识符，30 硬拒会让语料大面积不可用）
- 入口复用 `ValidateTable`（无 PK 拒绝、全 PK 列拒绝、标识符校验）——保证 DDL 产物表可被全量/增量任务直接接管

## 3. API 契约（对齐前端草案 + leader 六点修正）

- `POST /jobs`（type='ddl-sync'）：payload 如上
- `GET /jobs/{id}` → `ddlResults[]`：
  `{ table, targetTable, status: 'success'|'failed'|'skipped', ddl, errorCode?, errorText?, compat[] }`
  - compat 项：`{level:'table'|'column', column?, preCheckType:'DATA_TYPE'|'CHARSET'|'COLLATION'|'FK'|'PARTITION', sourceValue, targetValue, suggest}`（t2o 真实字段 + FK/PARTITION 扩展；FK/分区必须显式记录「目标未创建该对象」，不写裸 warning 文案）
  - ENUM/SET = **列级** compat（`ErrUnsupportedType` 已带列名），非整表标红
  - `errorCode` = 从执行错误解析 `ORA-\d+`（复用 oraclesink oraErrorCode 口径）
  - `ddl` 在 **failed 时亦必填**（#t10 终评 low 项，P1 已落地）：convert 期列级不兼容的表用 `SkipBadColumns` 模式生成基线——不兼容列跳过、引用该列的 PK/UK/索引/注释剔除、语句头部 `--` 注释标记中断点，供前端 FixedDDL 编辑器起稿（绝不直接执行）
  - `skipped` 仅两态：表已存在且 overwrite=false（ORA-00955）/ 表级 charset 不兼容
- `POST /jobs/{id}/ddl-rerun`：`{table, fixedDdl?}`——fixedDdl **base64**（TMS 线上语义）；只重跑 failed 表；幂等（重跑前该表按 overwrite 语义处理）
- `POST /oracle/ddl-preview`：契约已有（前端 client/mock），**后端实现列入 P2 清单**（当前无 Go handler），响应含 `compat[]`（同上结构）
- `/tidb/schemas`：复用

## 4. DB 变更

console 任务库（SQLite，M3 既定）：`jobs` + `ddl_results(job_id, table, status, ddl, error_code, error_text, compat_json, created_at)`；重跑 upsert 对应行。**不引入 tms meta DB**（列级自定义映射规则引擎二期；一期 FixedDDL 已可人工纠偏）。

## 5. 与既有 MIGRATE_STRUCTURE 的关系

tms 侧 `TASK_TYPE_MIGRATE_STRUCTURE=2`（Oracle→TiDB）**不动**；本特性只落 console（tidb-full-init），不回写 tms。方向互补：tms 管 Oracle→TiDB 迁移，console ddl-sync 管 TiDB→Oracle 回退准备（DDL）+ 回退数据链（全量/增量，已验收）。

## 6. 测试（#t9）

- L1 单测：convert 黄金文件（每类型一表：默认值/注释/UK/索引/大小写三态）；compat 断言（ENUM/SET 列级、charset 表级、FK/分区显式记录不执行）；默认值 quoting 边界（内嵌单引号/函数默认/NULL）；**一致性守卫（review #1，最关键）——ddl-sync 产物逐列与管道 `RenderCreateTable` 口径一致（同 MapType/精度/NULL/DEFAULT），golden/round-trip 断言防两条路径漂移**；标识符 30/128 参数化
- L2 集成：真 TiDB 建表 → 生成 → Oracle-free 执行 → `ALL_TAB_COLUMNS`/`ALL_CONSTRAINTS`/`ALL_IND_COLUMNS`/注释反读断言；ORA-955/overwrite 幂等与重跑；断点重启
- 全类型矩阵复用 R1 `types_matrix` 黄金 DDL → Oracle 执行 → 反读断言；错误路径 ORA 码覆盖
- 与 #t12 衔接：语料 schema 清洗后先过 ddl-sync，再跑全量→增量→校验端到端

## 7. 分期

- **P1**：introspect + convert + compat + CLI（预览/直写）+ L1/L2（#t8/#t9 主体）
- **P2**：M3 JobManager REST（/jobs ddl-sync，含 /oracle/ddl-preview 后端实现）+ 前端联调 + 数据管道标识符探测同改
- **P2.5（#t12 验收前必须落地，不无限递延）**：分区表 DDL 转换（RANGE/HASH→Oracle 语法；part_range/part_hash 语料等价性）——期间分区表产物与源不等价须在报告显式列出；FK 执行（依赖排序/级联，一期只报告为已知差异）
- 二期预留：列级映射规则引擎、FixedDDL 编辑 UI（契约字段已保留）
