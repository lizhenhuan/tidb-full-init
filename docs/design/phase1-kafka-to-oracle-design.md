# TiDB → Kafka → Oracle 数据回退链路 一期设计方案（含任务拆分）

> 版本：v0.2（2026-09-24，已按 mac-ds-flash-v4-1 评审意见修订：P0×5 + P1×5 决策已落入正文，见 D2/D3/D4/D5/D8/D10-D13）
> 范围：一期 = Web 控制台 + Kafka DML 数据消费写入 Oracle；为 DDL 同步、数据校验、其它异构库（SQL Server / DB2）预留扩展接口。

---

## 1. 现状盘点（基于现有代码）

现有 `tidb-full-init` 是纯 Go CLI（Go 1.25，仅 3 个直接依赖），已完成：

- **全量初始化**：走 TiKV 快照扫描（绕过 SQL 层），解码 rowcodec（新/旧格式，含 binary JSON），编码为 **Canal JSON** 发 Kafka；topic 默认按表拆分（`tidb-data-<db>-<table>`，单分区），消息 key = 主键值；分区表按 partition ID 扫描。
- **增量**：由外部 **TiCDC changefeed（canal-json 协议）** 接续，`--start-ts` = 全量 start-tso。增量消息带 `_tidb.commitTs`、UPDATE 带 `old` 字段。
- **没有的东西**（全部需要新建）：消费者（仅有 debug 用的 `cmd/kafka-consume`）、任何 sink/写库能力、checkpoint/offset 管理、Web/HTTP 层、Oracle 驱动、真正的集成测试（仅 1 个 encoder 单测，且部分已过时）。

**关键约束**：
1. 全量消息 `commitTs=0`、无顺序保证 → 消费侧必须**幂等写入**。
2. topic 单分区 → 一期按**表级并行**消费即可满足吞吐。
3. 消息 schema 统一为 `internal/encoder/canal_json.go` 的 `CanalMessage` —— 消费侧的解码目标就是它（增量是 TiCDC 的 canal-json，两者字段兼容）。

## 2. 业界调研结论

| 产品 | 可借鉴点 | 对本项目的落地 |
|---|---|---|
| **Apache SeaTunnel**（connector-jdbc） | Source/Sink 连接器抽象 + **Dialect 层**（类型转换、upsert SQL、DDL 方言）；Oracle sink 有 testcontainers 化的 ITCase（嵌入式 Kafka → Oracle 容器端到端断言）；类型映射矩阵测试 | 抄其 **Dialect 抽象**与 **ITCase 模式**：`testcontainers` 拉起 `gvenzl/oracle-free`，produce canal-json → 消费 → 断言 Oracle 行；类型映射矩阵测试照搬其 JdbcTypeConverter 测试思路 |
| **Flink CDC** | canal-json format 解码器（changelog 语义 INSERT/UPDATE/DELETE）；JDBC sink 的 upsert/幂等语义：**at-least-once + 幂等 upsert ≈ effectively exactly-once** | 一期采用同样的语义：kafka-go 手动 commit offset，**flush 成功后才 commit**；upsert 用 Oracle `MERGE`；DELETE 容忍目标行不存在。canal-json 解码器单测参照其 decodable golden-file 测试 |
| **TiCDC canal-json 协议** | 官方协议规范（`isDdl`、`old`、`_tidb.commitTs/watermarkTs`、类型呈现规则） | 作为消费侧解码的权威规范；DDL 分支（`isDdl=true`）一期解析后跳过，接口预留 |
| **DSG、英方（Info2）** 等商业同步软件 | 表级并行、断点续传、全量+增量比对校验、延迟监控、双活仲裁 | 不抄实现（闭源），只对齐**产品能力面**：断点续传 → checkpoint；校验 → 二期 Verifier；监控 → 任务详情指标 |

## 3. 一期总体架构

```
┌─────────────────────────── Web Console (React+TS+AntD5, go:embed 进单二进制) ───────────────────────────┐
│  数据源管理 │ 全量初始化任务 │ Kafka→Oracle 同步任务 │ 任务中心(状态/进度/位点/速率/日志)                │
└──────────────────────────────────────────── HTTP API (REST /api/v1) ────────────────────────────────────┘
┌──────────────────────────────────────────── Job Manager ────────────────────────────────────────────────┐
│  任务状态机: pending→running⇄paused→success/failed │ 持久化: SQLite │ 指标采集 │ 日志                      │
└──────────────┬──────────────────────────────────────────────────────────────────────────────────────────┘
┌──────────────┴─────────────┐  ┌───────────────────── Pipeline Runtime（每任务一组 goroutine）────────┐
│ 复用: tidb-full-init 全量扫描 │  │ Kafka Consumer (consumer group, 手动 commit) → Decoder(canal-json)   │
│ (改造为可被 Job Manager 调起) │  │   → Applier(幂等) → Sink 接口 → Oracle Sink (go-ora, MERGE 批量写)    │
└─────────────────────────────┘  │   Checkpoint Store (topic/partition offset, flush 后提交)              │
                                 │   预留: DDL Handler(stub) │ Verifier 接口(stub)                        │
                                 └───────────────────────────────────────────────────────────────────────┘
```

### 3.1 新增包结构（沿用现有 interface+mock 的注入习惯）

```
internal/
  api/          # HTTP server + REST handler + go:embed 前端产物
  job/          # JobManager、状态机、任务持久化(SQLite)、进度/指标
  consumer/     # kafka-go Reader 封装: consumer group、手动 commit、优雅退出
  pipeline/     # 编排: decode → applier → sink; 限速/批参数
  decoder/      # canal-json → 统一内部事件模型 RowEvent/DDLEvent
  applier/      # 幂等 DML 应用: 按 PK 聚合、乱序容忍(全量重放场景)
  sink/         # Sink 接口 + oraclesink/ + mocksink/(console)
  dialect/      # TargetDialect 接口: TypeMapping / RenderUpsert / RenderDelete / RenderCreateTable
                #   实现: oracle/(一期)  预留: sqlserver/ db2/
  checkpoint/   # offset 持久化接口 + SQLite 实现
  verify/       # Verifier 接口(二期实现, 一期只定义)
  mapping/      # (预留) 表/列名映射规则；一期标识符逻辑在 dialect/oracle 内
web/            # React 前端(前端同学负责)
```

### 3.2 关键设计决策

**D1. Oracle 驱动选纯 Go 的 `github.com/sijms/go-ora/v2`**：无 CGO、无 Oracle Instant Client 依赖，部署单二进制不破。批量写用 prepared statement + array binding。

**D2. 一致性语义 = at-least-once + 幂等 sink（effectively exactly-once）**：
- consumer 手动管理 offset，**Oracle 批量 flush 成功后才 commit offset**（Flink CDC JDBC sink 同款语义）；
- INSERT/UPDATE 统一走 Oracle `MERGE INTO`（按 PK upsert）；DELETE 目标行不存在不算错误。全量消息可重放、`commitTs=0` 无序，幂等是硬要求。
- **PK 变更语义（采用 Flink CDC）**：UPDATE 事件若 `old` 中 PK 值 ≠ `data` 中 PK 值，拆分为 **DELETE(旧 PK) + MERGE(新行)** 两条写操作；golden 测试与 ITCase 必覆盖。

**D3. 路由与并行（消息级路由，不假设 topic==表）**：
- 全量按表拆 topic，TiCDC 增量默认**单 topic 混流** → 消费侧一律按消息内 `database`+`table` 路由，topic 与表的对应关系仅作展示；（TiCDC 可配 dispatcher 按表拆 topic，但不依赖）
- **并行单位 = Kafka 分区**：分区内严格顺序处理，按表聚合，整批 flush 成功后再提交该分区 offset；
- 表级进度 = 各分区 applied offset 高水位派生。后续扩多分区时按 key hash 分区即可保持行级有序。

**D4. 类型映射 MySQL→Oracle（核心矩阵，单测全覆盖）**：

| MySQL/TiDB | Oracle | 说明 |
|---|---|---|
| tinyint/bool/boolean | NUMBER(3) | |
| smallint | NUMBER(5) | |
| mediumint | NUMBER(8) | |
| int/integer | NUMBER(10) | |
| bigint | NUMBER(19) | unsigned → NUMBER(20) |
| decimal(p,s)/numeric | NUMBER(p,s) | 超 38 精度截断告警 |
| float / double | BINARY_FLOAT / BINARY_DOUBLE | |
| bit | NUMBER | 位串按数值 |
| varchar(n)/char(n) | `VARCHAR2(n CHAR)`（n×4≤4000），否则 CLOB | session NLS 走 utf8 |
| text 系 | CLOB | |
| varbinary(n) | RAW(n)（canal-json 中 Base64 解回） | |
| blob 系 | BLOB | |
| date | DATE | |
| datetime/timestamp | TIMESTAMP / TIMESTAMP | 微秒保留 |
| time | **VARCHAR2(18)**，存规范串 `[-]HH:MM:SS[.ffffff]` | 无损且驱动安全；INTERVAL 留二期可选 |
| year | NUMBER(4) | |
| json | CLOB（原文） | |
| **enum / set** | **一期硬拒绝**（任务创建时校验，报清晰错误） | 现有全量编码为序号/位掩码，写入无意义；二期用 TiDB schema 还原后写 VARCHAR2 |

配套：标识符映射（Oracle 保留字强制引号 + 注入对抗、**>30 字符一期直接拒绝**（M1.5 review P1#6 定稿：拒绝比截断安全，错误经 UI 透出给用户）、大小写策略）。一期实现集中在 `dialect/oracle` 内（`QuoteIdentifier`/`ValidateTable`），独立 `mapping` 包留待多方言时再抽。

**D5. 目标表准备（一期，schema 以 TiDB 为准）**：任务启动前用 `tidb.SchemaFetcher` 取权威表结构（带 PK，空表也能建），canal-json 的 `mysqlType`/`sqlType` 仅兜底。经 `dialect.RenderCreateTable` 生成 Oracle DDL，页面**预览确认**后执行；Oracle 无 IF NOT EXISTS → 先查 `ALL_TABLES`（OWNER=目标 schema，支持 TargetSchema 映射）再 CREATE。运行期 DDL 同步（TiCDC `isDdl` 消息应用）二期做，一期 decoder 识别后跳过并计数。

**D6. 任务持久化用 SQLite**（单文件、零运维）：任务定义/状态、checkpoint offsets、运行指标快照。放 `internal/job/store.go`。

**D7. 全量初始化服务化**：把 `cmd/tidb-full-init/main.go` 的编排逻辑抽成 `internal/initrunner`（输入=现有 `config.Config`），CLI 与 JobManager 共用，CLI 行为不变。

**D8. 扩展接口（一期冻结签名，字段为评审强制最小集）**：
```go
// internal/model — 共享事件/元数据模型（避免包循环）
type RowEvent struct {
    Op        OpType            // Insert/Update/Delete/DDL
    Database  string
    Table     string
    PKNames   []string
    Before    map[string]string // 变更前镜像（canal-json old）
    After     map[string]string // 变更后镜像（canal-json data）
    SQLType   map[string]int    // java.sql.Types
    MySQLType map[string]string
    CommitTs  uint64
    Topic     string            // 来源消息 Kafka 坐标
    Partition int
    Offset    int64
}
type TableMeta struct {
    Database string
    // TargetSchema 目标端 schema/OWNER；TiDB 库名 ≠ Oracle 用户是常态，
    // 由任务级映射配置填充，为空时回落 Database（M1.5 P0#4）。
    TargetSchema string
    Table        string
    Columns      []ColumnMeta
    PKNames      []string
}
type ColumnMeta struct {
    Name      string
    MySQLType string
    SQLType   int // java.sql.Types code
    IsPK      bool
}

// internal/sink（M1.5 P0#1 定稿失败契约）
type Sink interface {
    Prepare(ctx context.Context, meta TableMeta) error
    WriteBatch(ctx context.Context, meta TableMeta, events []RowEvent) (FlushResult, error)
    Close() error
}
type FlushResult struct {
    Applied int          // 本事务真正提交的行数（数据错误行跳过不计）
    Failed  []RowFailure // 行级失败明细
}
type RowFailure struct {
    Topic     string
    Partition int
    Offset    int64
    Table     string
    Reason    string
    Raw       []byte // 原始消息体（applier 填充），供 DLQ 落库与前端死信列表展示
    // Retryable 保留字段：RowFailure 一律为行级数据错误（Retryable=false，
    // 写 DLQ 并前进 offset）；可重试错误走整批 error 路径，不产生 RowFailure
    Retryable bool
}
// WriteBatch 失败契约（M1.5 P0#1 定稿 + 跟进复审 P1#1 三态化，真库实测确认）：
//   - retryable（网络/锁/ctx 取消，sink.IsRetryable(err)=true）→ 整体回滚并
//     返回错误；语义是"本批结果未知"（ctx 取消下事务可能未完全回滚，真库
//     实测有部分行已提交的样例），applier 必须整批重放，绝不部分续传，
//     error+Applied=0 不代表什么都没写；正确性由幂等 MERGE/DELETE +
//     flush-后-commit 位点保证；ctx 取消不是致命错误；
//   - row-data（行级数据错误，sink.IsRowData(err)=true，白名单 ORA 码
//     00001/01722/01400/01840/01858/01861/12899）→ 记 RowFailure
//     （Retryable=false）跳过该行，其余行照常提交，applier 写 DLQ 后前进 offset；
//   - fatal（其余一切，含 ORA-00942/00904/01031 等配置/权限错误与无 ORA 码
//     的未知驱动错误）→ 返回错误，applier 中止任务、不前进 offset，人工排查
//     （避免把配置错误伪装成"成功+死信"静默丢数据）；
//   - Applied 只统计真正提交的行；返回错误时 Applied/Failed 归零（已回滚，
//     重放时重新推导）。

// internal/dialect
type TargetDialect interface {
    Name() string // "oracle" / "sqlserver" / "db2"
    QuoteIdentifier(name string) string
    RenderCreateTable(meta TableMeta) (string, error)
    RenderUpsert(meta TableMeta) (string, error)
    RenderDelete(meta TableMeta) (string, error)
    // 任务创建前的表级校验（一期拒绝 ENUM/SET、无主键、超长标识符等）
    ValidateTable(meta TableMeta) error
    // 连接级准备（如 session 字符集），由 sink 打开后调用
    Prepare(ctx context.Context) error
    // 预留 schema evolution（二期实现，先定义不实现）
    RenderAlterTable(meta TableMeta, change SchemaChange) (string, error)
}

// internal/consumer（D3/D10：手动分配分区，不用 group rebalance）
type TopicPartition struct { Topic string; Partition int }
type Message struct { TopicPartition; Offset int64; Key, Value []byte }
type Consumer interface {
    Assign(ctx context.Context, partitions []TopicPartition) error
    Fetch(ctx context.Context) (Message, error)
    Commit(ctx context.Context, msg Message) error // best-effort，仅观测用
    HighWatermark(ctx context.Context, tp TopicPartition) (int64, error) // lag 观测
    Close() error
}

// internal/checkpoint — SQLite 为唯一权威位点
// 位点语义（M1.5 P1#9 定稿）：offset 一律指"已应用的最后一个 offset"（last applied），
// 下一条待消费 = offset+1；无记录返回 -1 表示从头开始。
type OffsetStore interface {
    Load(ctx context.Context, tp TopicPartition) (int64, error)
    Save(ctx context.Context, tp TopicPartition, offset int64) error
}

// internal/verify — 二期
type Verifier interface { Verify(ctx context.Context, spec VerifySpec) (VerifyReport, error) }
```
SQL Server / DB2 即新增 `dialect/` 实现 + 驱动接入；DDL 同步即启用 decoder 的 DDLEvent 分支 + dialect 的 DDL 渲染/翻译。

**D9. HTTP API 契约（与前端方案对齐定稿）**：采用前端同学草案，补充两点——
- `GET /api/v1/kafka/topics?datasource=...` 增加 `partitionCount`、`totalLag`；
- `POST /api/v1/jobs` body 中 `type: full-init | dml-sync`（枚举已预留 `ddl-sync`/`data-verify`），`targetType: oracle`（预留 `sqlserver`/`db2`）；
- 任务详情含 `checkpoints`（各 topic/partition 位点）与 `metrics`（消费/写入速率、跳过的 DDL 计数、错误计数）。

**D10. 位点管理（评审 P1-7）**：一期**手动分区分配**（不用 consumer group 自动 rebalance）；**SQLite 为唯一权威位点**，Kafka offset 提交仅用于观测 lag。顺序：flush → Save(SQLite) → best-effort commit Kafka。

**D11. 批次与 DLQ（评审 P1-8）**：一批 = 单表 + 单分区窗口，**一个 Oracle 事务**，全成才 commit。失败分类：可重试（网络/ORA-00054 锁等）原地指数退避重试；数据错误（ORA-00001/01722/12899 等）写 `kafka_dlq` 表、**前进 offset** 并计数。DLQ 表结构 M1 定稿（topic/partition/offset/table/reason/raw payload/时间）。

**D12. 安全（评审 P1-9）**：API 绑可配置地址（默认 127.0.0.1）+ 可选 token；凭据 AES-GCM 加密落 SQLite（密钥来自环境变量，缺失则拒绝明文落盘）；禁止日志打印凭据；标识符严格引号 + 保留字/注入对抗测试。

**D13. 列 ID 缺陷防护（评审 P1-10）**：现有 `schema.go` 用 ORDINAL_POSITION 当列 ID，DROP COLUMN 后会静默丢列。M1 加启动校验（ordinal 与真实列 ID 不一致即拒绝并提示），权威列 ID 修复排 M2 校验阶段前完成。

## 4. 测试策略（本项目的第一优先级 —— 稳定性优先）

> 定位：这不是 demo，是客户核心场景的 TiDB→Oracle 回退链路。**测试用例的充足程度决定项目成败**，开发和测试同批提交，无测试的 PR 不合入。分五层：

### L1 单元测试（不依赖容器，每个 PR 必跑）
1. **canal-json 解码 golden 测试（移植 Flink CDC 思路）**：TiCDC 官方文档全部样例消息 + 联调期抓取的**真实消息**入库 `testdata/*.json`，decoder 逐条断言：字段/类型/null 与空字符串区别/时间进制/unsigned/`old` 字段语义/`_tidb.commitTs`。
2. **类型映射矩阵测试（照搬 SeaTunnel JdbcTypeConverter 测试思路）**：MySQL 全类型 × 边界值（无符号极值、超长 varchar、decimal 38 精度、时区边界、binary Base64 回转）→ 映射 → DDL 生成断言（容器内写→读回归 L2 集成测试），一类型一组 case。
3. **幂等语义单测**：mocksink 重放同一批事件 N 次、乱序、UPDATE 先于 INSERT 到达、DELETE 不存在的行。
4. **go 原生 fuzz**：decoder 对任意 JSON 输入不 panic、不死循环（`go test -fuzz`，进 CI 定期跑）。
5. 顺手修复：`internal/encoder` 两个已过时的断言（Base64 化后未更新）。

### L2 集成测试（testcontainers，移植 SeaTunnel ITCase 模式，CI 每日 + 发版前必跑）
`testcontainers-go` 拉起 `gvenzl/oracle-free` + Kafka 容器，端到端：构造 canal-json（**复用现有 `encoder` 生产消息，全量格式零成本**）→ 写 Kafka → pipeline 消费 → 断言 Oracle。用例族：
- 全量批量导入 → 行数+抽样值断言；
- 增量 INSERT/UPDATE/DELETE 序列 → 终态断言；
- **全量重放（offset 回零再消费一遍）→ 终态不变（幂等验证）**；
- 全类型端到端（L2 矩阵的容器版）；
- 空表、无主键表（显式拒绝并报清晰错误）、大宽表(200 列)、大行(8MB blob)；
- Oracle 版本矩阵：19c / 23ai 容器各跑一遍核心用例。

### L3 故障注入 / 混沌测试（稳定性核心，回退链路必须扛住）
| 故障 | 断言 |
|---|---|
| 消费中途 kill -9 进程，重启 | 从 checkpoint 恢复，无丢数、无重复副作用 |
| 手动 seek 重放任意 offset 段 | 终态幂等不变 |
| Kafka broker 重启 / 不可用 30s | 自动重连，任务不失败，恢复后追平 |
| Oracle 网络抖动 / 闪断 / 慢（tc 注入延迟） | 退避重试、背压不 OOM、不丢 offset |
| flush 失败后 commit 未发生 | 重启后重写，终态正确 |
| 毒消息（畸形 JSON、未知类型） | 进死信记录（表+partition+offset+原文），任务继续跑，页面可见 |
| 全量+增量交界重复/乱序消息 | 终态正确 |

### L4 长稳 / 压力测试（发版准入）
- 24h soak：百万级全量 + 持续增量，监控内存/goroutine/句柄无泄漏，lag 能追平；
- 吞吐基线：记录 rows/s、batch 大小、延迟分位数，作为回归基准。

### L5 一致性校验工具（测试基础设施，同时是二期 Verifier 的雏形）
- 通用比对工具：TiDB `SELECT *`（现有 `cmd/tidb-verify` 可直接改造）vs Oracle 全量 count + 分块 checksum + 抽样比对，所有 L2/L4 用例的断言都基于它，而不是手写 SQL。

**工程约定**：外部依赖全部 interface+mock（沿用仓库习惯），单测不碰容器；集成测试打 build tag `integration`；测试数据/夹具与 SeaTunnel 一样集中管理可复用。

## 5. 任务拆分（一期）

里程碑按依赖排序；🅐=cc_glm-5.3-pro（后端/整体）、🅕=cc_glm-5-3-pro-前端开发、🅛=mac-ds-flash-v4-1（review/把关）。

### M1 基础设施与核心接口（先行，纯单测）
- T1.0 🅐 按评审修订 design doc（D2 PK 变更 / D3 分区路由 / D4 类型矩阵 / D8 冻结签名 / D10-D13）✅
- T1.1 🅐 项目脚手架：新包骨架、go-ora 依赖引入、`integration` build tag 约定、**CI 流水线（M1 即建）**
- T1.2 🅐 `decoder`：canal-json → RowEvent/DDLEvent + golden-file 单测（TiCDC 样例，含 PK 变更 UPDATE、old 语义、DDL 跳过）
- T1.3 🅐 `dialect/oracle` + `mapping`：类型映射矩阵（含 enum/set/bit/bool/mediumint 明确处理）、DDL/MERGE/delete 渲染、保留字/注入对抗 + 矩阵单测
- T1.4 🅐 `sink`/`consumer`/`checkpoint` 可编译接口 + `oraclesink`（MERGE 批量写、array binding）+ `mocksink` + DLQ 表结构定稿
- T1.5 🅐 `schema.go` 列 ID 启动校验（D13）
- T1.6 🅛 review M1（M1.5：接口冻结审查，影响后面所有扩展）

### M2 消费链路（核心价值，端到端可演示）
- T2.1 🅐 `consumer`：consumer group、手动 commit、优雅退出、限速
- T2.2 🅐 `applier` + `pipeline`：批聚合、幂等应用、错误分类（可重试/需人工）
- T2.3 🅐 `checkpoint`：SQLite offset 存储，flush-后-commit 语义
- T2.4 🅐 一致性比对工具（改造 `cmd/tidb-verify`）：TiDB vs Oracle count/分块 checksum/抽样 —— 所有集成与长稳测试的断言基础，二期 Verifier 雏形
- T2.5 🅐 端到端集成测试（oracle-free + kafka 容器）：全量导入、增量 UPDATE/DELETE、全量重放幂等、全类型矩阵、大宽表/大行/无主键拒绝（=移植 SeaTunnel ITCase 的主力）
- T2.6 🅐 故障注入测试：kill -9 恢复、offset 重放、broker/Oracle 闪断重连、慢 Oracle 背压、毒消息死信
- T2.7 🅛 review M2 + 用 mock 数据人工跑一遍

### M3 服务化（可被页面驱动）
- T3.1 🅐 `initrunner`：抽取 CLI 编排逻辑，行为不变
- T3.2 🅐 `job`：JobManager、状态机、SQLite 持久化、指标/日志
- T3.3 🅐 `api`：REST 全量接口（按定稿契约）+ 连接测试（TiDB/Kafka/Oracle）
- T3.4 🅐 API 单测 + OpenAPI 描述文件（给前端对齐用）

### M4 Web 控制台（🅕 主导，M3 契约冻结后开工，mock 先行可与 M3 并行）
- T4.1 🅕 页面骨架 + 路由 + 数据源管理页（含连接测试）
- T4.2 🅕 全量初始化任务页（库表树、参数、topic 预览）
- T4.3 🅕 Kafka→Oracle 同步任务页（topic 多选+lag、映射预览、启停）
- T4.4 🅕 任务中心（列表/详情/进度/位点/日志，轮询 5s）
- T4.5 🅐+🅕 go:embed 集成、联调
- T4.6 🅛 前端 review

### M5 收尾（发版准入）
- T5.1 🅐 24h 长稳测试：百万级全量 + 持续增量，资源泄漏监控 + lag 追平；吞吐基线报告（作为后续回归基准）
- T5.2 🅐 整体联调：真实 TiDB(或 mock)→Kafka→Oracle 全流程页面驱动演示
- T5.3 🅐 修 encoder 过时单测、补 README/部署文档、fuzz 跑一轮入库
- T5.4 🅛 一期验收 review（重点看扩展接口是否守住：DDL/校验/异构库加实现不动主干；测试覆盖是否达标）

### 二期+ 预留（不在本期排期，接口已留）
- DDL 同步（decoder DDLEvent → dialect DDL 翻译/ APPLY）
- 数据校验（Verifier：全量 count/checksum + 增量比对，参照 DSG/英方能力面）
- SQL Server / DB2 dialect + 各自 ITCase
- 多分区 topic、行级有序扩容方案

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| go-ora 成熟度/性能 | M1 先做 array binding 基准测试；不行则评估 godror（需 CGO，最后手段） |
| canal-json 与 TiCDC 细节差异（时间进制、null 表示、binlog 类型） | decoder golden 测试以 TiCDC 官方样例为准；联调期抓真实消息补充 |
| Oracle 标识符 30 字符/保留字冲突 | 一期直接拒绝 + 建表预览人工确认（constraint 名超长用短哈希） |
| 单分区 topic 吞吐上限 | 一期表级并行；预留 key-hash 多分区方案 |
| 大事务/大批次内存 | 批参数上限 + 背压（consumer 逐批处理，不预取爆） |
| 全量与增量交界重复/乱序 | 幂等 MERGE 天然覆盖；二期校验兜底 |

## 7. 参考

- TiCDC Canal-JSON 协议规范（github.com/pingcap/tiflow docs）
- Apache SeaTunnel connector-jdbc（Dialect 抽象 / Oracle ITCase）
- Apache Flink CDC 文档：canal-json format、JDBC sink upsert/exactly-once 语义
