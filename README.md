# tidb-full-init

TiDB 全量数据初始化工具。通过 TiKV TxnKV API 直接读取 TiDB 历史数据快照，解码 TiDB rowcodec 格式，以 Canal JSON 格式发送到 Kafka，用于与 TiCDC 增量同步配合实现全量 + 增量数据对齐。

## 特性

- 通过 TiKV PD 接口获取快照时间戳，支持指定 `--start-tso` 与 TiCDC 起始 TSO 对齐
- 直接读取 TiKV 原始 KV 数据，不经过 TiDB SQL 层扫描，对线上业务无影响
- 支持 TiDB rowcodec 新格式和旧格式的自动识别与解码
- 支持所有 MySQL 字段类型：整数、浮点、Decimal、字符串、Text、Blob、日期时间、Year、Time、JSON、Bit、Enum、Set
- 完整的 TiDB JSON 二进制格式解码：Object、Array、Literal、Int16/32/64、Uint16/32/64、Float64、String、Opaque、Date、Datetime、Timestamp、Duration
- BLOB/BINARY/VARBINARY 字段自动 Base64 编码（兼容 Flink Canal JSON 格式）
- 输出 Canal JSON 格式，兼容下游 Canal/Debezium 消费端
- TiDB 连接预检：启动时验证用户名密码和 SELECT 权限，通过后才创建 TiKV/Kafka 连接
- Kafka Topic 自动创建（不存在时自动创建，单 Partition）
- 多分区并行扫描，可配置并行度

## 编译

```bash
go build -o tidb-full-init ./cmd/tidb-full-init/
go build -o kafka-consume ./cmd/kafka-consume/
go build -o tidb-verify ./cmd/tidb-verify/
```

## 使用方式

### 命令行参数

```bash
./tidb-full-init \
  -pd-addresses "127.0.0.1:2379" \
  -tidb-host "127.0.0.1" \
  -tidb-port "4000" \
  -username "root" \
  -password "your_password" \
  -database "mydb" \
  -tables "orders,users,products" \
  -kafka-bootstrap "127.0.0.1:9092" \
  -kafka-topic-prefix "tidb-data-" \
  -parallelism 4 \
  -start-tso 0
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `-config` | string | | JSON 配置文件路径（指定后其他参数忽略） |
| `-pd-addresses` | string | `127.0.0.1:2379` | TiDB PD 地址 |
| `-tidb-host` | string | `127.0.0.1` | TiDB SQL 连接地址 |
| `-tidb-port` | string | `4000` | TiDB SQL 连接端口 |
| `-username` | string | `root` | TiDB 用户名（必填） |
| `-password` | string | | TiDB 密码（必填） |
| `-database` | string | `test` | 数据库名 |
| `-tables` | string | | 表名，逗号分隔（默认 `orders`） |
| `-kafka-bootstrap` | string | `127.0.0.1:9092` | Kafka Broker 地址 |
| `-kafka-topic-prefix` | string | `tidb-data-` | Kafka Topic 前缀（Topic = prefix + db + "-" + table） |
| `-kafka-topic` | string | | Kafka Topic 全名（固定Topic，所有表数据发送到同一个Topic，设置后覆盖 -kafka-topic-prefix） |
| `-parallelism` | int | `4` | 每张表的并行扫描协程数 |
| `-start-tso` | uint64 | `0` | 快照起始 TSO（0=自动从 PD 获取） |
| `-mock` | bool | `false` | Mock 模式（不连接真实 TiKV/Kafka，用于调试） |

### JSON 配置文件

```json
{
  "pd-addresses": "127.0.0.1:2379",
  "tidb-host": "127.0.0.1",
  "tidb-port": "4000",
  "username": "root",
  "password": "your_password",
  "kafka-bootstrap-servers": "127.0.0.1:9092",
  "kafka-topic-prefix": "tidb-data-",
  "kafka-topic": "",
  "tables": [
    {
      "database": "mydb",
      "tables": ["orders", "users"],
      "topic": "custom-topic-orders",
      "parallelism": 4
    }
  ]
}
```

```bash
./tidb-full-init -config config.json
```

### 执行流程

1. 解析配置参数
2. 连接 TiDB SQL 验证用户名密码
3. 查询当前用户权限（`SHOW GRANTS`）
4. 逐表验证 SELECT 权限（`SELECT * FROM db.table LIMIT 0`）
5. 连接 TiKV PD，获取快照时间戳
6. 从 TiDB `INFORMATION_SCHEMA` 获取表结构和 Table ID
7. 创建 Kafka Topic（不存在则自动创建）
8. 并行扫描 TiKV 指定 Table ID 的 Key Range
9. 解码 TiDB rowcodec 格式，编码为 Canal JSON 发送到 Kafka

### 与 TiCDC 配合使用

```bash
# 1. 获取当前 TSO（或使用 --start-tso 指定）
# 2. 执行全量初始化
./tidb-full-init -start-tso 451234567890123456 \
  -tidb-host "tidb.example.com" -password "xxx" \
  -database "mydb" -tables "orders" \
  -pd-addresses "pd.example.com:2379" \
  -kafka-bootstrap "kafka.example.com:9092"

# 3. 启动 TiCDC 从相同 TSO 开始增量同步
# tiup cdc changefeed create --start-ts=451234567890123456 ...
```

## 输出格式

每行数据以 Canal JSON INSERT 消息发送到 Kafka：

```json
{
  "id": 0,
  "database": "mydb",
  "table": "orders",
  "pkNames": ["id"],
  "isDdl": false,
  "type": "INSERT",
  "es": 1716123456789,
  "ts": 1716123456789,
  "sql": "",
  "sqlType": {"id": 4, "amount": 3, "name": 12},
  "mysqlType": {"id": "int", "amount": "decimal(10,2)", "name": "varchar(100)"},
  "data": [{"id": "1", "amount": "99.99", "name": "Alice"}],
  "old": null
}
```

## 数据类型处理

| MySQL 类型 | Canal JSON 输出格式 |
|---|---|
| TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT | 十进制字符串（如 `"42"`） |
| TINYINT UNSIGNED, SMALLINT UNSIGNED, ... | 十进制字符串（无符号范围） |
| FLOAT, DOUBLE | Go `%g` 格式（如 `"3.14"`, `"1.41421356237"`） |
| DECIMAL | 精确十进制字符串（如 `"-1234567.89"`） |
| CHAR, VARCHAR | UTF-8 原始字符串 |
| TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT | UTF-8 原始字符串 |
| BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB | **Base64 编码**（如 `"vu/O/g=="`） |
| BINARY, VARBINARY | **Base64 编码** |
| DATE | `"YYYY-MM-DD"` |
| DATETIME, TIMESTAMP | `"YYYY-MM-DD HH:MM:SS"` 或 `"YYYY-MM-DD HH:MM:SS.ffffff"` |
| TIME | `"HH:MM:SS"` 或 `"-HH:MM:SS"`（负数时间） |
| YEAR | 四位年份字符串（如 `"2024"`） |
| JSON | 标准 JSON 字符串（解码 TiDB JSON 二进制格式） |
| BIT | 十进制字符串（如 `"255"`） |
| ENUM | 索引值十进制字符串（如 `"1"`, `"2"`） |
| SET | 位掩码十进制字符串（如 `"5"`） |

## 辅助工具

### kafka-consume

Kafka 消费工具，用于验证发送到 Kafka 的数据：

```bash
./kafka-consume tidb-data-mydb-orders 127.0.0.1:9092
```

### tidb-verify

SQL 方式验证工具，通过 `SELECT *` 读取 TiDB 数据并编码为 Canal JSON 输出到文件或标准输出：

```bash
./tidb-verify -addr "127.0.0.1:4000" -database "mydb" -table "orders" -username root -password xxx
```

## 项目结构

```
cmd/
  tidb-full-init/     # 主工具：全量快照扫描 → Kafka
  kafka-consume/      # Kafka 消费验证工具
  tidb-verify/        # SQL 方式数据导出验证工具
  debug/              # TiKV 行编码调试工具
internal/
  config/             # 配置加载与校验
  encoder/            # Canal JSON 消息编码
  kafka/              # Kafka 生产者（真实 + Console Mock）
  snapshot/           # TiKV 快照扫描器 + Rowcodec 解码器
  tidb/               # TiDB SQL Schema 获取 + 连接验证
  tikv/               # TiKV 客户端抽象（生产 + Mock）
```

## 依赖

- Go 1.25+
- [tikv/client-go](https://github.com/tikv/client-go) — TiKV PD/TxnKV 客户端
- [segmentio/kafka-go](https://github.com/segmentio/kafka-go) — Kafka Go 客户端
- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) — MySQL 驱动（Schema 获取 + 权限验证）
