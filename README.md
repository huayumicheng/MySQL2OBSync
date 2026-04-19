# MySQL2OBSync - MySQL to OceanBase(MySQL) Sync & Validate Tool

一个用于 **MySQL → OceanBase（MySQL 模式）** 的全量同步与校验工具（Go 实现），流程包含：
- 同步表结构（从源端拉取 `SHOW CREATE TABLE`，在目标端创建）
- 同步数据（多表并发 + 单表分段并发/顺序模式 + 批量写入）
- 数据校验（行数对比 + 抽样对比；无 PK/UK 表退化为 Min/Max 简化校验）

## 目录结构

```
MySQL2OBSync-main/
├── cmd/sync/                 # 可执行入口
├── internal/
│   ├── config/               # 配置解析（YAML + 环境变量）
│   ├── database/             # MySQL 连接与通用操作（count/truncate/pk range/retry）
│   ├── schema/               # schema 同步（SHOW CREATE TABLE）、结构校验
│   ├── sync/                 # 同步引擎（并发、分段、批量写入）
│   ├── compare/              # 数据对比（count + sample + no pk/uk）
│   ├── monitor/              # 进度统计输出
│   └── logger/               # 日志（stdout + 文件）
└── config/example.yaml       # 配置示例
```

## 编译

在项目根目录执行：

```bash
go mod download
go build -o mysql2ob-sync ./cmd/sync
```

## 运行示例

### 1) 使用配置文件同步（结构 + 数据 + 自动对比）

```bash
./mysql2ob-sync -c config.yaml
```

### 2) 只同步表结构（从源端拉 DDL 创建到目标端）

```bash
./mysql2ob-sync -c config.yaml --create-schema
```

仅生成脚本不执行：

```bash
./mysql2ob-sync -c config.yaml --create-schema --dry-run
```

生成建表脚本到文件：

```bash
./mysql2ob-sync -c config.yaml --create-schema --gen-script create_tables.sql
```

### 3) 只同步指定表

```bash
./mysql2ob-sync -c config.yaml --tables users,orders
```

### 4) 只做数据校验

```bash
./mysql2ob-sync -c config.yaml --compare-only
```

只比行数：

```bash
./mysql2ob-sync -c config.yaml --compare-only --count-only
```

### 5) 校验源端与目标端表结构

```bash
./mysql2ob-sync -c config.yaml --validate-schema
```

## 参数说明（CLI）

- `-c`：配置文件路径（默认 `config.yaml`）
- `--log-dir`：日志目录（默认可执行文件所在目录）
- `--tables`：指定要同步/对比的表（逗号分隔）。未指定则自动发现源库全部用户表
- `--table-workers`：覆盖配置中的 `sync.table_workers`
- `--create-schema`：仅同步表结构（不同步数据）
- `--dry-run`：配合 `--create-schema`，只输出/生成 DDL，不执行
- `--gen-script`：配合 `--create-schema`，把 DDL 生成到指定文件
- `--validate-schema`：对比源端与目标端的表结构差异
- `--compare-only`：仅做数据对比
- `--count-only`：对比时只比行数（跳过抽样内容对比）
- `-v`：显示版本

## 配置文件说明（config.yaml）

可以从 [config/example.yaml](file:///d:/shell_scripts/MySQL2OBSync-main/config/example.yaml) 复制修改：

```bash
cp config/example.yaml config.yaml
```

### source / target

两端都是 MySQL 协议（目标端为 OceanBase MySQL 模式）：

- `host`/`port`/`database`/`username`/`password`
- `params`：追加 DSN 参数（默认已设置 `charset=utf8mb4, parseTime=true, loc=UTC`）
- `max_open_conns`/`max_idle_conns`/`conn_max_lifetime`：连接池配置

支持环境变量：

```yaml
password: "${SOURCE_DB_PASSWORD}"
```

### sync

- `table_workers`：并发同步表数量（表级并发）
- `workers`：单表分段并发度（reader 数量；writer 使用 `workers/2`）
- `read_buffer`：reader 单次发送 batch 行数
- `channel_buffer`：reader→writer 通道容量（影响背压与内存上界）
- `batch_size`：写入批次行数（顺序模式）/ writer 聚合行数；实际写入会再按 MySQL 参数上限拆分
- `max_retries`/`retry_interval`：写入失败重试

### tables

若不配置 `tables`，会自动同步源库当前 database 下的全部用户表。

表项字段：
- `source`：源表名（支持 `db.table` 或 `table`，建议与连接的 database 一致）
- `target`：目标表名
- `truncate_before_sync`：同步前是否清空目标表
- `split_column`：分段字段（默认使用主键第一列）。仅当其为可扫描为整数的字段时才启用范围分段
- `where`：额外过滤条件（直接拼到 SQL，需自行保证安全性与正确性）

### compare

- `auto_compare`：同步完成后自动执行对比
- `compare_workers`：对比并发度
- `sample_rate`：抽样率（默认 1%）
- `max_sample_rows`：单表最多抽样行数上限（默认 10000；设为 0 表示不限制）
- `count_only`：只比行数

### schema

- `strip_auto_increment`：同步 DDL 时移除表尾部的 `AUTO_INCREMENT=xxx`（避免迁移时把历史自增值固化）

## 说明与限制

- 当前版本为全量同步，不支持断点续传。
- 抽样对比在大表上使用 `ORDER BY RAND()` 可能较慢，如需更高性能抽样，可按主键范围抽样进行二次优化。

