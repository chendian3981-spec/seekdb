# pipe 分支开发记录

基线：官方 `2026-competition`，`5bce9e06d1281347ad61344e4b01fab9158c58a2`。
开发分支：`pipe/seekdb-solutions`。

## 赛题一：AI 函数增强

已实现 Prompt 的 JSON/LOB 参数、嵌套和重复占位符；Completion 的
NULL/JSON 配置与响应校验；Ollama Endpoint 与原生非流式协议；Embedding
的跨行批量、NULL 传播、响应索引恢复和向量校验；Rerank 的分批、全局
索引、对象 ID 保留与稳定排序。

本地验证：WSL Ubuntu 22.04，官方依赖和 Rust 1.98.1，Clang 17，
`bash build.sh release`、`make -C build_release -j8 seekdb` 成功。
连接该产物启动的隔离实例，运行以下测试，6 项全部通过：

```bash
python3 unittest/sql/engine/expr/test_ai_function_integration.py
```

最终回归补充了修改 Ollama Provider 后继续调用的覆盖，并通过官方
`ai_function_enhancement` 与 `ai_prompt` 用例（使用本地 HTTP Mock，原始
SQL 和预期结果不变）。修复了 Endpoint 更新时空密钥被保存为 NULL 的兼容问题。

测试仅调用临时本地 HTTP Mock，不需要真实模型凭据。连接参数可以通过
`SEEKDB_HOST`、`SEEKDB_PORT`、`SEEKDB_USER`、`SEEKDB_PASSWORD` 指定。

这属于本地回归结果，尚未推送比赛仓库或获得官方评测分数。

## 赛题二：文件即数据库

已实现 `read_csv`、`read_json` / `read_ndjson`、`read_parquet`、`glob`、
`DESCRIBE SELECT` 和 CSV `COPY`。文件扫描接入原生 SQL 执行计划，支持
投影、筛选、排序、聚合、LIMIT 和跨格式连接。默认关闭的 `enable_file_sql`
控制功能入口，并兼容缺少该变量元数据的旧实例。

CSV / JSONL 进行完整类型推断后逐行读取；Parquet 使用 Apache Parquet Rust
读取器，支持平铺标量列、列投影、多个行组和无列投影的 footer COUNT。
路径受 `secure_file_priv` 限制，文件描述符检查阻止路径替换和目录符号链接
竞态。COPY 流式写临时文件，成功后发布，拒绝覆盖已有目标。

本地 release 完整编译成功；9 项 C++ 文件读写单元测试、1 项 Rust Parquet
测试和 6 项 seekdb SQL 集成测试通过，AI 函数的 6 项回归测试再次通过。
官方 `file_sql_basic` 的 SQL 与原始预期结果也全部通过，仅将 SSH 复制
夹具的准备/清理步骤适配为隔离本机目录。回归同时检查错误码 1064、
无小数秒的 DATETIME 输出和微秒数据不丢失。

```bash
python3 unittest/sql/engine/basic/test_file_sql_integration.py
cd rust && cargo test -p local parquet_scan
```

当前实现限制：单条 CSV / JSONL / 导出记录上限 16 MiB，最多 4096 列；
拒绝 JSON 嵌套结构和 Parquet 嵌套 / 重复列；显式采样只接受 `sample_size=-1`；
CSV COPY 使用双引号及双引号转义。以上均返回明确错误，不进行静默转换。

## 赛题三：异步 AI 列

已实现 CREATE / ALTER ADD 的 `AI COLUMN (...) WITH (...)`、普通隐藏物理
控制列、`__all_ai_column` 定义表和 `__all_ai_column_task` 持久化任务表。
AI_COMPLETE 的 JSON / 整数 / 数值结果使用类型提示，AI_EMBED 校验向量维度。
输入表达式和依赖项在 DDL 阶段解析，拒绝 AI 列之间的依赖。

用户 DML 在原事务和语句保存点中捕获输入、写入任务并使旧结果失效；
无关字段更新保留结果。后台线程只处理已提交任务，HTTP 调用在结果事务
之外执行。结果事务锁定目标行并核对请求 ID，以原子方式发布同一请求组的
结果、状态和任务删除。旧请求不能覆盖新结果，失败支持有限重试和退避。
向量失效及结果写回复用原生索引维护，同时维护内部向量 ID。

`ALTER TABLE ... RETRY AI COLUMN ... WHERE ...` 支持普通业务谓词与 STATUS
伪列；带反引号的 `status` 指向业务列。`ai_column_strict_read` 默认关闭，
开启后检查语句实际引用的 AI 列是否存在未完成行，返回错误 11120。
检查包含本事务未提交的修改，并避开内部维护表达式。

本地 release 编译成功。官方 AI 列的以下 5 组 mysqltest 原始用例和结果
文件均未修改，全部通过：

- `ai_column_ddl`
- `ai_column_t1_dml`
- `ai_column_worker`
- `ai_column_retry_strict`
- `ai_column_vector_index`

另有 3 项 Python 集成回归覆盖严格读取的事务内可见性及回滚、非法 JSON
结果和非有限数值进入终止失败状态：

```bash
SEEKDB_PORT=2882 python3 unittest/sql/engine/expr/test_ai_column_integration.py
```

开发与测试使用重新初始化的单节点实例，以创建新增内部表。当前不提供
存量实例内部表升级、历史行自动回填或 AI 列的重命名 / 删除依赖迁移。
ALTER ADD 后的历史行可以显式使用 RETRY 入队。后台为单线程，模型请求
允许在进程重启或结果事务失败后重复执行，结果发布有请求 ID 保护。

## 推送与提测

官方仓库已 Fork 至 `https://github.com/chendian3981-spec/seekdb`，代码已
推送到 Fork 的 `pipe/seekdb-solutions` 分支。本地 `fork` remote 指向此仓库，
`origin` 保留官方源码地址。赛题实现与兼容修复提交截至 `d023e6cb`。

原登记仓库 `su7-lu/seekdb-2026` 的认证访问返回 `Repository not found`。
比赛页面尚未切换到新 Fork，尚未触发正式评测或获得官方分数。
