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
测试和 5 项 seekdb SQL 集成测试通过，AI 函数的 6 项回归测试再次通过。

```bash
python3 unittest/sql/engine/basic/test_file_sql_integration.py
cd rust && cargo test -p local parquet_scan
```

当前实现限制：单条 CSV / JSONL / 导出记录上限 16 MiB，最多 4096 列；
拒绝 JSON 嵌套结构和 Parquet 嵌套 / 重复列；显式采样只接受 `sample_size=-1`；
CSV COPY 使用双引号及双引号转义。以上均返回明确错误，不进行静默转换。

## 后续

- 赛题三：异步 AI 列、事务任务、重试、严格读取和索引一致性，待实现。
- 推送与提测：当前登录的 GitHub 账号尚无报名仓库的访问权限。
