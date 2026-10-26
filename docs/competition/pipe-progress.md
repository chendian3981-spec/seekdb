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

## 后续

- 赛题二：流式文件读取、类型推断、SQL 接口和安全限制，待实现。
- 赛题三：异步 AI 列、事务任务、重试、严格读取和索引一致性，待实现。
- 推送与提测：当前登录的 GitHub 账号尚无报名仓库的访问权限。
