# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

PostgreSQL MCP Server：基于 FastMCP 的 MCP 服务器，把自然语言问题转换为安全的只读 SQL 并执行。核心链路：OpenAI 生成 SQL → sqlglot 安全验证 → asyncpg 只读执行 → OpenAI 结果验证（置信度评分）。包结构为 `src/pg_mcp/`，Python 3.14+（`requires-python = ">=3.14"`）。

## 常用命令

```bash
uv sync --all-extras                      # 安装依赖（含 dev）

uv run pytest tests/unit/                 # 单元测试（无外部依赖，日常开发用这个）
uv run pytest                             # 全部测试（integration/e2e 需真实 PostgreSQL + OpenAI key）
uv run pytest -m "not integration"        # 跳过集成测试
uv run pytest tests/unit/test_sql_validator.py -k test_name   # 单个测试
uv run pytest --cov=src --cov-report=html --cov-fail-under=80 # 覆盖率（阈值 80 已在 pyproject 配置）

uv run mypy src                           # 类型检查（strict 模式）
uv run ruff check --fix .                 # Lint + 自动修复
uv run ruff format .                      # 格式化

uv run python -m pg_mcp                   # 启动 MCP 服务器（stdio）
uv run pg-mcp                             # 同上（pyproject scripts 入口）

cd fixtures && make create-all            # 创建测试库 blog_small / ecommerce_medium / saas_crm_large
docker-compose up -d                      # PostgreSQL + pg-mcp 全套启动
```

**注意**：仓库根目录的 `main.py` 是遗留的演示 stub（"add two numbers"），不是真实入口。真实服务器是 `src/pg_mcp/server.py` 中的 `mcp` 实例，通过 `python -m pg_mcp` 启动（README 中 `uv run python main.py` 的说法是错的）。

## 架构

单个 MCP tool `query(question, database?, return_type)`（`server.py`），内部是一条由 `QueryOrchestrator` 编排的流水线：

```
FastMCP server.py (lifespan 中初始化全部组件为模块级全局变量)
  └─ QueryOrchestrator.execute_query()
       1. _resolve_database()           # 单库自动选择，多库必须指定
       2. SchemaCache.get()/load()      # information_schema 内省，TTL 缓存
       3. _generate_sql_with_retry()    # SQLGenerator(OpenAI) → SQLValidator(sqlglot)
                                        # 验证失败则带错误反馈重试，最多 resilience.max_retries 次
                                        # 外层有 CircuitBreaker 保护 LLM 调用
       4. return_type == "sql" 则提前返回
       5. SQLExecutor.execute()         # asyncpg，只读事务，max_rows 截断
       6. _validate_results_safely()    # ResultValidator(OpenAI)，非阻塞：失败不导致请求失败
                                        # validation.enabled=false 时直接返回置信度 100
       7. QueryResponse.to_dict()
```

模块职责（都在 `src/pg_mcp/` 下）：

- `services/` — 核心业务：`orchestrator`（编排+重试）、`sql_generator`（LLM 生成）、`sql_validator`（sqlglot 安全验证）、`sql_executor`（执行）、`result_validator`（LLM 结果验证）
- `prompts/` — LLM prompt 模板（SQL 生成含重试反馈、结果验证）
- `cache/` — `SchemaCache`：TTL 缓存 + `load()/get()` + 可选 auto_refresh 后台任务
- `db/` — `create_pool()/close_pools()`，asyncpg 连接池
- `resilience/` — `CircuitBreaker`、`MultiRateLimiter`
- `observability/` — 自定义结构化日志（含敏感信息过滤器）、prometheus-client 指标、tracing
- `config/` — `Settings`（pydantic-settings）
- `models/` — Pydantic 数据模型与异常层次

关键约定：

- **错误不穿透露 MCP 边界**：`query` tool 永不抛异常，所有错误以 `{"success": false, "error": {code, message, details}}` dict 返回。内部用 `models/errors.py` 的 `PgMcpError` 层次 + `ErrorCode` (StrEnum)。
- **配置**：pydantic-settings 嵌套分组，环境变量前缀 `DATABASE_` / `OPENAI_` / `SECURITY_` / `CACHE_` / `RESILIENCE_` / `OBSERVABILITY_` / `VALIDATION_`，从 `.env` 读取。主库用 `DATABASE_*`；附加库用 `DATABASES`（JSON 数组，元素字段同 `DATABASE_*`，库名全局唯一），`Settings.all_databases` 返回主库在前的完整列表，`QueryOrchestrator` 按请求解析出的库名选择对应 executor。LLM 模型代码默认 `gpt-4o-mini`，用 `OPENAI_MODEL` 覆盖。`Settings` 有全局单例模式（`reset_settings()`），测试 conftest 每个 test 自动重置。

## 安全模型（sql_validator.py，核心模块）

采用**白名单**而非黑名单为主：

- 顶层语句只允许 `SELECT / UNION / INTERSECT / EXCEPT / WITH(CTE)`，多语句直接拒绝（`FORBIDDEN_STATEMENT_TYPES` 覆盖 INSERT/UPDATE/DELETE/DROP/CREATE/ALTER 等）
- 危险函数黑名单：内置集合（`pg_sleep`、文件 I/O、`dblink`、大对象操作等）∪ `SECURITY_BLOCKED_FUNCTIONS` 配置
- 可选 blocked_tables / blocked_columns；执行时强制只读事务、`max_rows` 行数上限、超时限制
- 修改验证逻辑时必须同步更新 `tests/unit/test_sql_validator.py`，安全相关分支需全覆盖

## 测试

- `tests/unit/` — 无外部依赖，日常开发运行
- `tests/integration/`、`tests/e2e/` — 标记 `@pytest.mark.integration`，需要真实 PostgreSQL 和 OpenAI key（fixture 库见 `fixtures/README.md`）
- `tests/conftest.py` 有两个 autouse fixture：每 test 前重置全局 Settings、关闭 metrics（避免端口冲突）

## 代码规范

- mypy strict：所有公开 API 必须完整类型注解；公开类/函数用 Google style docstring
- ruff：line-length 100，启用 S（bandit）和 ASYNC 规则；`tests/**` 允许 S101/S105/S106
- 自定义异常继承 `PgMcpError`，不要裸 `except`；日志不记录密钥/密码/PII（`DatabaseConfig.safe_dsn` 用于日志）
- Git 提交：`feat:` / `fix:` / `docs:` / `refactor:` / `test:` / `perf:` / `security:`
