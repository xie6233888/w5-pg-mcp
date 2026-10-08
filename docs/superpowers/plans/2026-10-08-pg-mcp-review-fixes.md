# PostgreSQL MCP Server 代码审查修复实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落实 `0006-pg-mcp-code-review.md` 中 Priority 1–3 的全部修复与 Priority 4 中与本次改动相关的单元测试，使实现与设计规范对齐（多库路由、安全配置接线、限流/重试/指标接入请求路径）。

**Architecture:** 保持现有 FastMCP → QueryOrchestrator 流水线不变，做四类增强：(1) 配置层补全 SecurityConfig 字段与多库列表；(2) 编排层把已存在但未接线的组件（MultiRateLimiter、MetricsCollector、tracing、retry 配置）真正接入请求路径，并按解析出的库名选择 executor；(3) 模型层去重（to_dict、ErrorDetail）；(4) 新增 `resilience/retry.py` 通用重试助手。

**Tech Stack:** Python 3.14+、FastMCP、asyncpg、sqlglot、pydantic-settings v2、prometheus-client、pytest + pytest-asyncio。

**Spec:** `0006-pg-mcp-code-review.md`（审查报告，本计划逐条对应其 Recommendations Priority 1–4）

## Global Constraints

- 本目录**不是 git 仓库**（已验证 `git rev-parse` 失败）：所有任务跳过 commit 步骤，以"测试通过 + mypy/ruff 通过"作为任务完成标准。计划执行前可询问用户是否 `git init`。
- 所有命令通过 `uv run` 前缀执行（PowerShell 环境，路径用正斜杠亦可）。
- mypy strict（`uv run mypy src`）与 ruff（line-length 100，启用 S/ASYNC 规则）必须零错误。
- 单元测试 `uv run pytest tests/unit/` 必须全绿；改动验证逻辑必须同步更新 `tests/unit/test_sql_validator.py`（CLAUDE.md 硬性要求）。
- 公开 API 完整类型注解（mypy strict）；Google style docstring；自定义异常继承 `PgMcpError`；日志不记录密码/DSN 明文（用 `safe_dsn`）。
- `query` tool 边界行为不变：永不抛异常，错误以 `{"success": false, "error": {code, message, details}}` 返回。
- 现有行为保持项：`validation.enabled=false` 时置信度直接返回 100（CLAUDE.md 明文约定，不改）。

## Review Focus

审查报告隐含但无现有测试覆盖的五个失败面，每条已在其归属任务中加入测试：

1. `DATABASES` 环境变量为非法 JSON 或含重复库名 → 启动即报 ValidationError，而非运行期才炸（Task 4 `test_databases_from_json_env`、`test_duplicate_database_names_rejected`）。
2. 请求指定了 pools 里存在但 executors 字典缺失的库 → 返回干净的 `DatabaseError` 错误 dict，而不是裸 KeyError 穿透（Task 4 `test_execute_selects_executor_by_resolved_name`、`test_missing_executor_raises_clean_error`）。
3. question 长度恰好在 `max_question_length` 边界 → 等于上限放行、超 1 字符拒绝且错误码为 `question_too_long`（Task 7 两个边界测试）。
4. DB 连接瞬态抖动（如 `ConnectionDoesNotExistError`）→ 自动重试后成功；确定性错误（语法错误、语句超时）→ 不重试直接失败（Task 7 两个重试测试）。
5. 限流器饱和 → 等待至超时后返回 `rate_limit_exceeded` 错误 dict，绝不让裸 `TimeoutError` 泄漏到 MCP 边界（Task 7 `test_rate_limit_timeout_converted`）。

---

### Task 1: 模型层清理 — 统一 ErrorDetail、去重 QueryResponse.to_dict

**Files:**
- Modify: `src/pg_mcp/models/errors.py`（删除 plain ErrorDetail，新增 Pydantic 版）
- Modify: `src/pg_mcp/models/query.py`（删除本地 ErrorDetail 定义，改为 re-export；删除第二个 to_dict）
- Modify: `src/pg_mcp/server.py:358-362`（删除 tokens_used 兜底）
- Test: `tests/unit/test_models.py`

**Interfaces:**
- Produces: `pg_mcp.models.errors.ErrorDetail`（Pydantic，`code: str, message: str, details: dict | None`，带 `to_dict()` 方法）；`pg_mcp.models.query.ErrorDetail` 为同一对象的 re-export；`PgMcpError.to_error_detail() -> ErrorDetail`（code 转为 `.value` 字符串）；`QueryResponse.to_dict()` 唯一版本，`exclude_none=False` 且 `tokens_used` 永不为 None。
- Consumes: 现有 `ErrorCode`（StrEnum）。

- [ ] **Step 1: 在 test_models.py 写失败测试**

在 `TestErrorModels` 类中**替换** `test_error_detail`、`test_error_detail_to_dict`、`test_error_to_detail` 三个方法（旧版基于 plain 类、code 传枚举），并新增一个 to_dict 去重测试类：

```python
class TestErrorModels:
    """Tests for error models."""

    def test_error_detail(self) -> None:
        """Test ErrorDetail creation (Pydantic, string codes)."""
        detail = ErrorDetail(
            code=ErrorCode.SQL_PARSE_ERROR.value,
            message="Invalid syntax",
            details={"position": 10},
        )
        assert detail.code == "sql_parse_error"
        assert detail.message == "Invalid syntax"
        assert detail.details["position"] == 10

    def test_error_detail_to_dict(self) -> None:
        """Test ErrorDetail serialization."""
        detail = ErrorDetail(
            code=ErrorCode.DATABASE_ERROR.value,
            message="Connection failed",
        )
        d = detail.to_dict()
        assert d["code"] == "database_error"
        assert d["message"] == "Connection failed"

    def test_error_to_detail(self) -> None:
        """Test exception to ErrorDetail conversion returns Pydantic model with string code."""
        err = SecurityViolationError(
            message="Blocked function",
            details={"function": "pg_sleep"},
        )
        detail = err.to_error_detail()
        assert isinstance(detail, BaseModel)
        assert detail.code == "security_violation"
        assert detail.message == "Blocked function"
        assert detail.details == {"function": "pg_sleep"}


class TestQueryResponseToDict:
    """Tests for QueryResponse.to_dict dedup and tokens_used guarantee."""

    def test_to_dict_always_includes_tokens_used(self) -> None:
        """tokens_used=None must serialize as 0, never be dropped."""
        response = QueryResponse(success=True, generated_sql="SELECT 1", confidence=100)
        d = response.to_dict()
        assert d["tokens_used"] == 0

    def test_to_dict_keeps_none_fields(self) -> None:
        """exclude_none=False: data/error/validation keys always present."""
        response = QueryResponse(success=True, generated_sql="SELECT 1", confidence=100)
        d = response.to_dict()
        assert "data" in d
        assert "error" in d
        assert "validation" in d
        assert d["data"] is None
```

（去重本身由"删除第二个 to_dict"这一显式编辑保证；Python 类体内重复方法定义会在类创建时静默折叠，无法用运行时断言检测，故不加伪守护测试。）

`test_models.py` 顶部 import 区（第 13 行附近）确认 `ErrorDetail` 从 `pg_mcp.models.errors` 导入（原本就是），并补 `from pydantic import BaseModel`。

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_models.py::TestErrorModels::test_error_to_detail tests/unit/test_models.py::TestQueryResponseToDict -v`
Expected: FAIL（plain ErrorDetail 不是 BaseModel；`QueryResponse` 存在两个 to_dict 且第二个 `exclude_none=True` 会丢 tokens_used 字段）

- [ ] **Step 3: 修改 errors.py — 用 Pydantic ErrorDetail 替换 plain 类**

顶部 import 增加 `from pydantic import BaseModel, Field`。将 `errors.py:39-79` 的 plain `ErrorDetail` 类整体替换为：

```python
class ErrorDetail(BaseModel):
    """Structured error detail information (used in MCP responses)."""

    code: str = Field(..., description="Error code identifier")
    message: str = Field(..., description="Human-readable error message")
    details: dict[str, Any] | None = Field(None, description="Additional error context")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            dict: Dictionary containing error information.
        """
        return self.model_dump(exclude_none=True)
```

`PgMcpError.to_error_detail()`（errors.py:106-112）改为：

```python
    def to_error_detail(self) -> ErrorDetail:
        """Convert exception to ErrorDetail.

        Returns:
            ErrorDetail: Structured error detail with string error code.
        """
        return ErrorDetail(code=self.code.value, message=self.message, details=self.details or None)
```

- [ ] **Step 4: 修改 query.py — re-export + 删除重复 to_dict**

1. query.py:139-144 的本地 `ErrorDetail` 类定义删除，原位替换为 re-export：
```python
# ErrorDetail is defined in models.errors (Pydantic model, shared with the
# exception hierarchy). Re-exported here for backward compatibility.
from pg_mcp.models.errors import ErrorDetail  # noqa: E402
```
（import 放在文件头部的 import 区更符合 ruff 规范——直接并入头部 `from pg_mcp.models.errors import ErrorDetail`，原类定义处只留上述注释。）

2. 删除 query.py:214-220 的第二个 `to_dict`（`exclude_none=True` 版本，它覆盖了第一个）。

- [ ] **Step 5: server.py 删除 tokens_used 兜底**

删除 server.py:359-361 的：
```python
        # Ensure tokens_used is always present
        if "tokens_used" not in result:
            result["tokens_used"] = 0
```
`to_dict()` 已保证。

- [ ] **Step 6: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_models.py -v && uv run mypy src && uv run ruff check src tests`
Expected: 全部 PASS；mypy/ruff 零错误。若 `models/__init__.py` 的 `ErrorDetail` 导入路径断裂（它从 query.py 导入），re-export 已保证其继续工作，mypy 会验证。

---

### Task 2: SecurityConfig 补全安全字段 + ValidationConfig 清理

**Files:**
- Modify: `src/pg_mcp/config/settings.py:73-133`
- Modify: `src/pg_mcp/server.py:152-158`（validator 接线真实配置）
- Modify: `.env.example`（SECURITY 段补 3 项；VALIDATION 段删 MIN_CONFIDENCE_SCORE）
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces: `SecurityConfig.blocked_tables: list[str]`、`SecurityConfig.blocked_columns: list[str]`、`SecurityConfig.allow_explain: bool`（env: `SECURITY_BLOCKED_TABLES` 等逗号分隔）；`ValidationConfig` 不再有 `min_confidence_score` 字段（与 `confidence_threshold` 重复且从未使用，YAGNI 删除）。
- Consumes: `SQLValidator(config, blocked_tables, blocked_columns, allow_explain)` 签名（已存在）。

- [ ] **Step 1: 在 test_config.py 写失败测试**

先删除现有引用 `min_confidence_score` 的三个测试（test_config.py:200-220 附近，`assert config.min_confidence_score == 70`、`min_confidence_score=80`、两个越界用例），替换为：

```python
class TestValidationConfigCleanup:
    """ValidationConfig: min_confidence_score removed (duplicate of confidence_threshold)."""

    def test_min_confidence_score_removed(self) -> None:
        """The unused duplicate field must not exist."""
        config = ValidationConfig()
        assert not hasattr(config, "min_confidence_score")

    def test_confidence_threshold_still_enforced_default(self) -> None:
        config = ValidationConfig()
        assert config.confidence_threshold == 70


class TestSecurityConfigNewFields:
    """SecurityConfig: blocked_tables / blocked_columns / allow_explain."""

    def test_defaults_empty_and_false(self) -> None:
        config = SecurityConfig()
        assert config.blocked_tables == []
        assert config.blocked_columns == []
        assert config.allow_explain is False

    def test_blocked_tables_from_env_csv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SECURITY_BLOCKED_TABLES", "secret_table, audit_log ,")
        config = SecurityConfig()
        assert config.blocked_tables == ["secret_table", "audit_log"]

    def test_blocked_columns_from_env_csv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SECURITY_BLOCKED_COLUMNS", "password,ssn")
        config = SecurityConfig()
        assert config.blocked_columns == ["password", "ssn"]

    def test_allow_explain_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SECURITY_ALLOW_EXPLAIN", "true")
        config = SecurityConfig()
        assert config.allow_explain is True

    def test_blocked_tables_accept_list_directly(self) -> None:
        config = SecurityConfig(blocked_tables=["t1"], blocked_columns=["c1"], allow_explain=True)
        assert config.blocked_tables == ["t1"]
        assert config.blocked_columns == ["c1"]
        assert config.allow_explain is True
```

按文件现有 import 风格补 `SecurityConfig`（若尚未导入）。

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_config.py -k "TestSecurityConfigNewFields or TestValidationConfigCleanup" -v`
Expected: FAIL — `SecurityConfig` 无这些属性；`min_confidence_score` 仍存在。

- [ ] **Step 3: settings.py 实现**

`SecurityConfig` 中 `safe_search_path` 字段之后追加：

```python
    blocked_tables: list[str] = Field(
        default_factory=list,
        description="Table names that queries are not allowed to reference",
    )
    blocked_columns: list[str] = Field(
        default_factory=list,
        description="Column names that queries are not allowed to reference",
    )
    allow_explain: bool = Field(
        default=False, description="Whether EXPLAIN statements are allowed"
    )
```

`SecurityConfig` 内追加复用解析器（放在现有 `parse_blocked_functions` 之后）：

```python
    @field_validator("blocked_tables", "blocked_columns", mode="before")
    @classmethod
    def parse_string_list(cls, v: str | list[str]) -> list[str]:
        """Parse comma-separated string or list."""
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v
```

`ValidationConfig` 中删除 `min_confidence_score` 字段（settings.py:119-121）。

- [ ] **Step 4: server.py 接线**

server.py:153-158 替换为：

```python
        sql_validator = SQLValidator(
            config=_settings.security,
            blocked_tables=_settings.security.blocked_tables,
            blocked_columns=_settings.security.blocked_columns,
            allow_explain=_settings.security.allow_explain,
        )
```

- [ ] **Step 5: .env.example 更新**

SECURITY 段（`SECURITY_MAX_EXECUTION_TIME=30` 之后）追加：

```
# Comma-separated list of tables that queries must not reference
# Use to protect sensitive relations beyond built-in function blocking
# SECURITY_BLOCKED_TABLES=

# Comma-separated list of columns that queries must not reference
# Use qualified names (table.column) to scope a block to one table
# SECURITY_BLOCKED_COLUMNS=

# Allow EXPLAIN statements (read-only, shows query plans without executing)
# SECURITY_ALLOW_EXPLAIN=false
```

VALIDATION 段删除 `VALIDATION_MIN_CONFIDENCE_SCORE=70` 及其注释块（127-131 行）。

- [ ] **Step 6: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_config.py tests/unit/test_sql_validator.py -v && uv run mypy src && uv run ruff check src tests`
Expected: 全部 PASS（test_sql_validator.py 必须仍绿——validator 本身未改，只是接线变了）。

---

### Task 3: 新增 resilience/retry.py — 通用异步重试

**Files:**
- Create: `src/pg_mcp/resilience/retry.py`
- Test: `tests/unit/test_resilience.py`（追加测试类）

**Interfaces:**
- Produces:
  - `async def retry_async(operation: Callable[[], Awaitable[T]], *, max_retries: int, retry_delay: float, backoff_factor: float, retryable: Callable[[Exception], bool], operation_name: str = "operation") -> T`
  - `TRANSIENT_DB_ERRORS: tuple[type[Exception], ...]`
  - `def is_transient_db_error(error: Exception) -> bool`（检查 `error` 及其 `__cause__` 链）
- Consumes: `asyncpg` 顶层异常 `ConnectionDoesNotExistError / ConnectionFailureError / InterfaceError`。

- [ ] **Step 0: 验证 asyncpg 顶层异常存在**

Run: `uv run python -c "import asyncpg; print(asyncpg.ConnectionDoesNotExistError, asyncpg.ConnectionFailureError, asyncpg.InterfaceError)"`
Expected: 三个类都打印出来。若任一不存在，改用 `asyncpg.exceptions` 模块路径并在实现中相应调整。

- [ ] **Step 1: 在 test_resilience.py 写失败测试**

文件顶部补 import：

```python
import asyncio

import asyncpg

from pg_mcp.models.errors import DatabaseError
from pg_mcp.resilience.retry import (
    TRANSIENT_DB_ERRORS,
    is_transient_db_error,
    retry_async,
)
```

追加测试类（delay 全部用 0.01 量级避免测试变慢；backoff 顺序用 monkeypatch 掉 `asyncio.sleep` 验证）：

```python
class TestRetryAsync:
    """Tests for the generic async retry helper."""

    @pytest.mark.asyncio
    async def test_success_first_attempt_no_retry(self) -> None:
        calls: list[int] = []

        async def op() -> str:
            calls.append(1)
            return "ok"

        result = await retry_async(
            op,
            max_retries=3,
            retry_delay=0.01,
            backoff_factor=2.0,
            retryable=lambda e: True,
        )
        assert result == "ok"
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_succeeds_after_transient_failures(self) -> None:
        attempts: list[int] = []

        async def flaky() -> str:
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("transient")
            return "recovered"

        result = await retry_async(
            flaky,
            max_retries=3,
            retry_delay=0.01,
            backoff_factor=2.0,
            retryable=lambda e: isinstance(e, ConnectionError),
        )
        assert result == "recovered"
        assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_exhausts_retries_and_raises_last_error(self) -> None:
        attempts: list[int] = []

        async def always_fails() -> None:
            attempts.append(1)
            raise ConnectionError("down")

        with pytest.raises(ConnectionError, match="down"):
            await retry_async(
                always_fails,
                max_retries=2,
                retry_delay=0.01,
                backoff_factor=2.0,
                retryable=lambda e: isinstance(e, ConnectionError),
            )
        # initial attempt + 2 retries
        assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_non_retryable_error_raises_immediately(self) -> None:
        attempts: list[int] = []

        async def bad_sql() -> None:
            attempts.append(1)
            raise ValueError("syntax error")

        with pytest.raises(ValueError, match="syntax error"):
            await retry_async(
                bad_sql,
                max_retries=3,
                retry_delay=0.01,
                backoff_factor=2.0,
                retryable=lambda e: isinstance(e, ConnectionError),
            )
        assert len(attempts) == 1

    @pytest.mark.asyncio
    async def test_exponential_backoff_delays(self, monkeypatch: pytest.MonkeyPatch) -> None:
        delays: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            delays.append(seconds)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        attempts: list[int] = []

        async def always_fails() -> None:
            attempts.append(1)
            raise ConnectionError("down")

        with pytest.raises(ConnectionError):
            await retry_async(
                always_fails,
                max_retries=3,
                retry_delay=1.0,
                backoff_factor=2.0,
                retryable=lambda e: True,
            )
        assert delays == [1.0, 2.0, 4.0]


class TestTransientDbErrors:
    """Tests for transient database error classification."""

    def test_direct_asyncpg_connection_error_is_transient(self) -> None:
        err = asyncpg.ConnectionDoesNotExistError("connection closed")
        assert is_transient_db_error(err)

    def test_wrapped_connection_error_via_cause_is_transient(self) -> None:
        cause = asyncpg.ConnectionFailureError("server closed connection")
        wrapped = DatabaseError(message="Database query failed")
        wrapped.__cause__ = cause
        assert is_transient_db_error(wrapped)

    def test_statement_timeout_is_not_transient(self) -> None:
        err = DatabaseError(message="Query execution exceeded timeout")
        assert not is_transient_db_error(err)

    def test_plain_postgres_error_is_not_transient(self) -> None:
        cause = asyncpg.PostgresError("syntax error at or near SELEC")
        wrapped = DatabaseError(message="Database query failed")
        wrapped.__cause__ = cause
        assert not is_transient_db_error(wrapped)

    def test_transient_tuple_contains_expected_types(self) -> None:
        assert asyncpg.ConnectionDoesNotExistError in TRANSIENT_DB_ERRORS
        assert asyncpg.ConnectionFailureError in TRANSIENT_DB_ERRORS
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_resilience.py -k "TestRetryAsync or TestTransientDbErrors" -v`
Expected: FAIL — `ModuleNotFoundError: pg_mcp.resilience.retry`

- [ ] **Step 3: 实现 retry.py**

```python
"""Generic async retry helper with exponential backoff.

This module provides retry_async, a small reusable helper for retrying
transient failures (connection blips, rate limits) with exponential backoff.
Retryability is expressed as a predicate so callers can classify errors
including wrapped ones (SQLExecutor wraps asyncpg errors as DatabaseError
with the original exception as __cause__).
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: asyncpg exception types considered transient (worth retrying).
TRANSIENT_DB_ERRORS: tuple[type[Exception], ...] = (
    asyncpg.ConnectionDoesNotExistError,
    asyncpg.ConnectionFailureError,
    asyncpg.InterfaceError,
)


def is_transient_db_error(error: Exception) -> bool:
    """Check whether an error is a transient database failure worth retrying.

    SQLExecutor wraps original asyncpg errors as DatabaseError with the
    original exception as __cause__, so the cause chain is inspected.

    Args:
        error: The raised exception.

    Returns:
        bool: True if the error (or its cause) is a transient connection issue.
    """
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TRANSIENT_DB_ERRORS):
            return True
        current = current.__cause__
    return False


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    max_retries: int,
    retry_delay: float,
    backoff_factor: float,
    retryable: Callable[[Exception], bool],
    operation_name: str = "operation",
) -> T:
    """Retry an async operation with exponential backoff on retryable errors.

    Args:
        operation: Zero-argument async callable to execute.
        max_retries: Number of retries after the initial attempt.
        retry_delay: Initial delay in seconds before the first retry.
        backoff_factor: Multiplier applied to the delay after each retry.
        retryable: Predicate deciding whether an exception should be retried.
            Non-retryable exceptions are re-raised immediately.
        operation_name: Human-readable name used in log messages.

    Returns:
        T: The operation's return value.

    Raises:
        Exception: The last exception if all attempts fail, or immediately
            if a raised error is not retryable.
    """
    delay = retry_delay
    for attempt in range(max_retries + 1):
        try:
            return await operation()
        except Exception as e:
            if not retryable(e) or attempt >= max_retries:
                logger.error(
                    "%s failed permanently after %d attempt(s): %s",
                    operation_name,
                    attempt + 1,
                    e,
                )
                raise
            logger.warning(
                "%s failed (attempt %d/%d), retrying in %.1fs: %s",
                operation_name,
                attempt + 1,
                max_retries + 1,
                delay,
                e,
            )
            await asyncio.sleep(delay)
            delay *= backoff_factor
    raise AssertionError("unreachable")  # pragma: no cover
```

若 `src/pg_mcp/resilience/__init__.py` 有导出列表，追加 `retry_async`、`is_transient_db_error`。

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_resilience.py -v && uv run mypy src && uv run ruff check src tests`
Expected: 全部 PASS（既有 circuit breaker / rate limiter 测试不受影响）。

---

### Task 4: 多库支持 — Settings.databases + create_pools + 按库名选 executor

**Files:**
- Modify: `src/pg_mcp/config/settings.py`（Settings 增 `databases` 字段 + `all_databases` property + 唯一性校验）
- Modify: `src/pg_mcp/db/pool.py:50-79`（create_pools 并发化）
- Modify: `src/pg_mcp/server.py`（create_pools + 每库 executor 用各自 db_config；删除未使用的 `_circuit_breaker` 全局）
- Modify: `src/pg_mcp/services/orchestrator.py`（构造参数 `sql_executor` → `sql_executors: dict[str, SQLExecutor]`）
- Modify: `.env.example`（MULTI-DATABASE 段改写为 DATABASES JSON 用法）
- Modify: `CLAUDE.md`（配置段补一句 DATABASES 说明）
- Test: `tests/unit/test_config.py`、`tests/unit/test_orchestrator.py`

**Interfaces:**
- Produces:
  - `Settings.databases: list[DatabaseConfig]`（env `DATABASES` 为 JSON 数组）；`Settings.all_databases -> list[DatabaseConfig]`（主库在前，名字全局唯一）。
  - `create_pools(configs: list[DatabaseConfig]) -> dict[str, Pool]`（并发创建）。
  - `QueryOrchestrator.__init__(..., sql_executors: dict[str, SQLExecutor], ...)`；`execute_query` Step 5 用 `self._get_executor(database_name)` 取执行器，缺失时抛 `DatabaseError`（Review Focus #2）。
- Consumes: `SQLExecutor(pool=..., security_config=..., db_config=...)`。

- [ ] **Step 1: 在 test_config.py 写失败测试**

```python
class TestMultiDatabaseSettings:
    """Settings.databases list + all_databases property."""

    def test_all_databases_primary_only_by_default(self) -> None:
        settings = Settings(database=DatabaseConfig(name="main"))
        assert [db.name for db in settings.all_databases] == ["main"]

    def test_all_databases_includes_extra(self) -> None:
        settings = Settings(
            database=DatabaseConfig(name="main"),
            databases=[DatabaseConfig(name="extra", host="remote")],
        )
        assert [db.name for db in settings.all_databases] == ["main", "extra"]

    def test_databases_from_json_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASES", '[{"name": "db2", "host": "remote"}, {"name": "db3"}]')
        settings = Settings(database=DatabaseConfig(name="main"))
        assert [db.name for db in settings.databases] == ["db2", "db3"]
        assert settings.databases[0].host == "remote"

    def test_databases_malformed_json_rejected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATABASES", "not-json")
        with pytest.raises(Exception):  # pydantic ValidationError
            Settings(database=DatabaseConfig(name="main"))

    def test_duplicate_names_between_primary_and_extra_rejected(self) -> None:
        with pytest.raises(Exception):  # pydantic ValidationError
            Settings(
                database=DatabaseConfig(name="main"),
                databases=[DatabaseConfig(name="main")],
            )

    def test_duplicate_names_within_extras_rejected(self) -> None:
        with pytest.raises(Exception):
            Settings(
                database=DatabaseConfig(name="main"),
                databases=[DatabaseConfig(name="dup"), DatabaseConfig(name="dup")],
            )
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_config.py -k TestMultiDatabaseSettings -v`
Expected: FAIL — Settings 无 `databases`/`all_databases`

- [ ] **Step 3: settings.py 实现**

settings.py 头部 import 补 `from pydantic import Field, SecretStr, field_validator, model_validator`。`Settings` 类中 `database` 字段后追加：

```python
    databases: list[DatabaseConfig] = Field(
        default_factory=list,
        description=(
            "Additional databases; configured via the DATABASES env var as a JSON array "
            '(e.g. DATABASES=\'[{"name": "db2", "host": "remote"}]\')'
        ),
    )
```

`is_production` property 之前追加：

```python
    @property
    def all_databases(self) -> list[DatabaseConfig]:
        """Primary database plus any additional databases (primary first)."""
        return [self.database, *self.databases]

    @model_validator(mode="after")
    def validate_database_names_unique(self) -> "Settings":
        """Ensure all configured database names are unique."""
        names = [db.name for db in self.all_databases]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"Duplicate database names configured: {duplicates}")
        return self
```

- [ ] **Step 4: db/pool.py — create_pools 并发化**

`import asyncio` 提升到模块顶部；`create_pools` 函数体替换为：

```python
    pool_list = await asyncio.gather(*(create_pool(config) for config in configs))
    return {config.name: pool for config, pool in zip(configs, pool_list, strict=True)}
```

（docstring 中 "concurrently" 的描述从此真实成立。）

- [ ] **Step 5: orchestrator.py — executor 字典**

构造函数参数 `sql_executor: SQLExecutor` 改为 `sql_executors: dict[str, SQLExecutor]`；赋值 `self.sql_executors = sql_executors`，并删除 `self.sql_executor = sql_executor` 行；docstring 的 Args 同步更新。类内新增辅助方法（放在 `_resolve_database` 之后）：

```python
    def _get_executor(self, database_name: str) -> SQLExecutor:
        """Get the SQL executor for a resolved database name.

        Args:
            database_name: Resolved database name.

        Returns:
            SQLExecutor: Executor bound to that database's pool.

        Raises:
            DatabaseError: If no executor exists for the name.
        """
        executor = self.sql_executors.get(database_name)
        if executor is None:
            raise DatabaseError(
                message=f"No SQL executor configured for database '{database_name}'",
                details={
                    "database": database_name,
                    "available_executors": sorted(self.sql_executors.keys()),
                },
            )
        return executor
```

`execute_query` Step 5（orchestrator.py:194-198）替换为：

```python
            # Step 5: Execute SQL on the resolved database's executor
            executor = self._get_executor(database_name)
            logger.debug("Executing SQL", extra={"request_id": request_id})
            start_time = self._get_current_time_ms()

            results, total_count = await executor.execute(generated_sql)
```

- [ ] **Step 6: server.py — 多池 + 每库 executor + 删除冗余熔断器**

1. import 行改为 `from pg_mcp.db.pool import close_pools, create_pools`。
2. server.py:97-109 的建池段替换为：

```python
        # 3. Create database connection pools (primary + additional)
        logger.info("Creating database connection pools...")
        _pools = await create_pools(_settings.all_databases)
        for db_config in _settings.all_databases:
            logger.info(
                f"Created connection pool for database '{db_config.name}'",
                extra={"min_size": db_config.min_pool_size, "max_size": db_config.max_pool_size},
            )
```

3. server.py:160-169 的 executor 创建段替换为（修正潜在 bug：此前非主库也拿到主库 db_config）：

```python
        # SQL Executors (one per database, each with its own db config)
        sql_executors: dict[str, SQLExecutor] = {}
        for db_config in _settings.all_databases:
            executor = SQLExecutor(
                pool=_pools[db_config.name],
                security_config=_settings.security,
                db_config=db_config,
            )
            sql_executors[db_config.name] = executor
            logger.info(f"Created SQL executor for database '{db_config.name}'")
```

4. 删除 server.py:37 与 71 行的 `_circuit_breaker` 全局声明、180-184 行的未使用实例化（orchestrator 自己持有熔断器——审查项 3.2）。
5. orchestrator 构造（server.py:194-203）中 `sql_executor=sql_executors[...]` 改为 `sql_executors=sql_executors`。

- [ ] **Step 7: 更新 test_orchestrator.py 全部构造点**

文件内所有 `sql_executor=MagicMock()` 形式的构造替换为与 `pools` 键一致的字典，例如 fixture（orchestrator.py 测试:40-51）改为：

```python
    @pytest.fixture
    def mock_executors(self, mock_pools: dict[str, MagicMock]) -> dict[str, MagicMock]:
        """Create mock executors matching pool names."""
        return {name: MagicMock() for name in mock_pools}

    @pytest.fixture
    def orchestrator(
        self,
        mock_pools: dict[str, MagicMock],
        mock_executors: dict[str, MagicMock],
    ) -> QueryOrchestrator:
        """Create orchestrator with mocked components."""
        return QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors=mock_executors,
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools=mock_pools,
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )
```

其余构造点（单库 `{"only_db": ...}`、空 `{}` 等）按同样键对齐原则逐一更新（全文搜索 `sql_executor=` 确认清零）。

- [ ] **Step 8: 追加 orchestrator 多库路由测试（Review Focus #2）**

```python
class TestExecutorSelection:
    """Tests for per-database executor selection."""

    @pytest.mark.asyncio
    async def test_execute_selects_executor_by_resolved_name(self) -> None:
        """The request's database determines which executor runs the SQL."""
        exec_a, exec_b = AsyncMock(), AsyncMock()
        exec_a.execute.return_value = ([{"n": 1}], 1)
        exec_b.execute.return_value = ([{"m": 2}], 1)
        gen = AsyncMock()
        gen.generate.return_value = "SELECT 1;"
        val = MagicMock()
        val.validate_with_result.return_value = MagicMock(is_acceptable=True)
        rv = AsyncMock()
        rv.validate.return_value = ResultValidationResult(
            confidence=90, explanation="ok", suggestion=None, is_acceptable=True
        )
        cache = MagicMock()
        cache.get.return_value = DatabaseSchema(database_name="db1", tables=[], version="15")
        orch = QueryOrchestrator(
            sql_generator=gen,
            sql_validator=val,
            sql_executors={"db1": exec_a, "db2": exec_b},
            result_validator=rv,
            schema_cache=cache,
            pools={"db1": MagicMock(), "db2": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )
        response = await orch.execute_query(
            QueryRequest(question="count", database="db2")
        )
        assert response.success
        exec_b.execute.assert_awaited_once()
        exec_a.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_executor_raises_clean_error(self) -> None:
        """Executor dict missing a pool name must yield DatabaseError, not KeyError."""
        gen = AsyncMock()
        gen.generate.return_value = "SELECT 1;"
        val = MagicMock()
        val.validate_with_result.return_value = MagicMock(is_acceptable=True)
        cache = MagicMock()
        cache.get.return_value = DatabaseSchema(database_name="db1", tables=[], version="15")
        orch = QueryOrchestrator(
            sql_generator=gen,
            sql_validator=val,
            sql_executors={},  # deliberately empty
            result_validator=AsyncMock(),
            schema_cache=cache,
            pools={"db1": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )
        response = await orch.execute_query(QueryRequest(question="count", database="db1"))
        assert not response.success
        assert response.error is not None
        assert response.error.code == ErrorCode.DATABASE_ERROR.value
```

import 区补 `from pg_mcp.models.errors import ErrorCode`（若缺）。**注意**：本测试按 Task 4 时点的接口书写（`generate` 返回 `str`、`validate_with_result` 已存在于 Task 5——若 Task 5 未先执行，先用 `validate_or_raise` 占位）。Task 6 把 `generate` 改为返回 tuple 后，Task 7 Step 4 的全文清扫会统一把这些 mock 改为 `("SELECT 1;", 10)` 形式。

- [ ] **Step 9: .env.example 与 CLAUDE.md 文档更新**

`.env.example` 的 MULTI-DATABASE 段（227-238 行）替换为：

```
# ============================================================================
# OPTIONAL: MULTI-DATABASE CONFIGURATION
# ============================================================================
# The primary database is configured with the DATABASE_* variables above.
# Additional databases are configured via the DATABASES env var (JSON array):
#
# DATABASES=[{"name": "ecommerce_medium", "host": "localhost", "password": "postgres"}]
#
# Each object accepts the same fields as DATABASE_* (host, port, name, user,
# password, pool sizes). Database names must be unique across all entries.
# With multiple databases configured, MCP clients must pass the `database`
# parameter to the query tool.
```

`CLAUDE.md` 的配置说明行（"**配置**：pydantic-settings 嵌套分组……"）末尾追加一句：多库通过 `DATABASES` JSON 数组配置，主库仍用 `DATABASE_*`。

- [ ] **Step 10: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_config.py tests/unit/test_orchestrator.py -v && uv run mypy src && uv run ruff check src tests`
Expected: 全部 PASS。

---

### Task 5: SQLValidator.validate_with_result — 真实验证结果替代硬编码

**Files:**
- Modify: `src/pg_mcp/services/sql_validator.py`
- Modify: `src/pg_mcp/services/orchestrator.py:447-454`（删除硬编码 ValidationResult）
- Test: `tests/unit/test_sql_validator.py`（CLAUDE.md 硬性要求同步更新）

**Interfaces:**
- Produces: `SQLValidator.validate_with_result(sql: str) -> ValidationResult`——校验失败抛 `SQLParseError / SecurityViolationError`（与 `validate_or_raise` 语义一致）；成功返回字段真实的 `ValidationResult`（is_valid=True, is_select=True, allows_data_modification=False, uses_blocked_functions=[], error_message=None）。
- Consumes: `pg_mcp.models.query.ValidationResult`。

- [ ] **Step 1: 在 test_sql_validator.py 写失败测试**

追加测试类（沿用文件里已有的 fixture 构造 `SQLValidator` 的方式）：

```python
class TestValidateWithResult:
    """Tests for validate_with_result (detailed ValidationResult output)."""

    def test_valid_select_returns_populated_result(self) -> None:
        validator = self._make_validator()
        result = validator.validate_with_result("SELECT id, name FROM users")
        assert result.is_valid is True
        assert result.is_select is True
        assert result.allows_data_modification is False
        assert result.uses_blocked_functions == []
        assert result.error_message is None
        assert result.is_safe is True

    def test_cte_query_is_select(self) -> None:
        validator = self._make_validator()
        result = validator.validate_with_result(
            "WITH t AS (SELECT 1 AS x) SELECT x FROM t"
        )
        assert result.is_valid is True
        assert result.is_select is True

    def test_blocked_function_raises(self) -> None:
        validator = self._make_validator()
        with pytest.raises(SecurityViolationError, match="pg_sleep"):
            validator.validate_with_result("SELECT pg_sleep(10)")

    def test_write_statement_raises(self) -> None:
        validator = self._make_validator()
        with pytest.raises(SecurityViolationError):
            validator.validate_with_result("DELETE FROM users")

    def test_unparseable_sql_raises_parse_error(self) -> None:
        validator = self._make_validator()
        with pytest.raises(SQLParseError):
            validator.validate_with_result("SELEC FROM")

    def test_empty_sql_raises_parse_error(self) -> None:
        validator = self._make_validator()
        with pytest.raises(SQLParseError):
            validator.validate_with_result("   ")
```

`_make_validator` 辅助方法用该文件现有的最小 `SecurityConfig()` 构造（参照文件内其他测试类；若已有同类 fixture 则直接复用）。

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_sql_validator.py -k TestValidateWithResult -v`
Expected: FAIL — `AttributeError: 'SQLValidator' object has no attribute 'validate_with_result'`

- [ ] **Step 3: 实现**

sql_validator.py import 区补 `from pg_mcp.models.query import ValidationResult`。重构 `validate_or_raise`：把现有方法体（sql_validator.py:117-194 的检查序列）抽为私有 `_analyze(sql: str) -> ValidationResult`，检查逻辑与抛错行为**逐字节保持不变**，仅在所有检查通过后的末尾（原 `return None` 处）改为：

```python
        return ValidationResult(
            is_valid=True,
            is_select=True,
            allows_data_modification=False,
            uses_blocked_functions=[],
            error_message=None,
        )
```

两个公开方法变薄封装：

```python
    def validate_or_raise(self, sql: str) -> None:
        """Validate SQL query and raise exception on violation.

        Args:
            sql: SQL query string to validate.

        Raises:
            SQLParseError: If SQL cannot be parsed.
            SecurityViolationError: If SQL violates security constraints.
        """
        self._analyze(sql)

    def validate_with_result(self, sql: str) -> ValidationResult:
        """Validate SQL and return a detailed validation result.

        Behavior on failure is identical to validate_or_raise; on success the
        returned model reflects the actual checks performed instead of a
        hardcoded "valid" result.

        Args:
            sql: SQL query string to validate.

        Returns:
            ValidationResult: Populated validation outcome.

        Raises:
            SQLParseError: If SQL cannot be parsed.
            SecurityViolationError: If SQL violates security constraints.
        """
        return self._analyze(sql)
```

`validate()`（返回 tuple 的那个）内部改调 `_analyze` 亦可（行为等价）。

- [ ] **Step 4: orchestrator 删除硬编码结果**

orchestrator.py:407-409 的：
```python
                try:
                    self.sql_validator.validate_or_raise(generated_sql)
```
改为：
```python
                try:
                    validation_result = self.sql_validator.validate_with_result(generated_sql)
```
并删除 orchestrator.py:447-454 的硬编码 `validation_result = ValidationResult(...)` 块（变量已由上一行赋值）。同时 orchestrator.py 顶部若 `ValidationResult` import 仅为该硬编码服务，则从 import 列表移除（mypy 会提示）。

- [ ] **Step 5: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_sql_validator.py tests/unit/test_orchestrator.py -v && uv run mypy src && uv run ruff check src tests`
Expected: 全部 PASS。orchestrator 现有测试中 `mock_validator.validate_or_raise.return_value = None` 的 mock 需同步改为 `validate_with_result`（返回 MagicMock 或真实 ValidationResult）。

---

### Task 6: SQLGenerator 返回 token 用量

**Files:**
- Modify: `src/pg_mcp/services/sql_generator.py`
- Test: `tests/unit/test_sql_generator.py`

**Interfaces:**
- Produces: `SQLGenerator.generate(...) -> tuple[str, int | None]`——第二元素为 `usage.prompt_tokens + usage.completion_tokens`，usage 缺失时为 None。
- Consumes: OpenAI `ChatCompletion.usage`。

- [ ] **Step 1: 更新/新增测试**

文件中所有 `mock_generator.generate.return_value = "SELECT ..."`（在 orchestrator 测试里也一样——本步骤只改 test_sql_generator.py，orchestrator 的留到 Task 7 统一改）。test_sql_generator.py 中所有构造 mock response 的地方补 `usage`：

```python
mock_response.usage = None  # 或 SimpleNamespace(prompt_tokens=10, completion_tokens=5)
```

新增测试（放入现有成功路径测试类）：

```python
    @pytest.mark.asyncio
    async def test_generate_returns_token_usage(self) -> None:
        """Token usage is extracted from response.usage."""
        generator = SQLGenerator(self.config)  # 沿用文件现有 config fixture/构造
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "```sql\nSELECT 1;\n```"
        mock_response.usage = SimpleNamespace(prompt_tokens=12, completion_tokens=8)
        with patch.object(
            generator.client.chat.completions, "create", new=AsyncMock(return_value=mock_response)
        ):
            sql, tokens = await generator.generate(
                question="q", schema=self._make_schema()  # 沿用文件现有 schema 构造方式
            )
        assert sql == "SELECT 1;"
        assert tokens == 20

    @pytest.mark.asyncio
    async def test_generate_returns_none_tokens_when_usage_missing(self) -> None:
        generator = SQLGenerator(self.config)
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1;"
        mock_response.usage = None
        with patch.object(
            generator.client.chat.completions, "create", new=AsyncMock(return_value=mock_response)
        ):
            _, tokens = await generator.generate(question="q", schema=self._make_schema())
        assert tokens is None
```

顶部补 `from types import SimpleNamespace`。文件中既有断言 `sql = await generator.generate(...)` 的用例统一改为解包 `sql, _ = await generator.generate(...)`。

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_sql_generator.py -v`
Expected: FAIL — 现有测试解包失败 / 新用例中 generate 返回 str 不是 tuple

- [ ] **Step 3: 实现**

sql_generator.py `generate` 返回类型注解改为 `-> tuple[str, int | None]`；docstring Returns 段改为 "tuple[str, int | None]: (generated SQL without trailing semicolon, total tokens used or None if usage metadata is unavailable)"。在 `sql = self._extract_sql(content)` 之前插入：

```python
        # Extract token usage for cost tracking (tokens_used in responses/metrics)
        usage = getattr(response, "usage", None)
        tokens_used: int | None = None
        if usage is not None:
            tokens_used = int(usage.prompt_tokens) + int(usage.completion_tokens)
```

函数末尾 `return sql` 改为 `return sql, tokens_used`。

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_sql_generator.py -v && uv run mypy src && uv run ruff check src tests`
Expected: PASS（orchestrator 此时会因接口变化而红——**先跑本文件确认，orchestrator 测试的修复归 Task 7**；若希望每步全绿，可把本任务与 Task 7 顺序对调）。

---

### Task 7: Orchestrator 集成 — 限流、瞬态重试、指标、tracing、长度检查、置信度语义

**Files:**
- Modify: `src/pg_mcp/config/settings.py`（ResilienceConfig 增 3 个字段）
- Modify: `src/pg_mcp/observability/metrics.py`（增 3 个读取辅助方法）
- Modify: `src/pg_mcp/services/orchestrator.py`（主体改造）
- Test: `tests/unit/test_orchestrator.py`、`tests/unit/test_resilience.py`（ResilienceConfig 字段）

**Interfaces:**
- Produces:
  - `ResilienceConfig.max_concurrent_queries: int = 10`、`max_concurrent_llm_calls: int = 5`、`rate_limit_timeout: float = 60.0`（env: `RESILIENCE_MAX_CONCURRENT_QUERIES` 等）。
  - `MetricsCollector.get_query_request_count(status, database) -> int`、`get_llm_call_count(operation) -> int`、`get_sql_rejected_count(reason) -> int`、`observe_query_duration(seconds) -> None`。
  - `QueryOrchestrator.__init__(..., sql_executors, ..., rate_limiter: MultiRateLimiter | None = None, metrics: MetricsCollector | None = None)`。
  - 结果校验异常时置信度返回 **50**（中性值），不再返回 100；`enabled=False` 时仍返回 100（CLAUDE.md 约定保留）。审查报告关于"thresholds ignored"的说法部分不实——`confidence_threshold` 本就在 ResultValidator 内生效（result_validator.py:172），本次补齐的是低于阈值时的 warning 日志与失败路径语义。
- Consumes: Task 3 `retry_async / is_transient_db_error`；tracing `request_context`；Task 6 generate 返回 tuple。

- [ ] **Step 1: settings — ResilienceConfig 字段测试先行**

test_config.py 追加：

```python
class TestResilienceConfigNewFields:
    """ResilienceConfig: rate limit knobs."""

    def test_defaults(self) -> None:
        config = ResilienceConfig()
        assert config.max_concurrent_queries == 10
        assert config.max_concurrent_llm_calls == 5
        assert config.rate_limit_timeout == 60.0

    def test_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RESILIENCE_MAX_CONCURRENT_QUERIES", "20")
        monkeypatch.setenv("RESILIENCE_MAX_CONCURRENT_LLM_CALLS", "8")
        monkeypatch.setenv("RESILIENCE_RATE_LIMIT_TIMEOUT", "30.0")
        config = ResilienceConfig()
        assert config.max_concurrent_queries == 20
        assert config.max_concurrent_llm_calls == 8
        assert config.rate_limit_timeout == 30.0
```

settings.py `ResilienceConfig` 的 `circuit_breaker_timeout` 后追加：

```python
    max_concurrent_queries: int = Field(
        default=10, ge=1, le=1000, description="Maximum concurrent database queries"
    )
    max_concurrent_llm_calls: int = Field(
        default=5, ge=1, le=1000, description="Maximum concurrent LLM API calls"
    )
    rate_limit_timeout: float = Field(
        default=60.0, ge=1.0, le=600.0,
        description="Max seconds to wait for a rate limiter slot before failing",
    )
```

- [ ] **Step 2: metrics.py — 读取辅助**

`MetricsCollector` 末尾（`reset_all_metrics` 之前）追加：

```python
    def observe_query_duration(self, duration_seconds: float) -> None:
        """Record end-to-end query request duration.

        Args:
            duration_seconds: Wall-clock duration of a full query request.
        """
        self.query_duration.observe(duration_seconds)

    def get_query_request_count(self, status: str, database: str) -> int:
        """Read current query request counter value (tests/diagnostics)."""
        return int(self.query_requests.labels(status=status, database=database)._value.get())

    def get_llm_call_count(self, operation: str) -> int:
        """Read current LLM call counter value (tests/diagnostics)."""
        return int(self.llm_calls.labels(operation=operation)._value.get())

    def get_sql_rejected_count(self, reason: str) -> int:
        """Read current SQL rejection counter value (tests/diagnostics)."""
        return int(self.sql_rejected.labels(reason=reason)._value.get())
```

（`_value` 是 prometheus_client Counter 的公开约定内部量，收敛在这三个方法里，不外溢。）

- [ ] **Step 3: orchestrator 主体改造**

完整替换 orchestrator.py 相应部分：

1. import 区：删除 `import uuid`；补
```python
import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from pg_mcp.observability.metrics import MetricsCollector, metrics as default_metrics
from pg_mcp.observability.tracing import request_context
from pg_mcp.resilience.rate_limiter import MultiRateLimiter
from pg_mcp.resilience.retry import is_transient_db_error, retry_async
```
错误类 import 补 `RateLimitExceededError`、`PgMcpError`（`LLMTimeoutError`、`LLMUnavailableError` 供 `_is_transient_llm_error` 使用）。

2. 构造函数签名（在 Task 4 基础上）追加两个可选参数并赋值：

```python
        rate_limiter: MultiRateLimiter | None = None,
        metrics: MetricsCollector | None = None,
```
```python
        self.rate_limiter = rate_limiter
        self.metrics = metrics if metrics is not None else default_metrics
```

3. `execute_query` 骨架（保留 Step 1–7 的业务顺序与全部日志字段，改造点加粗说明）：

```python
    async def execute_query(self, request: QueryRequest) -> QueryResponse:
        request_id_holder: list[str] = []

        async with request_context() as request_id:
            request_id_holder.append(request_id)
            start = time.monotonic()
            database_name: str | None = None
            status = "success"

            logger.info(
                "Starting query execution",
                extra={"request_id": request_id, "question": request.question[:100]},
            )
            try:
                self._check_question_length(request.question)          # 新增
                database_name = self._resolve_database(request.database)
                # …Step 2 schema 逻辑不变…
                # …Step 3 _generate_sql_with_retry 不变（内部改造见 4）…
                # …Step 4 return_type==SQL 提前返回不变（返回前置 status="success"）…
                # Step 5 执行（限流 + 瞬态重试 + 指标）：
                executor = self._get_executor(database_name)
                async with self._rate_limited("query"):
                    db_start = time.monotonic()
                    results, total_count = await retry_async(
                        lambda: executor.execute(generated_sql),
                        max_retries=self.resilience_config.max_retries,
                        retry_delay=self.resilience_config.retry_delay,
                        backoff_factor=self.resilience_config.backoff_factor,
                        retryable=is_transient_db_error,
                        operation_name=f"SQL execution on '{database_name}'",
                    )
                self.metrics.observe_db_query_duration(time.monotonic() - db_start)
                # …Step 6/7 不变…
            except PgMcpError as e:
                status = "error"
                if isinstance(e, (SecurityViolationError, SQLParseError)):
                    self.metrics.increment_sql_rejected(e.code.value)
                # …原有 warning 日志与错误响应构造不变…
            except Exception:
                status = "error"
                logger.exception(
                    "Query execution failed with unexpected error",
                    extra={"request_id": request_id},
                )
                # …原有内部错误响应构造不变…
            finally:
                self.metrics.observe_query_duration(time.monotonic() - start)
                self.metrics.increment_query_request(
                    status, database_name if database_name is not None else "unknown"
                )
```

注意：SQL-only 提前返回路径发生在 try 内 `return`，`finally` 仍会执行计数（status="success"，database 已解析）——符合预期。`generated_sql` 变量在 try 内赋值后供 except 之外不可达，维持原结构即可（原代码即如此）。

4. `_generate_sql_with_retry` 内部改造：
   - 循环体首次进入前增加瞬态重试分支。生成调用改为：
     ```python
     async with self._rate_limited("llm"):
         llm_start = time.monotonic()
         generated_sql, tokens_used = await self.sql_generator.generate(
             question=question,
             schema=schema,
             previous_attempt=previous_sql,
             error_feedback=error_feedback,
         )
     self.metrics.increment_llm_call("generate_sql")
     self.metrics.observe_llm_latency("generate_sql", time.monotonic() - llm_start)
     if tokens_used:
         self.metrics.increment_llm_tokens("generate_sql", tokens_used)
     ```
   - 校验调用改为 `validation_result = self.sql_validator.validate_with_result(generated_sql)`（Task 5 已做）。
   - except 结构调整为：
     ```python
     except (LLMError, SecurityViolationError, SQLParseError) as e:
         if _is_transient_llm_error(e) and attempt < max_retries:
             delay = self.resilience_config.retry_delay * (
                 self.resilience_config.backoff_factor ** attempt
             )
             logger.warning(
                 "Transient LLM error, retrying with backoff",
                 extra={"request_id": request_id, "attempt": attempt + 1, "delay": delay},
             )
             await asyncio.sleep(delay)
             continue
         if isinstance(e, (SecurityViolationError, SQLParseError)):
             raise  # 语义校验失败：维持原有"带反馈重试在循环内 continue 处理"的逻辑不变
         self.circuit_breaker.record_failure()
         raise
     except Exception as e:
         # …原有兜底不变…
     ```
     注意与既有"校验失败 → 记录 previous_sql/error_feedback → continue"分支的先后关系：**校验失败的 continue 分支保持在最内层 try（校验处）不动**；上述 except 仅处理生成阶段抛出的 LLM 瞬态错误，勿把语义校验错误引入退避路径。
   - 模块级辅助函数（文件底部或顶部，置于类外）：
     ```python
     def _is_transient_llm_error(error: Exception) -> bool:
         """Classify LLM errors worth retrying: timeouts and provider rate limits.

         Authentication failures are permanent and are not retried.
         """
         if isinstance(error, LLMTimeoutError):
             return True
         if isinstance(error, LLMUnavailableError):
             detail = error.details.get("error", "") if isinstance(error.details, dict) else ""
             return "rate_limit" in str(detail).lower()
         return False
     ```
   - 顶部 `tokens_used` 注释块（orchestrator.py:396-397 "Note: tokens_used would come from…"）删除——已真实提取。

5. 新增私有方法（类内，`_resolve_database` 附近）：

```python
    def _check_question_length(self, question: str) -> None:
        """Reject questions exceeding the configured maximum length.

        Args:
            question: User's natural language question.

        Raises:
            PgMcpError: With code QUESTION_TOO_LONG if over the limit.
        """
        max_length = self.validation_config.max_question_length
        if len(question) > max_length:
            raise PgMcpError(
                message=(
                    f"Question length {len(question)} exceeds maximum of {max_length} characters"
                ),
                code=ErrorCode.QUESTION_TOO_LONG,
                details={"question_length": len(question), "max_length": max_length},
            )

    @asynccontextmanager
    async def _rate_limited(self, kind: str) -> AsyncIterator[None]:
        """Run an operation under the shared rate limiter.

        Args:
            kind: "llm" or "query" — selects the limiter bucket.

        Yields:
            None

        Raises:
            RateLimitExceededError: If no slot becomes available within
                resilience_config.rate_limit_timeout seconds.
        """
        if self.rate_limiter is None:
            yield
            return
        timeout = self.resilience_config.rate_limit_timeout
        try:
            if kind == "llm":
                async with self.rate_limiter.for_llm(timeout=timeout):
                    yield
            else:
                async with self.rate_limiter.for_queries(timeout=timeout):
                    yield
        except TimeoutError as e:
            raise RateLimitExceededError(
                message=f"Rate limit exceeded while waiting for a {kind} slot",
                details={"kind": kind, "timeout_seconds": self.resilience_config.rate_limit_timeout},
            ) from e
```
（注意：`for_llm`/`for_queries` 是 `@asynccontextmanager` 工厂，必须直接 `async with ... for_llm(timeout=...)` 调用，不能先取出再传参。）
（import 补：`from collections.abc import AsyncIterator`、`from contextlib import asynccontextmanager`、`RateLimitExceededError`。）

6. `_validate_results_safely` 两处修改：
   - 成功路径补阈值 warning：
     ```python
     if not validation_result.is_acceptable:
         logger.warning(
             "Result confidence below configured threshold",
             extra={
                 "request_id": request_id,
                 "confidence": validation_result.confidence,
                 "threshold": self.validation_config.confidence_threshold,
             },
         )
     ```
   - 失败路径 `return 100` 改为 `return 50`，注释改为 `# Neutral confidence: validation could not produce a signal`。

- [ ] **Step 4: 更新/新增 orchestrator 测试**

1. 现有构造点补可选参数默认即可（rate_limiter/metrics 缺省为 None），无需逐个改；Task 4 已改的 `sql_executors` 保持。
2. 所有 `mock_generator.generate.return_value = "SELECT ..."` 改为 `("SELECT ...;", 42)`；所有 `mock_validator.validate_or_raise...` 改为 `mock_validator.validate_with_result.return_value = ValidationResult(is_valid=True, is_select=True)`（或 MagicMock）。文件内全文搜索确认无遗漏。
3. 若存在断言"验证失败后置信度为 100"的 `_validate_results_safely` 用例，改为断言 50；`enabled=False` → 100 的用例保留。
4. 追加新测试类：

```python
class TestQuestionLengthGuard:
    """Review Focus: boundary behavior of max_question_length."""

    def _orchestrator(self, max_length: int) -> QueryOrchestrator:
        return QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": MagicMock()},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(max_question_length=max_length),
        )

    @pytest.mark.asyncio
    async def test_question_at_limit_is_accepted(self) -> None:
        orch = self._orchestrator(max_length=20)
        gen = orch.sql_generator
        gen.generate.return_value = ("SELECT 1;", 5)
        orch.sql_validator.validate_with_result.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        cache = orch.schema_cache
        cache.get.return_value = DatabaseSchema(database_name="db", tables=[], version="15")
        response = await orch.execute_query(QueryRequest(question="x" * 20, database="db"))
        assert response.success

    @pytest.mark.asyncio
    async def test_question_over_limit_rejected_with_code(self) -> None:
        orch = self._orchestrator(max_length=20)
        response = await orch.execute_query(QueryRequest(question="x" * 21, database="db"))
        assert not response.success
        assert response.error is not None
        assert response.error.code == ErrorCode.QUESTION_TOO_LONG.value


class TestOrchestratorMetrics:
    """Metrics are emitted on success and failure paths."""

    @pytest.mark.asyncio
    async def test_success_increments_counters(self) -> None:
        from pg_mcp.observability.metrics import metrics as m

        orch = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": MagicMock()},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
            metrics=m,
        )
        orch.sql_generator.generate.return_value = ("SELECT 1;", 7)
        orch.sql_validator.validate_with_result.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        orch.sql_executors["db"].execute = AsyncMock(return_value=([{"n": 1}], 1))
        orch.result_validator.validate.return_value = ResultValidationResult(
            confidence=90, explanation="ok", suggestion=None, is_acceptable=True
        )
        orch.schema_cache.get.return_value = DatabaseSchema(
            database_name="db", tables=[], version="15"
        )

        before = m.get_query_request_count("success", "db")
        llm_before = m.get_llm_call_count("generate_sql")
        response = await orch.execute_query(QueryRequest(question="q", database="db"))
        assert response.success
        assert m.get_query_request_count("success", "db") == before + 1
        assert m.get_llm_call_count("generate_sql") == llm_before + 1
        assert response.tokens_used == 7

    @pytest.mark.asyncio
    async def test_security_rejection_increments_rejected_counter(self) -> None:
        from pg_mcp.observability.metrics import metrics as m

        orch = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": MagicMock()},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
            metrics=m,
        )
        orch.sql_generator.generate.return_value = ("DELETE FROM users;", 5)
        orch.sql_validator.validate_with_result.side_effect = SecurityViolationError(
            "DELETE statements are not allowed"
        )
        orch.schema_cache.get.return_value = DatabaseSchema(
            database_name="db", tables=[], version="15"
        )
        before = m.get_sql_rejected_count("security_violation")
        response = await orch.execute_query(QueryRequest(question="q", database="db"))
        assert not response.success
        assert m.get_sql_rejected_count("security_violation") == before + 1


class TestOrchestratorRetry:
    """Transient DB errors are retried; deterministic errors are not."""

    def _orchestrator(self, executor: AsyncMock) -> QueryOrchestrator:
        orch = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": executor},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=2, retry_delay=0.01),
            validation_config=ValidationConfig(),
        )
        orch.sql_generator.generate.return_value = ("SELECT 1;", 1)
        orch.sql_validator.validate_with_result.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        orch.schema_cache.get.return_value = DatabaseSchema(
            database_name="db", tables=[], version="15"
        )
        return orch

    @pytest.mark.asyncio
    async def test_transient_db_error_is_retried(self) -> None:
        executor = AsyncMock()
        transient = asyncpg.ConnectionDoesNotExistError("closed")
        executor.execute.side_effect = [
            DatabaseError(message="Database query failed", details={}),
            ([{"n": 1}], 1),
        ]
        executor.execute.side_effect = [
            _database_error_with_cause(transient),
            ([{"n": 1}], 1),
        ]
        orch = self._orchestrator(executor)
        response = await orch.execute_query(QueryRequest(question="q", database="db"))
        assert response.success
        assert executor.execute.await_count == 2

    @pytest.mark.asyncio
    async def test_deterministic_db_error_is_not_retried(self) -> None:
        executor = AsyncMock()
        executor.execute.side_effect = [
            _database_error_with_cause(asyncpg.PostgresError("syntax error")),
        ]
        orch = self._orchestrator(executor)
        response = await orch.execute_query(QueryRequest(question="q", database="db"))
        assert not response.success
        assert executor.execute.await_count == 1


def _database_error_with_cause(cause: Exception) -> DatabaseError:
    """Build a DatabaseError mimicking SQLExecutor's wrapping."""
    err = DatabaseError(message="Database query failed")
    err.__cause__ = cause
    return err
```

import 区补 `import asyncpg`。第一个重试测试中重复的 side_effect 赋值保留一处即可（上面代码中第一行赋值应删除，保留 `_database_error_with_cause` 版本）。

5. 限流测试（Review Focus #5）：

```python
class TestRateLimitIntegration:
    """Rate limiter saturation yields a clean error, never a raw TimeoutError."""

    @pytest.mark.asyncio
    async def test_rate_limit_timeout_converted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from pg_mcp.resilience.rate_limiter import MultiRateLimiter

        limiter = MultiRateLimiter(query_limit=1, llm_limit=1)
        orch = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": AsyncMock()},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(rate_limit_timeout=0.05),
            validation_config=ValidationConfig(enabled=False),
            rate_limiter=limiter,
        )
        orch.sql_generator.generate.return_value = ("SELECT 1;", 1)
        orch.sql_validator.validate_with_result.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        orch.schema_cache.get.return_value = DatabaseSchema(
            database_name="db", tables=[], version="15"
        )

        # Occupy the only query slot; the request must wait, time out, and
        # surface as a clean rate_limit_exceeded error.
        async with limiter.for_queries():
            response = await orch.execute_query(QueryRequest(question="q", database="db"))

        assert not response.success
        assert response.error is not None
        assert response.error.code == ErrorCode.RATE_LIMIT_EXCEEDED.value
```

import 区补 `import asyncio`（若 Task 3 已加则复用）。

- [ ] **Step 5: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_orchestrator.py tests/unit/test_resilience.py tests/unit/test_config.py -v && uv run mypy src && uv run ruff check src tests`
Expected: 全部 PASS。

---

### Task 8: Server 最终接线 + health 资源

**Files:**
- Modify: `src/pg_mcp/server.py`
- Test: `tests/unit/test_server_health.py`（新建）

**Interfaces:**
- Produces: MCP resource `health://status` → `{"status": "ok"|"uninitialized", "databases": [...], "cache_enabled": bool, "metrics_enabled": bool, "circuit_breaker_state": str | None}`。
- Consumes: Task 4/7 后的 orchestrator 构造签名；`ResilienceConfig` 限流字段。

- [ ] **Step 1: 写失败测试（新建 tests/unit/test_server_health.py）**

```python
"""Unit tests for the server health resource."""

import pytest


class TestHealthStatus:
    """health://status resource payload."""

    def test_uninitialized_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pg_mcp.server as server_module

        monkeypatch.setattr(server_module, "_orchestrator", None)
        monkeypatch.setattr(server_module, "_pools", None)
        monkeypatch.setattr(server_module, "_settings", None)
        status = server_module._health_status()
        assert status["status"] == "uninitialized"
        assert status["databases"] == []
        assert status["circuit_breaker_state"] is None

    def test_ready_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pg_mcp.server as server_module

        class FakeOrchestrator:
            circuit_breaker = type("CB", (), {"state": "closed"})()

        fake = FakeOrchestrator()
        settings = type("S", (), {"cache": type("C", (), {"enabled": True})(),
                                  "observability": type("O", (), {"metrics_enabled": False})()})()
        monkeypatch.setattr(server_module, "_orchestrator", fake)
        monkeypatch.setattr(server_module, "_pools", {"db1": object(), "db2": object()})
        monkeypatch.setattr(server_module, "_settings", settings)
        status = server_module._health_status()
        assert status["status"] == "ok"
        assert status["databases"] == ["db1", "db2"]
        assert status["cache_enabled"] is True
        assert status["metrics_enabled"] is False
        assert status["circuit_breaker_state"] == "closed"


@pytest.mark.asyncio
async def test_health_resource_is_registered() -> None:
    """The FastMCP resource is registered on the mcp instance."""
    from pg_mcp.server import mcp

    resources = await mcp.list_resources()
    uris = {str(r.uri) for r in resources}
    assert "health://status" in uris
```

- [ ] **Step 2: 运行测试确认失败**

Run: `uv run pytest tests/unit/test_server_health.py -v`
Expected: FAIL — `no attribute _health_status`

- [ ] **Step 3: 实现**

server.py 中（`mcp = FastMCP(...)` 之后、`query` tool 之前）追加：

```python
def _health_status() -> dict[str, Any]:
    """Build the health status payload from current server globals."""
    return {
        "status": "ok" if _orchestrator is not None else "uninitialized",
        "databases": sorted(_pools.keys()) if _pools else [],
        "cache_enabled": bool(_settings and _settings.cache.enabled),
        "metrics_enabled": bool(_settings and _settings.observability.metrics_enabled),
        "circuit_breaker_state": (
            str(_orchestrator.circuit_breaker.state) if _orchestrator is not None else None
        ),
    }


@mcp.resource("health://status")
def health_status() -> dict[str, Any]:
    """Server health: configured databases and component status."""
    return _health_status()
```

同文件 lifespan 内 rate limiter 构造改为读配置（server.py:187-190）：

```python
        _rate_limiter = MultiRateLimiter(
            query_limit=_settings.resilience.max_concurrent_queries,
            llm_limit=_settings.resilience.max_concurrent_llm_calls,
        )
```

orchestrator 构造补传 `rate_limiter=_rate_limiter`。server.py:71-72 的全局声明中删除 `_circuit_breaker`（Task 4 已删），确认 `_rate_limiter` 仍在。

- [ ] **Step 4: 运行测试确认通过**

Run: `uv run pytest tests/unit/test_server_health.py -v && uv run mypy src && uv run ruff check src tests`
Expected: PASS。

---

### Task 9: ResultValidator 单元测试（mock OpenAI，审查 Priority 4.1）

**Files:**
- Test: `tests/unit/test_result_validator.py`（新建）

**Interfaces:**
- Consumes: `ResultValidator(openai_config, validation_config)`；mock 方式沿用 test_sql_generator.py 的 `patch.object(validator.client.chat.completions, "create", new=AsyncMock(...))` 模式。注意 `OpenAIConfig` 的 api_key 校验要求 `sk-` 前缀，构造用 `OpenAIConfig(api_key="sk-test")`。

- [ ] **Step 1: 写测试**

```python
"""Unit tests for ResultValidator (mocked OpenAI client)."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_mcp.config.settings import OpenAIConfig, ValidationConfig
from pg_mcp.models.errors import LLMError, LLMTimeoutError, LLMUnavailableError
from pg_mcp.services.result_validator import ResultValidator


def _make_validator(validation_config: ValidationConfig | None = None) -> ResultValidator:
    return ResultValidator(
        openai_config=OpenAIConfig(api_key="sk-test"),
        validation_config=validation_config or ValidationConfig(),
    )


def _response(content: str) -> MagicMock:
    response = MagicMock()
    response.choices = [MagicMock()]
    response.choices[0].message.content = content
    return response


class TestResultValidator:
    """Behavior coverage for the previously untested ResultValidator."""

    @pytest.mark.asyncio
    async def test_disabled_returns_confidence_100(self) -> None:
        validator = _make_validator(ValidationConfig(enabled=False))
        result = await validator.validate(
            question="q", sql="SELECT 1", results=[], row_count=0
        )
        assert result.confidence == 100
        assert result.is_acceptable

    @pytest.mark.asyncio
    async def test_success_high_confidence_acceptable(self) -> None:
        validator = _make_validator()
        payload = json.dumps(
            {"confidence": 85, "explanation": "matches", "suggestion": None}
        )
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=_response(payload)),
        ):
            result = await validator.validate(
                question="q", sql="SELECT 1", results=[{"x": 1}], row_count=1
            )
        assert result.confidence == 85
        assert result.is_acceptable is True  # default threshold 70
        assert result.explanation == "matches"

    @pytest.mark.asyncio
    async def test_below_threshold_not_acceptable(self) -> None:
        validator = _make_validator(ValidationConfig(confidence_threshold=90))
        payload = json.dumps({"confidence": 60, "explanation": "weak", "suggestion": None})
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=_response(payload)),
        ):
            result = await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )
        assert result.confidence == 60
        assert result.is_acceptable is False

    @pytest.mark.asyncio
    async def test_confidence_out_of_range_clamped(self) -> None:
        validator = _make_validator()
        payload = json.dumps({"confidence": 150, "explanation": "x", "suggestion": None})
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=_response(payload)),
        ):
            result = await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )
        assert result.confidence == 100

    @pytest.mark.asyncio
    async def test_invalid_json_returns_moderate_confidence(self) -> None:
        validator = _make_validator()
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=_response("not json {")),
        ):
            result = await validator.validate(
                question="q", sql="SELECT 1", results=[], row_count=0
            )
        assert result.confidence == 60
        assert result.is_acceptable is False

    @pytest.mark.asyncio
    async def test_empty_choices_raises_llm_error(self) -> None:
        validator = _make_validator()
        response = MagicMock()
        response.choices = []
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=response),
        ):
            with pytest.raises(LLMError):
                await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)

    @pytest.mark.asyncio
    async def test_timeout_raises_llm_timeout(self) -> None:
        validator = _make_validator()
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(side_effect=TimeoutError("timed out")),
        ):
            with pytest.raises(LLMTimeoutError):
                await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)

    @pytest.mark.asyncio
    async def test_auth_failure_raises_llm_unavailable(self) -> None:
        validator = _make_validator()
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(side_effect=Exception("authentication failed: invalid api_key")),
        ):
            with pytest.raises(LLMUnavailableError):
                await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)

    @pytest.mark.asyncio
    async def test_sample_rows_limit_applied(self) -> None:
        """Only validation_config.sample_rows rows reach the LLM prompt."""
        validator = _make_validator(ValidationConfig(sample_rows=2))
        captured_kwargs: dict[str, object] = {}

        async def fake_create(**kwargs: object) -> MagicMock:
            captured_kwargs.update(kwargs)
            return _response(json.dumps({"confidence": 80, "explanation": "e"}))

        with patch.object(validator.client.chat.completions, "create", new=fake_create):
            await validator.validate(
                question="q",
                sql="SELECT 1",
                results=[{"x": i} for i in range(10)],
                row_count=10,
            )

        messages = captured_kwargs["messages"]
        user_prompt = messages[1]["content"]
        # The prompt header reports sampled-vs-total; sampling happens in the
        # validator (results[:sample_rows]) before prompt building.
        assert "showing 2 of 10 rows" in user_prompt
```

- [ ] **Step 2: 运行测试**

Run: `uv run pytest tests/unit/test_result_validator.py -v`
Expected: 全部 PASS（这是纯新增测试任务；若有用例失败，修复的是测试对行为的错误假设而非产品代码——逐一核对 result_validator.py 实际行为）。

---

### Task 10: 文档收尾 + 全量验证

**Files:**
- Modify: `.env.example`（RESILIENCE 段补 3 个新字段说明）
- Verify: 全部

- [ ] **Step 1: .env.example RESILIENCE 段补字段**

在 `RESILIENCE_CIRCUIT_BREAKER_TIMEOUT=60` 之后追加：

```
# Maximum number of concurrent database queries
# RESILIENCE_MAX_CONCURRENT_QUERIES=10

# Maximum number of concurrent LLM API calls
# RESILIENCE_MAX_CONCURRENT_LLM_CALLS=5

# Max seconds to wait for a rate limiter slot before failing the request
# RESILIENCE_RATE_LIMIT_TIMEOUT=60
```

- [ ] **Step 2: 全量单元测试**

Run: `uv run pytest tests/unit/ -v`
Expected: 全部 PASS

- [ ] **Step 3: 类型检查与 Lint**

Run: `uv run mypy src && uv run ruff check src tests && uv run ruff format --check src tests`
Expected: 零错误（format 若有差异，运行 `uv run ruff format src tests` 后重跑测试）

- [ ] **Step 4: 覆盖率阈值**

Run: `uv run pytest tests/unit/ --cov=src --cov-report=term --cov-fail-under=80`
Expected: 通过 ≥80% 阈值；若不达标，为缺口模块补最小单元测试后再验。

- [ ] **Step 5: 冒烟启动验证（无外部依赖部分）**

Run: `uv run python -c "from pg_mcp.server import mcp, _health_status; print(_health_status()); print('import ok')"`
Expected: 打印 `{'status': 'uninitialized', ...}` 与 `import ok`（不连数据库即可导入、health 函数可用）。

- [ ] **Step 6: 对照审查报告逐项复核**

对照 `0006-pg-mcp-code-review.md` 的 Recommendations 表逐条确认：
- 1.1 多库 → Task 4；1.2 安全配置 + 长度检查 → Task 2/7
- 2.1 限流 → Task 7；2.2 retry.py → Task 3/7；2.3 指标/tracing/health → Task 7/8
- 3.1 to_dict/ErrorDetail/tokens_used → Task 1/6；3.2 冗余熔断器/无用配置字段 → Task 4/2
- 4.x 测试 → Task 7/8/9 的新增测试覆盖限流、多库路由、validator、ResultValidator；mock 化的 integration/e2e 重构不在本计划范围（保持标记 `@pytest.mark.integration` 不进 CI 单测）

输出一份简短对照清单（每项：审查条目 → 落点文件/测试），向用户汇报。
