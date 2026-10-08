# PostgreSQL MCP Server — 代码审查修复说明

## 文档信息

| 项目 | 内容 |
|------|------|
| 日期 | 2026-10-08 |
| 依据 | `0006-pg-mcp-code-review.md`（代码审查报告） |
| 实施计划 | `docs/superpowers/plans/2026-10-08-pg-mcp-review-fixes.md` |
| 分支 | `fix/code-review-findings`（自 `master` 分出，13 个提交） |
| 基线提交 | `30c8515` |
| 范围 | 审查报告 Priority 1–3 全部修复 + Priority 4 中与本次改动相关的测试 |

---

## 一、结论摘要

审查报告中的全部声明经逐条对照源码验证后**均属实**。本次修复的核心是把"配置了但没接线"的组件真正接入请求路径，并修掉模型层与响应层的缺陷。

三条最关键的问题（审查报告未提及，实施过程中发现）：

1. **`.env` 配置文件对所有嵌套配置段完全无效** — `DATABASE_*`、`SECURITY_*`、`OBSERVABILITY_*` 等写在 `.env` 里会被静默忽略，运维以为配好了安全策略，实际毫无保护。
2. **列表型配置的 CSV 写法会让服务启动即崩溃** — pydantic-settings 对 list 字段强行走 JSON 解码，而 `.env.example` 文档化的正是 CSV 写法（既有的 `blocked_functions` 也中招）。
3. **非主库的 SQL executor 拿到的是主库的连接配置** — 多库场景下会连错库。

---

## 二、变更总览

### 对照审查报告 Recommendations

| 审查条目 | 落点文件 | 状态 |
|---|---|---|
| 1.1 实现多库支持 | `config/settings.py`、`db/pool.py`、`server.py`、`services/orchestrator.py` | ✅ |
| 1.2 恢复安全控制 | `config/settings.py`、`server.py`、`services/sql_validator.py` | ✅ |
| 2.1 接入限流 | `services/orchestrator.py`、`server.py` | ✅ |
| 2.2 实现重试/退避 | `resilience/retry.py`（新建）、`services/orchestrator.py` | ✅ |
| 2.3 指标 / tracing / health | `observability/metrics.py`、`services/orchestrator.py`、`server.py` | ✅ |
| 3.1 修模型 bug | `models/errors.py`、`models/query.py`、`services/sql_generator.py` | ✅ |
| 3.2 清理无用代码 | `server.py`、`config/settings.py` | ✅ |
| 4.x 补测试 | `tests/unit/` 六个测试文件 | ✅（集成/e2e mock 化不在范围内） |

### 提交清单

```
71e3bb6 fix: address code-review findings on the review-fixes branch
b02d3f5 test: cover the query tool boundary and tracing; close docs and coverage gaps
48948c8 test: add ResultValidator unit coverage with a mocked OpenAI client
4646d8b feat: health resource and configurable rate limiter on the server
434d823 feat: wire rate limiting, retry, metrics and tracing into the query path
a672e70 feat: SQLGenerator.generate returns (sql, tokens_used)
d73393b feat: SQLValidator.validate_with_result replaces hardcoded validation result
259f3eb feat: per-database executors with DATABASES multi-database config
170157c feat: add resilience/retry.py with exponential backoff
fa4c831 feat: add blocked_tables/blocked_columns/allow_explain to SecurityConfig
30cc47e test: fix ObservabilityConfig default log_format assertion
29c335c refactor: unify ErrorDetail and deduplicate QueryResponse.to_dict
```

---

## 三、逐项变更详情

### 3.1 多库支持（审查 §1.1 / 安全发现 #1）

**问题**：`_resolve_database` 能解析出库名，但 `QueryOrchestrator` 始终使用绑在默认池上的单一 executor——请求指定库 B，实际跑在库 A 上，构成跨库数据泄漏。

**变更**：

- `config/settings.py` — `Settings` 新增 `databases: list[DatabaseConfig]`（env `DATABASES` 为 JSON 数组）与 `all_databases` 属性（主库在前），并加唯一性校验：

```python
@property
def all_databases(self) -> list[DatabaseConfig]:
    """Primary database plus any additional databases (primary first)."""
    return [self.database, *self.databases]

@model_validator(mode="after")
def validate_database_names_unique(self) -> "Settings":
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"Duplicate database names configured: {duplicates}")
```

- `db/pool.py` — `create_pools` 由串行改为真并发（此前 docstring 声称并发但实现是 for 循环）：

```python
pool_list = await asyncio.gather(*(create_pool(config) for config in configs))
return {config.name: pool for config, pool in zip(configs, pool_list, strict=True)}
```

- `services/orchestrator.py` — 构造参数 `sql_executor` → `sql_executors: dict[str, SQLExecutor]`，执行前按解析出的库名取执行器：

```python
executor = self._get_executor(database_name)   # 缺失时抛 DatabaseError，而非 KeyError
```

- `server.py` — 为每个已配置库建池与 executor，**每个 executor 使用各自的 `DatabaseConfig`**（修正了非主库拿到主库配置的 bug）。

### 3.2 安全配置接线（审查 §1.2 / 安全发现 #2）

**问题**：`SecurityConfig` 没有 `blocked_tables`/`blocked_columns`/`allow_explain` 字段，`server.py` 以硬编码 `None`/`False` 构造校验器——敏感资源无法保护。

**变更**：三个字段加入 `SecurityConfig` 并接线到 `SQLValidator`；同时删除从未被读取的 `min_confidence_score`（真正生效的是 `confidence_threshold`）。

### 3.3 CSV 配置崩溃修复（实施中新发现）

**问题**：pydantic-settings 在 `EnvSettingsSource` 中对 list 字段强制 JSON 解码，`.env.example` 文档化的 CSV 写法（`SECURITY_BLOCKED_FUNCTIONS=pg_sleep,lo_import`）会在启动时抛 `SettingsError`。原设计的 `mode="before"` 校验器**根本不可达**。

**变更**：新增 CSV/JSON 双兼容的源，同时覆盖环境变量与 `.env` 文件两条路径：

```python
class _CsvOrJsonMixin:
    def prepare_field_value(self, field_name, field, value, value_is_complex):
        if isinstance(value, str) and _is_sequence_field(field):
            text = value.strip()
            if text.startswith("["):
                return json.loads(text)
            return [item.strip() for item in text.split(",") if item.strip()]
        return super().prepare_field_value(...)

class _CsvOrJsonEnvSource(_CsvOrJsonMixin, EnvSettingsSource): ...
class _CsvOrJsonDotEnvSource(_CsvOrJsonMixin, DotEnvSettingsSource): ...
```

### 3.4 `.env` 嵌套配置静默失效修复（实施中新发现）

**问题**：`Settings.model_config` 设了 `env_file=".env"`，但每个配置段都是独立的 `BaseSettings`（通过 `default_factory` 构造），**不会继承父级的 `env_file`**。实测（真实临时 `.env` 文件）：

```
environment  (顶层字段, .env) : staging      <- 文件生效
database.host (嵌套, .env)    : localhost    <- 文件被忽略
security.blocked_tables       : []           <- 文件被忽略
observability.log_level       : INFO         <- 文件被忽略
```

后果：运维在 `.env` 里设置 `SECURITY_BLOCKED_TABLES` 无任何报错也无任何保护，主库静默回落到 `localhost/postgres`。

**变更**：每个配置段声明 `env_file=".env"` 与 `extra="ignore"`（env 文件本就包含其他段的键，不能因多出的键而报错）。

### 3.5 重试与限流（审查 §2.1 / §2.2）

**新增 `resilience/retry.py`**：通用 `retry_async` + 瞬态错误判定。注意 `SQLExecutor` 会把 asyncpg 异常包装成 `DatabaseError` 并把原异常放进 `__cause__`，故判定需沿 cause 链回溯：

```python
TRANSIENT_DB_ERRORS = (
    asyncpg.ConnectionDoesNotExistError,
    asyncpg.ConnectionFailureError,
    asyncpg.InterfaceError,
)

def is_transient_db_error(error: Exception) -> bool:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TRANSIENT_DB_ERRORS):
            return True
        current = current.__cause__
    return False
```

**接入请求路径**（`services/orchestrator.py`）：

- LLM 调用与 DB 执行均置于 `MultiRateLimiter` 之下；限流超时转换为 `rate_limit_exceeded` 错误码，**绝不让裸 `TimeoutError` 穿透 MCP 边界**
- DB 瞬态错误重试（退避）；确定性错误（语法错、语句超时）**不重试**
- LLM 瞬态错误（超时、上游限流）重试；认证失败**不重试**
- 限流饱和**不计入熔断器**——否则流量高峰会把熔断器推到 open，把"服务器忙"变成"硬故障"

### 3.6 指标与 tracing（审查 §2.3）

- `observability/metrics.py` 新增读取辅助方法（`get_query_request_count` / `get_llm_call_count` / `get_sql_rejected_count` / `observe_query_duration`）
- orchestrator 在成功与失败路径均上报：查询计数与耗时、LLM 调用/延迟/token、SQL 拒绝原因
- `request_id` 发布到 tracing contextvar，供日志关联
- `server.py` 新增 MCP resource `health://status`，返回状态、已配置库列表、缓存/指标开关、熔断器状态

### 3.7 模型层清理与置信度语义（审查 §3.1 / Best Practice 2 / 3）

| 问题 | 修复 |
|---|---|
| `QueryResponse.to_dict` **重复定义**，第二个（`exclude_none=True`）覆盖第一个，导致 `tokens_used` 被丢弃 | 删除第二个定义；`tokens_used` 为 None 时序列化为 0 |
| 两个 `ErrorDetail` 类（Pydantic vs plain），plain 版从未被响应使用 | 统一为 `models/errors.py` 中的 Pydantic 版，`models/query.py` 再导出以保持向后兼容 |
| `tokens_used` 恒为 `None`（生成器从不提取用量） | `SQLGenerator.generate` 返回 `(sql, tokens_used)`，从 `response.usage` 提取 |
| orchestrator 硬编码 `ValidationResult(is_valid=True, ...)`，屏蔽真实校验结果 | 新增 `SQLValidator.validate_with_result`，校验逻辑抽为 `_analyze`，失败语义逐字节不变 |
| 结果校验失败时置信度返回 100，掩盖问题 | 改为中性值 **50**；`validation.enabled=false` 时仍返回 100（CLAUDE.md 约定保留） |
| `ValidationConfig.max_question_length` 从未被检查 | 新增 `_check_question_length`，超限返回 `question_too_long` |
| server 层熔断器实例化但从未使用 | 删除（orchestrator 自有熔断器） |

### 3.8 阻塞列黑名单绕过修复（实施中新发现）

**问题**：`_check_blocked_columns` 只按字面量比对。当按 `.env.example` 推荐写成限定名 `blocked_columns=["accounts.ssn"]` 时：

```
BLOCKED:          SELECT accounts.ssn FROM accounts
ALLOWED (bypass): SELECT ssn FROM accounts
ALLOWED (bypass): SELECT a.ssn FROM accounts a
```

即：文档推荐的写法**恰恰不设防**，而裸名写法反而有效——运维会产生虚假的安全感。

**变更**：限定条目同时屏蔽其裸列名（无 catalog 无法解析列的归属表，故取 fail-safe 的过度屏蔽），并修正 `.env.example` 的措辞。

---

## 四、测试与验证

### 新增/修改的测试

| 文件 | 内容 |
|---|---|
| `tests/unit/test_config.py` | 多库配置、SecurityConfig 新字段、CSV/JSON 双形式、ResilienceConfig 限流字段、**`.env` 文件生效回归测试** |
| `tests/unit/test_orchestrator.py` | 按库名路由 executor、执行器缺失错误、问题长度边界、指标计数、瞬态/确定性重试、**查询与 LLM 两条限流路径** |
| `tests/unit/test_sql_validator.py` | `validate_with_result`、限定名黑名单三种绕过写法 |
| `tests/unit/test_resilience.py` | `retry_async` 成功/重试/耗尽/不可重试/退避时序、瞬态错误判定 |
| `tests/unit/test_sql_generator.py` | token 用量提取与缺失时返回 None |
| `tests/unit/test_result_validator.py` | **新建**：此前零覆盖（禁用模式、阈值边界、置信度钳制、非法 JSON、空响应、超时、认证失败、采样上限） |
| `tests/unit/test_query_tool.py` | **新建**：MCP 边界全部错误码与成功路径（"永不抛异常"契约） |
| `tests/unit/test_tracing.py` | **新建**：request_id 传播与 tracing 装饰器/日志器 |

### 验证结果

| 检查项 | 命令 | 结果 |
|---|---|---|
| 单元测试 | `uv run pytest tests/unit/ -q` | **331 passed** |
| 类型检查 | `uv run mypy src` | **Success: no issues found in 31 source files**（strict） |
| Lint | `uv run ruff check src tests` | **All checks passed!** |
| 格式 | `uv run ruff format --check src tests` | **50 files already formatted** |
| 覆盖率 | `--cov=src --cov-fail-under=80` | **82.37%**（修复前 77.98%） |
| 冒烟 | `python -c "from pg_mcp.server import mcp, _health_status"` | 正常导入，health 返回 `uninitialized` |

### 集成/e2e 测试说明

`tests/integration/`、`tests/e2e/` 需真实 PostgreSQL + OpenAI key，本机无这些服务。已用同一命令在基线对照：

| 树 | 失败 | 通过 | 错误 |
|---|---|---|---|
| `master`（基线） | 30 | 248 | 1 |
| `fix/code-review-findings` | **29** | **331** | 1 |

失败数在基线上同样存在（本分支还少 1 个——修掉的那个坏测试），故为环境依赖而非本次改动引入。

> **注**：CLAUDE.md 称 integration/e2e 均标记 `@pytest.mark.integration`，实测仅 15 个被标记，其余约 29 个会污染任何裸 `pytest` 调用。CI 配置需注意，建议补齐标记。

---

## 五、实施过程中的裁决记录

| # | 裁决 | 依据 | 若有误的代价 |
|---|---|---|---|
| 1 | 安装 uv 0.12.23（系统代理失效，pip 加 `--proxy=""`） | 全部命令走 `uv run` | 机器级工具安装，可移除 |
| 2 | 在首个任务内修掉基线自带的 4 mypy + 3 ruff 错误 | 计划要求零错误门禁 | 触及任务清单外文件，均为机械修复 |
| 3 | 基线坏测试 `log_format == "json"` 改为先匹配代码 (`text`) | 当时认为文档支持 text | **后经审查者指出前提错误，已推翻**（见 #12） |
| 4 | 新增 CSV/JSON 双兼容配置源 | `mode="before"` 校验器不可达 | 约 20 行配置管道代码 |
| 5 | `min_confidence_score` 的 env 条目映射为 `VALIDATION_CONFIDENCE_THRESHOLD` | 后者才真正生效 | 旧变量名需改名（从未被读取，无行为变化） |
| 6 | 给多库测试补上必需的 API key 前置 | 否则 6 个测试全在 api_key 上假绿 | 测试比计划更严格 |
| 7 | 把 orchestrator 解包与 mock 更新提前到 Task 6 | 计划预期变红，实测全绿但接口已坏（mock 掩盖，mypy 抓到） | 保持每个提交可二分 |
| 8 | tracing 用 contextvar API 而非嵌套 `async with` | 嵌套需重排 ~160 行错误处理，行为等价 | 无（该层是最外层） |
| 9 | 提高测试值以符合配置自身下界 | 计划的测试值与它自己定义的 `ge` 冲突 | 两个测试各多耗时 |
| 10 | 为覆盖率缺口补 query-tool 与 tracing 测试 | 计划授权；77.98% < 80% 阈值 | 两个清单外测试文件 |
| 11 | 全仓执行 `ruff format` | 计划要求；基线本就不干净 | 一个与审查无关的文件 |
| 12 | **推翻 #3**：`log_format` 代码默认改为 `json`，断言复原 | `.env.example`/README/docker-compose 三处都写 json，我的原前提有误 | 默认日志输出由 text 变 json |
| 13 | 修掉部署文件里的 `MIN_CONFIDENCE` 残留 | 属本次改动自身的遗留 | 安全修复提交含两个部署文件 |

---

## 六、推迟事项（未修，供决策）

**功能/健壮性**

1. `QueryRequest` 硬编码 `max_length=10000` 遮蔽配置项——`VALIDATION_MAX_QUESTION_LENGTH` 设为 >10000 时被静默截断，客户端拿到 `INVALID_REQUEST` 而非 `question_too_long`
2. `create_pools` 部分失败会泄漏已建连接池（`gather` 抛首个异常，`_pools` 保持 `None`）
3. DB 重试的退避期间仍占查询限流槽（LLM 路径先释放）——可辩解为背压，但属未记录的**不对称**
4. `_rate_limited` 的 `except TimeoutError` 也包住被守护的代码体，内部抛出的 `TimeoutError` 会被误报为 `rate_limit_exceeded`（当前潜伏：`SQLExecutor` 会转换自身超时）
5. `query` tool 返回两种响应形状——错误路径缺 `data`/`validation`/`confidence`
6. CSV 列表解析在 `settings.py` 中重复实现三遍；`mode="before"` 的字符串分支仍无覆盖
7. `sql_generator.generate` docstring 称"无尾分号"，实际 `_extract_sql` 总是补上

**既有问题（不在审查报告范围）**

8. `SecurityConfig.allow_write_operations` 无任何生产代码读取（写操作本就无条件禁止）
9. `MetricsCollector.reset_all_metrics()` 会抛 `Duplicated timeseries`
10. tracing 装饰器与 `TracingLogger` 未被请求路径使用
11. `SQLValidator.ALLOWED_TOP_LEVEL` 为死常量；`db/pool.py` 局部重复 import `asyncio`
12. `main.py` 仍是遗留 stub，README 中 `uv run python main.py` 的说法有误

---

## 七、后续建议

1. **优先修推迟事项 1、2**——一个削弱输入保护，一个在启动失败路径上泄漏资源
2. **决定 `.env` 的定位**：本次已让嵌套段真正读取它；若后续改用 secret manager，需同步更新 CLAUDE.md 与 `.env.example`
3. **补齐 integration/e2e 的 `@pytest.mark.integration` 标记**，否则 CI 的裸 `pytest` 必然失败
4. **加一个跑真实 `SQLExecutor`（配假连接池）的分类器测试**，让 `is_transient_db_error` 面对真实的异常包装而非手工构造的 `__cause__`
5. `.env.example` 已同步全部新增配置项（`SECURITY_BLOCKED_TABLES/COLUMNS/ALLOW_EXPLAIN`、`RESILIENCE_MAX_CONCURRENT_QUERIES/LLM_CALLS/RATE_LIMIT_TIMEOUT`、`DATABASES`）
