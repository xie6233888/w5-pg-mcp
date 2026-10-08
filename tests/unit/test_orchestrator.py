"""Unit tests for QueryOrchestrator.

This module tests the orchestrator's coordination of the query pipeline,
including retry logic, error handling, and integration with all components.
"""

from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest

from pg_mcp.config.settings import ResilienceConfig, ValidationConfig
from pg_mcp.models.errors import (
    DatabaseError,
    ErrorCode,
    LLMError,
    SecurityViolationError,
    SQLParseError,
)
from pg_mcp.models.query import (
    QueryRequest,
    ResultValidationResult,
    ReturnType,
    ValidationResult,
)
from pg_mcp.models.schema import ColumnInfo, DatabaseSchema, TableInfo
from pg_mcp.resilience.circuit_breaker import CircuitState
from pg_mcp.services.orchestrator import QueryOrchestrator


def _valid_validation() -> ValidationResult:
    """A passing SQL validation result, for validator mocks."""
    return ValidationResult(is_valid=True, is_select=True)


class TestDatabaseResolution:
    """Test database name resolution logic."""

    @pytest.fixture
    def mock_pools(self) -> dict[str, MagicMock]:
        """Create mock connection pools."""
        return {
            "db1": MagicMock(),
            "db2": MagicMock(),
        }

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

    def test_resolve_database_specified_valid(self, orchestrator: QueryOrchestrator) -> None:
        """Test resolving a specified valid database."""
        result = orchestrator._resolve_database("db1")
        assert result == "db1"

    def test_resolve_database_specified_invalid(self, orchestrator: QueryOrchestrator) -> None:
        """Test resolving a specified but invalid database."""
        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database("nonexistent")

        assert "not found" in str(exc_info.value).lower()
        assert "db1" in exc_info.value.details["available_databases"]
        assert "db2" in exc_info.value.details["available_databases"]

    def test_resolve_database_auto_select_single(self) -> None:
        """Test auto-selecting when only one database available."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"only_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"only_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        result = orchestrator._resolve_database(None)
        assert result == "only_db"

    def test_resolve_database_auto_select_multiple_fails(
        self, orchestrator: QueryOrchestrator
    ) -> None:
        """Test that auto-select fails when multiple databases available."""
        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database(None)

        assert "multiple databases" in str(exc_info.value).lower()
        assert "db1" in exc_info.value.details["available_databases"]

    def test_resolve_database_no_databases(self) -> None:
        """Test error when no databases configured."""
        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        with pytest.raises(DatabaseError) as exc_info:
            orchestrator._resolve_database(None)

        assert "no databases configured" in str(exc_info.value).lower()


class TestSQLGenerationWithRetry:
    """Test SQL generation with retry logic."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="name",
                            data_type="varchar(255)",
                            is_nullable=False,
                        ),
                    ],
                )
            ],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_generate_sql_success_first_attempt(self, mock_schema: DatabaseSchema) -> None:
        """Test successful SQL generation on first attempt."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("SELECT * FROM users;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.return_value = _valid_validation()

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(),
        )

        # Execute
        sql, validation_result, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
        )

        # Verify
        assert sql == "SELECT * FROM users;"
        assert validation_result.is_valid is True
        assert validation_result.is_select is True
        mock_generator.generate.assert_called_once()
        mock_validator.validate_with_result.assert_called_once_with("SELECT * FROM users;")

    @pytest.mark.asyncio
    async def test_generate_sql_retry_on_validation_failure(
        self, mock_schema: DatabaseSchema
    ) -> None:
        """Test retry logic when validation fails."""
        # Setup mocks - first attempt fails validation, second succeeds
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = [
            ("SELECT * FROM user;", 10),  # First attempt (wrong table name)
            ("SELECT * FROM users;", 12),  # Second attempt (correct)
        ]

        mock_validator = MagicMock()
        # First call raises error, second call succeeds
        mock_validator.validate_with_result.side_effect = [
            SQLParseError('relation "user" does not exist'),
            ValidationResult(is_valid=True, is_select=True),  # Success on second attempt
        ]

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=3),
            validation_config=ValidationConfig(),
        )

        # Execute
        sql, validation_result, _tokens = await orchestrator._generate_sql_with_retry(
            question="Get all users",
            schema=mock_schema,
            request_id="test-123",
        )

        # Verify
        assert sql == "SELECT * FROM users;"
        assert validation_result.is_valid is True
        assert mock_generator.generate.call_count == 2
        assert mock_validator.validate_with_result.call_count == 2

        # Verify retry included error feedback
        second_call = mock_generator.generate.call_args_list[1]
        assert second_call.kwargs["previous_attempt"] == "SELECT * FROM user;"
        assert 'relation "user" does not exist' in second_call.kwargs["error_feedback"]

    @pytest.mark.asyncio
    async def test_generate_sql_fails_after_max_retries(self, mock_schema: DatabaseSchema) -> None:
        """Test failure after exhausting all retries."""
        # Setup mocks - all attempts fail validation
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("DELETE FROM users;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.side_effect = SecurityViolationError(
            "DELETE statements are not allowed"
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=2),
            validation_config=ValidationConfig(),
        )

        # Execute and verify exception
        with pytest.raises(SecurityViolationError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Delete all users",
                schema=mock_schema,
                request_id="test-123",
            )

        assert "DELETE statements are not allowed" in str(exc_info.value)
        # Should attempt max_retries + 1 times (initial + retries)
        assert mock_generator.generate.call_count == 3
        assert orchestrator.circuit_breaker.failure_count == 1

    @pytest.mark.asyncio
    async def test_generate_sql_circuit_breaker_open(self, mock_schema: DatabaseSchema) -> None:
        """Test that open circuit breaker prevents SQL generation."""
        orchestrator = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(circuit_breaker_threshold=1),
            validation_config=ValidationConfig(),
        )

        # Manually open the circuit breaker
        orchestrator.circuit_breaker._state = CircuitState.OPEN
        orchestrator.circuit_breaker._failure_count = 5

        # Attempt should fail immediately
        with pytest.raises(LLMError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Get all users",
                schema=mock_schema,
                request_id="test-123",
            )

        assert "temporarily unavailable" in str(exc_info.value).lower()
        assert "circuit breaker" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_generate_sql_unexpected_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of unexpected errors during generation."""
        mock_generator = AsyncMock()
        mock_generator.generate.side_effect = RuntimeError("Unexpected error")

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=1),
            validation_config=ValidationConfig(),
        )

        with pytest.raises(LLMError) as exc_info:
            await orchestrator._generate_sql_with_retry(
                question="Get all users",
                schema=mock_schema,
                request_id="test-123",
            )

        assert "unexpectedly" in str(exc_info.value).lower()
        assert orchestrator.circuit_breaker.failure_count == 1


class TestResultValidation:
    """Test result validation logic."""

    @pytest.mark.asyncio
    async def test_validate_results_success(self) -> None:
        """Test successful result validation."""
        mock_validator = AsyncMock()
        mock_validator.validate.return_value = ResultValidationResult(
            confidence=85,
            explanation="Results match the question well",
            suggestion=None,
            is_acceptable=True,
        )

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 85
        mock_validator.validate.assert_called_once()

    @pytest.mark.asyncio
    async def test_validate_results_disabled(self) -> None:
        """Test that validation is skipped when disabled."""
        mock_validator = AsyncMock()

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=False),
        )

        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 100
        mock_validator.validate.assert_not_called()

    @pytest.mark.asyncio
    async def test_validate_results_failure_does_not_raise(self) -> None:
        """Test that validation failures don't raise exceptions."""
        mock_validator = AsyncMock()
        mock_validator.validate.side_effect = Exception("Validation failed")

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=mock_validator,
            schema_cache=MagicMock(),
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        # Should not raise; a failed validation yields neutral (not maximal) confidence
        confidence = await orchestrator._validate_results_safely(
            question="Count users",
            sql="SELECT COUNT(*) FROM users",
            results=[{"count": 42}],
            row_count=1,
            request_id="test-123",
        )

        assert confidence == 50


class TestExecuteQueryFlow:
    """Test complete query execution flow."""

    @pytest.fixture
    def mock_schema(self) -> DatabaseSchema:
        """Create mock database schema."""
        return DatabaseSchema(
            database_name="test_db",
            tables=[
                TableInfo(
                    schema_name="public",
                    table_name="users",
                    columns=[
                        ColumnInfo(
                            name="id",
                            data_type="integer",
                            is_nullable=False,
                            is_primary_key=True,
                        ),
                        ColumnInfo(
                            name="name",
                            data_type="varchar(255)",
                            is_nullable=False,
                        ),
                    ],
                )
            ],
            version="15.0",
        )

    @pytest.mark.asyncio
    async def test_execute_query_sql_only(self, mock_schema: DatabaseSchema) -> None:
        """Test executing query with return_type=SQL."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("SELECT * FROM users;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.return_value = _valid_validation()

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        assert response.generated_sql == "SELECT * FROM users;"
        assert response.validation is not None
        assert response.validation.is_valid is True
        assert response.data is None  # No execution for SQL-only
        assert response.error is None

    @pytest.mark.asyncio
    async def test_execute_query_with_results(self, mock_schema: DatabaseSchema) -> None:
        """Test executing query with return_type=RESULT."""
        # Setup mocks
        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("SELECT id, name FROM users;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.return_value = _valid_validation()

        mock_executor = AsyncMock()
        mock_executor.execute.return_value = (
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            2,  # total count
        )

        mock_result_validator = AsyncMock()
        mock_result_validator.validate.return_value = ResultValidationResult(
            confidence=90,
            explanation="Good results",
            suggestion=None,
            is_acceptable=True,
        )

        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=mock_result_validator,
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(enabled=True),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.RESULT,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        assert response.generated_sql == "SELECT id, name FROM users;"
        assert response.data is not None
        assert response.data.row_count == 2
        assert len(response.data.rows) == 2
        assert response.data.columns == ["id", "name"]
        assert response.confidence == 90
        assert response.error is None

    @pytest.mark.asyncio
    async def test_execute_query_schema_not_cached(self) -> None:
        """Test loading schema when not in cache."""
        mock_schema = DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = None  # Not in cache
        mock_cache.load = AsyncMock(return_value=mock_schema)

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("SELECT 1;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.return_value = _valid_validation()

        mock_pool = MagicMock()

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": mock_pool},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify schema was loaded
        mock_cache.load.assert_called_once_with("test_db", mock_pool)
        assert response.success is True

    @pytest.mark.asyncio
    async def test_execute_query_schema_load_fails(self) -> None:
        """Test handling of schema load failure."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = None
        mock_cache.load = AsyncMock(side_effect=Exception("DB connection failed"))

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Test query",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "schema" in response.error.message.lower()
        assert response.generated_sql is None

    @pytest.mark.asyncio
    async def test_execute_query_validation_error(self) -> None:
        """Test handling of SQL validation errors."""
        mock_schema = DatabaseSchema(
            database_name="test_db",
            tables=[],
            version="15.0",
        )

        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("DELETE FROM users;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.side_effect = SecurityViolationError(
            "DELETE not allowed"
        )

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(max_retries=1),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Delete all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "DELETE not allowed" in response.error.message
        assert response.error.code == "security_violation"

    @pytest.mark.asyncio
    async def test_execute_query_execution_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of SQL execution errors."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("SELECT * FROM users;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.return_value = _valid_validation()

        mock_executor = AsyncMock()
        mock_executor.execute.side_effect = DatabaseError("Query execution failed")

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": mock_executor},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.RESULT,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert "execution failed" in response.error.message.lower()
        assert response.error.code == "database_error"

    @pytest.mark.asyncio
    async def test_execute_query_unexpected_error(self, mock_schema: DatabaseSchema) -> None:
        """Test handling of unexpected errors."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.side_effect = RuntimeError("Unexpected error")

        orchestrator = QueryOrchestrator(
            sql_generator=MagicMock(),
            sql_validator=MagicMock(),
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"test_db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute
        request = QueryRequest(
            question="Get all users",
            database="test_db",
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify error response
        assert response.success is False
        assert response.error is not None
        assert response.error.code == "internal_error"
        assert "internal server error" in response.error.message.lower()

    @pytest.mark.asyncio
    async def test_execute_query_auto_select_database(self, mock_schema: DatabaseSchema) -> None:
        """Test auto-selecting database when only one available."""
        # Setup mocks
        mock_cache = MagicMock()
        mock_cache.get.return_value = mock_schema

        mock_generator = AsyncMock()
        mock_generator.generate.return_value = ("SELECT 1;", 42)

        mock_validator = MagicMock()
        mock_validator.validate_with_result.return_value = _valid_validation()

        orchestrator = QueryOrchestrator(
            sql_generator=mock_generator,
            sql_validator=mock_validator,
            sql_executors={"test_db": MagicMock()},
            result_validator=MagicMock(),
            schema_cache=mock_cache,
            pools={"only_db": MagicMock()},  # Only one database
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(),
        )

        # Execute without specifying database
        request = QueryRequest(
            question="Test query",
            database=None,  # No database specified
            return_type=ReturnType.SQL,
        )
        response = await orchestrator.execute_query(request)

        # Verify
        assert response.success is True
        # Verify schema was fetched for auto-selected database
        mock_cache.get.assert_called_once_with("only_db")


class TestExecutorSelection:
    """Tests for per-database executor selection."""

    @pytest.mark.asyncio
    async def test_execute_selects_executor_by_resolved_name(self) -> None:
        """The request's database determines which executor runs the SQL."""
        exec_a, exec_b = AsyncMock(), AsyncMock()
        exec_a.execute.return_value = ([{"n": 1}], 1)
        exec_b.execute.return_value = ([{"m": 2}], 1)
        gen = AsyncMock()
        gen.generate.return_value = ("SELECT 1;", 42)
        val = MagicMock()
        val.validate_with_result.return_value = _valid_validation()
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
        response = await orch.execute_query(QueryRequest(question="count", database="db2"))
        assert response.success
        exec_b.execute.assert_awaited_once()
        exec_a.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_executor_raises_clean_error(self) -> None:
        """Executor dict missing a pool name must yield DatabaseError, not KeyError."""
        gen = AsyncMock()
        gen.generate.return_value = ("SELECT 1;", 42)
        val = MagicMock()
        val.validate_with_result.return_value = _valid_validation()
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


def _database_error_with_cause(cause: Exception) -> DatabaseError:
    """Build a DatabaseError mimicking how SQLExecutor wraps asyncpg errors."""
    err = DatabaseError(message="Database query failed")
    err.__cause__ = cause
    return err


class TestQuestionLengthGuard:
    """Review Focus: boundary behavior of max_question_length."""

    def _orchestrator(self, max_length: int) -> QueryOrchestrator:
        executor = AsyncMock()
        executor.execute.return_value = ([{"n": 1}], 1)
        return QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": executor},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(),
            validation_config=ValidationConfig(max_question_length=max_length),
        )

    @pytest.mark.asyncio
    async def test_question_at_limit_is_accepted(self) -> None:
        orch = self._orchestrator(max_length=20)
        orch.sql_generator.generate.return_value = ("SELECT 1;", 5)
        orch.sql_validator.validate_with_result.return_value = ValidationResult(
            is_valid=True, is_select=True
        )
        orch.schema_cache.get.return_value = DatabaseSchema(
            database_name="db", tables=[], version="15"
        )
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
            resilience_config=ResilienceConfig(max_retries=2, retry_delay=0.1),
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


class TestRateLimitIntegration:
    """Rate limiter saturation yields a clean error, never a raw TimeoutError."""

    @pytest.mark.asyncio
    async def test_rate_limit_timeout_converted(self) -> None:
        from pg_mcp.resilience.rate_limiter import MultiRateLimiter

        limiter = MultiRateLimiter(query_limit=1, llm_limit=1)
        orch = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": AsyncMock()},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(rate_limit_timeout=1.0),
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

    @pytest.mark.asyncio
    async def test_llm_rate_limit_is_not_reported_as_llm_error(self) -> None:
        """Saturating the *LLM* limiter must surface as rate_limit_exceeded.

        It must also not be charged to the circuit breaker: load spikes are
        not LLM faults, and accumulating failures would open the circuit and
        turn transient saturation into a hard outage.
        """
        from pg_mcp.resilience.rate_limiter import MultiRateLimiter

        limiter = MultiRateLimiter(query_limit=1, llm_limit=1)
        executor = AsyncMock()
        executor.execute.return_value = ([{"n": 1}], 1)
        orch = QueryOrchestrator(
            sql_generator=AsyncMock(),
            sql_validator=MagicMock(),
            sql_executors={"db": executor},
            result_validator=AsyncMock(),
            schema_cache=MagicMock(),
            pools={"db": MagicMock()},
            resilience_config=ResilienceConfig(rate_limit_timeout=1.0),
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

        failures_before = orch.circuit_breaker.failure_count
        async with limiter.for_llm():
            response = await orch.execute_query(QueryRequest(question="q", database="db"))

        assert not response.success
        assert response.error is not None
        assert response.error.code == ErrorCode.RATE_LIMIT_EXCEEDED.value
        assert orch.circuit_breaker.failure_count == failures_before
