"""Unit tests for the MCP `query` tool handler in server.py.

The handler is the MCP boundary: it must never raise, and every failure has to
come back as a structured error dict.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_mcp.models.query import QueryResponse


@pytest.fixture
def server_module(monkeypatch: pytest.MonkeyPatch):
    """Import server with `_orchestrator` reset to None (uninitialized)."""
    import pg_mcp.server as sm

    monkeypatch.setattr(sm, "_orchestrator", None)
    return sm


class TestQueryToolErrors:
    """Error paths all return structured dicts, never raise."""

    @pytest.mark.asyncio
    async def test_uninitialized_server(self, server_module) -> None:
        result = await server_module.query(question="count users")
        assert result["success"] is False
        assert result["error"]["code"] == "SERVER_NOT_INITIALIZED"

    @pytest.mark.asyncio
    async def test_invalid_return_type(self, server_module, monkeypatch) -> None:
        monkeypatch.setattr(server_module, "_orchestrator", MagicMock())
        result = await server_module.query(question="count users", return_type="yaml")
        assert result["success"] is False
        assert result["error"]["code"] == "INVALID_PARAMETER"
        assert result["error"]["details"] == {"return_type": "yaml"}

    @pytest.mark.asyncio
    async def test_invalid_request_when_question_blank(self, server_module, monkeypatch) -> None:
        monkeypatch.setattr(server_module, "_orchestrator", MagicMock())
        result = await server_module.query(question="   ")
        assert result["success"] is False
        assert result["error"]["code"] == "INVALID_REQUEST"

    @pytest.mark.asyncio
    async def test_orchestrator_exception_becomes_internal_error(
        self, server_module, monkeypatch
    ) -> None:
        orchestrator = MagicMock()
        orchestrator.execute_query = AsyncMock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr(server_module, "_orchestrator", orchestrator)

        result = await server_module.query(question="count users")

        assert result["success"] is False
        assert result["error"]["code"] == "INTERNAL_ERROR"
        assert result["tokens_used"] == 0


class TestQueryToolSuccess:
    """The success path returns the orchestrator's serialized response."""

    @pytest.mark.asyncio
    async def test_success_returns_serialized_response(self, server_module, monkeypatch) -> None:
        orchestrator = MagicMock()
        orchestrator.execute_query = AsyncMock(
            return_value=QueryResponse(
                success=True,
                generated_sql="SELECT 1;",
                confidence=90,
                tokens_used=12,
            )
        )
        monkeypatch.setattr(server_module, "_orchestrator", orchestrator)

        result = await server_module.query(question="count users", database="db")

        assert result["success"] is True
        assert result["generated_sql"] == "SELECT 1;"
        assert result["confidence"] == 90
        assert result["tokens_used"] == 12

    @pytest.mark.asyncio
    async def test_tokens_used_defaults_to_zero(self, server_module, monkeypatch) -> None:
        orchestrator = MagicMock()
        orchestrator.execute_query = AsyncMock(
            return_value=QueryResponse(success=True, generated_sql="SELECT 1;")
        )
        monkeypatch.setattr(server_module, "_orchestrator", orchestrator)

        result = await server_module.query(question="count users")

        assert result["tokens_used"] == 0
