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
        settings = type(
            "S",
            (),
            {
                "cache": type("C", (), {"enabled": True})(),
                "observability": type("O", (), {"metrics_enabled": False})(),
            },
        )()
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
