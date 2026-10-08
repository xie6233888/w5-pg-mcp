"""Tests for structured logging configuration.

The critical invariant here is the output stream: this server speaks MCP over
stdio, so stdout carries JSON-RPC frames and nothing else. A log line written
to stdout interleaves with protocol traffic and the client fails to parse it.
"""

from collections.abc import Iterator

import pytest

from pg_mcp.observability.logging import configure_logging, get_logger


@pytest.fixture
def restore_root_logger() -> Iterator[None]:
    """Snapshot and restore the root logger around a test."""
    import logging

    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


class TestLoggingOutputStream:
    """Regression: logs must never reach stdout."""

    @pytest.mark.parametrize("log_format", ["json", "text"])
    def test_logs_go_to_stderr(
        self,
        capsys: pytest.CaptureFixture[str],
        restore_root_logger: None,
        log_format: str,
    ) -> None:
        configure_logging(level="INFO", log_format=log_format)

        get_logger("pg_mcp.test").info("hello from the server")

        captured = capsys.readouterr()
        assert captured.out == "", "stdout must stay clean for the MCP JSON-RPC stream"
        assert "hello from the server" in captured.err

    def test_error_logs_also_go_to_stderr(
        self, capsys: pytest.CaptureFixture[str], restore_root_logger: None
    ) -> None:
        configure_logging(level="INFO", log_format="json")

        get_logger("pg_mcp.test").error("something broke")

        captured = capsys.readouterr()
        assert captured.out == ""
        assert "something broke" in captured.err
