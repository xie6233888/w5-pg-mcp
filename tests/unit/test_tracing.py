"""Unit tests for request tracing helpers.

Covers request-id generation/propagation and the tracing decorators/logger
that carry the request id into log records.
"""

import logging

import pytest

from pg_mcp.observability.tracing import (
    TracingLogger,
    clear_request_id,
    generate_request_id,
    get_request_id,
    get_tracing_logger,
    request_context,
    set_request_id,
    trace_async,
    trace_sync,
)


class TestRequestIdContext:
    """The request-id contextvar."""

    def test_generate_returns_unique_ids(self) -> None:
        assert generate_request_id() != generate_request_id()

    def test_set_and_get_roundtrip(self) -> None:
        set_request_id("req-1")
        try:
            assert get_request_id() == "req-1"
        finally:
            clear_request_id()

    def test_clear_resets_to_none(self) -> None:
        set_request_id("req-1")
        clear_request_id()
        assert get_request_id() is None

    @pytest.mark.asyncio
    async def test_request_context_provides_and_restores(self) -> None:
        assert get_request_id() is None
        async with request_context() as request_id:
            assert request_id
            assert get_request_id() == request_id
        assert get_request_id() is None

    @pytest.mark.asyncio
    async def test_request_context_accepts_explicit_id(self) -> None:
        async with request_context("explicit-id") as request_id:
            assert request_id == "explicit-id"


class TestTraceDecorators:
    """trace_async / trace_sync attach the request id to log records."""

    @pytest.mark.asyncio
    async def test_trace_async_injects_request_id(self, caplog: pytest.LogCaptureFixture) -> None:
        @trace_async(operation="demo_async")
        async def work() -> str:
            logging.getLogger("demo.async").info("inside")
            return "done"

        with caplog.at_level(logging.INFO):
            async with request_context("req-async"):
                result = await work()

        assert result == "done"
        record = next(r for r in caplog.records if r.message == "inside")
        assert record.request_id == "req-async"
        assert record.operation == "demo_async"

    @pytest.mark.asyncio
    async def test_trace_async_without_context_still_runs(self) -> None:
        @trace_async()
        async def work() -> str:
            return "done"

        assert await work() == "done"

    def test_trace_sync_injects_request_id(self, caplog: pytest.LogCaptureFixture) -> None:
        @trace_sync(operation="demo_sync")
        def work() -> str:
            logging.getLogger("demo.sync").info("inside sync")
            return "done"

        set_request_id("req-sync")
        try:
            with caplog.at_level(logging.INFO):
                result = work()
        finally:
            clear_request_id()

        assert result == "done"
        record = next(r for r in caplog.records if r.message == "inside sync")
        assert record.request_id == "req-sync"
        assert record.operation == "demo_sync"

    def test_trace_sync_without_context_still_runs(self) -> None:
        @trace_sync()
        def work() -> str:
            return "done"

        assert work() == "done"


class TestTracingLogger:
    """TracingLogger adds the ambient request id to every record."""

    def test_adds_request_id_when_context_present(self, caplog: pytest.LogCaptureFixture) -> None:
        logger = get_tracing_logger("demo.tracing")
        assert isinstance(logger, TracingLogger)

        set_request_id("req-logger")
        try:
            with caplog.at_level(logging.INFO):
                logger.info("hello")
        finally:
            clear_request_id()

        record = next(r for r in caplog.records if r.message == "hello")
        assert record.request_id == "req-logger"

    def test_explicit_request_id_is_not_overwritten(self, caplog: pytest.LogCaptureFixture) -> None:
        logger = get_tracing_logger("demo.tracing.explicit")

        set_request_id("ambient")
        try:
            with caplog.at_level(logging.INFO):
                logger.info("hello", extra={"request_id": "explicit"})
        finally:
            clear_request_id()

        record = next(r for r in caplog.records if r.message == "hello")
        assert record.request_id == "explicit"

    def test_all_levels_and_exception(self, caplog: pytest.LogCaptureFixture) -> None:
        logger = get_tracing_logger("demo.tracing.levels")

        with caplog.at_level(logging.DEBUG):
            logger.debug("d")
            logger.warning("w")
            logger.error("e")
            logger.critical("c")
            logger.exception("x")

        messages = {r.message for r in caplog.records}
        assert {"d", "w", "e", "c", "x"} <= messages
