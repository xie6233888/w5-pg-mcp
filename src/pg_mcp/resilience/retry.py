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
