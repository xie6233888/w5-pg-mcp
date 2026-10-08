"""Pytest configuration and shared fixtures.

This module provides shared fixtures and configuration for all tests.
"""

import os

import pytest

from pg_mcp.config.settings import (
    CacheConfig,
    DatabaseConfig,
    ObservabilityConfig,
    OpenAIConfig,
    ResilienceConfig,
    SecurityConfig,
    Settings,
    ValidationConfig,
    reset_settings,
)

_SETTINGS_CLASSES = (
    Settings,
    DatabaseConfig,
    OpenAIConfig,
    SecurityConfig,
    ValidationConfig,
    CacheConfig,
    ResilienceConfig,
    ObservabilityConfig,
)


@pytest.fixture(autouse=True)
def reset_config() -> None:
    """Reset global settings before each test."""
    reset_settings()


@pytest.fixture(autouse=True)
def isolate_from_dotenv(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep tests hermetic with respect to a developer's local .env file.

    Every settings class loads .env from the working directory, so a populated
    .env -- which the README instructs developers to create -- would silently
    override the defaults that tests assert. Tests that deliberately exercise
    .env loading opt out with the ``reads_dotenv`` marker.
    """
    if request.node.get_closest_marker("reads_dotenv") is not None:
        return
    for settings_cls in _SETTINGS_CLASSES:
        monkeypatch.setitem(settings_cls.model_config, "env_file", None)


@pytest.fixture(autouse=True)
def disable_metrics_for_tests():
    """Disable metrics for tests to avoid port conflicts."""
    os.environ["OBSERVABILITY_METRICS_ENABLED"] = "false"
    yield
    # Clean up
    if "OBSERVABILITY_METRICS_ENABLED" in os.environ:
        del os.environ["OBSERVABILITY_METRICS_ENABLED"]
