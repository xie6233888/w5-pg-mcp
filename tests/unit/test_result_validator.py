"""Unit tests for ResultValidator (mocked OpenAI client)."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_mcp.config.settings import OpenAIConfig, ValidationConfig
from pg_mcp.models.errors import LLMError, LLMTimeoutError, LLMUnavailableError
from pg_mcp.services.result_validator import ResultValidator


def _make_validator(validation_config: ValidationConfig | None = None) -> ResultValidator:
    """Create a ResultValidator with a dummy API key and given validation config."""
    return ResultValidator(
        openai_config=OpenAIConfig(api_key="sk-test"),
        validation_config=validation_config or ValidationConfig(),
    )


def _response(content: str) -> MagicMock:
    """Build a minimal ChatCompletion-shaped mock carrying `content`."""
    response = MagicMock()
    response.choices = [MagicMock()]
    response.choices[0].message.content = content
    return response


class TestResultValidator:
    """Behaviour coverage for the previously untested ResultValidator."""

    @pytest.mark.asyncio
    async def test_disabled_returns_confidence_100(self) -> None:
        validator = _make_validator(ValidationConfig(enabled=False))
        result = await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)
        assert result.confidence == 100
        assert result.is_acceptable

    @pytest.mark.asyncio
    async def test_success_high_confidence_acceptable(self) -> None:
        validator = _make_validator()
        payload = json.dumps({"confidence": 85, "explanation": "matches", "suggestion": None})
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=_response(payload)),
        ):
            result = await validator.validate(
                question="q", sql="SELECT 1", results=[{"x": 1}], row_count=1
            )
        assert result.confidence == 85
        assert result.is_acceptable is True  # default threshold is 70
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
            result = await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)
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
            result = await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)
        assert result.confidence == 100

    @pytest.mark.asyncio
    async def test_invalid_json_returns_moderate_confidence(self) -> None:
        validator = _make_validator()
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(return_value=_response("not json {")),
        ):
            result = await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)
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
        ), pytest.raises(LLMError):
            await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)

    @pytest.mark.asyncio
    async def test_timeout_raises_llm_timeout(self) -> None:
        validator = _make_validator()
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(side_effect=TimeoutError("timed out")),
        ), pytest.raises(LLMTimeoutError):
            await validator.validate(question="q", sql="SELECT 1", results=[], row_count=0)

    @pytest.mark.asyncio
    async def test_auth_failure_raises_llm_unavailable(self) -> None:
        validator = _make_validator()
        with patch.object(
            validator.client.chat.completions,
            "create",
            new=AsyncMock(side_effect=Exception("authentication failed: invalid api_key")),
        ), pytest.raises(LLMUnavailableError):
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
