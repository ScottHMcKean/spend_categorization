"""Tests for src.llm (Unity Gateway helpers)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.llm import _extract_json, _retry, chat_json, embed


def test_retry_succeeds_after_one_transient(monkeypatch) -> None:
    monkeypatch.setattr("src.llm.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("429 rate limit exceeded")
        return "ok"

    assert _retry(fn, attempts=3, base_delay=0.01) == "ok"
    assert calls["n"] == 2


def test_retry_gives_up_after_attempts(monkeypatch) -> None:
    monkeypatch.setattr("src.llm.time.sleep", lambda *_: None)

    def fn() -> None:
        raise RuntimeError("503 service unavailable")

    with pytest.raises(RuntimeError):
        _retry(fn, attempts=3, base_delay=0.01)


def test_retry_passes_through_non_transient() -> None:
    def fn() -> None:
        raise ValueError("400 bad request")

    with pytest.raises(ValueError):
        _retry(fn, attempts=5, base_delay=0.01)


def test_extract_json_plain() -> None:
    assert _extract_json('{"code": "X", "confidence": 4}') == {"code": "X", "confidence": 4}


def test_extract_json_from_noise() -> None:
    assert _extract_json('here you go: {"code":"Y"} thanks') == {"code": "Y"}


def test_extract_json_unparseable() -> None:
    assert _extract_json("no json here") is None


def test_chat_json_parses_reply() -> None:
    client = MagicMock()
    message = SimpleNamespace(content='{"code": "42", "confidence": 5}')
    client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=message)]
    )
    assert chat_json(client, "ep", "sys", "user") == {"code": "42", "confidence": 5}


def test_embed_returns_vectors() -> None:
    client = MagicMock()
    client.embeddings.create.return_value = SimpleNamespace(
        data=[SimpleNamespace(embedding=[0.1, 0.2]), SimpleNamespace(embedding=[0.3, 0.4])]
    )
    assert embed(client, "ep", ["a", "b"]) == [[0.1, 0.2], [0.3, 0.4]]
