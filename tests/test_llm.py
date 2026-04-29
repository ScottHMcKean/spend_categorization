"""Tests for src.llm retry helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.llm import _is_retryable, _retry_after_seconds, call_with_retry


class _Err(Exception):
    def __init__(self, msg: str, status_code: int | None = None, headers: dict | None = None):
        super().__init__(msg)
        self.status_code = status_code
        if headers is not None:
            resp = MagicMock()
            resp.status_code = status_code
            resp.headers = headers
            self.response = resp


def test_retryable_429() -> None:
    assert _is_retryable(_Err("REQUEST_LIMIT_EXCEEDED: foo", status_code=429))


def test_retryable_5xx_message() -> None:
    assert _is_retryable(_Err("Error code: 503 - Service Unavailable"))


def test_retryable_timeout() -> None:
    assert _is_retryable(_Err("connection timed out"))


def test_non_retryable_400() -> None:
    assert not _is_retryable(_Err("Bad request 400: invalid input", status_code=400))


def test_non_retryable_auth() -> None:
    assert not _is_retryable(_Err("401 unauthorized"))


def test_retry_after_header() -> None:
    err = _Err("rate limited", status_code=429, headers={"Retry-After": "7"})
    assert _retry_after_seconds(err) == 7.0


def test_retry_after_in_message() -> None:
    err = _Err("rate limited; retry-after: 12")
    assert _retry_after_seconds(err) == 12.0


def test_call_with_retry_succeeds_after_one_429(monkeypatch) -> None:
    monkeypatch.setattr("src.llm.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def fn() -> str:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _Err("REQUEST_LIMIT_EXCEEDED", status_code=429)
        return "ok"

    assert call_with_retry(fn, max_attempts=3, base_delay=0.01) == "ok"
    assert calls["n"] == 2


def test_call_with_retry_gives_up(monkeypatch) -> None:
    monkeypatch.setattr("src.llm.time.sleep", lambda *_: None)

    def fn() -> None:
        raise _Err("REQUEST_LIMIT_EXCEEDED", status_code=429)

    with pytest.raises(_Err):
        call_with_retry(fn, max_attempts=3, base_delay=0.01)


def test_call_with_retry_passes_through_non_retryable() -> None:
    def fn() -> None:
        raise _Err("400 bad request", status_code=400)

    with pytest.raises(_Err):
        call_with_retry(fn, max_attempts=5, base_delay=0.01)


def test_on_retry_hook_called(monkeypatch) -> None:
    monkeypatch.setattr("src.llm.time.sleep", lambda *_: None)
    seen = []

    def fn():
        if len(seen) < 2:
            raise _Err("429", status_code=429)
        return "done"

    def hook(attempt, err, delay):
        seen.append((attempt, delay))

    assert call_with_retry(fn, max_attempts=5, base_delay=0.01, on_retry=hook) == "done"
    assert len(seen) == 2
