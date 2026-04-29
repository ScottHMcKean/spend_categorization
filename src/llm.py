"""Shared LLM helpers — wraps the Foundation Models OpenAI client with
robust retry logic for transient errors (429s, 5xx, connection drops).

Use it from anywhere we make per-row LLM calls so we don't reinvent the
backoff loop in every notebook.
"""

from __future__ import annotations

import logging
import random
import re
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


# Errors we treat as transient — retry these.
_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}
_RETRYABLE_PATTERNS = (
    "REQUEST_LIMIT_EXCEEDED",
    "rate limit",
    "ratelimit",
    "too many requests",
    "timeout",
    "timed out",
    "connection reset",
    "connection refused",
    "connection aborted",
    "Server disconnected",
    "Bad Gateway",
    "Service Unavailable",
)


def _retry_after_seconds(err: Exception) -> Optional[float]:
    """Extract a `Retry-After` hint from an OpenAI/HTTP error if present."""
    headers = getattr(getattr(err, "response", None), "headers", None) or {}
    if hasattr(headers, "get"):
        ra = headers.get("Retry-After") or headers.get("retry-after")
        if ra:
            try:
                return float(ra)
            except (TypeError, ValueError):
                pass
    msg = str(err)
    m = re.search(r"retry[ -]?after[\s:=]+(\d+(?:\.\d+)?)", msg, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _is_retryable(err: Exception) -> bool:
    status = getattr(err, "status_code", None) or getattr(
        getattr(err, "response", None), "status_code", None
    )
    if status in _RETRYABLE_STATUS:
        return True
    msg = str(err)
    if any(p.lower() in msg.lower() for p in _RETRYABLE_PATTERNS):
        return True
    if " 4" in msg and any(s in msg for s in ("429", "408", "425")):
        return True
    if " 5" in msg and any(s in msg for s in ("500", "502", "503", "504")):
        return True
    return False


def call_with_retry(
    fn: Callable[[], Any],
    *,
    max_attempts: int = 8,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
) -> Any:
    """Run ``fn()`` with full-jitter exponential backoff on transient errors.

    Args:
        fn: zero-arg callable performing the request.
        max_attempts: total attempts (1 = no retry).
        base_delay: starting backoff in seconds.
        max_delay: cap on per-attempt sleep.
        on_retry: optional ``(attempt, err, sleep_for)`` hook for logging.

    Raises:
        the original exception if all attempts are exhausted or the error
        is non-retryable.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn()
        except Exception as err:  # noqa: BLE001 — we re-raise non-retryable
            if attempt >= max_attempts or not _is_retryable(err):
                raise
            hinted = _retry_after_seconds(err)
            cap = min(max_delay, base_delay * (2 ** (attempt - 1)))
            sleep_for = hinted if hinted is not None else random.uniform(0, cap)
            if on_retry is not None:
                try:
                    on_retry(attempt, err, sleep_for)
                except Exception:
                    pass
            logger.info(
                "retryable error on attempt %d/%d (sleep %.2fs): %s",
                attempt, max_attempts, sleep_for, err,
            )
            time.sleep(sleep_for)


def chat_with_retry(
    client: Any,
    *,
    model: str,
    messages: list[dict[str, str]],
    max_tokens: int = 64,
    temperature: float = 0.0,
    max_attempts: int = 8,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
    **extra: Any,
) -> Any:
    """Convenience wrapper around ``client.chat.completions.create``."""
    return call_with_retry(
        lambda: client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **extra,
        ),
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        on_retry=on_retry,
    )
