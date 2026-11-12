"""Unity Gateway access — one tiny place for every model call.

All LLM and embedding calls in this project go through **Unity Gateway**
(governed Databricks model serving). Notebooks never build an OpenAI client
themselves; they call `gateway_client()` once and pass it in.

Two things the pipeline needs:
  * `chat_json(...)`  — ask a chat model for a one-line JSON answer.
  * `embed(...)`      — turn text into vectors for pgvector retrieval.

Both retry a few times on transient errors (rate limits, 5xx) so a demo
doesn't die on a blip. That's the only "clever" part, and it's contained
in `_retry`.
"""

from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# HTTP statuses / messages we treat as transient and worth retrying.
_RETRYABLE = ("429", "500", "502", "503", "504", "rate limit",
              "timeout", "timed out", "connection", "unavailable")


def gateway_client():
    """Return an OpenAI-compatible client pointed at Unity Gateway."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient().serving_endpoints.get_open_ai_client()


def _retry(fn: Callable[[], Any], attempts: int = 5, base_delay: float = 2.0) -> Any:
    """Run `fn()`, retrying transient errors with exponential backoff."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as err:  # noqa: BLE001 - re-raised below if not transient
            transient = any(s in str(err).lower() for s in _RETRYABLE)
            if attempt >= attempts or not transient:
                raise
            sleep_for = random.uniform(0, min(30.0, base_delay * 2 ** (attempt - 1)))
            logger.info("retrying (%d/%d) after %.1fs: %s", attempt, attempts, sleep_for, err)
            time.sleep(sleep_for)


def _first_json_object(text: str) -> Optional[str]:
    """Return the first balanced ``{...}`` object in `text`.

    Scans with a brace counter that honours double-quoted strings and
    backslash escapes, so objects with nested braces (or braces inside string
    values) are captured whole — unlike a flat ``\\{[^{}]*\\}`` regex, which
    stops at the first inner ``}``.
    """
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    in_str = False
    escaped = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def _extract_json(text: str) -> Optional[dict]:
    """Pull the first JSON object out of a possibly-noisy model reply."""
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    snippet = _first_json_object(text)
    if snippet:
        try:
            return json.loads(snippet)
        except Exception:
            return None
    return None


def chat_json(client, endpoint: str, system: str, user: str, max_tokens: int = 256) -> Optional[dict]:
    """Send one chat request and return the model's reply parsed as JSON.

    Returns None if the model's reply can't be parsed — the caller decides
    what that means (we record it as a `parse_error`).
    """
    resp = _retry(lambda: client.chat.completions.create(
        model=endpoint,
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        temperature=0.0,
        max_tokens=max_tokens,
    ))
    return _extract_json((resp.choices[0].message.content or "").strip())


def embed(client, endpoint: str, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts. Returns one vector per input, in order."""
    resp = _retry(lambda: client.embeddings.create(model=endpoint, input=texts))
    return [d.embedding for d in resp.data]
