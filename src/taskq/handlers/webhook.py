from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import httpx
from pydantic import BaseModel

from taskq.errors import FatalError, RetriableError
from taskq.registry import registry
from taskq.settings import Settings

_client: Optional[httpx.AsyncClient] = None
_settings: Optional[Settings] = None


class WebhookPayload(BaseModel):
    delivery_id: str
    subscription_id: str
    event_id: str
    target_url: str
    event_type: str
    body: dict


def _get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        s = _get_settings()
        timeout = httpx.Timeout(
            connect=s.TASKQ_HTTP_CONNECT_TIMEOUT_S,
            read=s.TASKQ_HTTP_READ_TIMEOUT_S,
            write=s.TASKQ_HTTP_READ_TIMEOUT_S,
            pool=s.TASKQ_HTTP_CONNECT_TIMEOUT_S,
        )
        _client = httpx.AsyncClient(timeout=timeout)
    return _client


async def reset_client() -> None:
    """Test hook: drop the cached client so a different ASGITransport can be installed."""
    global _client, _settings
    if _client is not None:
        await _client.aclose()
    _client = None
    _settings = None


def _sign(secret: str, timestamp: str, body_bytes: bytes) -> str:
    signing_string = f"{timestamp}.".encode("utf-8") + body_bytes
    digest = hmac.new(secret.encode("utf-8"), signing_string, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _parse_retry_after(value: str) -> Optional[float]:
    value = value.strip()
    if not value:
        return None
    try:
        return float(int(value))
    except ValueError:
        pass
    try:
        seconds = float(value)
        if seconds >= 0:
            return seconds
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    delta = (when - datetime.now(timezone.utc)).total_seconds()
    return max(0.0, delta)


@registry.handler("webhook.deliver", payload_model=WebhookPayload, timeout_s=30)
async def deliver_webhook(payload: WebhookPayload) -> None:
    settings = _get_settings()
    client = _get_client()

    body_bytes = json.dumps(payload.body, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    signature = _sign(settings.TASKQ_WEBHOOK_SIGNING_SECRET, timestamp, body_bytes)

    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Id": payload.delivery_id,
        "X-Webhook-Event-Id": payload.event_id,
        "X-Webhook-Timestamp": timestamp,
        "X-Webhook-Signature": signature,
        "Idempotency-Key": payload.delivery_id,
    }

    try:
        response = await client.post(payload.target_url, content=body_bytes, headers=headers)
    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        raise RetriableError(f"network error: {exc!r}") from exc

    status = response.status_code

    if 200 <= status < 300:
        return

    if status >= 500:
        raise RetriableError(f"upstream returned {status}")

    if status in (408, 425):
        raise RetriableError(f"upstream returned {status}")

    if status == 429:
        retry_after_header = response.headers.get("Retry-After")
        retry_after = _parse_retry_after(retry_after_header) if retry_after_header else None
        err = RetriableError(f"upstream returned 429")
        if retry_after is not None:
            err.retry_after_seconds = retry_after  # type: ignore[attr-defined]
        raise err

    snippet = response.text[:200]
    raise FatalError(f"non-retriable {status}: {snippet}")
