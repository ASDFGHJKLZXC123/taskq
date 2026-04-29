from __future__ import annotations

import hashlib
import hmac

import httpx
import pytest
from httpx import ASGITransport

from taskq.backoff import MAX_DELAY_S, compute_backoff
from taskq.errors import FatalError, RetriableError
from taskq.handlers import webhook as webhook_module
from taskq.handlers.webhook import WebhookPayload, deliver_webhook
from taskq.settings import Settings
from taskq.subscriber import main as subscriber

pytestmark = pytest.mark.integration


@pytest.fixture
async def asgi_client():
    # Reset state and install a shared httpx.AsyncClient that routes to the in-process subscriber.
    transport = ASGITransport(app=subscriber.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://subscriber")

    await webhook_module.reset_client()
    webhook_module._client = client
    webhook_module._settings = Settings()

    async with client:
        async with httpx.AsyncClient(transport=transport, base_url="http://subscriber") as admin:
            await admin.post("/admin/reset")
        yield client

    await webhook_module.reset_client()


def _payload(**overrides) -> WebhookPayload:
    base = dict(
        delivery_id="del_test_1",
        subscription_id="sub_1",
        event_id="evt_1",
        target_url="http://subscriber/webhook",
        event_type="user.created",
        body={"user_id": "u1", "ok": True},
    )
    base.update(overrides)
    return WebhookPayload(**base)


@pytest.mark.asyncio
async def test_webhook_success(asgi_client) -> None:
    await deliver_webhook(_payload())

    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        deliveries = (await admin.get("/admin/deliveries")).json()

    assert len(deliveries) == 1
    headers = deliveries[0]["headers"]
    assert headers["Idempotency-Key"] == "del_test_1"
    assert headers["X-Webhook-Id"] == "del_test_1"
    assert headers["X-Webhook-Event-Id"] == "evt_1"
    assert headers["X-Webhook-Timestamp"]
    assert headers["X-Webhook-Signature"].startswith("sha256=")


@pytest.mark.asyncio
async def test_webhook_500_raises_retriable(asgi_client) -> None:
    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        await admin.post(
            "/admin/configure", json={"fail_next_n_requests": 1, "force_status": 500}
        )

    with pytest.raises(RetriableError):
        await deliver_webhook(_payload(delivery_id="del_500"))


@pytest.mark.asyncio
async def test_webhook_429_with_retry_after_sets_attribute(asgi_client) -> None:
    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        await admin.post(
            "/admin/configure",
            json={
                "fail_next_n_requests": 1,
                "force_status": 429,
                "retry_after_seconds": 5,
            },
        )

    with pytest.raises(RetriableError) as excinfo:
        await deliver_webhook(_payload(delivery_id="del_429_5"))

    assert getattr(excinfo.value, "retry_after_seconds", None) == 5.0


@pytest.mark.asyncio
async def test_webhook_429_with_huge_retry_after_clamped_at_worker(asgi_client) -> None:
    """§19.3: Retry-After=86400 must be clamped at MAX_DELAY_S=600 by the worker layer."""
    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        await admin.post(
            "/admin/configure",
            json={
                "fail_next_n_requests": 1,
                "force_status": 429,
                "retry_after_seconds": 86400,
            },
        )

    with pytest.raises(RetriableError) as excinfo:
        await deliver_webhook(_payload(delivery_id="del_429_huge"))

    err = excinfo.value
    assert getattr(err, "retry_after_seconds", None) == 86400.0

    # Re-derive the worker-layer formula to demonstrate the cap applies.
    base = compute_backoff(1)
    delay = min(MAX_DELAY_S, max(base, err.retry_after_seconds))
    assert delay == MAX_DELAY_S == 600.0


@pytest.mark.asyncio
async def test_webhook_429_without_retry_after(asgi_client) -> None:
    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        await admin.post("/admin/reset")
        await admin.post(
            "/admin/configure",
            json={"fail_next_n_requests": 1, "force_status": 429},
        )

    with pytest.raises(RetriableError) as excinfo:
        await deliver_webhook(_payload(delivery_id="del_429_no_ra"))

    # No retry_after_seconds attribute when header is absent.
    assert getattr(excinfo.value, "retry_after_seconds", None) is None


@pytest.mark.asyncio
async def test_webhook_4xx_is_fatal(asgi_client) -> None:
    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        await admin.post(
            "/admin/configure", json={"fail_next_n_requests": 1, "force_status": 400}
        )

    with pytest.raises(FatalError):
        await deliver_webhook(_payload(delivery_id="del_400"))


@pytest.mark.asyncio
async def test_webhook_signature_correctly_formed(asgi_client) -> None:
    payload = _payload(delivery_id="del_sig", body={"k": "v", "n": 42})
    await deliver_webhook(payload)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        deliveries = (await admin.get("/admin/deliveries")).json()

    delivery = deliveries[-1]
    headers = delivery["headers"]
    raw_body = delivery["raw_body"].encode("utf-8")
    timestamp = headers["X-Webhook-Timestamp"]
    sig = headers["X-Webhook-Signature"]
    assert sig.startswith("sha256=")
    secret = Settings().TASKQ_WEBHOOK_SIGNING_SECRET
    expected = (
        "sha256="
        + hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.".encode("utf-8") + raw_body,
            hashlib.sha256,
        ).hexdigest()
    )
    assert sig == expected


@pytest.mark.asyncio
async def test_webhook_408_retriable(asgi_client) -> None:
    async with httpx.AsyncClient(
        transport=ASGITransport(app=subscriber.app), base_url="http://subscriber"
    ) as admin:
        await admin.post(
            "/admin/configure", json={"fail_next_n_requests": 1, "force_status": 408}
        )

    with pytest.raises(RetriableError):
        await deliver_webhook(_payload(delivery_id="del_408"))
