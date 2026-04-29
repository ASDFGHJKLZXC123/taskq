from __future__ import annotations

import pytest
from pydantic import BaseModel

from taskq.registry import HandlerRegistry, HandlerSpec


class Payload(BaseModel):
    x: int


def test_register_and_get() -> None:
    reg = HandlerRegistry()

    @reg.handler("greet", payload_model=Payload, timeout_s=15)
    async def greet(p: Payload) -> None:
        return None

    spec = reg.get("greet")
    assert isinstance(spec, HandlerSpec)
    assert spec.func is greet
    assert spec.payload_model is Payload
    assert spec.timeout_s == 15


def test_decorator_returns_original_function() -> None:
    reg = HandlerRegistry()

    async def myhandler(p: Payload) -> None:
        return None

    decorated = reg.handler("a", payload_model=Payload)(myhandler)
    assert decorated is myhandler


def test_default_timeout() -> None:
    reg = HandlerRegistry()

    @reg.handler("default_to", payload_model=Payload)
    async def h(p: Payload) -> None:
        return None

    assert reg.get("default_to").timeout_s == 60


def test_get_unknown_raises() -> None:
    reg = HandlerRegistry()
    with pytest.raises(KeyError):
        reg.get("nope")


def test_duplicate_registration_raises() -> None:
    reg = HandlerRegistry()

    @reg.handler("dup", payload_model=Payload)
    async def h1(p: Payload) -> None:
        return None

    with pytest.raises(ValueError):

        @reg.handler("dup", payload_model=Payload)
        async def h2(p: Payload) -> None:
            return None


def test_module_level_registry_has_webhook() -> None:
    # Importing the handler module triggers registration on the shared registry.
    import taskq.handlers.webhook  # noqa: F401
    from taskq.registry import registry

    spec = registry.get("webhook.deliver")
    assert spec.timeout_s == 30
    assert spec.payload_model.__name__ == "WebhookPayload"
