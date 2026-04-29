from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel

app = FastAPI()

_state: dict[str, Any] = {
    "fail_next_n_requests": 0,
    "force_status": None,
    "fail_rate": 0.0,
    "retry_after_seconds": None,
    "deliveries": [],
}


def _reset_state() -> None:
    _state["fail_next_n_requests"] = 0
    _state["force_status"] = None
    _state["fail_rate"] = 0.0
    _state["retry_after_seconds"] = None
    _state["deliveries"] = []


def _failure_response() -> Response:
    status = _state.get("force_status") or 500
    headers: dict[str, str] = {}
    if status == 429 and _state.get("retry_after_seconds") is not None:
        headers["Retry-After"] = str(_state["retry_after_seconds"])
    return Response(
        content=f'{{"ok":false,"forced":true,"status":{status}}}',
        media_type="application/json",
        status_code=status,
        headers=headers,
    )


@app.post("/webhook")
async def webhook(request: Request) -> Response:
    if _state["fail_next_n_requests"] > 0:
        _state["fail_next_n_requests"] -= 1
        return _failure_response()

    fail_rate = float(_state.get("fail_rate") or 0.0)
    if fail_rate > 0 and random.random() < fail_rate:
        return Response(
            content='{"ok":false,"random":true}',
            media_type="application/json",
            status_code=500,
        )

    raw = await request.body()
    try:
        body = await request.json()
    except Exception:
        body = raw.decode("utf-8", errors="replace")

    _state["deliveries"].append(
        {
            "received_at": datetime.now(timezone.utc).isoformat(),
            "headers": {
                "Idempotency-Key": request.headers.get("Idempotency-Key"),
                "X-Webhook-Id": request.headers.get("X-Webhook-Id"),
                "X-Webhook-Event-Id": request.headers.get("X-Webhook-Event-Id"),
                "X-Webhook-Timestamp": request.headers.get("X-Webhook-Timestamp"),
                "X-Webhook-Signature": request.headers.get("X-Webhook-Signature"),
            },
            "body": body,
            "raw_body": raw.decode("utf-8", errors="replace"),
        }
    )
    return Response(content='{"ok":true}', media_type="application/json", status_code=200)


class ConfigureBody(BaseModel):
    fail_next_n_requests: Optional[int] = None
    force_status: Optional[int] = None
    fail_rate: Optional[float] = None
    retry_after_seconds: Optional[int] = None


@app.post("/admin/configure")
async def configure(body: ConfigureBody) -> dict:
    if body.fail_next_n_requests is not None:
        _state["fail_next_n_requests"] = int(body.fail_next_n_requests)
    if body.force_status is not None:
        _state["force_status"] = int(body.force_status)
    if body.fail_rate is not None:
        _state["fail_rate"] = float(body.fail_rate)
    if body.retry_after_seconds is not None:
        _state["retry_after_seconds"] = int(body.retry_after_seconds)
    return {
        "fail_next_n_requests": _state["fail_next_n_requests"],
        "force_status": _state["force_status"],
        "fail_rate": _state["fail_rate"],
        "retry_after_seconds": _state["retry_after_seconds"],
    }


@app.post("/admin/reset")
async def admin_reset() -> dict:
    _reset_state()
    return {"ok": True}


@app.get("/admin/deliveries")
async def admin_deliveries() -> list[dict]:
    return _state["deliveries"]


def run() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9000)


if __name__ == "__main__":
    run()
