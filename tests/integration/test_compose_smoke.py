"""Compose smoke test (opt-in via TASKQ_RUN_COMPOSE_TEST=1).

Builds the docker-compose stack, hits ``/health`` on the API, scales workers
to 2, enqueues a webhook delivery via the CLI in the API container, drains,
and tears down. Slow — only run when wired up explicitly.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.compose]


REPO_ROOT = Path(__file__).resolve().parents[2]


def _compose(*args, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        **kwargs,
    )


@pytest.mark.skipif(
    os.environ.get("TASKQ_RUN_COMPOSE_TEST") != "1",
    reason="set TASKQ_RUN_COMPOSE_TEST=1 to run this slow end-to-end stack test",
)
def test_compose_stack_smoke():
    """Boot the full stack, hit /health, enqueue, drain, teardown."""
    # Ensure .env exists — compose's env_file directive is mandatory.
    env_path = REPO_ROOT / ".env"
    created_env = False
    if not env_path.exists():
        env_path.write_text((REPO_ROOT / ".env.example").read_text())
        created_env = True

    try:
        # Bring everything up scaled to 2 workers.
        up = _compose("up", "-d", "--build", "--scale", "worker=2")
        assert up.returncode == 0, f"compose up failed:\n{up.stderr}"

        # Wait for /health.
        deadline = time.monotonic() + 60
        ready = False
        while time.monotonic() < deadline:
            r = subprocess.run(
                ["docker", "compose", "exec", "-T", "api", "curl", "-fsS", "http://localhost:8000/health"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
            )
            if r.returncode == 0 and "ok" in r.stdout:
                ready = True
                break
            time.sleep(2)
        assert ready, "API /health never returned ok"

        # Enqueue a webhook via CLI in the api container.
        send = _compose(
            "exec", "-T", "api", "taskq", "webhook", "send",
            "--url", "http://subscriber:9000/webhook",
            "--event-type", "user.created",
            "--data", '{"id":1}',
        )
        assert send.returncode == 0, f"taskq webhook send failed:\n{send.stderr}"

        # Wait for queue to drain via stats.
        drain_deadline = time.monotonic() + 30
        drained = False
        while time.monotonic() < drain_deadline:
            stats = _compose(
                "exec", "-T", "api", "taskq", "stats", "--queue", "webhooks",
            )
            if stats.returncode == 0 and '"PENDING": 0' in stats.stdout and '"RUNNING": 0' in stats.stdout:
                drained = True
                break
            time.sleep(2)
        assert drained, f"queue did not drain; last stats:\n{stats.stdout}"

    finally:
        _compose("down", "-v")
        if created_env:
            env_path.unlink(missing_ok=True)
