"""Unit tests for taskq.settings.Settings.

Note: pydantic-settings reads from the environment, so each test snapshots and
restores ``os.environ`` to keep the suite deterministic. The "prod + missing
admin token" rule is enforced inside the admin app at startup (§20.1) — here we
only verify the field defaults to ``None`` when the env var is unset.
"""
from __future__ import annotations

import os
from contextlib import contextmanager

import pytest

from taskq.settings import Settings


_ALL_TASKQ_VARS = [
    "TASKQ_ENV",
    "TASKQ_DATABASE_URL",
    "TASKQ_DB_POOL_MIN",
    "TASKQ_DB_POOL_MAX",
    "TASKQ_CONCURRENCY",
    "TASKQ_QUEUE",
    "TASKQ_POLL_INTERVAL_S",
    "TASKQ_ADMIN_TOKEN",
    "TASKQ_WEBHOOK_SIGNING_SECRET",
    "TASKQ_REAP_ENABLED",
    "TASKQ_REAP_INTERVAL_S",
    "TASKQ_REAP_BATCH",
    "TASKQ_REAP_RETENTION_DAYS",
    "TASKQ_HTTP_CONNECT_TIMEOUT_S",
    "TASKQ_HTTP_READ_TIMEOUT_S",
    "TASKQ_FAIL_RATE",
    "TASKQ_PAYLOAD_MAX_BYTES",
    "TASKQ_LOG_LEVEL",
]


@contextmanager
def _isolated_env(**overrides):
    """Clear all TASKQ_* env vars, then apply overrides; restore on exit."""
    saved = {k: os.environ.get(k) for k in _ALL_TASKQ_VARS}
    for k in _ALL_TASKQ_VARS:
        os.environ.pop(k, None)
    for k, v in overrides.items():
        os.environ[k] = v
    try:
        yield
    finally:
        for k, original in saved.items():
            if original is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = original


def _settings_no_envfile() -> Settings:
    # _env_file=None disables .env reading so tests don't pick up the local one.
    return Settings(_env_file=None)


def test_defaults_match_spec() -> None:
    with _isolated_env():
        s = _settings_no_envfile()
    assert s.TASKQ_ENV == "dev"
    assert s.TASKQ_DATABASE_URL == "postgresql://taskq:taskq@localhost:5432/taskq"
    assert s.TASKQ_DB_POOL_MIN == 2
    assert s.TASKQ_DB_POOL_MAX == 10
    assert s.TASKQ_CONCURRENCY == 4
    assert s.TASKQ_QUEUE == "default"
    assert s.TASKQ_POLL_INTERVAL_S == 0.5
    assert s.TASKQ_ADMIN_TOKEN is None
    assert s.TASKQ_PAYLOAD_MAX_BYTES == 65536
    assert s.TASKQ_REAP_ENABLED is True
    assert s.TASKQ_REAP_INTERVAL_S == 300
    assert s.TASKQ_REAP_BATCH == 1000
    assert s.TASKQ_REAP_RETENTION_DAYS == 7
    assert s.TASKQ_HTTP_CONNECT_TIMEOUT_S == 5.0
    assert s.TASKQ_HTTP_READ_TIMEOUT_S == 25.0
    assert s.TASKQ_FAIL_RATE == 0.0
    assert s.TASKQ_LOG_LEVEL == "INFO"


def test_env_overrides_str_fields() -> None:
    with _isolated_env(
        TASKQ_ENV="prod",
        TASKQ_QUEUE="emails",
        TASKQ_DATABASE_URL="postgresql://u:p@db:5432/q",
        TASKQ_LOG_LEVEL="DEBUG",
    ):
        s = _settings_no_envfile()
    assert s.TASKQ_ENV == "prod"
    assert s.TASKQ_QUEUE == "emails"
    assert s.TASKQ_DATABASE_URL == "postgresql://u:p@db:5432/q"
    assert s.TASKQ_LOG_LEVEL == "DEBUG"


def test_env_overrides_numeric_fields() -> None:
    with _isolated_env(
        TASKQ_CONCURRENCY="16",
        TASKQ_DB_POOL_MAX="32",
        TASKQ_POLL_INTERVAL_S="0.25",
        TASKQ_PAYLOAD_MAX_BYTES="131072",
    ):
        s = _settings_no_envfile()
    assert s.TASKQ_CONCURRENCY == 16
    assert s.TASKQ_DB_POOL_MAX == 32
    assert s.TASKQ_POLL_INTERVAL_S == 0.25
    assert s.TASKQ_PAYLOAD_MAX_BYTES == 131072


def test_admin_token_unset_is_none_in_dev() -> None:
    """§20.1: dev + token unset is allowed (admin.py logs a warning at startup);
    here we just verify the field surfaces as None so the admin module can detect it."""
    with _isolated_env(TASKQ_ENV="dev"):
        s = _settings_no_envfile()
    assert s.TASKQ_ADMIN_TOKEN is None
    assert s.TASKQ_ENV == "dev"


def test_admin_token_unset_in_prod_field_is_none() -> None:
    """§20.1: prod + token unset → process refuses to start. The actual raise lives
    in admin.py; here we only assert that Settings exposes the unset state cleanly."""
    with _isolated_env(TASKQ_ENV="prod"):
        s = _settings_no_envfile()
    assert s.TASKQ_ENV == "prod"
    assert s.TASKQ_ADMIN_TOKEN is None


def test_admin_token_set_is_passed_through() -> None:
    with _isolated_env(TASKQ_ENV="prod", TASKQ_ADMIN_TOKEN="hunter2"):
        s = _settings_no_envfile()
    assert s.TASKQ_ENV == "prod"
    assert s.TASKQ_ADMIN_TOKEN == "hunter2"


def test_invalid_env_value_raises() -> None:
    """TASKQ_ENV is a Literal['dev','prod'] — anything else should raise."""
    with _isolated_env(TASKQ_ENV="staging"):
        with pytest.raises(Exception):
            _settings_no_envfile()


def test_extra_env_vars_are_ignored() -> None:
    with _isolated_env(TASKQ_ENV="dev"):
        os.environ["TASKQ_NOT_A_REAL_FIELD"] = "ignored"
        try:
            s = _settings_no_envfile()
        finally:
            os.environ.pop("TASKQ_NOT_A_REAL_FIELD", None)
    assert s.TASKQ_ENV == "dev"
