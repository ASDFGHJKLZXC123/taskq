from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # dev|prod; controls log renderer and admin token enforcement (§20.1)
    TASKQ_ENV: Literal["dev", "prod"] = "dev"

    # Postgres DSN used by asyncpg pool and Alembic.
    TASKQ_DATABASE_URL: str = "postgresql://taskq:taskq@localhost:5432/taskq"

    # asyncpg pool sizing (§20.3). Rule: pool_max >= TASKQ_CONCURRENCY + 2.
    TASKQ_DB_POOL_MIN: int = 2
    TASKQ_DB_POOL_MAX: int = 10

    # Coroutines per worker process (§20.2).
    TASKQ_CONCURRENCY: int = 4

    # Queue this worker process pulls from.
    TASKQ_QUEUE: str = "default"

    # Base sleep between empty claims (§19.6). Effective sleep = poll + uniform(0, poll).
    TASKQ_POLL_INTERVAL_S: float = 0.5

    # Bearer token for admin endpoints (§20.1).
    # prod + None  -> process refuses to start
    # dev  + None  -> auth disabled, loud warning at startup
    TASKQ_ADMIN_TOKEN: Optional[str] = None

    # HMAC secret for outbound webhook signatures (§21).
    TASKQ_WEBHOOK_SIGNING_SECRET: str = "dev-secret-change-me"

    # Reaper for SUCCEEDED rows (§20.5). Runs in API service only.
    TASKQ_REAP_ENABLED: bool = True
    TASKQ_REAP_INTERVAL_S: int = 300
    TASKQ_REAP_BATCH: int = 1000
    TASKQ_REAP_RETENTION_DAYS: int = 7

    # httpx timeouts for the webhook handler (§21).
    TASKQ_HTTP_CONNECT_TIMEOUT_S: float = 5.0
    TASKQ_HTTP_READ_TIMEOUT_S: float = 25.0

    # Random failure rate in the demo subscriber (§19.4). Tests prefer deterministic flags.
    TASKQ_FAIL_RATE: float = 0.0

    # Hard payload cap enforced by enqueue (§4).
    TASKQ_PAYLOAD_MAX_BYTES: int = 65536

    # structlog level.
    TASKQ_LOG_LEVEL: str = "INFO"


def get_settings() -> Settings:
    return Settings()
