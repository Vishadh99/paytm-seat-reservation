"""Runtime configuration, read once from environment variables."""
import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/seats")
    # Some platforms hand out postgres:// — asyncpg accepts both, normalise anyway.
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


@dataclass(frozen=True)
class Settings:
    database_url: str = _database_url()
    db_pool_min: int = _int("DB_POOL_MIN", 2)
    db_pool_max: int = _int("DB_POOL_MAX", 20)
    # How long a request may wait for a pooled connection before we give up.
    # Kept generous: under a burst, queueing is the correct behaviour, not failing.
    db_acquire_timeout_s: float = float(os.environ.get("DB_ACQUIRE_TIMEOUT_S", "60"))
    jwt_secret: str = os.environ.get("JWT_SECRET", "dev-only-secret-change-me")
    jwt_ttl_s: int = _int("JWT_TTL_S", 24 * 3600)
    # Demo identity provider: lets anyone mint a token for a user id. Admin tokens
    # additionally require ADMIN_KEY unless it is left empty.
    demo_auth: bool = os.environ.get("DEMO_AUTH", "true").lower() == "true"
    admin_key: str = os.environ.get("ADMIN_KEY", "")
    default_per_user_limit: int = _int("DEFAULT_PER_USER_LIMIT", 4)
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")


settings = Settings()
