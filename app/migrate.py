"""Apply schema.sql at startup. An advisory lock makes concurrent boots (e.g. two
replicas starting together) safe."""
from pathlib import Path

import asyncpg

_SCHEMA = (Path(__file__).parent / "schema.sql").read_text()
_LOCK_ID = 7_412_001


async def migrate(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", _LOCK_ID)
        try:
            await conn.execute(_SCHEMA)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _LOCK_ID)
