"""Seat reservation service — HTTP entrypoint."""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response

from . import auth, db, reservations, shows
from .errors import ApiError, api_error_handler, validation_error_handler
from .migrate import migrate
from .observability import ObservabilityMiddleware, log, render_metrics, setup_logging

setup_logging()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Retry the initial connection so a cold start where the DB comes up a bit
    # later than the app still converges to healthy instead of crash-looping.
    for attempt in range(30):
        try:
            pool = await db.init_pool()
            break
        except Exception as exc:  # noqa: BLE001
            log.warning("db not reachable at startup, retrying", extra={"fields": {"attempt": attempt, "error": repr(exc)}})
            if attempt == 29:
                raise
            await asyncio.sleep(2)
    await migrate(pool)
    log.info("startup complete", extra={"fields": {"pool_max": pool.get_max_size()}})
    yield
    await db.close_pool()


app = FastAPI(title="seat-reservation", lifespan=lifespan)
app.add_middleware(ObservabilityMiddleware)
app.add_exception_handler(ApiError, api_error_handler)
app.add_exception_handler(RequestValidationError, validation_error_handler)
app.include_router(auth.router)
app.include_router(shows.router)
app.include_router(reservations.router)


@app.get("/healthz")
async def healthz():
    """Liveness: the process is up and serving. Deliberately no dependency checks."""
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    """Readiness: fails closed (503) if Postgres is not reachable within 2s."""
    try:
        async with db.pool().acquire(timeout=2) as conn:
            await conn.fetchval("SELECT 1", timeout=2)
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=503, content={"status": "unavailable", "db": "down", "error": type(exc).__name__})
    return {"status": "ready", "db": "ok"}


@app.get("/metrics")
async def metrics():
    body, ctype = await render_metrics(db.pool())
    return Response(content=body, media_type=ctype)
