"""Structured JSON logs with a correlation id, and Prometheus metrics.

Design choice: business counters are incremented only *after* the transaction has
committed (in the route handler, once the DB call returns), so a counter can never
run ahead of the database. Seat gauges are not tracked in-process at all — they are
read from Postgres at scrape time, so they reconcile with GET /shows by construction.
"""
import contextvars
import json
import logging
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone

import os

from prometheus_client import (CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram,
                               generate_latest, multiprocess)
from prometheus_client.core import GaugeMetricFamily

from .config import settings

# ----------------------------------------------------------------------------- logging

# Per-request mutable context. Handlers write into it (user_id, outcome, ...) and the
# access-log line emitted by the middleware picks it up.
request_ctx: contextvars.ContextVar[dict | None] = contextvars.ContextVar("request_ctx", default=None)


def annotate(**fields) -> None:
    ctx = request_ctx.get()
    if ctx is not None:
        ctx.update(fields)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        ctx = request_ctx.get()
        if ctx and "request_id" in ctx:
            out["request_id"] = ctx["request_id"]
        extra = getattr(record, "fields", None)
        if extra:
            out.update(extra)
        if record.exc_info:
            out["exc"] = "".join(traceback.format_exception(*record.exc_info))
        return json.dumps(out, default=str)


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(settings.log_level)
    for noisy in ("uvicorn.access",):
        logging.getLogger(noisy).disabled = True
    for name in ("uvicorn", "uvicorn.error"):
        logging.getLogger(name).handlers[:] = []
        logging.getLogger(name).propagate = True


log = logging.getLogger("seats")

# ----------------------------------------------------------------------------- metrics

RESERVATIONS_CONFIRMED = Counter(
    "reservations_confirmed_total", "Reservations committed as confirmed")
SEATS_CONFIRMED = Counter(
    "seats_confirmed_total", "Seats moved to confirmed by committed reservations")
RESERVATIONS_DECLINED = Counter(
    "reservations_declined_total", "Reserve requests that did not create a reservation, by reason",
    ["reason"])  # seat_taken | per_user_limit | idempotent_replay | idempotency_key_reuse | unknown_seats
RESERVATIONS_CANCELLED = Counter(
    "reservations_cancelled_total", "Reservations cancelled by their owner")
SEATS_RELEASED = Counter(
    "seats_released_total", "Seats returned to available by cancellation")
DB_RETRIES = Counter(
    "db_tx_retries_total", "Transactions retried after deadlock/serialization failure")

HTTP_REQUESTS = Counter(
    "http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
HTTP_IN_FLIGHT = Gauge("http_requests_in_flight", "Requests currently being served", multiprocess_mode="livesum")
DB_POOL_SIZE = Gauge("db_pool_size", "Open DB connections in the pool", multiprocess_mode="livesum")
DB_POOL_IDLE = Gauge("db_pool_idle", "Idle DB connections in the pool", multiprocess_mode="livesum")

# With WEB_CONCURRENCY > 1 the entrypoint sets PROMETHEUS_MULTIPROC_DIR and every worker
# writes its counters there; a scrape (served by any worker) aggregates all of them.
MULTIPROC = bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR"))

METRICS_SHOW_LIMIT = 25  # bound label cardinality: most recent shows only


async def render_metrics(pool) -> tuple[bytes, str]:
    # 1. process-local (or multiprocess-aggregated) counters/histograms
    if MULTIPROC:
        reg = CollectorRegistry()
        multiprocess.MultiProcessCollector(reg)
        app_metrics = generate_latest(reg)
    else:
        app_metrics = generate_latest()

    # 2. state gauges, built fresh per scrape from one Postgres snapshot. They are plain
    #    metric families (no stored value), so in multi-worker mode they are not written to
    #    per-process files and can never go stale or be duplicated per pid.
    seats = GaugeMetricFamily("seats", "Seats per show by status (read from Postgres at scrape time)",
                              labels=["show_id", "status"])
    seats_available = GaugeMetricFamily("seats_available", "Available seats per show", labels=["show_id"])
    invariant = GaugeMetricFamily("reconciliation_invariant_ok",
                                  "1 if available+held+confirmed == total_seats for the show", labels=["show_id"])
    seats_all = GaugeMetricFamily("seats_all_shows", "Seats across all shows by status", labels=["status"])
    db_up = 0
    try:
        async with pool.acquire(timeout=2) as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                rows = await conn.fetch(
                    """
                    WITH recent AS (SELECT id, total_seats FROM shows ORDER BY created_at DESC LIMIT $1)
                    SELECT r.id, r.total_seats, s.status, count(s.*) AS n
                      FROM recent r LEFT JOIN seats s ON s.show_id = r.id
                     GROUP BY r.id, r.total_seats, s.status
                    """, METRICS_SHOW_LIMIT)
                totals = await conn.fetch("SELECT status, count(*) AS n FROM seats GROUP BY status")
        per_show: dict[str, dict] = {}
        for r in rows:
            d = per_show.setdefault(str(r["id"]), {"total": r["total_seats"], "available": 0, "held": 0, "confirmed": 0})
            if r["status"]:
                d[r["status"]] = r["n"]
        for sid, d in per_show.items():
            for st in ("available", "held", "confirmed"):
                seats.add_metric([sid, st], d[st])
            seats_available.add_metric([sid], d["available"])
            invariant.add_metric([sid], 1 if d["available"] + d["held"] + d["confirmed"] == d["total"] else 0)
        t = {r["status"]: r["n"] for r in totals}
        for st in ("available", "held", "confirmed"):
            seats_all.add_metric([st], t.get(st, 0))
        db_up = 1
    except Exception:  # noqa: BLE001 — metrics must still render when the DB is down
        pass
    up = GaugeMetricFamily("db_up", "1 if this scrape could query Postgres", value=db_up)

    class _Snapshot:
        def collect(self):
            return [seats, seats_available, invariant, seats_all, up]

    reg = CollectorRegistry()
    reg.register(_Snapshot())
    DB_POOL_SIZE.set(pool.get_size())
    DB_POOL_IDLE.set(pool.get_idle_size())
    return app_metrics + generate_latest(reg), CONTENT_TYPE_LATEST


# ----------------------------------------------------------------------------- middleware

class ObservabilityMiddleware:
    """Pure ASGI middleware (cheaper than BaseHTTPMiddleware under load).

    - assigns/propagates X-Request-ID
    - emits one JSON access-log line per request with domain annotations
    - records RED metrics
    - last-resort guard: an unhandled exception becomes a logged JSON 500
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        rid = None
        for k, v in scope.get("headers", []):
            if k == b"x-request-id":
                rid = v.decode("latin-1")[:128]
                break
        rid = rid or uuid.uuid4().hex
        ctx = {"request_id": rid}
        token = request_ctx.set(ctx)
        status_holder = {"status": 500, "started": False}
        start = time.perf_counter()
        HTTP_IN_FLIGHT.inc()

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                status_holder["started"] = True
                message.setdefault("headers", [])
                message["headers"] = list(message["headers"]) + [(b"x-request-id", rid.encode())]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:  # noqa: BLE001
            log.exception("unhandled error", extra={"fields": {"path": scope.get("path")}})
            if not status_holder["started"]:
                body = json.dumps({"error": "internal", "message": "internal error", "request_id": rid}).encode()
                await send_wrapper({"type": "http.response.start", "status": 500,
                                    "headers": [(b"content-type", b"application/json")]})
                await send({"type": "http.response.body", "body": body})
        finally:
            HTTP_IN_FLIGHT.dec()
            dur = time.perf_counter() - start
            route = scope.get("route")
            route_path = getattr(route, "path", None) or "unmatched"
            method = scope.get("method", "")
            status = status_holder["status"]
            if route_path not in ("/metrics", "/healthz"):
                HTTP_REQUESTS.labels(method, route_path, str(status)).inc()
                HTTP_LATENCY.labels(method, route_path).observe(dur)
                fields = {"method": method, "path": scope.get("path"), "route": route_path,
                          "status": status, "duration_ms": round(dur * 1000, 2)}
                fields.update({k: v for k, v in ctx.items() if k != "request_id"})
                log.log(logging.ERROR if status >= 500 else logging.INFO, "request", extra={"fields": fields})
            request_ctx.reset(token)
