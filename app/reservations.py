"""Reserve and cancel — where every correctness decision lives.

Reserve runs in two phases:

  1. Fast path (one read-only query, no transaction, no locks). Answers the cheap
     cases: idempotent replay, key reused with a different body, unknown seats, a seat
     that is visibly taken, a user visibly at their limit. This is an *optimisation*
     only — in a 500-way hot-seat storm, 499 losers are turned away here without ever
     queueing on the seat's row lock. A stale "looks free" read just falls through to
     phase 2; a "looks taken" read is a seat that really was taken during the request.

  2. Decision transaction. Every guard is a conditional write, never read-then-write:
       a. INSERT idempotency key ON CONFLICT DO NOTHING  — exactly one tx owns the key
       b. UPSERT user_quota ... WHERE held + n <= limit  — per-user limit
       c. lock seats ORDER BY label FOR UPDATE, flip only WHERE status='available'
       d. INSERT reservation + bind its response to the key
     Any decline raises and rolls the whole transaction back, so nothing moves and the
     key is released (a retry is re-evaluated against current state). Only successful
     reservations are bound to a key.

Lock acquisition order (identical for every path, which is why we cannot deadlock):

    reserve:  idempotency_keys(user,show,key) -> user_quota(show,user) -> seats ORDER BY label
    cancel:   reservations(id)                -> user_quota(show,user) -> seats ORDER BY label

A reserve never waits on a reservations row and a cancel never touches an idempotency
row, so the two orders cannot form a cycle. Deadlock / serialization errors are still
caught and the whole transaction retried as defence in depth.
"""
import hashlib
import json
import uuid

import asyncpg
from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from . import db
from .auth import Principal, current_user
from .config import settings
from .errors import ApiError
from .observability import (DB_RETRIES, RESERVATIONS_CANCELLED, RESERVATIONS_CONFIRMED,
                            RESERVATIONS_DECLINED, SEATS_CONFIRMED, SEATS_RELEASED, annotate)
from .shows import SEAT_RE, parse_show_id

router = APIRouter()

MAX_SEATS_PER_REQUEST = 10
_RETRYABLE = (asyncpg.exceptions.DeadlockDetectedError, asyncpg.exceptions.SerializationError)


class ReserveRequest(BaseModel):
    # extra="ignore": a spoofed {"user_id": "victim"} in the body is silently dropped;
    # identity only ever comes from the token.
    model_config = ConfigDict(extra="ignore")
    seats: list[str] = Field(min_length=1, max_length=MAX_SEATS_PER_REQUEST)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("seats")
    @classmethod
    def _seats(cls, v: list[str]) -> list[str]:
        if any(not SEAT_RE.match(s) for s in v):
            raise ValueError("invalid seat label")
        if len(set(v)) != len(v):
            raise ValueError("duplicate seats in request")
        return v


class Outcome(Exception):
    """A terminal non-success outcome. Raised inside the transaction => full rollback."""

    def __init__(self, status: int, reason: str, message: str, **extra):
        self.status = status
        self.reason = reason
        self.body = {"error": reason, "message": message, **extra}


def _request_hash(show_id: uuid.UUID, seats: list[str]) -> str:
    # Canonical form: the order of seats in the request does not change its meaning.
    return hashlib.sha256(f"{show_id}|{','.join(sorted(seats))}".encode()).hexdigest()


def _from_prior(prior: str, req_hash: str):
    """Resolve a request whose key is already bound to a committed reservation."""
    p = json.loads(prior)
    if p["h"] != req_hash:
        raise Outcome(409, "idempotency_key_reuse",
                      "idempotency key was already used with a different request body")
    return "idempotent_replay", 201, p["r"]


_FAST_PATH = """
SELECT s.price_paise,
       s.per_user_limit,
       (SELECT json_build_object('h', k.request_hash, 'r', k.response)::text
          FROM idempotency_keys k
         WHERE k.user_id = $1 AND k.show_id = $2 AND k.key = $3)                     AS prior,
       (SELECT count(*) FROM seats WHERE show_id = $2 AND label = ANY($4::text[]))   AS known,
       (SELECT array_agg(label ORDER BY label) FROM seats
         WHERE show_id = $2 AND label = ANY($4::text[]) AND status <> 'available')   AS taken,
       COALESCE((SELECT seats_held FROM user_quota
                  WHERE show_id = $2 AND user_id = $1), 0)                           AS held
  FROM shows s
 WHERE s.id = $2
"""


def _precheck(row, user_id: str, seats: list[str], req_hash: str):
    if row is None:
        raise ApiError(404, "show_not_found", "no such show")
    if row["prior"] is not None:
        return _from_prior(row["prior"], req_hash)
    if row["known"] != len(seats):
        raise Outcome(422, "unknown_seats", "seat(s) do not exist for this show")
    if row["taken"]:
        raise Outcome(409, "seat_taken", "seat(s) already taken", seats=list(row["taken"]))
    if row["held"] + len(seats) > row["per_user_limit"]:
        raise Outcome(409, "per_user_limit", f"limit is {row['per_user_limit']} seats per user for this show",
                      limit=row["per_user_limit"])
    return None


async def _decide(conn, show_id: uuid.UUID, user_id: str, seats: list[str], key: str, req_hash: str,
                  price: int, limit: int):
    """The transaction. Returns (outcome, status, body) or raises Outcome (=> rollback)."""
    n = len(seats)

    # a. Idempotency gate. A concurrent request with the same (user, show, key) blocks
    #    here on our uncommitted row; when we commit it sees DO NOTHING and replays us,
    #    when we roll back its INSERT succeeds and it makes its own attempt.
    owned = await conn.fetchval(
        "INSERT INTO idempotency_keys (user_id, show_id, key, request_hash) VALUES ($1, $2, $3, $4) "
        "ON CONFLICT DO NOTHING RETURNING true",
        user_id, show_id, key, req_hash)
    if not owned:
        prior = await conn.fetchval(
            "SELECT json_build_object('h', request_hash, 'r', response)::text FROM idempotency_keys "
            "WHERE user_id = $1 AND show_id = $2 AND key = $3", user_id, show_id, key)
        return _from_prior(prior, req_hash)

    # b. Per-user limit: check-and-increment in ONE guarded statement. The (show, user)
    #    row lock serialises one user's parallel requests; other users never touch it.
    #    (n <= limit is guaranteed by the fast path, so the initial INSERT is in bounds.)
    held = await conn.fetchval(
        """
        INSERT INTO user_quota AS q (show_id, user_id, seats_held) VALUES ($1, $2, $3)
        ON CONFLICT (show_id, user_id) DO UPDATE SET seats_held = q.seats_held + EXCLUDED.seats_held
         WHERE q.seats_held + EXCLUDED.seats_held <= $4
        RETURNING seats_held
        """, show_id, user_id, n, limit)
    if held is None:
        raise Outcome(409, "per_user_limit", f"limit is {limit} seats per user for this show", limit=limit)

    # c. THE atomic seat decision. Lock candidate rows in deterministic (label) order; after
    #    waiting on a lock Postgres re-evaluates `status = 'available'` against the latest
    #    committed version, so a seat someone else just took drops out. Only rows we hold
    #    the lock on *and* that are still available are flipped.
    reservation_id = uuid.uuid4()
    claimed = await conn.fetch(
        """
        WITH lockable AS (
            SELECT label FROM seats
             WHERE show_id = $1 AND label = ANY($2::text[]) AND status = 'available'
             ORDER BY label
               FOR UPDATE
        )
        UPDATE seats s
           SET status = 'confirmed', reservation_id = $3, user_id = $4, updated_at = now()
          FROM lockable
         WHERE s.show_id = $1 AND s.label = lockable.label
        RETURNING s.label
        """, show_id, seats, reservation_id, user_id)
    if len(claimed) != n:
        # All-or-nothing: lost the race for at least one seat. Raising rolls back the
        # partial claim, the quota increment and the key.
        got = {r["label"] for r in claimed}
        raise Outcome(409, "seat_taken", "seat(s) already taken", seats=sorted(set(seats) - got))

    # d. Record the reservation and bind its response to the key, in one statement.
    ordered = sorted(seats)
    body = {
        "reservation_id": str(reservation_id),
        "show_id": str(show_id),
        "user_id": user_id,
        "seats": ordered,
        "amount_paise": price * n,  # integer paise; Python ints never round
        "status": "confirmed",
    }
    await conn.execute(
        """
        WITH r AS (
            INSERT INTO reservations (id, show_id, user_id, seats, amount_paise, status, idempotency_key)
            VALUES ($1, $2, $3, $4, $5, 'confirmed', $6)
        )
        UPDATE idempotency_keys SET status_code = 201, response = $7::json, reservation_id = $1
         WHERE user_id = $3 AND show_id = $2 AND key = $6
        """, reservation_id, show_id, user_id, ordered, price * n, key, json.dumps(body))
    return "confirmed", 201, body


async def _with_retry(fn, *args):
    async with db.pool().acquire(timeout=settings.db_acquire_timeout_s) as conn:
        for attempt in range(5):
            try:
                async with conn.transaction():
                    return await fn(conn, *args)
            except _RETRYABLE:
                DB_RETRIES.inc()
                if attempt == 4:
                    raise


async def _reserve(show_id: uuid.UUID, user_id: str, seats: list[str], key: str):
    req_hash = _request_hash(show_id, seats)
    async with db.pool().acquire(timeout=settings.db_acquire_timeout_s) as conn:
        row = await conn.fetchrow(_FAST_PATH, user_id, show_id, key, seats)
        try:
            early = _precheck(row, user_id, seats, req_hash)
            if early:
                return early
            for attempt in range(5):
                try:
                    async with conn.transaction():
                        return await _decide(conn, show_id, user_id, seats, key, req_hash,
                                             row["price_paise"], row["per_user_limit"])
                except _RETRYABLE:
                    DB_RETRIES.inc()
                    if attempt == 4:
                        raise
        except Outcome as o:
            return o.reason, o.status, o.body


@router.post("/shows/{show_id}/reserve")
async def reserve(
    show_id: str,
    body: ReserveRequest,
    user: Principal = Depends(current_user),
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
):
    sid = parse_show_id(show_id)
    annotate(user_id=user.user_id, show_id=str(sid), seats=body.seats)
    if idempotency_key_header and body.idempotency_key and idempotency_key_header != body.idempotency_key:
        raise ApiError(422, "invalid_request", "Idempotency-Key header and body idempotency_key differ")
    key = idempotency_key_header or body.idempotency_key
    if key is None:
        # No key supplied: still safe, just not deduplicated on retry.
        key = f"auto:{uuid.uuid4()}"

    outcome, status, resp = await _reserve(sid, user.user_id, body.seats, key)

    # Counters move only after the transaction has committed (or nothing was written).
    if outcome == "confirmed":
        RESERVATIONS_CONFIRMED.inc()
        SEATS_CONFIRMED.inc(len(body.seats))
    else:
        RESERVATIONS_DECLINED.labels(outcome).inc()
    annotate(outcome=outcome, reservation_id=resp.get("reservation_id"))
    headers = {"Idempotent-Replayed": "true"} if outcome == "idempotent_replay" else None
    return JSONResponse(status_code=status, content=resp, headers=headers)


# ----------------------------------------------------------------------------- cancel

def _reservation_view(r) -> dict:
    return {
        "reservation_id": str(r["id"]), "show_id": str(r["show_id"]), "user_id": r["user_id"],
        "seats": list(r["seats"]), "amount_paise": r["amount_paise"], "status": r["status"],
    }


async def _cancel_tx(conn, rid: uuid.UUID, user: Principal):
    r = await conn.fetchrow("SELECT * FROM reservations WHERE id = $1 FOR UPDATE", rid)
    if r is None:
        raise ApiError(404, "reservation_not_found", "no such reservation")
    if r["user_id"] != user.user_id:
        raise ApiError(403, "forbidden", "only the owner can cancel this reservation")
    if r["status"] == "cancelled":
        return "already_cancelled", _reservation_view(r)

    await conn.execute(
        "UPDATE user_quota SET seats_held = seats_held - $3 WHERE show_id = $1 AND user_id = $2",
        r["show_id"], r["user_id"], len(r["seats"]))
    # Guarded on reservation_id: we only ever free seats this reservation owns, so a
    # cancel can never resurrect a seat that is now someone else's.
    await conn.execute(
        """
        WITH mine AS (
            SELECT label FROM seats WHERE show_id = $1 AND reservation_id = $2 ORDER BY label FOR UPDATE
        )
        UPDATE seats s SET status = 'available', reservation_id = NULL, user_id = NULL, updated_at = now()
          FROM mine WHERE s.show_id = $1 AND s.label = mine.label
        """,
        r["show_id"], rid)
    r = await conn.fetchrow(
        "UPDATE reservations SET status = 'cancelled', cancelled_at = now() WHERE id = $1 RETURNING *", rid)
    return "cancelled", _reservation_view(r)


@router.post("/reservations/{reservation_id}/cancel")
async def cancel(reservation_id: str, user: Principal = Depends(current_user)):
    try:
        rid = uuid.UUID(reservation_id)
    except ValueError:
        raise ApiError(404, "reservation_not_found", "no such reservation") from None
    annotate(user_id=user.user_id, reservation_id=str(rid))
    outcome, view = await _with_retry(_cancel_tx, rid, user)
    if outcome == "cancelled":
        RESERVATIONS_CANCELLED.inc()
        SEATS_RELEASED.inc(len(view["seats"]))
    annotate(outcome=outcome)
    return view


@router.get("/reservations/{reservation_id}")
async def get_reservation(reservation_id: str, user: Principal = Depends(current_user)):
    try:
        rid = uuid.UUID(reservation_id)
    except ValueError:
        raise ApiError(404, "reservation_not_found", "no such reservation") from None
    async with db.pool().acquire(timeout=settings.db_acquire_timeout_s) as conn:
        r = await conn.fetchrow("SELECT * FROM reservations WHERE id = $1", rid)
    if r is None or r["user_id"] != user.user_id:
        raise ApiError(404, "reservation_not_found", "no such reservation")
    return _reservation_view(r)
