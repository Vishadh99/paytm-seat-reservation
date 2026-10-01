"""Reserve and cancel — where every correctness decision lives.

Lock acquisition order (identical for every code path, which is why we cannot deadlock):

    reserve:  idempotency_keys(user,key) -> user_quota(show,user) -> seats ORDER BY label
    cancel:   reservations(id)           -> user_quota(show,user) -> seats ORDER BY label

A reserve never waits on a reservations row, and a cancel never waits on an
idempotency row, so the two orders cannot form a cycle. Deadlock / serialization
errors are still caught and the whole transaction retried as a defence in depth.
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


class Decline(Exception):
    """A domain decline: rolled back to the savepoint, then recorded against the key."""

    def __init__(self, status: int, reason: str, message: str, **extra):
        self.status = status
        self.reason = reason
        self.body = {"error": reason, "message": message, **extra}


def _request_hash(show_id: uuid.UUID, seats: list[str]) -> str:
    # Canonical form: order of seats in the request does not change its meaning.
    return hashlib.sha256(f"{show_id}|{','.join(sorted(seats))}".encode()).hexdigest()


async def _claim(conn, show, user_id: str, seats: list[str], key: str) -> dict:
    """Runs inside a savepoint. Raises Decline (=> savepoint rollback) or returns body."""
    show_id = show["id"]
    n = len(seats)

    # 0. Cheap pre-check, *not* the decision: if any seat is already gone we can decline
    #    without queueing on row locks. A stale "available" read is harmless because the
    #    guarded UPDATE below re-checks under lock; a stale "taken" read means the seat
    #    really was taken at a point during this request (linearizable decline).
    rows = await conn.fetch(
        "SELECT label, status FROM seats WHERE show_id = $1 AND label = ANY($2::text[])", show_id, seats)
    if len(rows) != n:
        known = {r["label"] for r in rows}
        raise Decline(422, "unknown_seats", "seat(s) do not exist for this show",
                      seats=sorted(s for s in seats if s not in known))
    taken = sorted(r["label"] for r in rows if r["status"] != "available")
    if taken:
        raise Decline(409, "seat_taken", "seat(s) already taken", seats=taken)

    # 1. Per-user limit: check-and-increment in ONE guarded statement. The row lock on
    #    (show,user) serialises a single user's parallel requests; other users never touch it.
    await conn.execute(
        "INSERT INTO user_quota (show_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", show_id, user_id)
    held = await conn.fetchval(
        "UPDATE user_quota SET seats_held = seats_held + $3 "
        "WHERE show_id = $1 AND user_id = $2 AND seats_held + $3 <= $4 RETURNING seats_held",
        show_id, user_id, n, show["per_user_limit"])
    if held is None:
        raise Decline(409, "per_user_limit", f"limit is {show['per_user_limit']} seats per user for this show",
                      limit=show["per_user_limit"])

    # 2. THE atomic decision. Lock candidate rows in deterministic (label) order, keeping
    #    only those still 'available' once we hold the lock (Postgres re-evaluates the
    #    WHERE against the latest committed version after waiting), then flip them.
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
        """,
        show_id, seats, reservation_id, user_id)
    if len(claimed) != n:
        # All-or-nothing: lost the race for at least one seat. Savepoint rollback undoes
        # the partial claim and the quota increment.
        got = {r["label"] for r in claimed}
        raise Decline(409, "seat_taken", "seat(s) already taken", seats=sorted(s for s in seats if s not in got))

    amount = show["price_paise"] * n
    ordered = sorted(seats)
    await conn.execute(
        "INSERT INTO reservations (id, show_id, user_id, seats, amount_paise, status, idempotency_key) "
        "VALUES ($1, $2, $3, $4, $5, 'confirmed', $6)",
        reservation_id, show_id, user_id, ordered, amount, key)
    return {
        "reservation_id": str(reservation_id),
        "show_id": str(show_id),
        "user_id": user_id,
        "seats": ordered,
        "amount_paise": amount,
        "status": "confirmed",
    }


async def _reserve_tx(conn, show_id: uuid.UUID, user: Principal, seats: list[str], key: str, req_hash: str):
    show = await conn.fetchrow("SELECT id, price_paise, per_user_limit FROM shows WHERE id = $1", show_id)
    if show is None:
        raise ApiError(404, "show_not_found", "no such show")

    # Idempotency gate. If another request with the same (user, key) is in flight, this
    # INSERT blocks on its uncommitted row and then resolves: DO NOTHING if it committed,
    # insert if it rolled back. Either way exactly one transaction owns the key.
    inserted = await conn.fetchval(
        "INSERT INTO idempotency_keys (user_id, key, request_hash) VALUES ($1, $2, $3) "
        "ON CONFLICT DO NOTHING RETURNING 1",
        user.user_id, key, req_hash)
    if inserted is None:
        prior = await conn.fetchrow(
            "SELECT request_hash, status_code, response FROM idempotency_keys WHERE user_id = $1 AND key = $2",
            user.user_id, key)
        if prior["request_hash"] != req_hash:
            return "idempotency_key_reuse", 409, {
                "error": "idempotency_key_reuse",
                "message": "idempotency key was already used with a different request body"}
        return "idempotent_replay", prior["status_code"], json.loads(prior["response"])

    try:
        async with conn.transaction():  # savepoint
            body = await _claim(conn, show, user.user_id, seats, key)
        status, outcome, rid = 201, "confirmed", body["reservation_id"]
    except Decline as d:
        status, outcome, body, rid = d.status, d.reason, d.body, None

    await conn.execute(
        "UPDATE idempotency_keys SET status_code = $3, response = $4::json, reservation_id = $5 "
        "WHERE user_id = $1 AND key = $2",
        user.user_id, key, status, json.dumps(body), rid and uuid.UUID(rid))
    return outcome, status, body


async def _with_retry(fn, *args):
    async with db.pool().acquire(timeout=settings.db_acquire_timeout_s) as conn:
        for attempt in range(5):
            try:
                async with conn.transaction():
                    return await fn(conn, *args)
            except _RETRYABLE:
                if attempt == 4:
                    raise


@router.post("/shows/{show_id}/reserve")
async def reserve(
    show_id: str,
    body: ReserveRequest,
    user: Principal = Depends(current_user),
    idempotency_key_header: str | None = Header(default=None, alias="Idempotency-Key"),
):
    sid = parse_show_id(show_id)
    if idempotency_key_header and body.idempotency_key and idempotency_key_header != body.idempotency_key:
        raise ApiError(422, "invalid_request", "Idempotency-Key header and body idempotency_key differ")
    key = idempotency_key_header or body.idempotency_key
    if key is None:
        # No key supplied: the request is still safe, just not deduplicated on retry.
        key = f"auto:{uuid.uuid4()}"
    outcome, status, resp = await _with_retry(_reserve_tx, sid, user, body.seats, key, _request_hash(sid, body.seats))
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
    _, view = await _with_retry(_cancel_tx, rid, user)
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
