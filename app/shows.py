"""Show creation and read-side state."""
import re
import uuid

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field, field_validator

from . import db
from .auth import Principal, current_admin
from .config import settings
from .errors import ApiError

router = APIRouter()

SEAT_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")
MAX_SEATS = 20_000


def parse_show_id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise ApiError(404, "show_not_found", "no such show") from None


class CreateShow(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    seats: list[str] = Field(min_length=1, max_length=MAX_SEATS)
    price_paise: int = Field(ge=0, le=10**12, strict=True)  # strict: 250.5 or "250" rejected
    per_user_limit: int = Field(default=settings.default_per_user_limit, ge=1, le=100, strict=True)

    @field_validator("seats")
    @classmethod
    def _seats(cls, v: list[str]) -> list[str]:
        bad = [s for s in v if not SEAT_RE.match(s)]
        if bad:
            raise ValueError(f"invalid seat labels: {bad[:5]}")
        if len(set(v)) != len(v):
            raise ValueError("duplicate seat labels")
        return v


async def load_show_state(conn, show_id: uuid.UUID) -> dict:
    show = await conn.fetchrow("SELECT * FROM shows WHERE id = $1", show_id)
    if show is None:
        raise ApiError(404, "show_not_found", "no such show")
    # One statement => one MVCC snapshot, so the per-seat list and the counts derived
    # from it are mutually consistent even while reservations are landing.
    rows = await conn.fetch("SELECT label, status FROM seats WHERE show_id = $1 ORDER BY ordinal", show_id)
    counts = {"available": 0, "held": 0, "confirmed": 0}
    for r in rows:
        counts[r["status"]] += 1
    return {
        "id": str(show["id"]),
        "name": show["name"],
        "price_paise": show["price_paise"],
        "per_user_limit": show["per_user_limit"],
        "total_seats": show["total_seats"],
        "counts": {**counts, "total": len(rows)},
        "invariant_ok": sum(counts.values()) == show["total_seats"],
        "seats": [{"seat": r["label"], "status": r["status"]} for r in rows],
    }


@router.post("/shows", status_code=201)
async def create_show(body: CreateShow, admin: Principal = Depends(current_admin)):
    show_id = uuid.uuid4()
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO shows (id, name, price_paise, per_user_limit, total_seats) VALUES ($1,$2,$3,$4,$5)",
                show_id, body.name, body.price_paise, body.per_user_limit, len(body.seats),
            )
            await conn.execute(
                "INSERT INTO seats (show_id, label, ordinal) "
                "SELECT $1, label, ord::int FROM unnest($2::text[]) WITH ORDINALITY AS t(label, ord)",
                show_id, body.seats,
            )
        return await load_show_state(conn, show_id)


@router.get("/shows/{show_id}")
async def get_show(show_id: str):
    sid = parse_show_id(show_id)
    async with db.pool().acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            return await load_show_state(conn, sid)
