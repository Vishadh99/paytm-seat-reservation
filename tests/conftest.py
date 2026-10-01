"""Integration tests run against a real Postgres (DATABASE_URL), in-process via ASGI.

    docker compose up -d db
    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/seats pytest -q
"""
import uuid

import httpx
import pytest_asyncio

from app import db
from app.auth import issue_token
from app.main import app
from app.migrate import migrate


@pytest_asyncio.fixture
async def client():
    pool = await db.init_pool()
    await migrate(pool)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test", timeout=60) as c:
        yield c
    await db.close_pool()


def auth(user: str, admin: bool = False) -> dict:
    return {"authorization": f"Bearer {issue_token(user, admin)}"}


async def make_show(client, seats, price=25000, limit=None) -> dict:
    body = {"name": f"t-{uuid.uuid4().hex[:6]}", "seats": seats, "price_paise": price}
    if limit is not None:
        body["per_user_limit"] = limit
    r = await client.post("/shows", json=body, headers=auth("admin", admin=True))
    assert r.status_code == 201, r.text
    return r.json()


async def reserve(client, show_id, user, seats, key=None, extra=None):
    body = {"seats": seats, "idempotency_key": key or uuid.uuid4().hex, **(extra or {})}
    return await client.post(f"/shows/{show_id}/reserve", json=body, headers=auth(user))


async def state(client, show_id) -> dict:
    r = await client.get(f"/shows/{show_id}")
    assert r.status_code == 200
    d = r.json()
    c = d["counts"]
    assert c["available"] + c["held"] + c["confirmed"] == d["total_seats"]
    return d
