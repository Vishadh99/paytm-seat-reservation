import asyncio
import collections
import uuid

import pytest

from app import db
from tests.conftest import auth, make_show, reserve, state

pytestmark = pytest.mark.asyncio


async def test_hot_seat_has_exactly_one_winner(client):
    show = await make_show(client, ["A12", "A13"])
    rs = await asyncio.gather(*(reserve(client, show["id"], f"user{i}", ["A12"]) for i in range(300)))
    codes = collections.Counter(r.status_code for r in rs)
    assert codes == {201: 1, 409: 299}
    assert all(r.json()["error"] == "seat_taken" for r in rs if r.status_code == 409)
    s = await state(client, show["id"])
    assert s["counts"] == {"available": 1, "held": 0, "confirmed": 1, "total": 2}


async def test_parallel_retries_same_key_create_one_reservation(client):
    show = await make_show(client, ["A1", "A2", "A3"])
    rs = await asyncio.gather(*(reserve(client, show["id"], "alice", ["A1"], key="k-1") for _ in range(25)))
    assert {r.status_code for r in rs} == {201}
    assert len({r.json()["reservation_id"] for r in rs}) == 1
    assert sum(r.headers.get("Idempotent-Replayed") == "true" for r in rs) == 24
    # Same key, different seats -> 409, and nothing moves.
    r = await reserve(client, show["id"], "alice", ["A2"], key="k-1")
    assert r.status_code == 409 and r.json()["error"] == "idempotency_key_reuse"
    assert (await state(client, show["id"]))["counts"]["confirmed"] == 1


async def test_same_key_racing_different_bodies(client):
    show = await make_show(client, ["A1", "A2"])
    rs = await asyncio.gather(*(
        reserve(client, show["id"], "bob", ["A1"] if i % 2 else ["A2"], key="race") for i in range(20)))
    ok = [r for r in rs if r.status_code == 201]
    assert len({r.json()["reservation_id"] for r in ok}) == 1
    assert all(r.json()["error"] == "idempotency_key_reuse" for r in rs if r.status_code == 409)
    assert (await state(client, show["id"]))["counts"]["confirmed"] == 1


async def test_keys_are_scoped_per_user(client):
    show = await make_show(client, ["A1", "A2"])
    a = await reserve(client, show["id"], "u1", ["A1"], key="shared")
    b = await reserve(client, show["id"], "u2", ["A2"], key="shared")
    assert a.status_code == b.status_code == 201
    assert a.json()["reservation_id"] != b.json()["reservation_id"]


async def test_per_user_limit_under_parallel_requests(client):
    seats = [f"B{i}" for i in range(20)]
    show = await make_show(client, seats, limit=4)
    rs = await asyncio.gather(*(reserve(client, show["id"], "greedy", [s]) for s in seats[:10]))
    codes = collections.Counter(r.status_code for r in rs)
    assert codes[201] == 4 and codes[409] == 6
    assert all(r.json()["error"] == "per_user_limit" for r in rs if r.status_code == 409)
    s = await state(client, show["id"])
    assert s["counts"]["confirmed"] == 4


async def test_multi_seat_is_all_or_nothing_and_deadlock_free(client):
    seats = [f"C{i}" for i in range(6)]
    show = await make_show(client, seats, limit=10)
    # Overlapping multi-seat requests in opposite orders: the classic deadlock shape.
    reqs = []
    for i in range(60):
        pair = ["C1", "C2", "C3"] if i % 2 else ["C3", "C2", "C1"]
        reqs.append(reserve(client, show["id"], f"m{i}", pair))
        reqs.append(reserve(client, show["id"], f"n{i}", ["C3", "C4"] if i % 2 else ["C4", "C3"]))
    rs = await asyncio.gather(*reqs)
    assert all(r.status_code in (201, 409) for r in rs)
    winners = [r.json() for r in rs if r.status_code == 201]
    owned = collections.Counter(seat for w in winners for seat in w["seats"])
    assert all(v == 1 for v in owned.values())
    s = await state(client, show["id"])
    assert s["counts"]["confirmed"] == sum(owned.values())  # no partial claims leaked


async def test_cancel_owner_only_and_rebookable(client):
    show = await make_show(client, ["D1"])
    r = await reserve(client, show["id"], "owner", ["D1"])
    rid = r.json()["reservation_id"]
    bad = await client.post(f"/reservations/{rid}/cancel", headers=auth("intruder"))
    assert bad.status_code == 403
    ok = await client.post(f"/reservations/{rid}/cancel", headers=auth("owner"))
    assert ok.status_code == 200 and ok.json()["status"] == "cancelled"
    again = await client.post(f"/reservations/{rid}/cancel", headers=auth("owner"))
    assert again.status_code == 200  # idempotent
    rs = await asyncio.gather(*(reserve(client, show["id"], f"r{i}", ["D1"]) for i in range(50)))
    assert collections.Counter(x.status_code for x in rs) == {201: 1, 409: 49}


async def test_cancel_never_resurrects_someone_elses_seat(client):
    show = await make_show(client, ["E1"])
    first = (await reserve(client, show["id"], "u1", ["E1"])).json()["reservation_id"]
    await client.post(f"/reservations/{first}/cancel", headers=auth("u1"))
    second = await reserve(client, show["id"], "u2", ["E1"])
    assert second.status_code == 201
    await client.post(f"/reservations/{first}/cancel", headers=auth("u1"))  # stale cancel
    s = await state(client, show["id"])
    assert s["seats"] == [{"seat": "E1", "status": "confirmed"}]


async def test_cancel_racing_reserves_keeps_invariant(client):
    show = await make_show(client, [f"F{i}" for i in range(4)], limit=4)
    r = await reserve(client, show["id"], "holder", ["F0", "F1"])
    rid = r.json()["reservation_id"]
    tasks = [client.post(f"/reservations/{rid}/cancel", headers=auth("holder"))]
    tasks += [reserve(client, show["id"], f"x{i}", ["F0", "F1"]) for i in range(30)]
    rs = await asyncio.gather(*tasks)
    assert all(x.status_code < 500 for x in rs)
    s = await state(client, show["id"])
    assert s["counts"]["confirmed"] in (0, 2)


async def test_identity_comes_from_token_not_body(client):
    show = await make_show(client, ["G1"])
    r = await reserve(client, show["id"], "mallory", ["G1"], extra={"user_id": "victim"})
    assert r.status_code == 201 and r.json()["user_id"] == "mallory"


async def test_unauthenticated_and_bad_input_are_4xx(client):
    show = await make_show(client, ["H1"])
    r = await client.post(f"/shows/{show['id']}/reserve", json={"seats": ["H1"]})
    assert r.status_code == 401
    r = await reserve(client, show["id"], "u", ["NOPE"])
    assert r.status_code == 422
    r = await reserve(client, str(uuid.uuid4()), "u", ["H1"])
    assert r.status_code == 404
    r = await client.post("/shows", json={"name": "x", "seats": ["A1"], "price_paise": 99.5},
                          headers=auth("a", admin=True))
    assert r.status_code == 422
    r = await client.post("/shows", json={"name": "x", "seats": ["A1"], "price_paise": 100}, headers=auth("a"))
    assert r.status_code == 403


async def test_amount_is_integer_paise(client):
    show = await make_show(client, ["J1", "J2"], price=19999)
    r = await reserve(client, show["id"], "u", ["J2", "J1"])
    assert r.json()["amount_paise"] == 39998 and r.json()["seats"] == ["J1", "J2"]


async def test_readiness_fails_closed_when_db_down(client):
    assert (await client.get("/readyz")).status_code == 200
    await db.close_pool()
    await db.init_pool()
    real = db._pool
    await real.close()  # simulate the dependency going away
    r = await client.get("/readyz")
    assert r.status_code == 503 and r.json()["db"] == "down"
    assert (await client.get("/healthz")).status_code == 200  # liveness unaffected


async def test_metrics_reconcile_with_state(client):
    show = await make_show(client, ["K1", "K2", "K3"])
    await reserve(client, show["id"], "a", ["K1"])
    await reserve(client, show["id"], "b", ["K1"])
    text = (await client.get("/metrics")).text
    assert f'seats_available{{show_id="{show["id"]}"}} 2.0' in text
    assert f'reconciliation_invariant_ok{{show_id="{show["id"]}"}} 1.0' in text
    assert 'reservations_declined_total{reason="seat_taken"}' in text
