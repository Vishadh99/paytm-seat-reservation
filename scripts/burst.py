#!/usr/bin/env python3
"""On-sale stampede against a live deployment, then prove it stayed correct.

    python scripts/burst.py https://your-app.example.com            # ~20k requests
    python scripts/burst.py http://localhost:8080 --requests 5000 --concurrency 300

Fires one shuffled, concurrent burst that mixes:
  * hot-seat storm   — H hot seats, each wanted by C distinct users at once
  * idempotent retry — the same (user, key) fired several times in parallel, plus one
                       racing copy of that key with *different* seats
  * per-user limit   — users firing 10 parallel single-seat reserves on a limit-4 show
  * spoofing         — body carries "user_id" of someone else; cancel of others' holds
  * general traffic  — 1–2 seat requests skewed towards the "good" seats
while a sampler polls GET /shows/{id} to check the invariant *during* the burst.

Afterwards it reconciles client-observed outcomes with GET /shows and /metrics and
exits non-zero if any check fails. Requires: pip install aiohttp
"""
import argparse
import asyncio
import collections
import json
import random
import resource
import string
import sys
import time
import uuid

import aiohttp

PASS, FAIL = "\033[32mPASS\033[0m", "\033[31mFAIL\033[0m"


def seat_labels(n: int) -> list[str]:
    per_row = 50
    out = []
    for i in range(n):
        r = i // per_row
        L = string.ascii_uppercase
        row = L[r] if r < 26 else L[r // 26 - 1] + L[r % 26]   # A..Z, AA..ZZ
        out.append(f"{row}{i % per_row + 1}")
    return out


async def post(session, url, payload, token=None, headers=None):
    h = {"content-type": "application/json", **(headers or {})}
    if token:
        h["authorization"] = f"Bearer {token}"
    async with session.post(url, data=json.dumps(payload), headers=h) as r:
        try:
            body = await r.json(content_type=None)
        except Exception:  # noqa: BLE001
            body = {"raw": (await r.text())[:200]}
        return r.status, body, r.headers.get("Idempotent-Replayed") == "true"


async def scrape_metrics(session, base) -> dict[str, float]:
    out = {}
    try:
        async with session.get(f"{base}/metrics") as r:
            for line in (await r.text()).splitlines():
                if line and not line.startswith("#"):
                    k, _, v = line.rpartition(" ")
                    try:
                        out[k] = float(v)
                    except ValueError:
                        pass
    except Exception:  # noqa: BLE001
        pass
    return out


def pct(xs, p):
    if not xs:
        return 0.0
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base_url")
    ap.add_argument("--requests", type=int, default=20000, help="total reserve requests in the burst")
    ap.add_argument("--concurrency", type=int, default=1000, help="max in-flight requests")
    ap.add_argument("--seats", type=int, default=2000, help="seats in the show")
    ap.add_argument("--users", type=int, default=4000, help="distinct buyers (tokens minted)")
    ap.add_argument("--hot-seats", type=int, default=10)
    ap.add_argument("--hot-contenders", type=int, default=500, help="distinct users per hot seat")
    ap.add_argument("--retry-users", type=int, default=200)
    ap.add_argument("--retry-copies", type=int, default=5)
    ap.add_argument("--limit-users", type=int, default=50)
    ap.add_argument("--admin-key", default=None)
    ap.add_argument("--timeout", type=float, default=120)
    ap.add_argument("--json-report", default=None, help="write a machine-readable report here")
    a = ap.parse_args()
    base = a.base_url.rstrip("/")
    try:  # macOS defaults to 256 open files; each in-flight request needs a socket
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        for want in (65536, 24576, 10240, 4096):
            if soft >= want:
                break
            if hard != resource.RLIM_INFINITY and want > hard:
                continue
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
                break
            except (ValueError, OSError):
                continue
    except (ValueError, OSError):
        pass
    fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    if a.concurrency > fd_limit - 64:
        print(f"» open-file limit is {fd_limit}; capping concurrency {a.concurrency} -> {max(16, fd_limit - 64)}"
              f" (raise it with `ulimit -n 10240`)")
        a.concurrency = max(16, fd_limit - 64)
    else:
        print(f"» open-file limit: {fd_limit}")
    rng = random.Random(42)

    timeout = aiohttp.ClientTimeout(total=a.timeout)
    conn = aiohttp.TCPConnector(limit=a.concurrency, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as s:
        # ---------------------------------------------------------------- setup
        print(f"» target {base}")
        t0 = time.perf_counter()
        for _ in range(60):  # tolerate a cold start
            try:
                async with s.get(f"{base}/readyz") as r:
                    if r.status == 200:
                        break
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(2)
        else:
            print("service never became ready"); return 2
        print(f"» ready after {time.perf_counter() - t0:.1f}s")

        st, body, _ = await post(s, f"{base}/auth/token", {"user_id": "burst-admin", "admin": True, "admin_key": a.admin_key})
        if st != 200:
            print("cannot mint admin token:", st, body); return 2
        admin = body["token"]
        labels = seat_labels(a.seats)
        st, show, _ = await post(s, f"{base}/shows",
                                 {"name": f"burst-{uuid.uuid4().hex[:6]}", "seats": labels, "price_paise": 25000}, admin)
        if st != 201:
            print("cannot create show:", st, show); return 2
        sid, limit, price = show["id"], show["per_user_limit"], show["price_paise"]
        print(f"» show {sid}: {a.seats} seats, per_user_limit={limit}")

        n_users = max(a.users, a.hot_contenders + 10)
        sem = asyncio.Semaphore(200)

        async def mint(uid):
            async with sem:
                return uid, (await post(s, f"{base}/auth/token", {"user_id": uid}))[1]["token"]
        users = [f"u{i:05d}" for i in range(n_users)]
        tokens = dict(await asyncio.gather(*(mint(u) for u in users)))
        print(f"» minted {len(tokens)} user tokens")
        metrics_before = await scrape_metrics(s, base)

        # ------------------------------------------------------- build the mix
        hot = labels[: a.hot_seats]                       # front-row "good" seats
        good = labels[: max(a.hot_seats * 5, a.seats // 10)]
        jobs = []  # (kind, user, payload, header_key)

        for seat in hot:                                   # hot-seat storm
            for u in rng.sample(users, a.hot_contenders):
                jobs.append(("hot", u, {"seats": [seat], "idempotency_key": uuid.uuid4().hex}))

        retry_users = rng.sample(users, a.retry_users)     # idempotent retries
        retry_keys = {}
        for u in retry_users:
            key, seat = f"retry-{uuid.uuid4().hex}", rng.choice(labels)
            retry_keys[u] = key
            for _ in range(a.retry_copies):
                jobs.append(("retry", u, {"seats": [seat], "idempotency_key": key}))
            other = rng.choice([x for x in labels[-50:] if x != seat])
            jobs.append(("retry_conflict", u, {"seats": [other], "idempotency_key": key}))

        limit_users = [f"limit-{i}" for i in range(a.limit_users)]  # per-user limit
        for u in limit_users:
            tokens[u] = (await post(s, f"{base}/auth/token", {"user_id": u}))[1]["token"]
            for seat in rng.sample(labels, 10):
                jobs.append(("limit", u, {"seats": [seat], "idempotency_key": uuid.uuid4().hex}))

        for _ in range(20):                                # spoofed identity in body
            u, victim = rng.sample(users, 2)
            jobs.append(("spoof", u, {"seats": [rng.choice(labels)], "idempotency_key": uuid.uuid4().hex,
                                      "user_id": victim}))

        while len(jobs) < a.requests:                      # general stampede
            u = rng.choice(users)
            pool = good if rng.random() < 0.8 else labels
            k = 1 if rng.random() < 0.7 else 2
            jobs.append(("general", u, {"seats": rng.sample(pool, k), "idempotency_key": uuid.uuid4().hex}))
        rng.shuffle(jobs)

        # ---------------------------------------------------------- the burst
        results = []
        lat = []
        gate = asyncio.Semaphore(a.concurrency)
        url = f"{base}/shows/{sid}/reserve"

        connect_retries = [0]

        async def fire(kind, user, payload):
            async with gate:
                t = time.perf_counter()
                for attempt in range(6):
                    try:
                        st, body, replay = await post(s, url, payload, tokens[user])
                        break
                    except aiohttp.ClientConnectorError as e:
                        # TCP/TLS connect failed => the request never left this machine, so a
                        # retry cannot double-book (and it carries the same idempotency key anyway).
                        connect_retries[0] += 1
                        st, body, replay = 0, {"error": f"network:{type(e).__name__}"}, False
                        await asyncio.sleep(0.2 * 2 ** attempt)
                    except Exception as e:  # noqa: BLE001
                        st, body, replay = 0, {"error": f"network:{type(e).__name__}"}, False
                        break
                lat.append(time.perf_counter() - t)
                results.append((kind, user, payload, st, body, replay))

        during_bad, samples = [], 0
        done = asyncio.Event()

        async def sampler():
            nonlocal samples
            while not done.is_set():
                try:
                    async with s.get(f"{base}/shows/{sid}") as r:
                        d = await r.json()
                        c = d["counts"]
                        samples += 1
                        if c["available"] + c["held"] + c["confirmed"] != d["total_seats"]:
                            during_bad.append(c)
                except Exception:  # noqa: BLE001
                    pass
                await asyncio.sleep(0.5)

        print(f"» firing {len(jobs)} reserve requests at concurrency {a.concurrency} …")
        samp = asyncio.create_task(sampler())
        t_burst = time.perf_counter()
        await asyncio.gather(*(fire(*j) for j in jobs))
        elapsed = time.perf_counter() - t_burst
        done.set(); await samp

        # spoofed cancels: try to cancel someone else's reservation
        winners = [(u, b) for (_, u, _, st, b, rp) in results if st == 201 and not rp]
        spoof_cancel = []
        for owner, b in rng.sample(winners, min(10, len(winners))):
            attacker = rng.choice([x for x in users if x != owner])
            async with s.post(f"{base}/reservations/{b['reservation_id']}/cancel",
                              headers={"authorization": f"Bearer {tokens[attacker]}"}) as r:
                spoof_cancel.append(r.status)

        final = (await (await s.get(f"{base}/shows/{sid}")).json())
        metrics_after = await scrape_metrics(s, base)

    # ------------------------------------------------------------------ analysis
    def reason(st, body, replay):
        if st == 0:
            return body["error"]
        if replay:
            return "idempotent_replay"
        if st == 201:
            return "confirmed"
        return body.get("error", str(st)) if isinstance(body, dict) else str(st)

    by_status = collections.Counter(st for (_, _, _, st, _, _) in results)
    by_reason = collections.Counter(reason(st, b, rp) for (_, _, _, st, b, rp) in results)
    by_kind = collections.defaultdict(collections.Counter)
    for kind, _, _, st, b, rp in results:
        by_kind[kind][reason(st, b, rp)] += 1

    reservations = {}  # reservation_id -> body (unique, from all 201s incl. replays)
    for _, _, _, st, b, _ in results:
        if st == 201:
            reservations[b["reservation_id"]] = b
    seat_owner = collections.defaultdict(set)
    user_seats = collections.Counter()
    for r in reservations.values():
        for seat in r["seats"]:
            seat_owner[seat].add(r["reservation_id"])
        user_seats[r["user_id"]] += len(r["seats"])

    checks = []

    def check(name, ok, detail=""):
        checks.append((name, ok, detail))

    double = {k: v for k, v in seat_owner.items() if len(v) > 1}
    check("no seat confirmed to two reservations", not double, f"{len(double)} double-sold" if double else "")

    hot_ok = True
    hot_detail = []
    for seat in hot:
        w = sum(1 for (k, _, p, st, _, rp) in results if k == "hot" and st == 201 and not rp and p["seats"] == [seat])
        g = sum(1 for (k, _, p, st, b, _) in results if k == "hot" and p["seats"] == [seat] and st == 409)
        # The seat may also have been won by a 'general' request; then 0 hot winners is right.
        owners = seat_owner.get(seat, set())
        answered = sum(1 for (k, _, p, st, _, _) in results if k == "hot" and p["seats"] == [seat] and st != 0)
        if len(owners) != 1 or w > 1 or w + g != answered:
            hot_ok = False
        hot_detail.append(f"{seat}:{w}w/{g}x409")
    check(f"hot seats: exactly one owner each, rest clean 409 ({a.hot_contenders} contenders/seat)", hot_ok,
          " ".join(hot_detail[:5]) + (" …" if len(hot_detail) > 5 else ""))

    n5xx = sum(v for k, v in by_status.items() if k >= 500)
    nnet = by_status.get(0, 0)
    check("zero 5xx across the burst", n5xx == 0, f"{n5xx} x 5xx")
    check("zero network errors / timeouts", nnet == 0, f"{nnet} errors")

    c = final["counts"]
    check("invariant after: available+held+confirmed == total",
          c["available"] + c["held"] + c["confirmed"] == final["total_seats"], str(c))
    check(f"invariant during burst ({samples} samples)", not during_bad and samples > 0, str(during_bad[:2]))
    confirmed_seats_client = sum(len(r["seats"]) for r in reservations.values())
    check("server confirmed seats == seats in client-observed 201s",
          c["confirmed"] == confirmed_seats_client, f"server={c['confirmed']} client={confirmed_seats_client}")
    status_by_seat = {x["seat"]: x["status"] for x in final["seats"]}
    check("every seat in a 201 is confirmed on the server",
          all(status_by_seat.get(s_) == "confirmed" for s_ in seat_owner), "")
    check("amounts are integer paise == price * seats",
          all(isinstance(r["amount_paise"], int) and r["amount_paise"] == price * len(r["seats"])
              for r in reservations.values()), "")

    retry_ok = True
    for u, key in retry_keys.items():
        same_key = [(st, b, rp) for (k, uu, p, st, b, rp) in results if uu == u and p.get("idempotency_key") == key]
        rids = {b["reservation_id"] for st, b, _ in same_key if st == 201}
        creations = sum(1 for st, _, rp in same_key if st == 201 and not rp)
        if len(rids) > 1 or creations > 1:
            retry_ok = False
    check("idempotency: one reservation per key; different body on same key -> 409", retry_ok,
          f"replays={by_reason.get('idempotent_replay', 0)} reuse409={by_reason.get('idempotency_key_reuse', 0)}")

    over = {u: n for u, n in user_seats.items() if n > limit}
    lim_max = max((user_seats.get(u, 0) for u in limit_users), default=0)
    check(f"per-user limit holds (limit={limit}; max seats by a 10-parallel user = {lim_max})", not over,
          f"{len(over)} users over" if over else "")

    spoof_ok = all(b.get("user_id") == u for (k, u, _, st, b, _) in results if k == "spoof" and st == 201)
    check("spoofed body user_id ignored (identity from token)", spoof_ok, "")
    check("cancel of another user's reservation is refused (403)",
          all(x == 403 for x in spoof_cancel), f"statuses={collections.Counter(spoof_cancel)}")

    d_conf = metrics_after.get("reservations_confirmed_total", 0) - metrics_before.get("reservations_confirmed_total", 0)
    new_res = sum(1 for (_, _, _, st, _, rp) in results if st == 201 and not rp)
    m_avail = metrics_after.get(f'seats_available{{show_id="{sid}"}}')
    if metrics_after:
        check("metrics: Δreservations_confirmed_total == new reservations", d_conf == new_res,
              f"metric Δ={d_conf:.0f} client={new_res} (assumes single replica, no concurrent traffic)")
        check("metrics: seats_available gauge == GET /shows available", m_avail == c["available"],
              f"gauge={m_avail} api={c['available']}")

    # ------------------------------------------------------------------ report
    print()
    print(f"burst: {len(results)} requests in {elapsed:.2f}s  ({len(results) / elapsed:.0f} req/s)   "
          f"latency p50={pct(lat, .5) * 1000:.0f}ms p95={pct(lat, .95) * 1000:.0f}ms p99={pct(lat, .99) * 1000:.0f}ms")
    print("\nHTTP status        ", dict(sorted(by_status.items())),
          f"  (client connect retries: {connect_retries[0]})")
    print("\noutcome distribution")
    for k, v in by_reason.most_common():
        print(f"  {k:<24}{v:>7}")
    print("\nby scenario")
    for kind, cnt in by_kind.items():
        print(f"  {kind:<16}", dict(cnt.most_common()))
    print(f"\nfinal show state    {c}   total_seats={final['total_seats']}")
    print(f"distinct reservations {len(reservations)}   seats confirmed (client view) {confirmed_seats_client}")
    print("\nchecks")
    for name, ok, detail in checks:
        print(f"  [{PASS if ok else FAIL}] {name}" + (f"  — {detail}" if detail else ""))
    failed = [c_ for c_ in checks if not c_[1]]
    print(f"\n{'ALL CHECKS PASSED' if not failed else f'{len(failed)} CHECK(S) FAILED'}")

    if a.json_report:
        with open(a.json_report, "w") as f:
            json.dump({"show_id": sid, "elapsed_s": elapsed, "status": by_status, "reasons": by_reason,
                       "final_counts": c, "checks": [{"name": n, "ok": o, "detail": d} for n, o, d in checks]},
                      f, indent=2, default=str)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
