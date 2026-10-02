# Seat Reservation at Scale

A small JSON HTTP service that sells assigned seats for a show and stays correct when
tens of thousands of buyers hit "book" in the same second. It never double-sells a
seat, never lets a user go over their limit, and never double-books a retried request.
FastAPI + asyncpg + a single Postgres.

**Live URL:** https://seat-reservation-production-9e4d.up.railway.app

| | |
|---|---|
| Liveness / readiness | `GET /healthz` · `GET /readyz` (pings Postgres, 503 when it can't) |
| Metrics | `GET /metrics` (Prometheus text) |
| Logs | `GET /logs?tail=200` · `GET /logs?request_id=<X-Request-ID>` · `GET /logs?contains=seat_taken` |
| Design and trade-offs | [WRITEUP.md](WRITEUP.md) |

---

## Run the on-sale burst (one command)

```bash
pip install aiohttp
python scripts/burst.py https://seat-reservation-production-9e4d.up.railway.app          # ~20,000 requests
# or
make burst BASE_URL=https://seat-reservation-production-9e4d.up.railway.app
```

The script creates a fresh 2,000-seat show and mints tokens for about 4,000 users.
Then it fires **one shuffled, concurrent burst** that mixes:

| scenario | what it does | must hold |
|---|---|---|
| hot-seat storm | 10 front-row seats × 500 distinct users each, same instant | exactly one 201 per seat, every other request gets 409 |
| idempotent retries | 200 users × 5 parallel copies of the same key, plus one racing copy with *different* seats | one reservation per key; replays return the original; the different body gets 409 |
| per-user limit | 50 users × 10 parallel single-seat reserves (limit 4) | at most 4 seats each |
| spoofing | body carries `"user_id": "<someone else>"`; then cross-user cancels | identity comes from the token; cancel gets 403 |
| general stampede | 1–2 seat requests, 80% aimed at the "good" seats | no double-sell |

While the burst runs it polls `GET /shows/{id}` to check the invariant *during* the
burst. Afterwards it reconciles what the client saw with `GET /shows` and `/metrics`.
It prints the outcome distribution and a PASS/FAIL checklist, and exits non-zero on
any failure.

**Live result** against the deployed Railway service (2 workers, run from a laptop in Mumbai on 2026-10-02; full log in [`docs/live-burst-2026-10-02.txt`](docs/live-burst-2026-10-02.txt); the live metrics view during the same run is in [`docs/live-watch-2026-10-02.txt`](docs/live-watch-2026-10-02.txt)):

```
burst: 20000 requests in 26.28s  (761 req/s)   latency p50=656ms p95=2168ms p99=9022ms

HTTP status         {201: 1953, 409: 18047}   (client connect retries: 0)

outcome distribution
  seat_taken                17760
  confirmed                  1433
  idempotent_replay           520
  idempotency_key_reuse       247
  per_user_limit               40

by scenario
  general          {'seat_taken': 12195, 'confirmed': 1084, 'per_user_limit': 1}
  hot              {'seat_taken': 4990, 'confirmed': 10}
  retry            {'idempotent_replay': 520, 'seat_taken': 226, 'confirmed': 130, 'idempotency_key_reuse': 124}
  limit            {'seat_taken': 288, 'confirmed': 173, 'per_user_limit': 39}
  retry_conflict   {'idempotency_key_reuse': 123, 'seat_taken': 49, 'confirmed': 28}
  spoof            {'seat_taken': 12, 'confirmed': 8}

final show state    {'available': 364, 'held': 0, 'confirmed': 1636, 'total': 2000}   total_seats=2000
distinct reservations 1433   seats confirmed (client view) 1636

checks
  [PASS] no seat confirmed to two reservations
  [PASS] hot seats: exactly one owner each, rest clean 409 (500 contenders/seat)  — A1:1w/499x409 A2:1w/499x409 …
  [PASS] zero 5xx across the burst  — 0 x 5xx
  [PASS] zero network errors / timeouts  — 0 errors
  [PASS] invariant after: available+held+confirmed == total  — {'available': 364, 'held': 0, 'confirmed': 1636, 'total': 2000}
  [PASS] invariant during burst (22 samples)  — []
  [PASS] server confirmed seats == seats in client-observed 201s  — server=1636 client=1636
  [PASS] every seat in a 201 is confirmed on the server
  [PASS] amounts are integer paise == price * seats
  [PASS] idempotency: one reservation per key; different body on same key -> 409  — replays=520 reuse409=247
  [PASS] per-user limit holds (limit=4; max seats by a 10-parallel user = 4)
  [PASS] spoofed body user_id ignored (identity from token)
  [PASS] cancel of another user's reservation is refused (403)  — statuses=Counter({403: 10})
  [PASS] metrics: Δreservations_confirmed_total == new reservations  — metric Δ=1433 client=1433 (assumes single replica, no concurrent traffic)
  [PASS] metrics: seats_available gauge == GET /shows available  — gauge=364.0 api=364

ALL CHECKS PASSED
```

For comparison, a local run:

Real output from a fresh database, 2 workers, run locally (`WEB_CONCURRENCY=2`):

```
burst: 20000 requests in 18.70s  (1069 req/s)   latency p50=865ms p95=2216ms p99=3108ms

HTTP status         {201: 1973, 409: 18027}

outcome distribution
  seat_taken                17756
  confirmed                  1445
  idempotent_replay           528
  idempotency_key_reuse       231
  per_user_limit               40

by scenario
  general          {'seat_taken': 12181, 'confirmed': 1097, 'per_user_limit': 2}
  retry            {'idempotent_replay': 528, 'seat_taken': 234, 'confirmed': 132, 'idempotency_key_reuse': 106}
  hot              {'seat_taken': 4991, 'confirmed': 9}
  limit            {'seat_taken': 289, 'confirmed': 173, 'per_user_limit': 38}
  retry_conflict   {'idempotency_key_reuse': 125, 'seat_taken': 50, 'confirmed': 25}
  spoof            {'seat_taken': 11, 'confirmed': 9}

final show state    {'available': 360, 'held': 0, 'confirmed': 1640, 'total': 2000}   total_seats=2000
distinct reservations 1445   seats confirmed (client view) 1640

checks
  [PASS] no seat confirmed to two reservations
  [PASS] hot seats: exactly one owner each, rest clean 409 (500 contenders/seat)  — A1:1w/499x409 A2:1w/499x409 …
  [PASS] zero 5xx across the burst  — 0 x 5xx
  [PASS] zero network errors / timeouts  — 0 errors
  [PASS] invariant after: available+held+confirmed == total  — {'available': 360, 'held': 0, 'confirmed': 1640, 'total': 2000}
  [PASS] invariant during burst (14 samples)  — []
  [PASS] server confirmed seats == seats in client-observed 201s  — server=1640 client=1640
  [PASS] every seat in a 201 is confirmed on the server
  [PASS] amounts are integer paise == price * seats
  [PASS] idempotency: one reservation per key; different body on same key -> 409  — replays=528 reuse409=231
  [PASS] per-user limit holds (limit=4; max seats by a 10-parallel user = 4)
  [PASS] spoofed body user_id ignored (identity from token)
  [PASS] cancel of another user's reservation is refused (403)  — statuses=Counter({403: 10})
  [PASS] metrics: Δreservations_confirmed_total == new reservations  — metric Δ=1445 client=1445 (assumes single replica, no concurrent traffic)
  [PASS] metrics: seats_available gauge == GET /shows available  — gauge=360.0 api=360

ALL CHECKS PASSED
```

Useful knobs: `--requests`, `--concurrency`, `--seats`, `--hot-seats`,
`--hot-contenders`, `--admin-key`, and `--json-report out.json`.

To **watch it live**, run this in a second terminal:

```bash
python scripts/watch.py https://seat-reservation-production-9e4d.up.railway.app
# 16:06:38  1278 req/s | confirmed 416 | taken 5136 limit 228 replay 684 reuse 256 | 5xx 0 | show c7881cb7 avail 1584 held 0 conf 416 invariant OK
```

---

## Run locally

```bash
docker compose up -d --build --wait      # api on :8080, postgres on :5432  (or: make up)
curl -s localhost:8080/readyz
python scripts/burst.py http://localhost:8080
```

To run the tests against the compose Postgres:

```bash
pip install -r requirements.txt -r requirements-dev.txt
make test        # = DATABASE_URL=postgresql://postgres:postgres@localhost:5432/seats pytest -q
```

The 14 integration tests run in-process against real Postgres and cover:

- a 300-way hot-seat race
- parallel same-key retries
- same key with racing different bodies
- the per-user limit under parallel requests
- opposite-order multi-seat requests (the deadlock shape)
- owner-only cancel
- a stale cancel never resurrecting a seat
- cancel racing reserves
- spoofed identity
- readiness failing closed
- metrics reconciling with state

---

## API

All money is **integer paise**. Identity comes **only** from the bearer token.

### Auth (demo identity provider)
```bash
# user token
curl -s -XPOST $BASE/auth/token -H 'content-type: application/json' -d '{"user_id":"alice"}'
# admin token (if ADMIN_KEY is set on the server, also pass "admin_key")
curl -s -XPOST $BASE/auth/token -H 'content-type: application/json' -d '{"user_id":"ops","admin":true}'
```
→ `{"token": "<HS256 JWT>", ...}`. `POST /auth/token` stands in for a real IdP so that
load tests can mint tokens for many users. The verification path is the same one a
real IdP's tokens would use.

### Create a show — `POST /shows` (admin)
```bash
curl -s -XPOST $BASE/shows -H "authorization: Bearer $ADMIN" -H 'content-type: application/json' \
  -d '{"name":"friday-night","seats":["A1","A2","A3"],"price_paise":25000,"per_user_limit":4}'
```
`201` returns the show with `id`, every seat `available`, and the counts. `per_user_limit`
is optional (default 4). A float `price_paise` is rejected with 422.

### Reserve — `POST /shows/{id}/reserve` (user)
```bash
curl -s -XPOST $BASE/shows/$SHOW/reserve -H "authorization: Bearer $ALICE" \
  -H 'content-type: application/json' -H 'Idempotency-Key: 7b1c…' -d '{"seats":["A12","A13"]}'
```
The key can go in the `Idempotency-Key` header or in the body as `idempotency_key`. If you
send both, they must match.

| status | `error` | meaning |
|---|---|---|
| 201 | — | `{"reservation_id","show_id","user_id","seats","amount_paise","status":"confirmed"}` |
| 201 + `Idempotent-Replayed: true` | — | retry of a key that already succeeded: the original response, byte for byte |
| 409 | `seat_taken` | one or more seats are already held or confirmed (`seats` lists them) |
| 409 | `per_user_limit` | this would take the user over the show's limit |
| 409 | `idempotency_key_reuse` | key already used by this user on this show with a different seat set |
| 422 | `unknown_seats` / `invalid_request` | seat not in the show / malformed body |
| 401 / 404 | `unauthenticated` / `show_not_found` | |
| 503 | `unavailable` | Postgres unreachable — fails closed, `Retry-After: 1` (safe to retry with the same key) |

**Multi-seat requests are all-or-nothing.** If any requested seat is taken, nothing is
reserved.

### Cancel — `POST /reservations/{id}/cancel` (owner only)
Returns `200` with `"status":"cancelled"` and the seats go straight back to `available`.
Cancelling again is a no-op `200`. Anyone other than the owner gets `403`.

`GET /reservations/{id}` (owner only) returns the reservation's current state.

### Show state — `GET /shows/{id}`
```json
{"id":"…","total_seats":2000,"counts":{"available":363,"held":0,"confirmed":1637,"total":2000},
 "invariant_ok":true,"seats":[{"seat":"A1","status":"confirmed"}, …]}
```
`available + held + confirmed == total_seats` always holds, read from a single snapshot.

---

## Observability

**Metrics** (`/metrics`):

| metric | type | notes |
|---|---|---|
| `reservations_confirmed_total`, `seats_confirmed_total` | counter | incremented only after COMMIT |
| `reservations_declined_total{reason}` | counter | `seat_taken`, `per_user_limit`, `idempotent_replay`, `idempotency_key_reuse`, `unknown_seats` |
| `reservations_cancelled_total`, `seats_released_total` | counter | |
| `seats_available{show_id}`, `seats{show_id,status}`, `seats_all_shows{status}` | gauge | read from Postgres at scrape time (25 newest shows), so they always match `GET /shows` |
| `reconciliation_invariant_ok{show_id}` | gauge | 1/0 — alert on 0 |
| `http_requests_total{method,route,status}`, `http_request_duration_seconds` | counter / histogram | RED metrics |
| `http_requests_in_flight`, `db_pool_size`, `db_pool_idle`, `db_up`, `db_tx_retries_total` | gauge / counter | saturation |

With `WEB_CONCURRENCY>1`, counters are aggregated across workers (`PROMETHEUS_MULTIPROC_DIR`).

**Logs.** One JSON line per request on stdout. Each line carries `request_id` (taken from
`X-Request-ID` if you send one, otherwise generated, and always echoed back), `route`,
`status` and `duration_ms`, plus the domain fields `user_id`, `show_id`, `seats`,
`outcome` and `reservation_id`. The last 5,000 lines per worker can be read publicly at
`GET /logs` (filter with `?request_id=` or `?contains=`). The full stream is in the
Railway log viewer.

---

## Deploy (Railway — primary)

Why Railway: no idle spin-down, 2 vCPU / 1 GB per service on the trial, and managed
Postgres on a private network. Render's free tier sleeps after 15 minutes (about a 1-minute
cold start) and its free Postgres expires after 30 days. A blueprint is still included as a
fallback (`render.yaml`).

1. Push this repo to GitHub (public).
2. On railway.com go to **New Project → Deploy from GitHub repo** and pick the repo.
   `railway.json` builds the Dockerfile and health-checks `/readyz`.
3. **+ New → Database → PostgreSQL** in the same project.
4. On the API service, open **Variables** and set:
   ```
   DATABASE_URL     = ${{Postgres.DATABASE_URL}}
   JWT_SECRET       = <long random string>
   WEB_CONCURRENCY  = 2
   DB_POOL_MAX      = 20
   ```
5. Go to **Settings → Networking → Generate Domain**, then:
   `curl https://seat-reservation-production-9e4d.up.railway.app/readyz` → `{"status":"ready","db":"ok"}`
6. `make burst BASE_URL=https://seat-reservation-production-9e4d.up.railway.app`

The schema is created on boot, under an advisory lock, so redeploys and multiple
replicas are safe. Startup retries the DB connection for about 60 seconds, so a cold
start where Postgres comes up late still converges to healthy.

### Configuration

| env | default | |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/seats` | |
| `JWT_SECRET` | dev value | **set in prod** |
| `WEB_CONCURRENCY` | 1 | worker processes, about 1 per vCPU |
| `DB_POOL_MAX` / `DB_POOL_MIN` | 20 / 2 | per worker; keep `workers × max` under Postgres `max_connections` |
| `DB_ACQUIRE_TIMEOUT_S` | 60 | a burst queues for a connection instead of failing |
| `DEFAULT_PER_USER_LIMIT` | 4 | |
| `DEMO_AUTH` | true | enables `POST /auth/token` |
| `ADMIN_KEY` | empty | if set, minting an admin token requires it |

## Layout

```
app/
  main.py            app wiring, health, /metrics, /logs
  reservations.py    reserve + cancel — every correctness decision lives here
  shows.py           create/read shows
  auth.py            token-derived identity (HS256 JWT)
  schema.sql         tables + constraints (applied at boot)
  observability.py   JSON logs, request-id middleware, Prometheus
scripts/burst.py     the on-sale stampede + reconciliation
scripts/watch.py     live metrics view
tests/               concurrency integration tests
```
