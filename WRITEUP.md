# WRITEUP — Seat Reservation at Scale

## 1. The atomic decision

**Postgres makes every decision, as a conditional write. The app never reads and then writes.**
A seat is one row, `seats (show_id, label) PRIMARY KEY`. A CHECK constraint ties
`status` to ownership, so an `available` seat with an owner, or a `confirmed` seat
without one, cannot be represented.

A reserve is one transaction with three guarded steps. Their lock order is fixed:

```
a. INSERT INTO idempotency_keys (user, show, key, hash) ON CONFLICT DO NOTHING RETURNING true
b. INSERT INTO user_quota ... ON CONFLICT DO UPDATE SET seats_held = seats_held + n
       WHERE seats_held + n <= per_user_limit RETURNING seats_held
c. WITH lockable AS (SELECT label FROM seats
                      WHERE show_id = $1 AND label = ANY($2) AND status = 'available'
                      ORDER BY label FOR UPDATE)
   UPDATE seats SET status='confirmed', reservation_id=$r, user_id=$u FROM lockable ... RETURNING label
d. INSERT reservation + bind its response to the key (one statement)
```

**Why (c) is race-free.** Under READ COMMITTED, a `FOR UPDATE` that has to wait on a row
lock re-checks its `WHERE` against the row's newest committed version once it gets the
lock. Take 500 transactions racing for A12. Exactly one gets the lock and flips
the row. The other 499 wait. When they wake, `status = 'available'` is false, so the row
drops out of `lockable` and they update **0 rows**. Fewer returned rows than requested
means a `409 seat_taken`. That is a domain outcome, not an error. There is no window
between "is it free?" and "take it": both happen in one statement, under the row lock.

**Multi-seat requests are all-or-nothing, and they cannot deadlock.** Rows are locked in
`ORDER BY label`, so two requests for `{A12, A13}` and `{A13, A12}` take the locks in the
same order and can never hold one each. If fewer rows come back than were asked for, the
code raises. That rolls back the *whole* transaction, including any seats already claimed
and the quota increment. Across tables, the order is always
`idempotency_keys → user_quota → seats` for reserve and `reservations → user_quota → seats`
for cancel. Reserve never waits on a `reservations` row and cancel never touches
`idempotency_keys`, so no cycle is possible. As defence in depth, a deadlock or
serialization failure retries the transaction (`db_tx_retries_total`, 0 in every burst so
far). The test `test_multi_seat_is_all_or_nothing_and_deadlock_free` fires
opposite-order overlapping requests to exercise exactly this.

**The per-user limit** (step b) is a single guarded upsert on a `(show, user)` row.
Its row lock serializes *one user's* parallel requests, and other users never contend
on it. Ten parallel reserves on a limit-4 show give exactly four 201s.

**The fast path is an optimisation, not the decision.** Load testing showed the 499
losers queueing on the hot seat's row lock while holding pool connections. Before the
transaction, one read-only query (no locks) now answers the cheap cases:

- an idempotent replay
- a key reused with a different body
- unknown seats
- a seat that is *already visibly* taken
- a user who is *already visibly* at their limit

A stale "looks free" read just falls through to the guarded write. A "looks taken"
read means the seat really was taken at some instant during the request, so that
decline is linearizable.

**A mutation test proves the tests can see a double-sell.** I removed
`AND status = 'available'` from (c), and 3 of the 14 tests failed (hot seat, multi-seat,
re-book after cancel).

## 2. Idempotency

- **Where it's stored:** `idempotency_keys (user_id, show_id, key) PRIMARY KEY`, plus the
  `request_hash`, which is a sha256 of the show id and the *sorted* seat list. Order
  doesn't change the request's meaning. Each row also stores the exact JSON response
  (`json`, not `jsonb`, so a replay is byte-identical).
- **Scope:** per authenticated user *and* show. User B can never collide with or
  replay user A's key, and reusing "k1" on another show is a new request.
- **Exactly once:** step (a) is the gate. Two copies of the same key in flight: the
  second `INSERT … ON CONFLICT DO NOTHING` **blocks** on the first one's uncommitted row.
  - If the first commits, the second gets nothing back. It reads the stored response and
    returns it as a replay (`201`, `Idempotent-Replayed: true`).
  - If the first rolls back, the second's insert succeeds and it makes its own attempt.

  So exactly one transaction ever owns a key, and the reservation and key binding commit
  atomically. In the burst, 200 users each fired 5 parallel copies and every key mapped
  to at most one `reservation_id`.
- **Same key, different body:** the hashes differ, so the request gets `409
  idempotency_key_reuse` and nothing moves. This also holds when the two bodies *race*
  each other (`test_same_key_racing_different_bodies`).
- **Declines aren't bound to the key.** A decline rolls back the transaction, key row
  included, so a retry of a declined request is evaluated again against the current
  state. I chose this because a decline "moved nothing", so a retry can't move anything
  extra. The trade-off is that a retry *after* a cancel freed the seat could now succeed.
  I think that's the correct behaviour for a buyer.
- **No key supplied:** the request is still safe, it just isn't deduplicated
  (`auto:<uuid>`).
- **Not done yet:** key expiry/TTL cleanup (a periodic `DELETE … WHERE created_at < now()
  - interval '24h'`).

## 3. Holds & expiry (release model)

I chose an **explicit cancel**. A reserve confirms immediately, as in the spec's 201
example, and `POST /reservations/{id}/cancel` releases it.

- Only the owner can cancel: the token's `sub` must equal `reservations.user_id`, or the
  cancel gets `403`.
- The transaction locks the reservation row, decrements the quota, and frees seats
  **guarded on `reservation_id = $this`**. So a stale or duplicate cancel cannot bring
  back a seat that has since been resold to someone else
  (`test_cancel_never_resurrects_someone_elses_seat`). A repeated cancel is a no-op 200.
- A released seat is cleanly re-bookable. After a cancel, a 50-way race on that seat
  produces exactly one winner.

The schema already has a `held` status, because I'd extend this to time-boxed holds next:
reserve → `held` with `held_until`, then `POST /reservations/{id}/confirm` (payment)
→ `confirmed`. Expiry would be **lazy plus a sweeper**:

- The seat-claim predicate becomes
  `status = 'available' OR (status = 'held' AND held_until < now())`, so an expired hold
  is reclaimable inside the same atomic step.
- Confirm is guarded on `status='held' AND held_until >= now() AND reservation_id=$me`.
- A periodic sweeper returns expired holds to `available` and fixes the quota, so the
  counts stay truthful.

## 4. Consistency vs availability under a partition

**This service is CP.** It is the system of record for a unique, scarce thing, and a
double-sell is worse than turning someone away. There is one Postgres primary, and every
decision is a transaction on it.

- If the app can't reach Postgres, `/readyz` returns 503, so the load balancer stops
  routing to it. Reserve, cancel and show reads return **503 `unavailable` with
  `Retry-After`**. They never guess, and they never serve a decision from a cache or a
  replica.
- **The ambiguous commit.** If the connection drops *during* COMMIT, the client gets a 503
  but the reservation may have committed. The idempotency key makes this safe: the client
  retries with the same key and either gets the original reservation as a replay or makes
  a fresh attempt. It can never get two.
- **Scaling reads (not done):** `GET /shows` could be served from a replica or cache, but
  it must be labelled possibly-stale and must never feed a decision.
- **Scaling writes:** shard by `show_id`. A show is the natural unit, since no transaction
  spans shows. More workers or replicas need no coordination, because the database is the
  only shared state. `WEB_CONCURRENCY=2` and four local workers both pass the same burst.

## 5. Observability — what pages me at 2am

**Page (correctness):**

- `reconciliation_invariant_ok == 0` for any show
- any 5xx on `/shows/{id}/reserve`: `rate(http_requests_total{route=~".*reserve",status=~"5.."}[1m]) > 0`
- `db_up == 0`, or `/readyz` failing for more than 1 minute
- `db_tx_retries_total` increasing (a deadlock means the lock-order assumption broke)

**Ticket / dashboard:**

- p99 of `http_request_duration_seconds{route="/shows/{show_id}/reserve"}`
- `http_requests_in_flight` and `db_pool_idle == 0` (saturation: are we queueing for
  connections?)
- the mix of `reservations_declined_total{reason}`. A sudden jump in `per_user_limit` or
  `idempotency_key_reuse` usually means a client bug, not an attack.

**Why the metrics reconcile.** Business counters are incremented only after the
transaction returns, that is, after COMMIT. Seat gauges aren't tracked in-process at all.
They're built on every scrape from one Postgres snapshot, so `seats_available` *is* the
`GET /shows` number. The burst script checks both: Δ`reservations_confirmed_total` ==
new 201s, and the gauge equals the API. In multi-worker mode I found that a regular
`Gauge` wrote per-pid duplicates into the shared multiprocess files. State gauges are now
emitted as metric families from a per-scrape collector.

**Logs.** Every request writes one JSON line with a `request_id` (propagated from
`X-Request-ID`, echoed back) and its domain outcome (`user_id`, `show_id`, `seats`,
`outcome`, `reservation_id`). `GET /logs?request_id=…` gives public access without
platform credentials.

## 6. Performance notes (found by load testing, not up front)

1. **Planner statistics on a fresh table.** `EXPLAIN ANALYZE` on a brand-new DB showed
   `label = ANY($seats)` planned as a *filter* over the whole show, not as an index
   condition. The cause: with no stats, Postgres thinks a show has about one seat. The
   query took 2.0 ms vs 0.15 ms after `ANALYZE`, and each reserve paid that 2–3 times. The
   fix is `ANALYZE seats` after creating a show, on the admin path, before the on-sale.
   This matters because the grader hits a fresh deploy.
2. **Hot-seat lock queueing.** This was fixed by the fast path described above.
3. **CPU.** A single Python process tops out around 900 reserve req/s locally.
   `WEB_CONCURRENCY` scales that with vCPUs, and correctness is unchanged.

## 7. AI usage — directed vs decided

I used Claude (Anthropic's assistant, in Cowork) heavily, as the assignment invites.
Here is the honest split.

**What I decided / directed**
- The stack: Python + FastAPI + Postgres, to play to my strengths.
- The working mode: pair-build in small commits, with each design decision explained
  before moving on, so that I can defend and extend it live.
- That the platform choice would be made against free-tier constraints.
- *(Vishadh: add anything you changed or pushed back on while reviewing — be specific.)*

**What the AI proposed and implemented, which I reviewed**
- The core mechanism: guarded `UPDATE … WHERE status='available'` with `ORDER BY label FOR
  UPDATE`, the idempotency gate via `INSERT … ON CONFLICT DO NOTHING`, the lock-order
  argument, and the quota upsert.
- Nearly all of the code, the tests, `burst.py`, `watch.py`, and the first draft of this
  write-up.
- Running the load tests, and diagnosing the two performance issues above (`EXPLAIN
  ANALYZE` on the fast-path query; profiling with py-spy).
- Finding the multiprocess-gauge duplication bug by stopping Postgres and inspecting
  `/metrics`.

**How the work was verified rather than trusted.** These checks were run during the
build session, and I re-ran them myself:
- The mutation test: remove the `status='available'` guard and watch 3 tests fail.
- The 20k burst locally, with 1, 2 and 4 workers, reading the reconciliation checks.
  Then against the live deploy.
- Stopping Postgres under a running service: `/readyz` and reserve both fail closed with
  503 and recover once Postgres is back.

## 8. What I'd do next

- Time-boxed holds and a confirm/payment step (section 3), with a sweeper.
- An idempotency-key TTL and cleanup job, and partitioning `idempotency_keys` by day.
- Rate limiting per user and IP at the edge. Today a single user can still generate load,
  just not seats.
- Optionally move the decision transaction into one PL/pgSQL function, cutting 6 round
  trips to 1. I deferred this to keep the logic readable and debuggable in Python.
- A Grafana dashboard plus alert rules checked into the repo. OpenTelemetry tracing
  across the API and DB.
- Real auth: an external IdP issuing RS256 tokens verified with JWKS, and removing
  `/auth/token`.
- A queue/waiting-room in front of truly massive on-sales, so the DB sees a smooth rate
  instead of a spike.
