#!/usr/bin/env python3
"""Live terminal view of /metrics while a burst runs (stdlib only).

    python scripts/watch.py https://your-app.example.com           # newest show
    python scripts/watch.py https://your-app.example.com <show_id>

Run it in one terminal and `make burst` in another; record the pair for the
"watch it behaving correctly in real time" deliverable.
"""
import re
import sys
import time
import urllib.request

LINE = re.compile(r'^([a-zA-Z_:][\w:]*)(\{.*\})?\s+(\S+)$')


def scrape(base):
    out = []
    with urllib.request.urlopen(f"{base}/metrics", timeout=5) as r:
        for line in r.read().decode().splitlines():
            m = LINE.match(line)
            if m and not line.startswith("#"):
                labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2) or ""))
                out.append((m.group(1), labels, float(m.group(3))))
    return out


def total(ms, name, **want):
    return sum(v for n, l, v in ms if n == name and all(l.get(k) == x for k, x in want.items()))


def main():
    base = sys.argv[1].rstrip("/")
    show = sys.argv[2] if len(sys.argv) > 2 else None
    prev, prev_t = None, None
    print(f"watching {base}/metrics  (Ctrl-C to stop)\n")
    while True:
        try:
            ms = scrape(base)
        except Exception as e:  # noqa: BLE001
            print(f"{time.strftime('%H:%M:%S')}  scrape failed: {e}")
            time.sleep(1)
            continue
        now = time.time()
        sid = show or next((l["show_id"] for n, l, _ in ms if n == "seats_available"), None)
        conf = total(ms, "reservations_confirmed_total")
        decl = {r: total(ms, "reservations_declined_total", reason=r)
                for r in ("seat_taken", "per_user_limit", "idempotent_replay", "idempotency_key_reuse")}
        x5 = sum(v for n, l, v in ms if n == "http_requests_total" and l.get("status", "").startswith("5"))
        reqs = sum(v for n, l, v in ms if n == "http_requests_total")
        rate = (reqs - prev) / (now - prev_t) if prev is not None else 0.0
        prev, prev_t = reqs, now
        a = total(ms, "seats", show_id=sid, status="available")
        h = total(ms, "seats", show_id=sid, status="held")
        c = total(ms, "seats", show_id=sid, status="confirmed")
        ok = total(ms, "reconciliation_invariant_ok", show_id=sid)
        print(f"{time.strftime('%H:%M:%S')}  {rate:7.0f} req/s | confirmed {conf:6.0f} | "
              f"taken {decl['seat_taken']:6.0f} limit {decl['per_user_limit']:4.0f} "
              f"replay {decl['idempotent_replay']:4.0f} reuse {decl['idempotency_key_reuse']:4.0f} | "
              f"5xx {x5:.0f} | show {str(sid)[:8]} avail {a:.0f} held {h:.0f} conf {c:.0f} "
              f"invariant {'OK' if ok == 1 else 'BROKEN'}", flush=True)
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
