#!/usr/bin/env python3
"""replaycheck - find webhook deliveries (or cron ticks) that ran the same side effect twice.

Two subcommands, Python 3 standard library only:

  audit     Read a log of received deliveries (JSONL) and, optionally, a log of the side
            effects your handler performed (JSONL). Group deliveries by the key a correct
            handler should have deduplicated on, and report every group that was delivered
            more than once and every group whose side effect ran more than once.

  simulate  Reproduce the "two cron ticks double-charge a customer" race in-process:
            two workers run check-then-act against a shared store at the same moment,
            then the same two workers run an atomic claim. Prints both outcomes.

Exit codes (audit): 0 = no side effect ran twice, 1 = at least one side effect ran more
than once (or, with no effects log, at least one event was delivered more than once and
would have run twice under a non-idempotent handler), 2 = input unreadable. An unreadable
line is never silently skipped: it makes the whole run exit 2.
"""
import argparse
import hashlib
import json
import sqlite3
import sys
import threading

# Headers that carry a provider-assigned, per-event unique id. Lower-cased.
EVENT_ID_HEADERS = (
    "idempotency-key",          # generic / your own senders
    "x-github-delivery",        # GitHub: one GUID per delivery attempt group
    "x-shopify-event-id",       # Shopify: stable across retries of the same event
    "x-shopify-webhook-id",     # Shopify: fallback
    "webhook-id",               # Standard Webhooks (Svix and others)
    "svix-id",
)


def _get(obj, dotted):
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _body_obj(delivery):
    body = delivery.get("body")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except ValueError:
            return None
    return body if isinstance(body, dict) else None


def dedupe_key(delivery):
    """Return (key, source). source says which rule produced the key."""
    headers = {str(k).lower(): v for k, v in (delivery.get("headers") or {}).items()}
    for h in EVENT_ID_HEADERS:
        if headers.get(h):
            return "%s:%s" % (h, headers[h]), "header " + h
    body = _body_obj(delivery)
    if body is not None:
        # Stripe event objects carry id "evt_..."; Slack Events API carries event_id.
        if isinstance(body.get("id"), str) and body["id"].startswith("evt_"):
            return "stripe:" + body["id"], "body.id (Stripe event)"
        if isinstance(body.get("event_id"), str):
            return "slack:" + body["event_id"], "body.event_id (Slack)"
    raw = delivery.get("body")
    if raw is None:
        return None, "no key and no body"
    canon = raw if isinstance(raw, str) else json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canon.encode()).hexdigest()[:16], "body hash (no event id present)"


def read_jsonl(path):
    rows, bad = [], []
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError("not an object")
                rows.append(obj)
            except ValueError as exc:
                bad.append((n, str(exc)))
    return rows, bad


def audit(args):
    try:
        deliveries, bad = read_jsonl(args.deliveries)
        effects, bad_fx = read_jsonl(args.effects) if args.effects else ([], [])
    except OSError as exc:
        print("UNREADABLE: %s" % exc)
        return 2
    if bad or bad_fx:
        print("VERDICT UNKNOWN - input unreadable, nothing was scored")
        for n, e in bad:
            print("  deliveries line %d: %s" % (n, e))
        for n, e in bad_fx:
            print("  effects line %d: %s" % (n, e))
        return 2

    groups, unkeyed = {}, []
    for i, d in enumerate(deliveries, 1):
        key, source = dedupe_key(d)
        if key is None:
            unkeyed.append(i)
            continue
        groups.setdefault(key, {"source": source, "rows": []})["rows"].append(i)

    fx_count = {}
    for fx in effects:
        k = fx.get("key")
        if k:
            fx_count[k] = fx_count.get(k, 0) + 1

    business = {}
    if args.business_key:
        fields = [f.strip() for f in args.business_key.split(",") if f.strip()]
        for key, g in groups.items():
            body = _body_obj(deliveries[g["rows"][0] - 1]) or {}
            vals = tuple(_get(body, f) for f in fields)
            if all(v is not None for v in vals):
                business.setdefault(vals, []).append(key)

    replays = {k: g for k, g in groups.items() if len(g["rows"]) > 1}
    doubled = {k: n for k, n in fx_count.items() if n > 1}
    same_action = {v: ks for v, ks in business.items() if len(ks) > 1}

    print("WEBHOOK REPLAY / DEDUPE AUDIT")
    print("deliveries=%d distinct_keys=%d replayed_keys=%d unkeyed=%d effects=%s"
          % (len(deliveries), len(groups), len(replays), len(unkeyed),
             len(effects) if args.effects else "not supplied"))
    for k, g in sorted(replays.items()):
        ran = fx_count.get(k) if args.effects else None
        state = ("RAN %dx" % ran) if ran is not None else "effects unknown"
        print("  REPLAY  %s  delivered %dx (rows %s)  key from %s  -> %s"
              % (k, len(g["rows"]), ",".join(map(str, g["rows"])), g["source"], state))
    for k, n in sorted(doubled.items()):
        print("  DOUBLE  %s  side effect ran %dx" % (k, n))
    for vals, ks in sorted(same_action.items(), key=lambda x: str(x[0])):
        print("  SAME-ACTION  %s -> %d different event keys: %s  (distinct ids, same business action)"
              % (list(vals), len(ks), ", ".join(sorted(ks))))
    if unkeyed:
        print("  UNKEYED rows %s: no event id and no body - cannot be deduplicated" % unkeyed)

    if doubled or same_action:
        print("VERDICT DOUBLE_EXECUTED" if doubled else "VERDICT SAME_ACTION_TWICE")
        return 1
    if replays and not args.effects:
        print("VERDICT REPLAYS_PRESENT - supply --effects to see whether they ran twice")
        return 1
    if unkeyed:
        print("VERDICT UNKNOWN - some deliveries cannot be keyed")
        return 2
    print("VERDICT CLEAN")
    return 0


def _race(store, mode, key, charges, lock_between):
    barrier = threading.Barrier(2)

    def tick(name):
        con = sqlite3.connect(store, timeout=5, isolation_level=None)
        try:
            if mode == "check-then-act":
                seen = con.execute("SELECT 1 FROM done WHERE key=?", (key,)).fetchone()
                barrier.wait()          # both ticks have now read "not done"
                if not seen:
                    with lock_between:
                        charges.append(name)
                    con.execute("INSERT OR IGNORE INTO done(key) VALUES (?)", (key,))
            else:
                barrier.wait()
                cur = con.execute("INSERT OR IGNORE INTO done(key) VALUES (?)", (key,))
                if cur.rowcount == 1:   # only the tick that won the claim acts
                    with lock_between:
                        charges.append(name)
        finally:
            con.close()

    threads = [threading.Thread(target=tick, args=(n,)) for n in ("tick-A", "tick-B")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def simulate(args):
    import os
    import tempfile
    results = {}
    for mode in ("check-then-act", "atomic-claim"):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            con = sqlite3.connect(path)
            con.execute("CREATE TABLE done(key TEXT PRIMARY KEY)")
            con.commit()
            con.close()
            charges = []
            _race(path, mode, args.key, charges, threading.Lock())
            results[mode] = charges
        finally:
            os.unlink(path)
    print("TWO CRON TICKS, SAME JOB KEY %r, STARTED AT THE SAME MOMENT" % args.key)
    for mode, charges in results.items():
        print("  %-15s charges=%d  %s" % (mode, len(charges), ",".join(sorted(charges)) or "-"))
    ok = len(results["atomic-claim"]) == 1 and len(results["check-then-act"]) == 2
    print("RESULT: check-then-act charged twice; atomic claim charged once" if ok
          else "RESULT: race did not reproduce as expected")
    return 0 if ok else 1


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("audit", help="audit a delivery log (and optional effects log)")
    a.add_argument("--deliveries", required=True, help="JSONL: {headers, body, received_at}")
    a.add_argument("--effects", help="JSONL: {key, action} - one row per side effect performed")
    a.add_argument("--business-key", help="comma-separated body fields that identify one business action, "
                                          "e.g. data.object.customer,data.object.amount")
    s = sub.add_parser("simulate", help="reproduce the two-cron-ticks double charge")
    s.add_argument("--key", default="invoice-2026-09-charge")
    args = p.parse_args(argv)
    return audit(args) if args.cmd == "audit" else simulate(args)


if __name__ == "__main__":
    sys.exit(main())
