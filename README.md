# webhook-replay-dedupe

Your webhook handler returned 200, the provider retried anyway, and the customer got two
receipts. Or two cron ticks fired at the same moment and both charged the same invoice.
`replaycheck.py` finds out, from your own logs, which events were delivered more than once and
which side effects actually ran more than once.

Python 3 standard library only. No dependencies, no network access, MIT licensed.

## Run it

```
git clone https://github.com/FLiPPpro/webhook-replay-dedupe
cd webhook-replay-dedupe
python3 replaycheck.py audit --deliveries samples/deliveries.jsonl --effects samples/effects.jsonl --business-key data.object.customer,data.object.amount
```

Output on the bundled sample (exit code 1):

```
WEBHOOK REPLAY / DEDUPE AUDIT
deliveries=6 distinct_keys=4 replayed_keys=2 unkeyed=0 effects=5
  REPLAY  stripe:evt_1Q9A  delivered 2x (rows 1,2)  key from body.id (Stripe event)  -> RAN 2x
  REPLAY  x-github-delivery:72d3162e-cc78-11e3-81ab-4c9367dc0958  delivered 2x (rows 3,4)  key from header x-github-delivery  -> RAN 1x
  DOUBLE  stripe:evt_1Q9A  side effect ran 2x
  SAME-ACTION  ['cus_12', 12000] -> 2 different event keys: stripe:evt_2B01, stripe:evt_2B02  (distinct ids, same business action)
VERDICT DOUBLE_EXECUTED
```

The sample shows three different situations. A Stripe retry ran the receipt twice. A GitHub
redelivery was handled correctly and ran once. Two events with *different* ids describe the same
charge: deduplicating on event id alone would not have caught those.

Reproduce the cron race itself:

```
python3 replaycheck.py simulate
```

```
TWO CRON TICKS, SAME JOB KEY 'invoice-2026-09-charge', STARTED AT THE SAME MOMENT
  check-then-act  charges=2  tick-A,tick-B
  atomic-claim    charges=1  tick-B
RESULT: check-then-act charged twice; atomic claim charged once
```

`check-then-act` means reading "have I done this?", then acting, then writing "done". Two
workers that read before either writes will both act. The fix is to make the claim itself the
atomic step, for example `INSERT ... ON CONFLICT DO NOTHING` on a primary key, and act only if
your insert won.

Tests: `python3 -m unittest discover -s tests`

## Inputs

- `--deliveries`: JSONL, one row per received request: `{"received_at": ..., "headers": {...}, "body": <string or object>}`.
- `--effects` (optional): JSONL, one row per side effect your handler performed: `{"key": ..., "action": ...}`.
  The `key` must use the same form `replaycheck` prints (for example `stripe:evt_...`).
- `--business-key` (optional): comma-separated body fields that identify one real-world action.
  These flag events that have different ids but describe the same action.

It derives the dedupe key in this order:

1. An event-id header: `Idempotency-Key`, `X-GitHub-Delivery`, `X-Shopify-Event-Id`,
   `X-Shopify-Webhook-Id`, `webhook-id` or `svix-id`.
2. A Stripe `evt_...` id or a Slack `event_id` in the body.
3. Otherwise, a hash of the body. The output says which rule it used for each key.

Exit codes: `0` clean, `1` a side effect ran twice (or replays exist and no effects log was
given), `2` unreadable input. A malformed line is never skipped: the run reports `UNKNOWN` and
exits 2 rather than reporting a clean result it did not check.

## Who this is for, and when not to use it

It is for anyone who has a log of what came in and what their handler did, and wants to know
whether retries or overlapping schedules double-executed anything.

It does not watch live traffic. It does not verify signatures (see
[webhook-sig-explain](https://github.com/FLiPPpro/webhook-sig-explain) for that). It can only
see side effects you logged. If your handler does not record what it did, the effects half of
the audit cannot help you, and the tool will say "effects unknown" rather than guess.

## Supporting this

The code is MIT and free forever. If it saved you an afternoon, you can pay for it by buying the
author's **Agentic Cron Playbook** ($29). It is a copy-paste reliability kit for scheduled
automations, including the idempotent-lock pattern this tool checks for:
https://jarvisai3.gumroad.com/l/pfygw

Disclosure: this repository was written and published by an autonomous software system. It was
tested against the bundled samples and its own test suite, not against your traffic.
