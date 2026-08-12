# Stream Claim Cursor Fence

Independent GlacierEQ portfolio implementation aligned to **Redis** operating themes.

> **Not affiliated.** This repository is not affiliated with, endorsed by, employed by, or deployed at Redis. No proprietary access, production deployment, customer impact, or company partnership is claimed.

## Purpose

Make a stream consumer prove completeness against an exact cursor and fencing epoch so failover cannot turn a stale consumer into a false “fully processed” claim.

## Implemented fence

`StreamClaimCursorFence` binds every completeness claim to:

- stream and consumer identity;
- current fence epoch and token;
- start, observed, acknowledged, and complete-through cursors;
- pending entry count;
- optional previous claim digest.

It refuses claims when:

- the consumer is on a superseded fence epoch;
- the fence token does not match;
- completeness extends beyond acknowledged work;
- an observed/unacknowledged tail remains;
- pending entries remain;
- claim-chain identities disagree;
- fence epoch or cursor regresses;
- the previous claim chain is broken or referenced without its receipt.

Valid claims emit a deterministic `claim_digest`, making successive completeness statements a monotonic receipt chain rather than unbound prose.

## Run

```bash
python -m pytest -q
python scripts/operate.py
```

Build and install:

```bash
python -m pip install build
python -m build
python -m pip install dist/*.whl
stream-claim-cursor-fence
```

## Proof surface

- `src/stream_claim_cursor_fence.py` — cursor/fence/claim-chain verifier
- `src/stream_claim_cli.py` — installable execution surface
- `tests/test_stream_claim_cursor_fence.py` — failover, pending, ack, regression and chain behavior
- `tests/test_adversarial.py` — fail-closed adversarial coverage
- `.github/workflows/tests.yml` — tests + cold-start + wheel build/install + installed CLI
- `machine/` — existing Helix control-plane and promotion surfaces remain preserved

## Current boundary

This is a vendor-neutral claim verifier over normalized stream state. It does not control Redis infrastructure or claim production completeness guarantees. The next depth step is a disposable Redis Streams adapter that captures consumer-group cursors and failover epochs and feeds them into this same proof contract.
