# DEV_UP_INSTRUCTIONS — implementation record

**Repository:** `GlacierEQ/redis-stream-claim-cursor-fence`  
**Independent company lens:** Redis  
**Innovation:** Stream Claim Cursor Fence

## Mission

Bind stream completeness claims to exact cursor and fencing state so failover cannot produce stale or regressed success claims.

## Implemented

The generic scaffold has been replaced by a failover-safe cursor/fence claim verifier.

`src/stream_claim_cursor_fence.py` now:

- binds consumer claims to stream/consumer identity, fence epoch/token and exact cursors;
- requires complete-through to be observed and acknowledged with zero pending entries;
- refuses superseded epochs and token mismatch;
- chains successive claims by deterministic prior-claim digest;
- refuses identity drift, fence regression, cursor regression and broken claim chains;
- emits deterministic completeness receipts.

`src/stream_claim_cli.py` and `scripts/operate.py` execute the verifier directly. The project is packaged with the `stream-claim-cursor-fence` console command.

## Verification contract

Behavioral tests cover valid completeness, stale epochs, token mismatch, pending entries, overclaim beyond acknowledgment, monotonic claim chaining, cursor regression and broken previous-claim chains. Existing adversarial coverage remains active.

CI must pass tests, cold-start, wheel build/install and installed CLI execution before Helix promotion evidence can be minted.

## Truth boundary

No Redis affiliation, proprietary access, production deployment, customer impact, or company partnership is claimed. A disposable Redis Streams/failover adapter remains the next end-to-end depth step.
