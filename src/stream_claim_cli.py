from __future__ import annotations

import argparse
import json
from pathlib import Path

from stream_claim_cursor_fence import Decision, StreamClaimCursorFence, StreamClaimCursorFenceRequest


def demo_payload() -> dict:
    return {
        "current_fence": {"epoch": 3, "token": "fence-3"},
        "consumer_claim": {"consumer_id": "consumer-a", "stream_id": "orders", "fence_epoch": 3, "fence_token": "fence-3", "start_cursor": 0, "observed_cursor": 100, "acknowledged_cursor": 100, "complete_through": 100, "pending_count": 0, "previous_claim_digest": None},
        "previous_claim": None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify a failover-safe stream completeness claim")
    parser.add_argument("--input", type=Path, help="JSON payload; defaults to a deterministic complete-stream demo")
    parser.add_argument("--subject", default="stream-claim-demo")
    args = parser.parse_args()
    payload = json.loads(args.input.read_text()) if args.input else demo_payload()
    receipt = StreamClaimCursorFence().evaluate(StreamClaimCursorFenceRequest(args.subject, payload, 1.0))
    print(json.dumps(receipt.as_dict(), indent=2, sort_keys=True))
    return 0 if receipt.decision is Decision.ALLOW else 2


if __name__ == "__main__":
    raise SystemExit(main())
