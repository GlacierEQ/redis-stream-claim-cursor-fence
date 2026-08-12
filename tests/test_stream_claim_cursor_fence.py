from __future__ import annotations

from stream_claim_cursor_fence import Decision, StreamClaimCursorFence, StreamClaimCursorFenceRequest


def payload(*, epoch=3, token="fence-3", start=0, observed=100, ack=100, complete=100, pending=0, previous=None, previous_digest=None):
    return {
        "current_fence": {"epoch": epoch, "token": token},
        "consumer_claim": {
            "consumer_id": "consumer-a",
            "stream_id": "orders",
            "fence_epoch": epoch,
            "fence_token": token,
            "start_cursor": start,
            "observed_cursor": observed,
            "acknowledged_cursor": ack,
            "complete_through": complete,
            "pending_count": pending,
            "previous_claim_digest": previous_digest,
        },
        "previous_claim": previous,
    }


def evaluate(data):
    return StreamClaimCursorFence().evaluate(StreamClaimCursorFenceRequest("orders-consumer", data, 1.0))


def test_complete_claim_requires_exact_current_fence_and_cursor() -> None:
    receipt = evaluate(payload())
    assert receipt.decision is Decision.ALLOW
    result = receipt.metrics["result"]
    assert result["complete"] is True
    assert result["complete_through"] == 100
    assert len(result["claim_digest"]) == 64


def test_superseded_fence_epoch_is_refused() -> None:
    data = payload()
    data["consumer_claim"]["fence_epoch"] = 2
    receipt = evaluate(data)
    assert receipt.decision is Decision.REFUSE
    assert "fence_epoch_superseded" in receipt.reasons


def test_fence_token_mismatch_is_refused() -> None:
    data = payload()
    data["consumer_claim"]["fence_token"] = "old-token"
    receipt = evaluate(data)
    assert receipt.decision is Decision.REFUSE
    assert "fence_token_mismatch" in receipt.reasons


def test_pending_entries_block_completeness_claim() -> None:
    receipt = evaluate(payload(pending=2))
    assert receipt.decision is Decision.REFUSE
    assert "pending_entries_remain" in receipt.reasons


def test_claim_cannot_extend_beyond_acknowledged_cursor() -> None:
    receipt = evaluate(payload(observed=110, ack=100, complete=110))
    assert receipt.decision is Decision.REFUSE
    assert "completeness_exceeds_acknowledged_cursor" in receipt.reasons


def test_valid_claim_chain_can_advance_monotonically() -> None:
    first = evaluate(payload(observed=100, ack=100, complete=100))
    first_result = first.metrics["result"]
    previous = {"claim_digest": first_result["claim_digest"], "stream_id": "orders", "consumer_id": "consumer-a", "fence_epoch": 3, "complete_through": 100}
    second = evaluate(payload(start=100, observed=150, ack=150, complete=150, previous=previous, previous_digest=first_result["claim_digest"]))
    assert second.decision is Decision.ALLOW
    assert second.metrics["result"]["complete_through"] == 150


def test_cursor_regression_after_failover_is_refused() -> None:
    previous = {"claim_digest": "a" * 64, "stream_id": "orders", "consumer_id": "consumer-a", "fence_epoch": 3, "complete_through": 100}
    receipt = evaluate(payload(observed=90, ack=90, complete=90, previous=previous, previous_digest="a" * 64))
    assert receipt.decision is Decision.REFUSE
    assert "cursor_regression" in receipt.reasons


def test_broken_previous_claim_chain_is_refused() -> None:
    previous = {"claim_digest": "a" * 64, "stream_id": "orders", "consumer_id": "consumer-a", "fence_epoch": 3, "complete_through": 100}
    receipt = evaluate(payload(start=100, observed=150, ack=150, complete=150, previous=previous, previous_digest="b" * 64))
    assert receipt.decision is Decision.REFUSE
    assert "previous_claim_chain_broken" in receipt.reasons
