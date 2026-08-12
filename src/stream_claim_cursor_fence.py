"""Stream Claim Cursor Fence.

Binds a consumer completeness claim to an exact stream cursor and fencing epoch.
Claims from a superseded failover epoch, regressed cursor, unacked events, or a
broken prior claim chain are refused instead of being reported complete.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class Decision(str, Enum):
    ALLOW = "ALLOW"
    REFUSE = "REFUSE"


@dataclass(frozen=True)
class StreamClaimCursorFenceRequest:
    subject_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    budget: float = 1.0
    grant_id: str | None = None
    not_after: float | None = None


@dataclass(frozen=True)
class StreamClaimCursorFenceReceipt:
    decision: Decision
    reasons: tuple[str, ...]
    digest: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"decision": self.decision.value, "reasons": list(self.reasons), "digest": self.digest, "metrics": self.metrics}


class CursorFenceError(ValueError):
    pass


class StreamClaimCursorFence:
    MIN_BUDGET = 0.0

    @staticmethod
    def _int(value: Any, label: str, *, minimum: int = 0) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise CursorFenceError(f"{label}_invalid")
        return value

    @staticmethod
    def _id(value: Any, label: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise CursorFenceError(f"{label}_missing")
        return value

    @classmethod
    def _fence(cls, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise CursorFenceError("current_fence_missing")
        epoch = cls._int(raw.get("epoch"), "fence_epoch", minimum=1)
        token = cls._id(raw.get("token"), "fence_token")
        return {"epoch": epoch, "token": token, "fingerprint": _digest({"epoch": epoch, "token": token})}

    @classmethod
    def _claim(cls, raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise CursorFenceError("consumer_claim_missing")
        start = cls._int(raw.get("start_cursor"), "start_cursor")
        observed = cls._int(raw.get("observed_cursor"), "observed_cursor")
        acknowledged = cls._int(raw.get("acknowledged_cursor"), "acknowledged_cursor")
        complete_through = cls._int(raw.get("complete_through"), "complete_through")
        pending = cls._int(raw.get("pending_count"), "pending_count")
        if not (start <= acknowledged <= observed):
            raise CursorFenceError("cursor_order_invalid")
        return {
            "consumer_id": cls._id(raw.get("consumer_id"), "consumer_id"),
            "stream_id": cls._id(raw.get("stream_id"), "stream_id"),
            "fence_epoch": cls._int(raw.get("fence_epoch"), "claim_fence_epoch", minimum=1),
            "fence_token": cls._id(raw.get("fence_token"), "claim_fence_token"),
            "start_cursor": start,
            "observed_cursor": observed,
            "acknowledged_cursor": acknowledged,
            "complete_through": complete_through,
            "pending_count": pending,
            "previous_claim_digest": str(raw.get("previous_claim_digest") or "").strip() or None,
        }

    @classmethod
    def _previous(cls, raw: Any) -> dict[str, Any] | None:
        if raw is None:
            return None
        if not isinstance(raw, dict):
            raise CursorFenceError("previous_claim_not_object")
        digest = str(raw.get("claim_digest", "")).strip()
        if not SHA256_RE.fullmatch(digest):
            raise CursorFenceError("previous_claim_digest_invalid")
        return {
            "claim_digest": digest,
            "stream_id": cls._id(raw.get("stream_id"), "previous_stream_id"),
            "consumer_id": cls._id(raw.get("consumer_id"), "previous_consumer_id"),
            "fence_epoch": cls._int(raw.get("fence_epoch"), "previous_fence_epoch", minimum=1),
            "complete_through": cls._int(raw.get("complete_through"), "previous_complete_through"),
        }

    def evaluate(self, req: StreamClaimCursorFenceRequest) -> StreamClaimCursorFenceReceipt:
        reasons: list[str] = []
        if not str(req.subject_id or "").strip():
            reasons.append("subject_id_missing")
        if isinstance(req.budget, bool) or not isinstance(req.budget, (int, float)) or not math.isfinite(float(req.budget)) or float(req.budget) <= self.MIN_BUDGET:
            reasons.append("budget_non_positive_or_invalid")
        payload = req.payload if isinstance(req.payload, dict) else {}
        if not isinstance(req.payload, dict):
            reasons.append("payload_not_object")
        result = None
        try:
            fence = self._fence(payload.get("current_fence"))
            claim = self._claim(payload.get("consumer_claim"))
            previous = self._previous(payload.get("previous_claim"))
            if claim["fence_epoch"] != fence["epoch"]:
                reasons.append("fence_epoch_superseded")
            if claim["fence_token"] != fence["token"]:
                reasons.append("fence_token_mismatch")
            if claim["complete_through"] > claim["acknowledged_cursor"]:
                reasons.append("completeness_exceeds_acknowledged_cursor")
            if claim["complete_through"] != claim["observed_cursor"]:
                reasons.append("unobserved_or_unacked_tail_remains")
            if claim["pending_count"] != 0:
                reasons.append("pending_entries_remain")
            if previous:
                if previous["stream_id"] != claim["stream_id"] or previous["consumer_id"] != claim["consumer_id"]:
                    reasons.append("claim_chain_identity_mismatch")
                if claim["fence_epoch"] < previous["fence_epoch"]:
                    reasons.append("fence_epoch_regression")
                if claim["complete_through"] < previous["complete_through"]:
                    reasons.append("cursor_regression")
                if claim["previous_claim_digest"] != previous["claim_digest"]:
                    reasons.append("previous_claim_chain_broken")
            elif claim["previous_claim_digest"]:
                reasons.append("previous_claim_reference_without_receipt")
            body = {
                "stream_id": claim["stream_id"],
                "consumer_id": claim["consumer_id"],
                "fence_epoch": claim["fence_epoch"],
                "fence_fingerprint": fence["fingerprint"],
                "start_cursor": claim["start_cursor"],
                "complete_through": claim["complete_through"],
                "previous_claim_digest": claim["previous_claim_digest"],
            }
            result = {
                **body,
                "complete": not reasons,
                "claim_digest": _digest(body),
            }
        except CursorFenceError as exc:
            reasons.append(str(exc))
        decision = Decision.REFUSE if reasons else Decision.ALLOW
        metrics = {"result": result}
        body = {"subject_id": req.subject_id, "decision": decision.value, "reasons": reasons, "metrics": metrics}
        return StreamClaimCursorFenceReceipt(decision, tuple(reasons or ["stream_completeness_claim_valid"]), _digest(body), metrics)


Mechanism = StreamClaimCursorFence
