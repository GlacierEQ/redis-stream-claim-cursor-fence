# Issue contract — Stream Claim Cursor Fence

## Problem
Consumers claim stream completeness without cursor/fence semantics under failover.

## Desired outcome
A bounded, open, testable implementation of **Stream Claim Cursor Fence** that demonstrates Bind consumer claims to cursor + fence tokens; refuse completeness claims after fence break.

## Non-goals
- Redis affiliation or proprietary integration
- Portfolio-wide scale/performance claims
- UI marketing site

## Acceptance
1. Mechanism module implements allow + refuse with structured receipts
2. pytest behavioral suite green
3. operate.py cold-start produces JSON receipt
4. Non-affiliation disclaimer preserved
