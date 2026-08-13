# Tax Engine

## Goal

Maintain a practical UK tax view from actual fills and cash events.

This is not a substitute for professional tax advice.
It is a ledger and reporting engine.

## Core outputs

- realized gains / losses by tax year
- unrealized P&L snapshot
- dividend totals
- disposal summaries
- estimated tax exposure
- audit trail back to fills

## Matching model

Implement disposal matching in this order:
1. same-day rule
2. acquisitions within the following 30 days
3. Section 104 holding pool

## Required event types

- buy fill
- sell fill
- dividend
- fee
- cash adjustment
- corporate action placeholder

## Notes

Do not bolt tax logic on later.
If the ledger is vague, the tax output will be wrong.

Keep:
- raw fills
- normalized matched disposals
- tax-year views
- notes for manual overrides where unavoidable
