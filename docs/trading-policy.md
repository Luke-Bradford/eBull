# Trading Policy

## Initial posture

This system is for long-only investing with a 6 month to 2 year default horizon.

## v1 constraints

- long only
- no leverage
- no shorting
- no options
- buys only from Tier 1 coverage
- no full automation on large allocations
- no averaging down after thesis break
- no trading on stale research
- no trade if data quality checks failed

## Position sizing

Suggested initial defaults:
- max active positions: 20
- max initial position size: 5%
- max fully built position: 10%
- max sector exposure: 25%
- keep cash reserve configurable

## Entry rules

A buy may only proceed if:
- stock is in the tradable eToro universe
- thesis is current
- valuation / buy zone still supports entry
- concentration limits pass
- spread / liquidity checks pass where applicable
- no kill switch is active

## Add rules

Adds require higher standards than an initial buy.
Typical required conditions:
- thesis strengthened
- valuation still acceptable
- no new red flags
- portfolio concentration remains sane

## Exit rules

Exits are allowed for:
- thesis break
- severe risk event
- valuation fully achieved
- superior capital rotation case
- tax-aware reduction where justified

## Audit requirement

Every trade recommendation must answer:
- Why this stock?
- Why now?
- What is the acceptable price band?
- What supports the thesis?
- What argues against it?
- What breaks the thesis?
