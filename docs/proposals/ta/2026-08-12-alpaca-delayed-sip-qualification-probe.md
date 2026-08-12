# Alpaca delayed-SIP qualification probe

Date: 2026-08-12
Issue: #2520
Status: executable probe complete; live qualification refused because no Alpaca
credentials are configured

## Source rule

- Alpaca's official [market-data FAQ](https://docs.alpaca.markets/docs/market-data-faq)
  says historical SIP data is queryable when the request ends at least 15
  minutes in the past.
- Alpaca's current official [historical market-data
  overview](https://docs.alpaca.markets/docs/historical-stock-data-1) describes
  IEX as the feed available without a subscription. Those statements do not
  establish the entitlement of this account, so the probe must prove `feed=sip`
  directly and must never fall back to IEX.
- The [single-symbol bars
  reference](https://docs.alpaca.markets/reference/stockbarsingle-1) defines the
  `feed`, `adjustment`, `asof`, limit and pagination fields frozen below.
- The response shape used for the corporate-action assertion is independently
  pinned by Alpaca's [official Python SDK
  model](https://github.com/alpacahq/alpaca-py/blob/master/alpaca/data/models/corporate_actions.py).

This qualification is intentionally narrower than #2520's full acceptance
contract. It answers whether the external account can support the next bounded
measurement; it does not predeclare research/holdout intervals, persist an
eligibility census or validate a strategy.

## Purpose

Official Alpaca pages disagree at the boundary eBull needs to rely on: the
market-data FAQ describes historical SIP requests ending at least 15 minutes in
the past, while the current historical-data overview describes IEX as the
no-subscription feed. The application must not implement a market-data source
from the more favourable sentence.

`app/services/alpaca_delayed_sip_probe.py` therefore qualifies the configured
account response directly. It is deliberately not an ingestion provider. It
writes no database rows or files and returns only compact structural evidence:
counts, time bounds, SHA-256 payload fingerprints, the matched split/asset
identity and rate-limit headers.

Run from the repository root after placing free-account credentials in the
process environment:

```bash
PYTHONPATH=. uv run python scripts/probe_2520_alpaca_delayed_sip.py
```

The script reads `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY`. It never includes
either value in output or exceptions. Missing or placeholder credentials return
exit status 2 and a JSON `refused` result.

## Frozen call panel

The probe is hard-capped at 12 HTTP requests and two pages per bar scenario. It
does not retry a denied SIP request as IEX. The panel checks:

- raw SIP one-minute AAPL history at the January 2016 boundary, with deliberately
  small pagination;
- recent raw SIP AAPL and JPM bars, covering Nasdaq- and NYSE-listed names;
- TWTR bars before its 2022 delisting, using an explicit historical `asof`;
- the 2025 Thanksgiving Friday session plus Alpaca's early-close calendar;
- raw versus split-adjusted TSLA bars across its August 2022 split;
- the corporate-action response contract for that split;
- current inactive-asset resolution for TWTR;
- the provider's returned rate-limit headers.

Every bar must have an offset-aware, strictly ascending timestamp, finite
positive and internally consistent OHLC, and non-negative integer volume.
Missing rows, repeated/unfinished pagination, malformed data, HTTP errors, an
absent/mismatched split or inactive asset, and the raw/split samples failing to
differ are refusals. The corporate-action response shape is pinned to Alpaca's
official Python SDK model (`corporate_actions.forward_splits`); a 200 response
with an empty collection is not evidence.

## Current result

The repo, `.env` and current process contain no Alpaca credentials. The real
invocation therefore returns:

```json
{"reason":"APCA_API_KEY_ID and APCA_API_SECRET_KEY are both required","status":"refused"}
```

That is the only honest result available without creating an external account.
It does not prove or disprove free historical SIP entitlement. #2520 remains
open until a credentialed run records the compact result and official terms are
checked for the intended internal research use.

Even a successful result authorises only the next bounded sample/footprint
measurement. It does not authorise full-market retention, ORB promotion or live
09:35 execution; #2521 has already refused the latter under the free-source
constraint.
