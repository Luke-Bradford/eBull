# Per-exchange-id provider sample fixtures

This directory holds raw API response captures, one per `(exchange_id, provider)` pair, for the workstream 2 capability matrix at [`docs/per-exchange-capability-matrix.md`](../../per-exchange-capability-matrix.md).

## Filename pattern

```
{exchange_id}_{provider}_{symbol}.json
```

Where:

- `{exchange_id}` — eBull-side `exchanges.exchange_id` (numeric string, e.g. `7`).
- `{provider}` — `CAPABILITY_PROVIDERS` enum value used in the capability matrix (e.g. `companies_house`, `lse_rns`, `hkex`, `tdnet`).
- `{symbol}` — the **provider-native ticker / company code** for the same instrument (NOT the eToro `symbolFull`). For Companies House this is the company number (`00048839` for Barclays); for HKEX the stock code (`0700`); for TDnet the JP securities code (`7203`). When the provider has no native ticker (regulator portals that key on company registration), use the provider's primary identifier and add a brief comment in the matrix cell that surfaces the cross-walk. Filename-sanitize any chars that aren't `[A-Za-z0-9-]` to `_`.

Examples:

```
7_companies_house_00048839.json   # exchange_id 7 (LSE) via Companies House for Barclays
7_lse_rns_BARC.json               # same instrument via LSE RNS (LSE-native ticker)
21_hkex_0700.json                 # exchange_id 21 (HKEX) via HKEX disclosure for Tencent
13_tdnet_7203.json                # exchange_id 13 (TYO) via TDnet for Toyota
```

This is intentionally different from the per-`instrument_type` etoro-side fixtures under [`../etoro-instrument-samples/`](../etoro-instrument-samples/) which use eToro's `symbolFull` because the source itself IS eToro. Per-exchange samples come from external sources, so the filename uses the external source's native key.

## When to add a sample

Each region investigation ticket (#516–#523) produces samples for the venues in its scope. PR 2 ships this directory with only the README; per-region tickets land their fixtures as they investigate.

eToro's own instruments-endpoint samples live in [`../etoro-instrument-samples/`](../etoro-instrument-samples/) — those are workstream 1 artefacts (#515 PR 1) and are out of scope for this directory.
