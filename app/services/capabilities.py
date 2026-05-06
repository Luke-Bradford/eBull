"""Per-instrument capability resolution (#515 PR 3).

The instrument-summary endpoint returns, for every v1 capability,
two fields:

* ``providers``: ordered list of provider tags from
  ``CAPABILITY_PROVIDERS`` — the operator-decided source list,
  possibly including providers that aren't yet wired.
* ``data_present``: dict keyed identically to ``providers``, value
  is a bool indicating whether ingest has landed at least one row
  for this instrument from that provider.

Frontend renders the panel iff
``providers.length > 0 AND any(data_present.values())``.

Resolution = exchange row's default ∪ per-instrument
``external_identifiers`` facts. A cross-listed instrument with
both a Companies House number AND a SEC CIK gets BOTH providers
in its ``filings`` list automatically.

This module is the single helper API consumers go through; the
schema migration (sql/071) seeds the exchange-row defaults; the
data-presence dict is computed via SQL EXISTS at API time per
provider.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import psycopg
import psycopg.rows
import psycopg.sql

# Closed set of provider tags. Empty list (= []) is the canonical
# absence-of-provider state — covers both "no public source
# available on this venue" AND "available but not decision-
# relevant on this venue, operator deferred". Never use [None],
# "none", or any sentinel string.
CapabilityProvider = Literal[
    # US — SEC family
    "sec_edgar",  # filings index → filing_events
    "sec_xbrl",  # fundamentals → financial_periods
    "sec_dividend_summary",  # dividends → instrument_dividend_summary
    "sec_8k_events",
    "sec_10k_item1",
    "sec_form4",
    "sec_13f",
    "sec_13d_13g",
    # UK
    "companies_house",
    "lse_rns",
    # EU
    "esma",
    "bafin",
    "amf",
    "consob",
    # Asia
    "hkex",
    "tdnet",
    "edinet",
    "asx",
    "krx",
    "kind",
    "twse",
    "mops",
    "sse",
    "szse",
    "nse_india",
    "bse_india",
    "sgx",
    # MENA
    "tadawul",
    "adx",
    "dfm",
    # Crypto
    "coingecko",
    "glassnode",
    # Commodity / FX
    "cme",
    "lme",
    "ecb",
    "fed",
    "boe",
    # Canada
    "tmx_group",
    "sedar_plus",
]


# v1 capability keys — fixed at 11. options + short_interest are
# explicitly deferred to a follow-up spec; news is tracked under
# #198 via a separate always-on surface.
CapabilityName = Literal[
    "filings",
    "fundamentals",
    "dividends",
    "insider",
    "analyst",
    "ratings",
    "esg",
    "ownership",
    "corporate_events",
    "business_summary",
    "officers",
]

V1_CAPABILITIES: tuple[CapabilityName, ...] = (
    "filings",
    "fundamentals",
    "dividends",
    "insider",
    "analyst",
    "ratings",
    "esg",
    "ownership",
    "corporate_events",
    "business_summary",
    "officers",
)


# Allowed provider tags as a set for runtime validation. The Python
# Literal above is the type-check guard; this set is the runtime
# guard the resolver uses to refuse a row whose JSONB has drifted
# (e.g. a manual operator UPDATE that typo'd a provider name).
_ALLOWED_PROVIDERS: frozenset[str] = frozenset(
    CapabilityProvider.__args__,  # type: ignore[attr-defined]
)


@dataclass(frozen=True)
class CapabilityCell:
    """One (capability × instrument) cell in the summary response.

    Mirrors the JSON shape the API returns — tests read these
    directly to assert on the contract without re-parsing JSON.
    """

    providers: tuple[str, ...]
    data_present: dict[str, bool]


@dataclass(frozen=True)
class ResolvedCapabilities:
    """All 11 v1 capabilities resolved for one instrument."""

    cells: dict[CapabilityName, CapabilityCell]


# Mapping from (capability, provider) tuple to the SQL EXISTS
# test that says "is there at least one row this instrument has
# from this source for this capability?". Keyed on the tuple
# because one provider tag could in principle serve multiple
# capabilities by reading different tables. The (capability,
# provider) keying makes that future-proof without ambiguity.
#
# Capability-agnostic providers (``sec_xbrl`` / ``sec_form4`` etc.)
# repeat the same SQL across the (capability, provider) pairs they
# back; the duplication is intentional — the lookup is by tuple,
# not by provider.
#
# Adding a new provider means adding rows here for every
# capability it serves AND in the Literal above. Out-of-sync = the
# type checker catches it (resolver narrows on ``_ALLOWED_PROVIDERS``).
_PRESENCE_QUERIES: dict[tuple[str, str], str] = {
    # SEC family — wired per #506.
    ("filings", "sec_edgar"): ("SELECT EXISTS(SELECT 1 FROM filing_events f WHERE f.instrument_id = %s)"),
    ("fundamentals", "sec_xbrl"): ("SELECT EXISTS(SELECT 1 FROM financial_periods f WHERE f.instrument_id = %s)"),
    ("dividends", "sec_dividend_summary"): (
        "SELECT EXISTS(SELECT 1 FROM instrument_dividend_summary d WHERE d.instrument_id = %s)"
    ),
    ("corporate_events", "sec_8k_events"): (
        "SELECT EXISTS(SELECT 1 FROM eight_k_filings e WHERE e.instrument_id = %s)"
    ),
    ("business_summary", "sec_10k_item1"): (
        "SELECT EXISTS(SELECT 1 FROM instrument_business_summary b WHERE b.instrument_id = %s)"
    ),
    ("insider", "sec_form4"): ("SELECT EXISTS(SELECT 1 FROM insider_transactions t WHERE t.instrument_id = %s)"),
    # ``ownership`` panel — wired via the post-#788 ``ownership_*_current``
    # tables (#905 read-path cutover). Without these entries, the
    # ownership panel stayed permanently hidden even when the
    # rollup had data — Codex pre-push catch on PR #941.
    ("ownership", "sec_13f"): (
        "SELECT EXISTS(SELECT 1 FROM ownership_institutions_current o WHERE o.instrument_id = %s)"
    ),
    ("ownership", "sec_13d_13g"): (
        "SELECT EXISTS(SELECT 1 FROM ownership_blockholders_current o WHERE o.instrument_id = %s)"
    ),
    # UK / EU / Asia / MENA / crypto / commodity / FX / Canada
    # providers — no eBull tables yet for any of these, so missing
    # entries fall through to ``data_present = False`` via the
    # default branch in ``_compute_data_present``. Each per-region
    # integration PR lands an entry above for the (capability,
    # provider) pair it newly wires.
}


def resolve_capabilities(
    conn: psycopg.Connection[object],
    *,
    instrument_id: int,
    exchange_id: str,
) -> ResolvedCapabilities:
    """Resolve the full capability set for one instrument.

    1. Reads ``exchanges.capabilities`` for the instrument's
       exchange row (the per-exchange_id default).
    2. Augments via per-instrument ``external_identifiers`` facts
       (e.g. an LSE-listed ADR with a SEC CIK adds ``sec_xbrl`` /
       ``sec_form4`` to its ``filings`` / ``insider`` lists).
    3. Computes ``data_present[provider]`` for every provider in
       the resulting list via the per-provider EXISTS query.

    Returns ``ResolvedCapabilities`` — frontend gates panels on
    ``providers AND any(data_present.values())``.
    """
    raw_capabilities: dict[str, list[str]] = {}
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            "SELECT capabilities FROM exchanges WHERE exchange_id = %s",
            (exchange_id,),
        )
        row = cur.fetchone()
    if row is not None and isinstance(row[0], dict):
        raw_capabilities = row[0]

    # Augment via external_identifiers — each (provider,
    # identifier_type) row signals additional provider coverage on
    # the instrument. Today only SEC CIK is wired this way; future
    # per-region integrations file their own external_identifiers
    # entries. Filter on identifier_type='cik' to match every
    # other SEC gate in the codebase (per #506) — a non-CIK SEC
    # row (e.g. a future SEC EDGAR accession number) would NOT
    # imply full SEC capability coverage.
    # Filter on (provider='sec', identifier_type='cik',
    # is_primary=TRUE) to match _has_sec_cik() in app/api/instruments.py
    # — every SEC gate in the codebase trusts only the primary
    # CIK. The schema preserves historical non-primary CIKs (per
    # sql/003_external_identifiers.sql) but they don't imply
    # current SEC coverage.
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT provider, identifier_type, is_primary
              FROM external_identifiers
             WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        ext_rows = cur.fetchall()

    augmentations: dict[CapabilityName, list[str]] = {}
    for ext_provider, ext_identifier_type, ext_is_primary in ext_rows:
        if ext_provider == "sec" and ext_identifier_type == "cik" and ext_is_primary:
            # A primary SEC CIK on a non-US-equity instrument
            # (e.g. an LSE-listed Chinese ADR) adds the SEC
            # capabilities to whatever the exchange row already
            # provides.
            augmentations.setdefault("filings", []).append("sec_edgar")
            augmentations.setdefault("fundamentals", []).append("sec_xbrl")
            augmentations.setdefault("dividends", []).append("sec_dividend_summary")
            augmentations.setdefault("insider", []).append("sec_form4")
            augmentations.setdefault("ownership", []).extend(["sec_13f", "sec_13d_13g"])
            augmentations.setdefault("corporate_events", []).append("sec_8k_events")
            augmentations.setdefault("business_summary", []).append("sec_10k_item1")

    cells: dict[CapabilityName, CapabilityCell] = {}
    for cap in V1_CAPABILITIES:
        seen: set[str] = set()
        ordered: list[str] = []

        # Defensive merge of exchange-row + augmentation lists.
        # If an operator override has typoed the JSON shape (e.g.
        # ``"filings": null`` or ``"filings": "sec_xbrl"`` instead
        # of a list), skip silently rather than 500 — admin UI
        # surfaces overrides so the operator can fix without
        # breaking the instrument page.
        from_exchange = raw_capabilities.get(cap)
        from_augment = augmentations.get(cap, [])
        merged: list[str] = []
        if isinstance(from_exchange, list):
            merged.extend(from_exchange)
        merged.extend(from_augment)

        for raw_provider in merged:
            if not isinstance(raw_provider, str):
                continue
            if raw_provider not in _ALLOWED_PROVIDERS:
                continue
            if raw_provider in seen:
                continue
            seen.add(raw_provider)
            ordered.append(raw_provider)

        data_present = _compute_data_present(
            conn,
            instrument_id=instrument_id,
            capability=cap,
            providers=ordered,
        )
        cells[cap] = CapabilityCell(providers=tuple(ordered), data_present=data_present)

    return ResolvedCapabilities(cells=cells)


def _compute_data_present(
    conn: psycopg.Connection[object],
    *,
    instrument_id: int,
    capability: str,
    providers: list[str],
) -> dict[str, bool]:
    """Per-(capability, provider) EXISTS check for one instrument.

    Each (capability, provider) tuple has its own SQL EXISTS query
    in ``_PRESENCE_QUERIES``. Tuples without a wired entry report
    ``False`` — the panel stays hidden until each per-region
    integration PR lands a real EXISTS query for the pair.

    The (capability, provider) keying matters because the same
    provider tag can serve multiple capabilities by reading
    different tables. The keying makes that future-proof without
    ambiguity (Codex round 2 finding on PR 3a).
    """
    out: dict[str, bool] = {}
    for provider in providers:
        query = _PRESENCE_QUERIES.get((capability, provider))
        if query is None:
            out[provider] = False
            continue
        # Wrap in psycopg.sql.SQL so pyright accepts the str-typed
        # value from the dict lookup as a valid execute() query —
        # the dict values are all hand-authored constants in this
        # module so they're known-safe to wrap as SQL.
        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(psycopg.sql.SQL(query), (instrument_id,))  # type: ignore[arg-type]
            row = cur.fetchone()
        out[provider] = bool(row[0]) if row else False
    return out
