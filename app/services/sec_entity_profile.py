"""Extract SEC entity metadata from a submissions.json payload and
upsert into ``instrument_sec_profile`` (#427).

Zero new HTTP — the caller (``_run_cik_upsert`` in
``app/services/fundamentals.py``) already fetched the submissions
dict. This module normalises + persists the rich entity fields that
would otherwise be discarded.

The mapping is deliberately lossy: we take the handful of fields the
instrument page + ranking engine actually consume, not every top-level
key. Adding a new field later is cheap (one column, one mapping line).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class SecEntityProfile:
    """Normalised snapshot of a CIK's submissions.json entity section."""

    instrument_id: int
    cik: str
    sic: str | None
    sic_description: str | None
    owner_org: str | None
    description: str | None
    website: str | None
    investor_website: str | None
    ein: str | None
    lei: str | None
    state_of_incorporation: str | None
    state_of_incorporation_desc: str | None
    fiscal_year_end: str | None
    category: str | None
    exchanges: list[str]
    former_names: list[dict[str, Any]]
    has_insider_issuer: bool | None
    has_insider_owner: bool | None


def _non_empty_str(value: Any) -> str | None:
    """Treat SEC's pervasive empty-string fields as absent.

    ``submissions.json`` uses ``""`` for ``description`` / ``website``
    / ``investor_website`` on most filers. Storing an empty string
    would force every consumer to repeat a NULL-or-empty check —
    normalise at the boundary.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return str(value) or None
    stripped = value.strip()
    return stripped or None


def _int_to_bool(value: Any) -> bool | None:
    """``insiderTransactionFor*`` fields are published as 0/1 ints."""
    if value is None:
        return None
    try:
        return bool(int(value))
    except TypeError, ValueError:
        return None


def _string_list(value: Any) -> list[str]:
    """Coerce ``exchanges`` — SEC publishes [] when absent, string when one."""
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for entry in value:
        if isinstance(entry, str) and entry.strip():
            out.append(entry.strip())
    return out


def _former_names(value: Any) -> list[dict[str, Any]]:
    """Keep only the three fields we actually render (name / from / to)
    so a future schema-drift in SEC's payload cannot sneak unchecked
    text into our JSONB column."""
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        name = _non_empty_str(entry.get("name"))
        if name is None:
            continue
        out.append(
            {
                "name": name,
                "from": _non_empty_str(entry.get("from")),
                "to": _non_empty_str(entry.get("to")),
            }
        )
    return out


def parse_entity_profile(
    submissions: dict[str, Any],
    *,
    instrument_id: int,
    cik: str,
) -> SecEntityProfile:
    """Extract the entity subset. Never raises — every field falls back
    to None/[]/{} if the source omitted or malformed it."""
    return SecEntityProfile(
        instrument_id=instrument_id,
        cik=cik,
        sic=_non_empty_str(submissions.get("sic")),
        sic_description=_non_empty_str(submissions.get("sicDescription")),
        owner_org=_non_empty_str(submissions.get("ownerOrg")),
        description=_non_empty_str(submissions.get("description")),
        website=_non_empty_str(submissions.get("website")),
        investor_website=_non_empty_str(submissions.get("investorWebsite")),
        ein=_non_empty_str(submissions.get("ein")),
        lei=_non_empty_str(submissions.get("lei")),
        state_of_incorporation=_non_empty_str(submissions.get("stateOfIncorporation")),
        state_of_incorporation_desc=_non_empty_str(submissions.get("stateOfIncorporationDescription")),
        fiscal_year_end=_non_empty_str(submissions.get("fiscalYearEnd")),
        category=_non_empty_str(submissions.get("category")),
        exchanges=_string_list(submissions.get("exchanges")),
        former_names=_former_names(submissions.get("formerNames")),
        has_insider_issuer=_int_to_bool(submissions.get("insiderTransactionForIssuerExists")),
        has_insider_owner=_int_to_bool(submissions.get("insiderTransactionForOwnerExists")),
    )


_UPSERT_SQL = """
INSERT INTO instrument_sec_profile (
    instrument_id, cik, sic, sic_description, owner_org,
    description, website, investor_website, ein, lei,
    state_of_incorporation, state_of_incorporation_desc,
    fiscal_year_end, category, exchanges, former_names,
    has_insider_issuer, has_insider_owner, fetched_at
) VALUES (
    %(instrument_id)s, %(cik)s, %(sic)s, %(sic_description)s, %(owner_org)s,
    %(description)s, %(website)s, %(investor_website)s, %(ein)s, %(lei)s,
    %(state_of_incorporation)s, %(state_of_incorporation_desc)s,
    %(fiscal_year_end)s, %(category)s, %(exchanges)s, %(former_names)s,
    %(has_insider_issuer)s, %(has_insider_owner)s, NOW()
)
ON CONFLICT (instrument_id) DO UPDATE SET
    cik                         = EXCLUDED.cik,
    sic                         = EXCLUDED.sic,
    sic_description             = EXCLUDED.sic_description,
    owner_org                   = EXCLUDED.owner_org,
    description                 = EXCLUDED.description,
    website                     = EXCLUDED.website,
    investor_website            = EXCLUDED.investor_website,
    ein                         = EXCLUDED.ein,
    lei                         = EXCLUDED.lei,
    state_of_incorporation      = EXCLUDED.state_of_incorporation,
    state_of_incorporation_desc = EXCLUDED.state_of_incorporation_desc,
    fiscal_year_end             = EXCLUDED.fiscal_year_end,
    category                    = EXCLUDED.category,
    exchanges                   = EXCLUDED.exchanges,
    former_names                = EXCLUDED.former_names,
    has_insider_issuer          = EXCLUDED.has_insider_issuer,
    has_insider_owner           = EXCLUDED.has_insider_owner,
    fetched_at                  = NOW()
"""


def upsert_entity_profile(
    conn: psycopg.Connection[Any],
    profile: SecEntityProfile,
) -> None:
    """Insert-or-update the profile row for ``profile.instrument_id``."""
    conn.execute(
        _UPSERT_SQL,
        {
            "instrument_id": profile.instrument_id,
            "cik": profile.cik,
            "sic": profile.sic,
            "sic_description": profile.sic_description,
            "owner_org": profile.owner_org,
            "description": profile.description,
            "website": profile.website,
            "investor_website": profile.investor_website,
            "ein": profile.ein,
            "lei": profile.lei,
            "state_of_incorporation": profile.state_of_incorporation,
            "state_of_incorporation_desc": profile.state_of_incorporation_desc,
            "fiscal_year_end": profile.fiscal_year_end,
            "category": profile.category,
            "exchanges": profile.exchanges,
            "former_names": Jsonb(profile.former_names),
            "has_insider_issuer": profile.has_insider_issuer,
            "has_insider_owner": profile.has_insider_owner,
        },
    )


def get_entity_profile(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> SecEntityProfile | None:
    """Fetch the stored profile for an instrument, or None if none yet."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, cik, sic, sic_description, owner_org,
                   description, website, investor_website, ein, lei,
                   state_of_incorporation, state_of_incorporation_desc,
                   fiscal_year_end, category, exchanges, former_names,
                   has_insider_issuer, has_insider_owner
            FROM instrument_sec_profile
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    return SecEntityProfile(
        instrument_id=int(row["instrument_id"]),
        cik=str(row["cik"]),
        sic=row["sic"],
        sic_description=row["sic_description"],
        owner_org=row["owner_org"],
        description=row["description"],
        website=row["website"],
        investor_website=row["investor_website"],
        ein=row["ein"],
        lei=row["lei"],
        state_of_incorporation=row["state_of_incorporation"],
        state_of_incorporation_desc=row["state_of_incorporation_desc"],
        fiscal_year_end=row["fiscal_year_end"],
        category=row["category"],
        exchanges=list(row["exchanges"] or []),
        former_names=list(row["former_names"] or []),
        has_insider_issuer=row["has_insider_issuer"],
        has_insider_owner=row["has_insider_owner"],
    )
