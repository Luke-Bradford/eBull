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
    # #463 — submissions.json long-tail fields previously dropped.
    phone: str | None = None
    entity_type: str | None = None
    flags: str | None = None
    address_business: dict[str, Any] | None = None
    address_mailing: dict[str, Any] | None = None


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


_ADDRESS_FIELDS = (
    "street1",
    "street2",
    "city",
    "state_or_country",
    "state_or_country_description",
    "zip_code",
    "country",
    "country_code",
    "is_foreign_location",
    "foreign_state_territory",
)


def _address(value: Any) -> dict[str, Any] | None:
    """Normalise a SEC address block into a stable snake_case shape.

    Returns ``None`` when the source carries no non-empty fields —
    avoids storing empty-shell dicts that would force every consumer
    to check emptiness.
    """
    if not isinstance(value, dict):
        return None
    out: dict[str, Any] = {}
    mapping = {
        "street1": "street1",
        "street2": "street2",
        "city": "city",
        "stateOrCountry": "state_or_country",
        "stateOrCountryDescription": "state_or_country_description",
        "zipCode": "zip_code",
        "country": "country",
        "countryCode": "country_code",
        "isForeignLocation": "is_foreign_location",
        "foreignStateTerritory": "foreign_state_territory",
    }
    for src_key, dst_key in mapping.items():
        v = value.get(src_key)
        if isinstance(v, str):
            v = v.strip() or None
        out[dst_key] = v
    # Drop the dict only when every field is None / empty. Explicit
    # None check rather than truthiness — ``isForeignLocation: 0``
    # (SEC's "not foreign" marker) is meaningful and must survive.
    if all(out.get(k) is None for k in _ADDRESS_FIELDS):
        return None
    return out


def parse_entity_profile(
    submissions: dict[str, Any],
    *,
    instrument_id: int,
    cik: str,
) -> SecEntityProfile:
    """Extract the entity subset. Never raises — every field falls back
    to None/[]/{} if the source omitted or malformed it."""
    addresses = submissions.get("addresses")
    address_business = _address(addresses.get("business")) if isinstance(addresses, dict) else None
    address_mailing = _address(addresses.get("mailing")) if isinstance(addresses, dict) else None
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
        phone=_non_empty_str(submissions.get("phone")),
        entity_type=_non_empty_str(submissions.get("entityType")),
        flags=_non_empty_str(submissions.get("flags")),
        address_business=address_business,
        address_mailing=address_mailing,
    )


_UPSERT_SQL = """
INSERT INTO instrument_sec_profile (
    instrument_id, cik, sic, sic_description, owner_org,
    description, website, investor_website, ein, lei,
    state_of_incorporation, state_of_incorporation_desc,
    fiscal_year_end, category, exchanges, former_names,
    has_insider_issuer, has_insider_owner,
    phone, entity_type, flags, address_business, address_mailing,
    fetched_at
) VALUES (
    %(instrument_id)s, %(cik)s, %(sic)s, %(sic_description)s, %(owner_org)s,
    %(description)s, %(website)s, %(investor_website)s, %(ein)s, %(lei)s,
    %(state_of_incorporation)s, %(state_of_incorporation_desc)s,
    %(fiscal_year_end)s, %(category)s, %(exchanges)s, %(former_names)s,
    %(has_insider_issuer)s, %(has_insider_owner)s,
    %(phone)s, %(entity_type)s, %(flags)s,
    %(address_business)s, %(address_mailing)s,
    NOW()
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
    phone                       = EXCLUDED.phone,
    entity_type                 = EXCLUDED.entity_type,
    flags                       = EXCLUDED.flags,
    address_business            = EXCLUDED.address_business,
    address_mailing             = EXCLUDED.address_mailing,
    fetched_at                  = NOW()
"""


# Fields whose changes are captured in sec_entity_change_log. Tuple
# of (field_name, extractor) so adding a field here is a one-line
# drop. ``extractor`` returns the JSON-serialisable representation
# used for both diff comparison and log storage.
_TRACKED_FIELDS: tuple[tuple[str, Any], ...] = (
    ("sic", lambda p: p.sic),
    ("sic_description", lambda p: p.sic_description),
    ("owner_org", lambda p: p.owner_org),
    ("description", lambda p: p.description),
    ("website", lambda p: p.website),
    ("investor_website", lambda p: p.investor_website),
    ("state_of_incorporation", lambda p: p.state_of_incorporation),
    ("state_of_incorporation_desc", lambda p: p.state_of_incorporation_desc),
    ("fiscal_year_end", lambda p: p.fiscal_year_end),
    ("category", lambda p: p.category),
    ("exchanges", lambda p: p.exchanges),
    ("entity_type", lambda p: p.entity_type),
    ("phone", lambda p: p.phone),
    ("flags", lambda p: p.flags),
    ("address_business", lambda p: p.address_business),
    ("address_mailing", lambda p: p.address_mailing),
)


def _serialise(value: Any) -> str:
    """Canonical JSON serialisation for diff + log storage.

    ``json.dumps`` with ``sort_keys=True`` so dict-valued addresses
    compare equal regardless of field-ordering drift in the source
    JSON. String values pass through JSON-encoded ("foo" not foo)
    so the diff sees an explicit quoted form and the log stores a
    uniform shape.
    """
    import json

    return json.dumps(value, sort_keys=True, default=str)


def detect_profile_changes(
    prev: SecEntityProfile | None,
    current: SecEntityProfile,
) -> list[tuple[str, str | None, str]]:
    """Return (field_name, prev_value, new_value) for every field in
    ``_TRACKED_FIELDS`` whose serialised value differs between
    ``prev`` and ``current``.

    Returns an empty list on initial ingest (``prev is None``) so the
    first snapshot doesn't synthesise spurious "changed from NULL"
    events for every field — subsequent ingests are the ones that
    capture real change.
    """
    if prev is None:
        return []
    changes: list[tuple[str, str | None, str]] = []
    for field_name, extractor in _TRACKED_FIELDS:
        prev_val = extractor(prev)
        new_val = extractor(current)
        prev_serialised = _serialise(prev_val)
        new_serialised = _serialise(new_val)
        if prev_serialised != new_serialised:
            changes.append((field_name, prev_serialised, new_serialised))
    return changes


def _append_change_log(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    cik: str,
    changes: list[tuple[str, str | None, str]],
    source_accession: str | None = None,
) -> None:
    """Insert one row per detected change into ``sec_entity_change_log``.

    No-op when ``changes`` is empty. Caller is responsible for
    transactional scope — the append runs inline so it commits with
    the profile upsert.
    """
    if not changes:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO sec_entity_change_log
                (instrument_id, cik, field_name, prev_value, new_value,
                 source_accession)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [(instrument_id, cik, name, prev, new, source_accession) for name, prev, new in changes],
        )


def upsert_entity_profile(
    conn: psycopg.Connection[Any],
    profile: SecEntityProfile,
    *,
    source_accession: str | None = None,
) -> None:
    """Insert-or-update the profile row for ``profile.instrument_id``
    and append any detected field changes to
    ``sec_entity_change_log`` in the same transaction (#463).
    """
    prev = get_entity_profile(conn, instrument_id=profile.instrument_id)
    changes = detect_profile_changes(prev, profile)
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
            "phone": profile.phone,
            "entity_type": profile.entity_type,
            "flags": profile.flags,
            "address_business": Jsonb(profile.address_business) if profile.address_business else None,
            "address_mailing": Jsonb(profile.address_mailing) if profile.address_mailing else None,
        },
    )
    _append_change_log(
        conn,
        instrument_id=profile.instrument_id,
        cik=profile.cik,
        changes=changes,
        source_accession=source_accession,
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
                   has_insider_issuer, has_insider_owner,
                   phone, entity_type, flags,
                   address_business, address_mailing
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
        phone=row.get("phone"),
        entity_type=row.get("entity_type"),
        flags=row.get("flags"),
        address_business=(dict(row["address_business"]) if row.get("address_business") else None),
        address_mailing=(dict(row["address_mailing"]) if row.get("address_mailing") else None),
    )
