"""SIC → GICS-sector → sector-SPDR crosswalk (#1634).

`instruments.sector` is an opaque 1–9 code with no GICS/SPDR meaning. The SEC's
own SIC taxonomy (`instrument_sec_profile.sic`, 4-digit) is the real industry
classification for operating filers. This module maps a SIC to one of the 11
GICS sectors and its State-Street sector SPDR ETF (the benchmarks #591 PR-A
ingested), re-enabling sector-relative views.

Honest limitation: GICS is proprietary (MSCI/S&P) and NOT 1:1 with SIC, so this
is a documented best-effort SIC→GICS approximation — never authoritative GICS.
A SIC with no confident mapping, or an instrument with no SIC (ETFs, foreign
non-filers), resolves to ``None`` (no sector view), never a guessed sector.

Pure module — no DB, no I/O. The crosswalk is verified against the full SIC
population in `tests/` + the dev scan recorded on the PR.

Source: SEC SIC code list (4-digit division/major-group structure); SPDR/GICS
11-sector definitions. Spec: docs/specs/metrics/2026-06-18-sic-gics-spdr-crosswalk.md.
"""

from __future__ import annotations

from dataclasses import dataclass

# The 11 sector SPDRs → their GICS sector name. Single source of truth; the
# symbols are a subset of app.workers.scheduler.BENCHMARK_SYMBOLS (cross-checked
# in tests, not imported here to keep this module dependency-free / cycle-free).
SPDR_SECTORS: dict[str, str] = {
    "XLB": "Materials",
    "XLC": "Communication Services",
    "XLE": "Energy",
    "XLF": "Financials",
    "XLI": "Industrials",
    "XLK": "Information Technology",
    "XLP": "Consumer Staples",
    "XLRE": "Real Estate",
    "XLU": "Utilities",
    "XLV": "Health Care",
    "XLY": "Consumer Discretionary",
}


@dataclass(frozen=True)
class SectorClassification:
    """A resolved sector for an instrument. ``gics_sector`` is the human label;
    ``spdr_symbol`` is the sector-SPDR ETF benchmark."""

    gics_sector: str
    spdr_symbol: str


# Ordered (lo, hi, spdr) SIC ranges, inclusive. **Override ranges first**, then
# the enclosing major-group ranges — the resolver returns the FIRST match, so a
# carve-out (e.g. 2834 pharma out of 28xx chemicals) must precede its parent.
# Covers SIC 0100–8799; the bounds are GICS-driven (see the spec table). The
# gics_sector label is derived from SPDR_SECTORS so the two never drift.
#
# Carve-outs are population-backed (the 389-SIC dev scan, full list in the spec)
# — every range below corresponds to SIC codes actually present that the parent
# major group would mis-route. Carve-outs MUST precede their enclosing major
# group (first-match wins).
_CROSSWALK: tuple[tuple[int, int, str], ...] = (
    # --- GICS-driven carve-outs (most-specific first) ---
    (2833, 2836, "XLV"),  # drugs / pharma / biologicals (out of 28xx chemicals)
    (3570, 3579, "XLK"),  # computers & peripherals (out of 35xx machinery)
    (3630, 3639, "XLY"),  # household appliances (out of 36xx electrical → cons. durables)
    (3650, 3659, "XLY"),  # household audio/video equipment (out of 36xx)
    (3660, 3679, "XLK"),  # comms equipment + semiconductors/components (out of 36xx)
    (3710, 3716, "XLY"),  # autos & parts (out of 37xx transport equipment)
    (3812, 3812, "XLI"),  # search/detection/nav/guidance = defense electronics (out of 38xx)
    (3826, 3826, "XLV"),  # laboratory analytical instruments = life-sciences tools (out of 38xx)
    (3840, 3851, "XLV"),  # surgical/medical/dental/x-ray/electromedical/ophthalmic devices
    (3873, 3873, "XLY"),  # watches & clocks = consumer durables (out of 38xx)
    (4950, 4959, "XLI"),  # sanitary / refuse / hazardous-waste services (out of 49xx utilities)
    (5010, 5019, "XLY"),  # motor-vehicle wholesale (out of 50xx) → autos
    (5045, 5045, "XLK"),  # computer & software wholesale (out of 50xx)
    (5047, 5047, "XLV"),  # medical/dental/hospital-equipment wholesale (out of 50xx)
    (5065, 5065, "XLK"),  # electronic-parts wholesale (out of 50xx)
    (5122, 5122, "XLV"),  # drug wholesale (out of 51xx)
    (5160, 5169, "XLB"),  # chemicals wholesale (out of 51xx)
    (5171, 5172, "XLE"),  # petroleum wholesale (out of 51xx)
    (5400, 5499, "XLP"),  # food & grocery retail (out of 52–59xx retail)
    (6324, 6324, "XLV"),  # hospital/medical service plans = managed care (out of 63xx insurance)
    (6500, 6599, "XLRE"),  # real estate
    (6792, 6792, "XLE"),  # oil royalty traders (out of 67xx) → energy
    (6795, 6795, "XLB"),  # mineral royalty traders (out of 67xx) → materials
    (6798, 6798, "XLRE"),  # REITs (out of 67xx holding offices)
    (7310, 7319, "XLC"),  # advertising (out of 73xx services)
    (7370, 7379, "XLK"),  # software / data processing (out of 73xx services)
    (8731, 8731, "XLV"),  # commercial biological research (out of 87xx)
    # --- major groups ---
    (100, 999, "XLP"),  # agriculture / fishing
    (1000, 1099, "XLB"),  # metal mining
    (1200, 1299, "XLE"),  # coal
    (1300, 1399, "XLE"),  # oil & gas
    (1400, 1499, "XLB"),  # nonmetallic mineral mining
    (1500, 1799, "XLI"),  # construction
    (2000, 2099, "XLP"),  # food
    (2100, 2199, "XLP"),  # tobacco
    (2200, 2399, "XLY"),  # textiles & apparel
    (2400, 2499, "XLB"),  # lumber & wood
    (2500, 2599, "XLY"),  # furniture
    (2600, 2699, "XLB"),  # paper
    (2700, 2799, "XLC"),  # printing & publishing (media)
    (2800, 2899, "XLB"),  # chemicals (non-pharma)
    (2900, 2999, "XLE"),  # petroleum refining
    (3000, 3099, "XLB"),  # rubber & plastics
    (3100, 3199, "XLY"),  # leather & footwear
    (3200, 3399, "XLB"),  # stone/clay/glass, primary metal
    (3400, 3499, "XLI"),  # fabricated metal
    (3500, 3599, "XLI"),  # industrial machinery (non-computer)
    (3600, 3699, "XLI"),  # electrical equipment (transformers/motors/lighting/misc) → Industrials
    (3700, 3799, "XLI"),  # aerospace & other transport equip (autos carved out)
    (3800, 3899, "XLK"),  # electronic/measuring instruments (medical carved to XLV above)
    (3900, 3999, "XLY"),  # misc manufacturing
    (4000, 4499, "XLI"),  # rail / transit / trucking / water transport
    (4500, 4599, "XLI"),  # air transport
    (4600, 4699, "XLE"),  # pipelines
    (4700, 4799, "XLI"),  # transportation services
    (4800, 4899, "XLC"),  # telephone / cable / broadcasting
    (4900, 4999, "XLU"),  # electric / gas / water utilities (waste carved out)
    (5000, 5099, "XLI"),  # durable-goods wholesale (distributors)
    (5100, 5199, "XLP"),  # nondurable wholesale (food/drug plurality)
    (5200, 5999, "XLY"),  # retail (food retail 54xx carved out above)
    (6000, 6199, "XLF"),  # banks & credit
    (6200, 6299, "XLF"),  # brokers / investment advice
    (6300, 6399, "XLF"),  # insurance (managed-care 6324 carved out)
    (6400, 6499, "XLF"),  # insurance agents
    (6700, 6799, "XLF"),  # holding/investment offices (REIT/royalty carved out)
    (7000, 7099, "XLY"),  # hotels
    (7200, 7299, "XLY"),  # personal services
    (7300, 7399, "XLI"),  # business services (software/advertising carved out)
    (7500, 7699, "XLI"),  # auto repair/rental, repair services
    (7800, 7899, "XLC"),  # motion pictures
    (7900, 7999, "XLY"),  # amusement & recreation (leisure)
    (8000, 8099, "XLV"),  # health services
    (8100, 8199, "XLI"),  # legal services
    (8200, 8299, "XLY"),  # educational services
    (8300, 8399, "XLY"),  # social/child-care services
    (8700, 8799, "XLI"),  # engineering / consulting (bio research 8731 carved out)
)


def resolve_sector_spdr(sic: str | int | None) -> SectorClassification | None:
    """Map a 4-digit SIC to its sector SPDR, or ``None`` when unmappable.

    Fail-closed: a missing / non-numeric / out-of-range SIC returns ``None``
    (the instrument simply has no sector-relative view — never a guessed
    sector). The first matching range wins, so carve-outs precede major groups.
    """
    if sic is None:
        return None
    try:
        code = int(str(sic).strip())
    except TypeError, ValueError:
        return None
    if code <= 0:
        return None
    for lo, hi, spdr in _CROSSWALK:
        if lo <= code <= hi:
            return SectorClassification(gics_sector=SPDR_SECTORS[spdr], spdr_symbol=spdr)
    return None


def sector_spdr_case_sql() -> str:
    """SQL ``CASE`` that resolves ``instrument_sec_profile.sic`` (aliased ``p.sic``)
    to its sector-SPDR symbol, or ``NULL`` when unmappable — the SQL-side mirror of
    :func:`resolve_sector_spdr` used by the ``sector_spdr`` filter (#1675).

    Generated from ``_CROSSWALK`` in first-match order so the SQL projection cannot
    drift from the Python resolver (a DB-backed parity test asserts equivalence over
    the full SIC domain). The outer regex guard fires the ``::int`` cast only for
    digit strings (SEC SIC is a bare 4-digit code; leading zeros parse fine, e.g.
    ``'0100'::int = 100`` matching Python ``int('0100')``).

    Safe to inline into SQL: embeds only module constants — the range bounds are
    ``int``-formatted and every SPDR literal is asserted ∈ :data:`SPDR_SECTORS`. No
    caller input. Callers MUST alias ``instrument_sec_profile`` AS ``p``.
    """
    branches: list[str] = []
    for lo, hi, spdr in _CROSSWALK:
        assert spdr in SPDR_SECTORS, f"crosswalk SPDR {spdr!r} not in SPDR_SECTORS"
        branches.append(f"WHEN (btrim(p.sic))::int BETWEEN {int(lo)} AND {int(hi)} THEN '{spdr}'")
    inner = "\n        ".join(branches)
    return (
        "CASE WHEN btrim(p.sic) ~ '^[0-9]+$' THEN (\n"
        "      CASE\n"
        f"        {inner}\n"
        "        ELSE NULL\n"
        "      END\n"
        "    ) ELSE NULL END"
    )
