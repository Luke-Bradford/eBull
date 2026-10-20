"""#2327 — an SEC-native issuer-CUSIP bridge from Schedule 13D/G.

Spec: ``docs/specs/etl/2026-10-20-2327-13dg-cusip-bridge.md``. Every Schedule 13D/G row in ``blockholder_filings``
carries the issuer's CIK (an EDGAR entity key) and the CUSIP of the class it reports on (filer-typed). The bridge binds
that CUSIP to the CIK's instrument under ``provider = 'sec'`` when the SEC Official List evidence makes the class
unambiguous, and records the decision's inputs in ``cusip_bridge_bindings``.

Rule v1, gates in order, the first refusal deciding (spec "Rule v1"):

- **R1** the CUSIP has the CUSIP shape and its check digit holds;
- **R2** every 13D/G row naming it names one CIK;
- **R3** that CIK maps to exactly one instrument (#1628's never-guess rule);
- **R4** it is a share row on at least one available window list;
- **R6** no ``cusip`` row of any provider maps it to another instrument (the bridge never overwrites);
- **R5a** no two of the instrument's CUSIPs (candidates and existing rows) are co-listed on two or more lists or both
  on the latest list;
- **R5b** no share row of the same issuer number that the instrument does not hold is co-listed with it that way;
- **R5c** when the instrument already holds a CUSIP, one of them shares its issuer number.

A gap quarter counts as a co-listing for a pair co-listed on the nearest available list before or after it. No source
rule separates a CUSIP succession from two concurrent classes; R5 is a heuristic chosen by the spec and frozen by
:data:`CUSIP_BRIDGE_RULE_VERSION`.

:func:`decide` is evaluated against the mapping state WITHOUT the bridge's own rows, so its outcome on the same SEC
inputs does not depend on earlier bridge runs; a bridge row whose CUSIP would now refuse is reported, never deleted.
"""

from __future__ import annotations

import itertools
import logging
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal

import psycopg

from app.services.cusip_check_digit import is_valid_cusip
from app.services.cusip_resolver import lock_cusip
from app.services.sec_13f_securities_list import WindowList
from app.services.sec_identity import siblings_for_issuer_cik

logger = logging.getLogger(__name__)

CUSIP_BRIDGE_RULE_VERSION: Final[str] = "13dg-bridge-v1"

Outcome = Literal["R1", "R2", "R3_none", "R3_multi", "R4", "R6", "R5a", "R5b", "R5c", "noop", "confirm", "bind"]
_WRITE_OUTCOMES: Final[frozenset[str]] = frozenset({"bind", "confirm"})


@dataclass(frozen=True)
class Decision:
    cusip: str
    outcome: Outcome
    issuer_cik: str | None = None
    instrument_id: int | None = None
    """Set for every candidate that passed R1–R4, refused later or not."""


@dataclass(frozen=True)
class BridgeInputs:
    filing_ciks: Mapping[str, frozenset[str]]
    """Normalised CUSIP → the normalised issuer CIKs of the 13D/G rows naming it."""
    cik_instruments: Mapping[str, tuple[int, ...]]
    """10-digit CIK → its instruments (``provider = 'sec'``, ``identifier_type = 'cik'``)."""
    cusip_rows: Mapping[str, frozenset[tuple[str, int]]]
    """CUSIP → ``(provider, instrument_id)`` of its non-bridge ``cusip`` rows."""
    instrument_cusips: Mapping[int, frozenset[str]]
    """Instrument → the CUSIPs of its non-bridge ``cusip`` rows, any provider."""
    lists: Sequence[WindowList]


def normalise_cusip(raw: str) -> str:
    return raw.strip().upper()


def normalise_cik(raw: str) -> str:
    """Zero-pad a numeric CIK to 10 digits; anything else is kept as stripped text, so R2 and R3 see it as written."""
    cik = raw.strip()
    return cik.zfill(10) if cik.isascii() and cik.isdigit() else cik


def filing_maps(
    rows: Iterable[tuple[str, str, str]],
) -> tuple[dict[str, frozenset[str]], dict[str, tuple[str, ...]]]:
    """Fold ``(issuer_cusip, issuer_cik, accession_number)`` rows into CUSIP → CIKs and CUSIP → sorted accessions,
    after normalising both identifiers. Blank CUSIPs are dropped."""
    ciks: dict[str, set[str]] = defaultdict(set)
    accessions: dict[str, set[str]] = defaultdict(set)
    for raw_cusip, raw_cik, accession in rows:
        cusip = normalise_cusip(raw_cusip)
        if not cusip:
            continue
        ciks[cusip].add(normalise_cik(raw_cik))
        accessions[cusip].add(accession.strip())
    return (
        {c: frozenset(v) for c, v in ciks.items()},
        {c: tuple(sorted(v)) for c, v in accessions.items()},
    )


def _co_listed(a: str, b: str, lists: Sequence[WindowList]) -> bool:
    """True when ``a`` and ``b`` are co-listed share rows on two or more window lists or on the latest one.

    A gap quarter counts as a co-listing when the pair is co-listed on the nearest available list before or after
    it (spec "Duplicate lists": a gap counts against binding)."""
    available = [i for i, wl in enumerate(lists) if wl.available]
    hit = {i for i in available if a in lists[i].share_cusips and b in lists[i].share_cusips}
    if not hit:
        return False
    for g, wl in enumerate(lists):
        if wl.available:
            continue
        before = max((i for i in available if i < g), default=None)
        after = min((i for i in available if i > g), default=None)
        if before in hit or after in hit:
            hit.add(g)
    return len(hit) >= 2 or len(lists) - 1 in hit


def _decide_instrument(
    instrument_id: int,
    candidates: Mapping[str, str],
    cusip_rows: Mapping[str, frozenset[tuple[str, int]]],
    instrument_cusips: Mapping[int, frozenset[str]],
    lists: Sequence[WindowList],
    prefix_index: Sequence[Mapping[str, frozenset[str]]],
) -> dict[str, Decision]:
    """Gates R6, R5a, R5b, R5c and the write outcome for one instrument's candidates (all past R1–R4), given as
    CUSIP → its own issuer CIK: two CIKs can map to one instrument (a superseded CIK row is kept)."""
    cands = frozenset(candidates)
    existing = instrument_cusips.get(instrument_id, frozenset())
    held = cands | existing
    concurrent = any(_co_listed(a, b, lists) for a, b in itertools.combinations(sorted(held), 2))
    out: dict[str, Decision] = {}
    for x in sorted(cands):
        rows = cusip_rows.get(x, frozenset())
        outcome: Outcome
        if any(iid != instrument_id for _, iid in rows):
            outcome = "R6"
        elif concurrent:
            outcome = "R5a"
        elif _unheld_sibling_class(x, held, lists, prefix_index):
            outcome = "R5b"
        elif (other := existing - {x}) and not any(c[:6] == x[:6] for c in other):
            outcome = "R5c"
        elif ("sec", instrument_id) in rows:
            outcome = "noop"
        elif rows:
            outcome = "confirm"
        else:
            outcome = "bind"
        out[x] = Decision(x, outcome, candidates[x], instrument_id)
    return out


def _unheld_sibling_class(
    x: str,
    held: frozenset[str],
    lists: Sequence[WindowList],
    prefix_index: Sequence[Mapping[str, frozenset[str]]],
) -> bool:
    siblings: set[str] = set()
    for i, wl in enumerate(lists):
        if wl.available and x in wl.share_cusips:
            siblings |= prefix_index[i].get(x[:6], frozenset())
    siblings -= held
    siblings.discard(x)
    return any(_co_listed(x, s, lists) for s in siblings)


def _prefix_index(lists: Sequence[WindowList]) -> list[dict[str, frozenset[str]]]:
    index: list[dict[str, frozenset[str]]] = []
    for wl in lists:
        by: dict[str, set[str]] = defaultdict(set)
        for c in wl.share_cusips:
            by[c[:6]].add(c)
        index.append({k: frozenset(v) for k, v in by.items()})
    return index


def decide(inputs: BridgeInputs) -> dict[str, Decision]:
    """Rule v1 over every 13D/G CUSIP. Pure. The latest window list must be available."""
    lists = inputs.lists
    if not lists or not lists[-1].available:
        raise ValueError("decide() needs an available latest window list")
    out: dict[str, Decision] = {}
    by_instrument: dict[int, dict[str, str]] = defaultdict(dict)
    for cusip in sorted(inputs.filing_ciks):
        ciks = inputs.filing_ciks[cusip]
        if not is_valid_cusip(cusip):
            out[cusip] = Decision(cusip, "R1")
            continue
        if len(ciks) != 1:
            out[cusip] = Decision(cusip, "R2")
            continue
        (cik,) = ciks
        instruments = inputs.cik_instruments.get(cik, ())
        if len(instruments) != 1:
            out[cusip] = Decision(cusip, "R3_none" if not instruments else "R3_multi", cik)
            continue
        if not any(wl.available and cusip in wl.share_cusips for wl in lists):
            out[cusip] = Decision(cusip, "R4", cik)
            continue
        by_instrument[instruments[0]][cusip] = cik
    index = _prefix_index(lists)
    for iid, cands in by_instrument.items():
        out.update(_decide_instrument(iid, cands, inputs.cusip_rows, inputs.instrument_cusips, lists, index))
    return out


@dataclass
class BridgeResult:
    latest_quarter: str
    gaps: tuple[str, ...]
    refused: str | None = None
    """Set when the bridge did not run (the latest window list is a gap)."""
    outcomes: Counter[str] = field(default_factory=Counter)
    inserted: Counter[str] = field(default_factory=Counter)
    """Rows written this run, by ``bind`` / ``confirm``."""
    held: int = 0
    """Bridge rows whose CUSIP still decides ``bind`` / ``confirm``."""
    raced: Counter[str] = field(default_factory=Counter)
    """Writes abandoned because the state re-read under the CUSIP lock changed the decision, by the new outcome
    (``R3`` when the CIK's instruments changed, ``conflict`` when the insert lost to a ``sec`` row elsewhere)."""
    invalidated: list[tuple[str, int, str]] = field(default_factory=list)
    """``(cusip, instrument_id, outcome)`` of bridge rows that would now refuse. Reported for hand adjudication."""

    def note(self) -> str:
        gaps = ",".join(self.gaps) or "none"
        if self.refused is not None:
            return f"13D/G bridge {CUSIP_BRIDGE_RULE_VERSION} did not run: {self.refused}; gaps={gaps}"
        counts = " ".join(f"{k}={v}" for k, v in sorted(self.outcomes.items()))
        raced = " ".join(f"{k}={v}" for k, v in sorted(self.raced.items())) or "0"
        invalid = ",".join(f"{c}->{i}:{o}" for c, i, o in self.invalidated) or "none"
        return (
            f"13D/G bridge {CUSIP_BRIDGE_RULE_VERSION} latest={self.latest_quarter} gaps={gaps}; "
            f"inserted bind={self.inserted['bind']} confirm={self.inserted['confirm']} held={self.held}; "
            f"outcomes {counts}; raced {raced}; invalidated {invalid}"
        )


_CUSIP_ROWS_SQL: Final[str] = """
    SELECT ei.provider, upper(btrim(ei.identifier_value)), ei.instrument_id, b.external_identifier_id IS NOT NULL
      FROM external_identifiers ei
      LEFT JOIN cusip_bridge_bindings b ON b.external_identifier_id = ei.external_identifier_id
     WHERE ei.identifier_type = 'cusip'
"""


def read_bridge_inputs(
    conn: psycopg.Connection[Any],
    lists: Sequence[WindowList],
) -> tuple[BridgeInputs, dict[str, tuple[str, ...]], dict[str, int]]:
    """Returns the inputs, CUSIP → accessions, and the bridge's own rows as CUSIP → instrument."""
    with conn.cursor() as cur:
        cur.execute("SELECT issuer_cusip, issuer_cik, accession_number FROM blockholder_filings")
        filing_ciks, accessions = filing_maps((str(c), str(k), str(a)) for c, k, a in cur.fetchall())
        cur.execute(
            "SELECT identifier_value, instrument_id FROM external_identifiers "
            "WHERE provider = 'sec' AND identifier_type = 'cik' ORDER BY instrument_id"
        )
        cik_instruments: dict[str, list[int]] = defaultdict(list)
        for value, iid in cur.fetchall():
            cik_instruments[str(value)].append(int(iid))
        cur.execute(_CUSIP_ROWS_SQL)
        cusip_rows: dict[str, set[tuple[str, int]]] = defaultdict(set)
        instrument_cusips: dict[int, set[str]] = defaultdict(set)
        own: dict[str, int] = {}
        for provider, cusip, iid, is_bridge in cur.fetchall():
            if is_bridge:
                own[str(cusip)] = int(iid)
                continue
            cusip_rows[str(cusip)].add((str(provider), int(iid)))
            instrument_cusips[int(iid)].add(str(cusip))
    inputs = BridgeInputs(
        filing_ciks=filing_ciks,
        cik_instruments={k: tuple(v) for k, v in cik_instruments.items()},
        cusip_rows={k: frozenset(v) for k, v in cusip_rows.items()},
        instrument_cusips={k: frozenset(v) for k, v in instrument_cusips.items()},
        lists=lists,
    )
    return inputs, accessions, own


def _reread_under_lock(
    conn: psycopg.Connection[Any],
    cusip: str,
    instrument_id: int,
) -> tuple[frozenset[tuple[str, int]], frozenset[str]]:
    """The CUSIP's non-bridge rows and the instrument's non-bridge CUSIPs, read after the CUSIP lock (R6, R5)."""
    with conn.cursor() as cur:
        cur.execute(
            _CUSIP_ROWS_SQL + " AND upper(btrim(ei.identifier_value)) = %(cusip)s",
            {"cusip": cusip},
        )
        rows = frozenset((str(p), int(i)) for p, _, i, is_bridge in cur.fetchall() if not is_bridge)
        cur.execute(
            _CUSIP_ROWS_SQL + " AND ei.instrument_id = %(iid)s",
            {"iid": instrument_id},
        )
        held = frozenset(str(c) for _, c, _, is_bridge in cur.fetchall() if not is_bridge)
    return rows, held


def bind_13dg_cusips(
    conn: psycopg.Connection[Any],
    *,
    lists: Sequence[WindowList],
    job_run_id: int,
) -> BridgeResult:
    """Evaluate rule v1 over every 13D/G CUSIP and write each ``bind`` / ``confirm``.

    Each write is one ``conn.transaction()``: its own committed transaction when the connection is idle (the job
    commits before calling), a savepoint inside a caller's transaction (the rolled-back A/B). Inside it: the CUSIP
    lock (:func:`~app.services.cusip_resolver.lock_cusip`, shared with every other ``cusip`` writer), a re-read of
    the CIK's instruments (R3) and of the CUSIP's and the instrument's rows (R6, R5), the decision re-applied, then
    ``INSERT … ON CONFLICT DO NOTHING RETURNING`` and the provenance row only when a row came back. R2's and R4's
    inputs (filings, lists) are fixed for the run.
    """
    gaps = tuple(wl.quarter for wl in lists if not wl.available)
    if not lists:
        return BridgeResult(latest_quarter="", gaps=gaps, refused="no window lists")
    latest = lists[-1]
    if not latest.available:
        refused = f"latest window list {latest.quarter} is a gap ({latest.gap_reason})"
        return BridgeResult(latest_quarter=latest.quarter, gaps=gaps, refused=refused)

    with conn.transaction():  # ends the read's transaction, so on an idle connection each write below is its own
        inputs, accessions, own = read_bridge_inputs(conn, lists)
    decisions = decide(inputs)
    result = BridgeResult(
        latest_quarter=latest.quarter, gaps=gaps, outcomes=Counter(d.outcome for d in decisions.values())
    )
    list_quarters = [wl.quarter for wl in lists if wl.available]
    list_sha = [wl.sha256 or "" for wl in lists if wl.available]
    index = _prefix_index(lists)
    candidates: dict[int, dict[str, str]] = defaultdict(dict)
    for d in decisions.values():
        if d.instrument_id is not None and d.issuer_cik is not None:
            candidates[d.instrument_id][d.cusip] = d.issuer_cik

    for cusip, owner in sorted(own.items()):
        d = decisions.get(cusip)
        if d is not None and d.instrument_id == owner and d.outcome in _WRITE_OUTCOMES:
            result.held += 1
        else:
            gate = d.outcome if d is not None else "no_13dg_row"
            result.invalidated.append((cusip, owner, gate))
            logger.warning(
                "cusip_bridge_13dg: bridge row %s -> instrument_id=%d would now decide %s", cusip, owner, gate
            )

    for cusip in sorted(decisions):
        d = decisions[cusip]
        if d.outcome not in _WRITE_OUTCOMES or cusip in own:
            continue
        assert d.instrument_id is not None and d.issuer_cik is not None
        iid, cik = d.instrument_id, d.issuer_cik
        with conn.transaction():
            lock_cusip(conn, cusip)
            if siblings_for_issuer_cik(conn, cik) != [iid]:
                result.raced["R3"] += 1
                continue
            rows, held = _reread_under_lock(conn, cusip, iid)
            redo = _decide_instrument(
                iid,
                candidates[iid],
                {c: inputs.cusip_rows.get(c, frozenset()) for c in candidates[iid]} | {cusip: rows},
                {iid: held},
                lists,
                index,
            )[cusip]
            if redo.outcome not in _WRITE_OUTCOMES:
                result.raced[redo.outcome] += 1
                continue
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO external_identifiers (
                        instrument_id, provider, identifier_type, identifier_value, is_primary
                    ) VALUES (%(iid)s, 'sec', 'cusip', %(cusip)s, FALSE)
                    ON CONFLICT (provider, identifier_type, identifier_value)
                        WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
                    DO NOTHING
                    RETURNING external_identifier_id
                    """,
                    {"iid": iid, "cusip": cusip},
                )
                inserted = cur.fetchone()
                if inserted is None:
                    cur.execute(
                        "SELECT instrument_id FROM external_identifiers "
                        "WHERE provider = 'sec' AND identifier_type = 'cusip' AND identifier_value = %s",
                        (cusip,),
                    )
                    existing = cur.fetchone()
                    result.raced["noop" if existing is not None and int(existing[0]) == iid else "conflict"] += 1
                    continue
                cur.execute(
                    """
                    INSERT INTO cusip_bridge_bindings (
                        external_identifier_id, cusip, instrument_id, issuer_cik, outcome, rule_version,
                        accessions, list_quarters, list_sha256, job_run_id
                    ) VALUES (
                        %(eid)s, %(cusip)s, %(iid)s, %(cik)s, %(outcome)s, %(rule)s,
                        %(accessions)s, %(quarters)s, %(sha)s, %(run)s
                    )
                    """,
                    {
                        "eid": int(inserted[0]),
                        "cusip": cusip,
                        "iid": iid,
                        "cik": cik,
                        "outcome": redo.outcome,
                        "rule": CUSIP_BRIDGE_RULE_VERSION,
                        "accessions": list(accessions.get(cusip, ())),
                        "quarters": list_quarters,
                        "sha": list_sha,
                        "run": job_run_id,
                    },
                )
            result.inserted[redo.outcome] += 1
    logger.info("cusip_bridge_13dg: %s", result.note())
    return result
