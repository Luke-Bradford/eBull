"""Full-population rollup A/B for #3227 tier (IM) (read-only).

Spec: docs/specs/ownership/2026-10-22-3227-13f-included-managers.md § Change 5.

Builds every rollup on THIS checkout inside one REPEATABLE READ snapshot (session time zone UTC), with the fold, the
reads and the co-filer pass wrapped so their real inputs, outputs and costs are recorded; the fold is also asked for its
pre-uniqueness matched sets. In the same snapshot, per instrument:
  * BASE: the real code at a pinned SHA (``--base``, default the merge-base with origin/main), loaded from git as its
    own module, reads its own covers and observations and runs its fold on the fold's real input, then the real
    co-filer pass;
  * ORACLE 1-3: ``scripts.ab_3227_13f_statement_date.oracle`` in mode E (tiers 1-3, written independently of the fold);
  * ORACLE (IM), stored: § Rule written independently of ``_fold_13f_duplicates`` and ``_read_13f_im_evidence``, over
    its own reads of the observation, capture record and list (its own grammar on ``sequence_number_raw``).
After the snapshot loop:
  * ORACLE (IM), archives: the same rule over the four cached Form 13F Data Sets, through the ingest A/B's independent
    reader (``scripts.ab_3227_13f_included_managers_ingest._read_archive``: SUBMISSION, SUMMARYPAGE, OTHERMANAGER2,
    INFOTABLE), for every candidate whose accession is in an archive.

Decisions are ``(tier, M, k, T, C)``: tier 1 the 13D arm, 2 (U), 3 (O), 4 (IM); M and k digit CIKs; C the 13D/G
accession, or M's 13F accession for (IM).

Gates (non-zero exit on any failure):
  * built folds: every base decision is a treated decision with an identical correction, and tiers 1-3 of the treated
    code equal oracle 1-3;
  * matched sets: the fold's pre-uniqueness matched set equals the oracles' per tier and per instrument (1-3 against
    oracle 1-3, (IM) against the stored oracle);
  * the treated (IM) decisions equal the stored oracle's; the archive oracle's (IM) matched set equals the stored
    oracle's on every candidate whose accession is in an archive (a difference is a capture error);
  * date precondition: no accession carries two date-of-event values, or a mix of NULL and non-NULL;
  * per instrument: treated total = base total − Σ T over the new decisions; input − output = Σ ``shares_removed``;
    #3898's edges, cliques, chains and corrections unchanged; newly oversubscribed = 0;
  * population accounted; treated, base and oracle errors each 0;
  * latency, over all instruments and the affected ones: the cost share (co-filer read + cover reads + (IM) read +
    fold + co-filer pass) / (the rest of ``get_ownership_rollup``) p95 ≤ 10%, and the (IM) read alone / the rest
    p95 ≤ 10%.

  * census: every difference from the spec census set (16 duplicates on 16 instruments, −110,742,341) is reconciled
    to source in ``--reconciled`` (live ingest after the census, or a declared method difference, per case).

Reported, not gated: the cases (input to the EDGAR case review).

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_13f_included_managers ARCHIVE ... [--base SHA] [--out PATH]
[--limit N] [--reconciled JSON]``
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Callable, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

import app.services.ownership_rollup as R
from app.config import settings
from app.db.snapshot import snapshot_read
from app.services.cusip_resolver import load_bulk_cusip_map
from scripts.ab_3227_13f_13g_covers import _load_base
from scripts.ab_3227_13f_duplicates import _total
from scripts.ab_3227_13f_included_managers_ingest import _read_archive, _seq
from scripts.ab_3227_13f_statement_date import _DATES_SQL, _TREAT_SQL, oracle

# The spec census (revision 3): symbol → T.
_CENSUS = {
    "HHH": 27_852_064,
    "INTA": 17_146_805,
    "IMDX": 12_684_884,
    "ASPN": 12_280_426,
    "RHI": 9_877_840,
    "DAO": 6_563_497,
    "CAVA": 6_507_990,
    "AVBP": 3_929_117,
    "ARVN": 3_495_153,
    "YSWY": 2_654_505,
    "BCAL": 2_340_719,
    "WNC": 2_068_519,
    "CTNM": 1_979_173,
    "AMR": 644_025,
    "CABO": 477_500,
    "HYPD": 240_124,
}
_NAMED = {"CTNM": Decimal(-1_979_173), "HHH": Decimal(-27_852_064)}
_PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")
_TIER_KEY = {1: "13d", 2: "u", 3: "o"}

# The stored oracle's own reads (not ``_IM_*_SQL``): the observation by a canonical CIK comparison, the capture record
# joined on, and the list's raw sequence numbers.
_ORACLE_OBS_SQL = """
    SELECT o.shares, o.thirteen_f_segments, c.capture_id::text, c.status
      FROM ownership_institutions_observations o
      LEFT JOIN institutional_13f_captures c ON c.accession_number = o.source_document_id
     WHERE o.instrument_id = %s AND ltrim(o.filer_cik, '0') = %s AND o.period_end = %s
       AND o.source_document_id = %s AND o.source = '13f' AND o.ownership_nature = 'economic'
       AND o.exposure_kind = 'EQUITY' AND o.known_to IS NULL
"""
_ORACLE_LIST_SQL = "SELECT sequence_number_raw, cik FROM institutional_13f_other_managers WHERE accession_number = %s"


def _dig(ident: str) -> str | None:
    return R._digit_cik(R._identity_canon(ident))


def _cik(raw: str | None) -> str | None:
    text = (raw or "").strip().lstrip("0")
    return text if text and re.fullmatch(r"[0-9]+", text) else None


def decisions(corrections: list[Any]) -> set[tuple[Any, ...]]:
    """``(tier, M, k, T, C)`` of each 13F-duplicate correction, parsed from its fixed wording."""
    out = set()
    for c in corrections:
        d = c.detail
        tier = 4 if "included manager" in d else 3 if "counted here as" in d else 2 if "joint Schedule 13G" in d else 1
        mm = re.search(r"\(#3227\): .* \(([^)]*)\) 13F ([\d.]+) for", d)
        acc = re.search(r"\b(\d{10}-\d{2}-\d{6})\b", d)
        if mm is None or acc is None:
            raise ValueError(f"unparsable correction: {d}")
        out.add((tier, R._digit_cik(mm.group(1)), R._digit_cik(c.filer_cik), str(c.shares_removed), acc.group(1)))
    return out


def im_inputs(fold_in: Any, E: dict[str, Any]) -> dict[str, Any]:
    """What both (IM) oracles need from the fold input: candidates outside oracle 1-3's matched sets with one
    accession, the reserved twins, and the nodes by canonical CIK with their totals."""
    rows, cat = R._cofiler_identities(fold_in)
    cand = R._13f_candidates(rows, cat)
    tot = {k: sum((h.shares for h in hs), Decimal(0)) for k, hs in rows.items()}
    matched = E.get("matched", {1: {}, 2: {}, 3: {}})
    prior = {m for t in matched.values() for m in t}
    reserved = {k for t in matched.values() for tw in t.values() for k in tw}
    nodes: dict[str, list[str]] = {}
    for k, hs in rows.items():
        c = R._identity_canon(k)
        if c is not None and R._is_cofiler_node(hs, tot[k], c, R._COFILER_EXCLUDED_SOURCES):
            nodes.setdefault(c, []).append(k)
    cands = {}
    for m, p in cand.items():
        accs = {h.winning_accession.strip() for h in rows[m]}
        if m in prior or len(accs) != 1:
            continue
        cands[m] = {"cik": _dig(m), "P": p, "A": next(iter(accs)), "T": tot[m]}
    return {"cands": cands, "reserved": reserved, "nodes": nodes, "tot": tot}


def im_oracle(inp: Mapping[str, Any], evidence: Callable[[str, Mapping[str, Any]], Any]) -> tuple[dict, dict]:
    """§ Rule over ``evidence(m, cand)``, which returns ``None`` (refuse) or ``(lines, entries)``: the position's
    lines as ``(discretion, numbers or None, shares)`` already checked against T, and the list as ``{number: cik}``.
    Returns the matched set (candidate → twins) and the decisions (candidate → twin)."""
    matched: dict[str, list[str]] = {}
    for m, c in inp["cands"].items():
        # No node of total T other than M: no twin is possible, whatever the evidence says.
        if not any(ck != c["cik"] and inp["tot"][k] == c["T"] for ck, ks in inp["nodes"].items() for k in ks):
            continue
        ev = evidence(m, c)
        if ev is None:
            continue
        lines, entries = ev
        if not lines or any(d not in ("DFND", "OTR") for d, _, _ in lines):
            continue
        q = set.intersection(*(set(nums or ()) for _, nums, _ in lines))
        named = {cik for n in q if (cik := _cik(entries.get(n))) is not None}
        twins = sorted(k for ck in named if ck != c["cik"] for k in inp["nodes"].get(ck, ()) if inp["tot"][k] == c["T"])
        if twins:
            matched[m] = twins
    count = Counter(k for tw in matched.values() for k in tw)
    decided = {
        m: tw[0] for m, tw in matched.items() if len(tw) == 1 and count[tw[0]] == 1 and tw[0] not in inp["reserved"]
    }
    return matched, decided


def stored_evidence(conn: psycopg.Connection[Any], iid: int) -> Callable[[str, Mapping[str, Any]], Any]:
    def ev(m: str, c: Mapping[str, Any]) -> Any:
        obs = conn.execute(_ORACLE_OBS_SQL, (iid, c["cik"], c["P"], c["A"])).fetchall()
        if len(obs) != 1:
            return None
        shares, seg, capture_id, status = obs[0]
        if shares != c["T"] or seg is None or status != "complete" or seg.get("capture_id") != capture_id:
            return None
        lines = [(x["discretion"], x["other_managers"], Decimal(x["shares"])) for x in seg["lines"]]
        if sum((s for _, _, s in lines), Decimal(0)) != shares:
            return None
        raw = conn.execute(_ORACLE_LIST_SQL, (c["A"],)).fetchall()
        numbers = [_seq(r) for r, _ in raw]
        if None in numbers or len(set(numbers)) != len(numbers):
            return None
        return lines, dict(zip(numbers, (cik for _, cik in raw), strict=True))

    return ev


def archive_evidence(
    iid: int, captures: Mapping[str, Any], keyed: Mapping[tuple, Any]
) -> Callable[[str, Mapping[str, Any]], Any]:
    def ev(m: str, c: Mapping[str, Any]) -> Any:
        cap = captures.get(c["A"])
        slot = keyed.get((iid, c["cik"].zfill(10), c["A"], "EQUITY"))
        if cap is None or cap["status"] != "complete" or slot is None:
            return None
        lines = [(d, None if om is None else list(om), s) for (d, om), s in slot["lines"].items()]
        if sum((s for _, _, s in lines), Decimal(0)) != c["T"]:
            return None
        return lines, {e[2]: e[3] for e in cap["entries"]}

    return ev


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("archives", nargs="+", type=Path)
    ap.add_argument("--base", default="")
    ap.add_argument("--out", default="/tmp/ab_3227_13f_included_managers.json")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument(
        "--reconciled",
        type=Path,
        help="JSON {symbol: evidence}: census differences reconciled to source (live ingest after the census, or a "
        "declared method difference), one entry per symbol",
    )
    args = ap.parse_args()
    reconciled: dict[str, str] = json.loads(args.reconciled.read_text()) if args.reconciled else {}
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    base_sha = (
        args.base
        or subprocess.run(["git", "merge-base", "HEAD", "origin/main"], capture_output=True, text=True).stdout.strip()
    )
    print(f"HEAD {head}; base {base_sha}")
    base = _load_base(base_sha)
    if "_read_13f_im_evidence" in base.__dict__:
        print("FAIL base must not have tier (IM)")
        return 1

    cap: dict[str, Any] = {}
    real_fold, real_pass = R._fold_13f_duplicates, R._reconcile_cofiler_groups
    reads = {
        n: getattr(R, n) for n in ("_read_13f_covers", "_read_13g_filers", "_read_13f_im_evidence", "_read_cofilers")
    }

    def timed(name: str, fn: Any) -> Any:
        def wrapper(*a: Any) -> Any:
            sets: dict[str, dict[str, list[str]]] = {}
            t0 = time.perf_counter()
            out = fn(*a, matched_sets=sets) if name == "fold" else fn(*a)
            cap[f"t_{name}"] = time.perf_counter() - t0
            t1 = time.perf_counter()
            cap[f"{name}_in"], cap[f"{name}_out"] = a, out
            if name == "fold":
                cap["sets"] = sets
            if name in ("fold", "pass"):
                # Copied: the caller appends family holders to the pass output's lists afterwards.
                cap[f"{name}_in"] = ({c: list(hs) for c, hs in a[0].items()}, *a[1:])
                cap[f"{name}_out"] = ({c: list(hs) for c, hs in out[0].items()}, out[1])
            cap["copy"] = cap.get("copy", 0.0) + time.perf_counter() - t1
            return out

        return wrapper

    R._fold_13f_duplicates = timed("fold", real_fold)  # type: ignore[assignment]
    R._reconcile_cofiler_groups = timed("pass", real_pass)  # type: ignore[assignment]
    for n, fn in reads.items():
        setattr(R, n, timed(n, fn))
    timed_parts = ("fold", "pass", *reads)

    st: Counter[str] = Counter()
    fails: list[str] = []
    lat: dict[str, list[float]] = {
        "all": [],
        "affected": [],
        "read all": [],
        "read affected": [],
        "all without the (IM) read (reported)": [],
    }
    cases: list[dict[str, Any]] = []
    deferred: list[tuple[int, str, dict[str, Any], dict[str, list[str]]]] = []
    delta = Decimal(0)
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        conn.execute("SET TIME ZONE 'UTC'")
        inconsistent = conn.execute(_DATES_SQL).fetchone()
        if inconsistent is None or inconsistent[0]:
            fails.append(f"accessions with inconsistent dates of event: {inconsistent}")
        cusip_map = load_bulk_cusip_map(conn)
        insts = conn.execute(
            """SELECT i.instrument_id, i.symbol FROM instruments i WHERE i.instrument_id IN (
                 SELECT instrument_id FROM ownership_insiders_current WHERE shares > 0 UNION
                 SELECT instrument_id FROM ownership_blockholders_current WHERE aggregate_amount_owned > 0 UNION
                 SELECT instrument_id FROM ownership_institutions_current WHERE shares > 0) ORDER BY 1"""
        ).fetchall()
        if args.limit:
            insts = insts[: args.limit]
        st["selected"] = len(insts)
        for n, (iid, symbol) in enumerate(insts):
            if n % 1000 == 0:
                print("progress", n, len(insts), flush=True)
            cap.clear()
            try:
                t0 = time.perf_counter()
                r = R.get_ownership_rollup(conn, symbol, iid)
                elapsed = time.perf_counter() - t0 - cap.get("copy", 0.0)
            except Exception as exc:  # noqa: BLE001 — counted and gated at 0
                st["error: treated"] += 1
                fails.append(f"{symbol}: {exc!r}")
                continue
            if "fold_in" not in cap:
                st[f"no_data: {r.no_data_reason}"] += 1
                if r.no_data_reason is None:
                    fails.append(f"{symbol}: did not reach the pass and has no no_data_reason")
                continue
            st["reached"] += 1
            cost = sum(cap.get(f"t_{x}", 0.0) for x in timed_parts)
            rest = max(elapsed - cost, 1e-9)
            (fold_in, covers_in, filers_in, _), (fold_out, f13) = cap["fold_in"], cap["fold_out"]
            (_, cofilers), (pass_out, cof_corr) = cap["pass_in"], cap["pass_out"]
            sets = cap["sets"]
            try:
                base_covers = base._read_13f_covers(conn, iid, fold_in)
                base_filers = base._read_13g_filers(conn, iid, base_covers)
                base_fold_out, base_f13 = base._fold_13f_duplicates(fold_in, base_covers, base_filers)
                base_out, base_cof = real_pass(base_fold_out, cofilers)
            except Exception as exc:  # noqa: BLE001
                st["error: base"] += 1
                fails.append(f"{symbol}: base {exc!r}")
                continue
            try:
                treated, built = decisions(f13), decisions(base_f13)
                cand = R._13f_candidates(*R._cofiler_identities(fold_in))
                tr = conn.execute(_TREAT_SQL, {"iid": iid, "p": max(cand.values())}).fetchall() if cand else []
                E = oracle(fold_in, list(covers_in), filers_in, "E", {x[1]: x[8] for x in tr})
                e_set = {v[:5] for v in E["f"].values()}
                if e_set != {d for d in treated if d[0] != 4}:
                    fails.append(f"{symbol}: oracle 1-3 {sorted(e_set ^ treated)} != treated tiers 1-3")
                if not built <= treated:
                    fails.append(f"{symbol}: built folds lost, changed or moved {sorted(built - treated)}")
                kept = sorted(
                    (c.filer_cik, c.shares_removed, c.detail) for c in f13 if "included manager" not in c.detail
                )
                if kept != sorted((c.filer_cik, c.shares_removed, c.detail) for c in base_f13):
                    fails.append(f"{symbol}: built correction changed")
                for tier, key in _TIER_KEY.items():
                    if sets[key] != E.get("matched", {}).get(tier, {}):
                        fails.append(f"{symbol}: matched set {key} != oracle")
                inp = im_inputs(fold_in, E)
                im_matched, im_decided = im_oracle(inp, stored_evidence(conn, iid))
                if sets["im"] != im_matched:
                    fails.append(f"{symbol}: matched set im {sets['im']} != stored oracle {im_matched}")
                im_set = {
                    (4, _dig(m), _dig(k), str(inp["tot"][m]), inp["cands"][m]["A"]) for m, k in im_decided.items()
                }
                if im_set != {d for d in treated if d[0] == 4}:
                    fails.append(f"{symbol}: (IM) decisions != stored oracle {sorted(im_set ^ treated)}")
                if inp["cands"]:
                    deferred.append((iid, symbol, inp, im_matched))
                new = treated - built
                sum_t = sum((Decimal(d[3]) for d in new), Decimal(0))
                d = _total(pass_out) - _total(base_out)
                if d != -sum_t:
                    fails.append(f"{symbol}: treated − base {d} != −Σ T {-sum_t}")
                if _total(fold_in) - _total(pass_out) != sum((c.shares_removed for c in (*f13, *cof_corr)), Decimal(0)):
                    fails.append(f"{symbol}: input − output != Σ shares_removed")
                g0 = R._cofiler_graph(base_fold_out, cofilers)
                g1 = R._cofiler_graph(fold_out, cofilers)
                if g0.edges != g1.edges or g0.cliques != g1.cliques or g0.chains != g1.chains:
                    fails.append(f"{symbol}: #3898 edges or collapse list changed")
                if [(c.filer_cik, c.shares_removed) for c in base_cof] != [
                    (c.filer_cik, c.shares_removed) for c in cof_corr
                ]:
                    fails.append(f"{symbol}: #3898 corrections changed")
            except Exception as exc:  # noqa: BLE001
                st["error: oracle"] += 1
                fails.append(f"{symbol}: oracle {exc!r}")
                continue
            share = cost / rest
            read_share = cap.get("t__read_13f_im_evidence", 0.0) / rest
            lat["all"].append(share)
            lat["read all"].append(read_share)
            lat["all without the (IM) read (reported)"].append(share - read_share)
            st["(IM) read issued"] += bool(cap.get("_read_13f_im_evidence_out"))
            pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
            if symbol in _NAMED and d != _NAMED[symbol]:
                fails.append(f"{symbol}: named figure {d} != {_NAMED[symbol]}")
            if symbol in _NAMED or symbol in _PANEL:
                print(f"named {symbol}: pie {pie:,} (base {pie - d:,}), fold {d:,}")
            if not new:
                continue
            out_ = r.shares_outstanding
            if r.no_data_reason is not None:
                st[f"changed, then no_data: {r.no_data_reason}"] += 1
                st["duplicates, then no_data"] += len(new)
            else:
                st["changed"] += 1
                st["duplicates"] += len(new)
                lat["affected"].append(share)
                lat["read affected"].append(read_share)
                delta += d
                base_pie = pie - d
                if out_:
                    st["over in main"] += base_pie > out_
                    st["fixed"] += base_pie > out_ >= pie
                    if pie > out_ >= base_pie:
                        fails.append(f"{symbol}: newly oversubscribed")
            rows = R._cofiler_identities(fold_in)[0]
            g1 = R._cofiler_graph(fold_out, cofilers)
            for m, k in im_decided.items():
                c = inp["cands"][m]
                cases.append(
                    {
                        "symbol": symbol,
                        "iid": iid,
                        "M": [rows[m][0].filer_name, c["cik"]],
                        "A": c["A"],
                        "P": str(c["P"]),
                        "T": str(c["T"]),
                        "k": [rows[k][0].filer_name, _dig(k)],
                        "k_rows": [
                            [h.winning_source, h.winning_accession, str(h.shares), str(h.as_of_date)] for h in rows[k]
                        ],
                        "k_in_clique": any(k in members for members in g1.cliques),
                        "pie": str(pie),
                        "base_pie": str(pie - d),
                        "outstanding": str(out_),
                        "no_data": r.no_data_reason,
                    }
                )

    # The archive oracle, after the snapshot: files, read once, filtered to the candidates' accessions.
    keep = {c["A"] for _, _, inp, _ in deferred for c in inp["cands"].values()}
    captures: dict[str, Any] = {}
    keyed: dict[tuple, Any] = {}
    for path in args.archives:
        t0 = time.perf_counter()
        cp, ky = _read_archive(path, cusip_map, keep)
        captures.update({a: v for a, v in cp.items() if a in keep})
        keyed.update(ky)
        print(f"archive {path.name}: {time.perf_counter() - t0:.0f}s", flush=True)
    for iid, symbol, inp, im_matched in deferred:
        covered = {m: c for m, c in inp["cands"].items() if c["A"] in captures}
        st["(IM) candidates"] += len(inp["cands"])
        st["(IM) candidates in an archive"] += len(covered)
        st["(IM) stored matches outside the archives"] += sum(1 for m in im_matched if m not in covered)
        a_matched, _ = im_oracle({**inp, "cands": covered}, archive_evidence(iid, captures, keyed))
        s_matched = {m: tw for m, tw in im_matched.items() if m in covered}
        if a_matched != s_matched:
            fails.append(f"{symbol}: archive oracle {a_matched} != stored oracle {s_matched}")

    accounted = st["reached"] + st["error: treated"] + sum(v for k, v in st.items() if k.startswith("no_data"))
    if accounted != st["selected"]:
        fails.append(f"population: {accounted} accounted != {st['selected']} selected")
    for gate in ("error: treated", "error: base", "error: oracle"):
        if st[gate]:
            fails.append(f"{gate} = {st[gate]}")
    for name, xs in lat.items():
        if len(xs) < 20:
            # Below 20 samples the p95 estimator is not evaluated; the affected sets gate on their max instead, which
            # bounds every sample. A sparse full-population set fails; a ``--limit`` run only reports it.
            worst = max(xs, default=0.0)
            print(f"latency {name}: {len(xs)} samples, max {worst:.2%} (p95 not evaluated)")
            if name.endswith("affected") and worst > 0.10:
                fails.append(f"latency {name}: max {worst:.1%} > 10%")
            elif not name.endswith("affected") and not args.limit:
                fails.append(f"latency {name}: {len(xs)} samples")
            continue
        p95 = statistics.quantiles(xs, n=20)[-1]
        print(f"latency {name}: n {len(xs)} p50 {statistics.median(xs):.2%} p95 {p95:.2%} max {max(xs):.2%}")
        if p95 > 0.10 and "(reported)" not in name:
            fails.append(f"latency {name}: p95 {p95:.1%} > 10%")
    got = {c["symbol"]: int(Decimal(c["T"])) for c in cases}
    recon = {s: (_CENSUS.get(s), got.get(s)) for s in sorted(set(_CENSUS) | set(got)) if _CENSUS.get(s) != got.get(s)}
    # Spec § Change 5: a difference from the census set is reconciled to source, per case, or the A/B fails. A
    # ``--limit`` run does not reach every census instrument, so it only reports.
    for symbol, (census_t, ab_t) in recon.items():
        if symbol in reconciled:
            print(f"RECONCILED {symbol} census {census_t} A/B {ab_t}: {reconciled[symbol]}")
        elif not args.limit:
            fails.append(f"{symbol}: census {census_t} != A/B {ab_t}, not reconciled")
    for symbol in sorted(set(reconciled) - set(recon)):
        fails.append(f"{symbol}: reconciliation given but no census difference")
    with open(args.out, "w") as fh:
        json.dump(
            {
                "head": head,
                "base": base_sha,
                "stats": st,
                "delta": str(delta),
                "census_reconciliation": recon,
                "cases": cases,
                "failures": fails,
            },
            fh,
            indent=1,
            default=str,
        )
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    print(
        f"delta {delta:,}; {len(cases)} (IM) duplicates on {len({c['symbol'] for c in cases})} instruments "
        f"(spec census: 16 on 16, -110,742,341); census differences (census, A/B): {recon}; written {args.out}"
    )
    for c in sorted(cases, key=lambda c: -Decimal(c["T"])):
        print("CASE", json.dumps(c, default=str))
    for f in fails[:40]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
