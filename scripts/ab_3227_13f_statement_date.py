"""Full-population A/B for the #3227 13F-duplicate statement-date selection (read-only).

Spec: docs/specs/ownership/2026-10-22-3227-13f-statement-as-of.md (Change 2).

Builds every rollup on THIS checkout inside one REPEATABLE READ snapshot (session time zone UTC), with the fold,
the cover reads, the co-filer read and the co-filer pass wrapped so their real inputs, outputs and costs are
recorded. In the same snapshot, per instrument:
  * BASE: the real code at a pinned SHA (``--base``, default the merge-base with origin/main), loaded from git as
    its own module, reads its own covers and observations (filing-date bound) and runs its fold on the fold's real
    input, then the real co-filer pass;
  * ORACLE: a frozen copy of the spec census's oracle (tiers 1-3), run twice: mode F (filing-date selection) on the
    base's covers, and mode E (statement-date selection) on covers it reads itself (``_TREAT_SQL``) with the
    production ``_read_13g_filers`` over them.

Decisions are ``(tier, M, k, T, C)``: tier 1 the 13D arm, 2 (U), 3 (O); M and k digit CIKs; C the accession.

Gates (non-zero exit on any failure):
  * the oracle's mode F decisions equal the base's, and mode E's equal the treated code's (so the increment equals
    the snapshot's oracle set);
  * every base decision is a treated decision (no built fold lost, changed or moved to another tier or C), and each
    base correction's text equals its treated text once ``, date of event YYYY-MM-DD`` is removed;
  * the treated cover rows equal ``_TREAT_SQL``'s;
  * no accession carries two date-of-event values, or a mix of NULL and non-NULL (the oracle dates by accession);
  * per instrument: treated total = base total − Σ T over the new decisions; input − output = Σ ``shares_removed``;
  * #3898's edges, cliques, chains and corrections unchanged; newly oversubscribed = 0;
  * population accounted; treated, base and oracle errors each 0;
  * latency, on the full population and on the affected instruments: the cost share (co-filer read + both cover
    reads + fold + co-filer pass) / (the rest of ``get_ownership_rollup``) p95 ≤ 10%, and the cover read's regression
    (treated read − base read) / (the rest) p95 ≤ 10%.

Reported, not gated: the census reconciliation (spec: 73 duplicates on 72 instruments, −301,408,003; drift from live
ingest is listed per instrument) and the case-review rows.

Usage: ``PYTHONPATH=. uv run python -m scripts.ab_3227_13f_statement_date [--base SHA] [--out PATH] [--limit N]``
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
from datetime import UTC, date
from decimal import Decimal
from typing import Any

import psycopg

import app.services.ownership_rollup as R
from app.config import settings
from app.db.snapshot import snapshot_read
from scripts.ab_3227_13f_13g_covers import _load_base, _plain
from scripts.ab_3227_13f_duplicates import _total

_TREAT_SQL = """
    SELECT reporter_cik, accession_number, filed_at, sole_dispositive_power, shared_dispositive_power,
           aggregate_amount_owned, reporter_name, submission_type, date_of_event
      FROM blockholder_filings
     WHERE instrument_id = %(iid)s AND filed_at IS NOT NULL
       AND LEAST(COALESCE(date_of_event, (filed_at AT TIME ZONE 'UTC')::date),
                 (filed_at AT TIME ZONE 'UTC')::date) <= %(p)s
     ORDER BY filed_at, accession_number, filing_id
"""
_DATES_SQL = """
    SELECT count(*) FROM (
        SELECT accession_number FROM blockholder_filings GROUP BY 1
        HAVING count(DISTINCT date_of_event) > 1 OR (count(date_of_event) > 0 AND count(date_of_event) < count(*))
    ) x
"""
_DOE_TEXT = re.compile(r", date of event \d{4}-\d{2}-\d{2}")
_NAMED = {"TDAY": Decimal(-12_128_756), "CALM": Decimal(-2_520_562), "DH": Decimal(0)}
_PANEL = ("AAPL", "GME", "MSFT", "JPM", "HD")
_G13 = R._13G_SUBMISSION_TYPES


def _dig(ident: str) -> str | None:
    return R._digit_cik(R._identity_canon(ident))


def _fdate(c: Any) -> date:
    return c.filed_at.astimezone(UTC).date()


def oracle(by_category: Any, covers: list[Any], filers: Any, mode: str, doe: dict[str, date | None]) -> dict[str, Any]:
    """The spec census's oracle (``f13q_census.py`` revision 4), frozen: tiers 1-3 written independently of
    ``_fold_13f_duplicates``. Mode F selects by filing date; mode E by statement date, dated per accession."""
    rows, cat = R._cofiler_identities(by_category)
    cand = R._13f_candidates(rows, cat)
    res: dict[str, Any] = {"f": {}}
    if not cand:
        return res

    def as_of(c: Any) -> date:
        e = doe.get(c.accession_number)
        return min(e, _fdate(c)) if e is not None else _fdate(c)

    if mode == "F":
        ok = lambda c, p: _fdate(c) <= p  # noqa: E731 — filed before P + 1 day UTC
        key = lambda c: (c.filed_at, c.accession_number)  # noqa: E731
    else:
        ok = lambda c, p: as_of(c) <= p  # noqa: E731
        key = lambda c: (as_of(c), c.filed_at, c.accession_number)  # noqa: E731
    tot = {k: sum((h.shares for h in hs), Decimal(0)) for k, hs in rows.items()}
    nodes: dict[str, list[str]] = {}
    for k, hs in rows.items():
        c = R._identity_canon(k)
        if c is not None and R._is_cofiler_node(hs, tot[k], c, R._COFILER_EXCLUDED_SOURCES):
            nodes.setdefault(c, []).append(k)
    by_rep: dict[str, list[Any]] = {}
    by_acc: dict[str, list[tuple[str, Any]]] = {}
    for c in covers:
        ck = R._digit_cik(c.reporter_cik)
        if ck:
            by_rep.setdefault(ck, []).append(c)
            by_acc.setdefault(c.accession_number, []).append((ck, c))
    t1: dict[str, list[str]] = {}
    c1: dict[str, str] = {}
    for m, p in cand.items():
        t, mc = tot[m], _dig(m)
        f = [c for c in by_rep.get(mc or "", ()) if ok(c, p)]
        if not f:
            continue
        cv = max(f, key=key)
        if not cv.reports(t):
            continue
        tw = sorted(
            {
                k
                for ck, r in by_acc[cv.accession_number]
                if ck != mc and r.reports(t)
                for k in nodes.get(ck, ())
                if tot[k] == t
            }
        )
        if tw:
            t1[m], c1[m] = tw, cv.accession_number
    blocks: dict[str, list[Any]] = {}
    for c in covers:
        if c.submission_type in _G13:
            blocks.setdefault(c.accession_number, []).append(c)
    filer = {a: R._13g_filer_of_record(b, list(filers.get(a, ()))) for a, b in blocks.items()}

    def names(k: str, src: set[str]) -> set[str]:
        return {n for h in rows[k] if h.winning_source in src if (n := R._normalised_name(h.filer_name))}

    t2: dict[str, list[str]] = {}
    t3: dict[str, list[str]] = {}
    cx: dict[str, str] = {}
    for m, p in cand.items() if blocks else ():
        if m in t1:
            continue
        t, mc = tot[m], _dig(m)
        mn = names(m, {"13f"})
        bycik = {key(c) for c in by_rep.get(mc or "", ()) if ok(c, p)}
        accs = {
            a
            for a, b in blocks.items()
            if (filer[a] is not None and filer[a] == mc) or mn & {R._normalised_name(x.reporter_name or "") for x in b}
        }
        st = bycik | {key(b) for a in accs for b in blocks[a] if ok(b, p)}
        if not st:
            continue
        latest = max(st)
        if latest in bycik:
            continue
        a = latest[-1]
        bl, f = blocks[a], filer[a]
        mf = f is not None and f == mc
        bn = {R._normalised_name(b.reporter_name or "") for b in bl} - {""}
        if mf:
            pool = [k for cn, ks in nodes.items() if cn != mc for k in ks if names(k, {"form3", "form4"}) & bn]
        else:
            pool = list(nodes.get(f, ())) if f is not None else []
        if all(b.reports(t) for b in bl):
            tw = sorted(k for k in pool if tot[k] == t)
            if tw:
                t2[m], cx[m] = tw, a
            continue
        if mf or f is None:
            continue
        obs = list(filers.get(a, ()))
        tw = []
        for k in pool:
            if tot[k] != t or not all(h.winning_source == "13g" and h.winning_accession == a for h in rows[k]):
                continue
            if len(obs) != 1 or obs[0][2] != t:
                continue
            ob = [b for b in bl if b.reporter_name == obs[0][1]]
            if len(ob) != 1 or not ob[0].reports(t):
                continue
            if any(b.aggregate is not None and b.aggregate > t for b in bl):
                continue
            mb = [b for b in bl if R._normalised_name(b.reporter_name or "") in mn]
            if not mb or not all(b.reports(t) for b in mb):
                continue
            tw.append(k)
        if tw:
            t3[m], cx[m] = sorted(tw), a

    def decide(matched: dict[str, list[str]], reserved: set[str]) -> dict[str, str]:
        cnt = Counter(k for tw in matched.values() for k in tw)
        return {m: tw[0] for m, tw in matched.items() if len(tw) == 1 and cnt[tw[0]] == 1 and tw[0] not in reserved}

    r1 = {k for tw in t1.values() for k in tw}
    r2 = r1 | {k for tw in t2.values() for k in tw}
    for tier, matched, reserved, cs in ((1, t1, set(), c1), (2, t2, r1, cx), (3, t3, r2, cx)):
        for m, k in decide(matched, reserved).items():
            res["f"][m] = (tier, _dig(m), _dig(k), str(tot[m]), cs[m], k)
    res["tot"], res["cand"] = tot, cand
    # Pre-uniqueness matched sets by tier, for the (IM) A/B (``scripts.ab_3227_13f_included_managers``).
    res["matched"] = {1: t1, 2: t2, 3: t3}
    return res


def decisions(corrections: list[Any]) -> set[tuple[Any, ...]]:
    """``(tier, M, k, T, C)`` of each 13F-duplicate correction, parsed from its fixed wording."""
    out = set()
    for c in corrections:
        tier = 3 if "counted here as" in c.detail else 2 if "joint Schedule 13G" in c.detail else 1
        mm = re.search(r"\(#3227\): .* \(([^)]*)\) 13F ([\d.]+) for", c.detail)
        acc = re.search(r"\b(\d{10}-\d{2}-\d{6})\b", c.detail)
        if mm is None or acc is None:
            raise ValueError(f"unparsable correction: {c.detail}")
        out.add((tier, R._digit_cik(mm.group(1)), R._digit_cik(c.filer_cik), str(c.shares_removed), acc.group(1)))
    return out


def _texts(corrections: list[Any]) -> list[tuple[Any, ...]]:
    return sorted(
        (c.filer_cik, c.shares_removed, c.winning_source, c.winning_accession, _DOE_TEXT.sub("", c.detail))
        for c in corrections
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="")
    ap.add_argument("--out", default="/tmp/ab_3227_13f_statement_date.json")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    base_sha = (
        args.base
        or subprocess.run(["git", "merge-base", "HEAD", "origin/main"], capture_output=True, text=True).stdout.strip()
    )
    print(f"HEAD {head}; base {base_sha}")
    base = _load_base(base_sha)
    if "_cover_bound" not in base.__dict__ or "date_of_event" in base._CoverRow.__dataclass_fields__:
        print("FAIL base must select by filing date")
        return 1

    cap: dict[str, Any] = {}
    real_fold, real_pass = R._fold_13f_duplicates, R._reconcile_cofiler_groups
    reads = {n: getattr(R, n) for n in ("_read_13f_covers", "_read_13g_filers", "_read_cofilers")}

    def timed(name: str, fn: Any) -> Any:
        def wrapper(*a: Any) -> Any:
            t0 = time.perf_counter()
            out = fn(*a)
            cap[f"t_{name}"] = time.perf_counter() - t0
            t1 = time.perf_counter()
            cap[f"{name}_in"], cap[f"{name}_out"] = a, out
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
    lat: dict[str, list[float]] = {"all": [], "affected": [], "read all": [], "read affected": []}
    cases: list[dict[str, Any]] = []
    delta = Decimal(0)
    with psycopg.connect(settings.database_url) as conn, snapshot_read(conn):
        conn.execute("SET TIME ZONE 'UTC'")
        inconsistent = conn.execute(_DATES_SQL).fetchone()
        if inconsistent is None or inconsistent[0]:
            fails.append(f"accessions with inconsistent dates of event: {inconsistent}")
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
            (fold_in, covers_in, filers_in), (fold_out, f13) = cap["fold_in"], cap["fold_out"]
            (_, cofilers), (pass_out, cof_corr) = cap["pass_in"], cap["pass_out"]
            try:
                t1 = time.perf_counter()
                base_covers = base._read_13f_covers(conn, iid, fold_in)
                t_base_read = time.perf_counter() - t1
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
                tcov = [R._CoverRow(*x) for x in tr]
                if _plain(tcov) != _plain(list(covers_in)):
                    fails.append(f"{symbol}: treated cover rows != the independent read")
                doe = {x[1]: x[8] for x in tr}
                F = oracle(fold_in, list(base_covers), base_filers, "F", {})
                tfil = reads["_read_13g_filers"](conn, iid, tcov)
                if dict(filers_in) != tfil:
                    fails.append(f"{symbol}: treated observations != the independent read")
                E = oracle(fold_in, tcov, tfil, "E", doe)
                f_set = {v[:5] for v in F["f"].values()}
                e_set = {v[:5] for v in E["f"].values()}
                if f_set != built:
                    fails.append(f"{symbol}: oracle F {sorted(f_set ^ built)} != base")
                if e_set != treated:
                    fails.append(f"{symbol}: oracle E {sorted(e_set ^ treated)} != treated")
                if not built <= treated:
                    fails.append(f"{symbol}: built folds lost, changed or moved {sorted(built - treated)}")
                new = treated - built
                kept = [c for c in f13 if next(iter(decisions([c]))) in built]
                if _texts(kept) != _texts(base_f13):
                    fails.append(f"{symbol}: built correction text changed beyond the date of event")
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
            read_reg = (cap.get("t__read_13f_covers", 0.0) - t_base_read) / rest
            lat["all"].append(share)
            lat["read all"].append(read_reg)
            if symbol in _NAMED and d != _NAMED[symbol]:
                fails.append(f"{symbol}: named figure {d} != {_NAMED[symbol]}")
            pie = sum((s.total_shares for s in r.slices if s.denominator_basis == "pie_wedge"), Decimal(0))
            if symbol in _NAMED or symbol in _PANEL:
                print(f"named {symbol}: pie {pie:,} (base {pie - d:,}), fold {d:,}")
            if not new:
                continue
            # Every new decision is a case-review row; a rollup with no data is counted apart from the pie figures.
            out_ = r.shares_outstanding
            if r.no_data_reason is not None:
                st[f"changed, then no_data: {r.no_data_reason}"] += 1
                st["duplicates, then no_data"] += len(new)
            else:
                st["changed"] += 1
                st["duplicates"] += len(new)
                lat["affected"].append(share)
                lat["read affected"].append(read_reg)
                delta += d
                base_pie = pie - d
                if out_:
                    st["over in main"] += base_pie > out_
                    st["fixed"] += base_pie > out_ >= pie
                    if pie > out_ >= base_pie:
                        fails.append(f"{symbol}: newly oversubscribed")
            rows = {k: v for k, v in R._cofiler_identities(fold_in)[0].items()}
            by_m = {v[1]: (m, v) for m, v in E["f"].items()}
            for tier, m_cik, k_cik, t, acc in sorted(new):
                m, v = by_m[m_cik]
                blocks = [c for c in tcov if c.accession_number == acc]
                cases.append(
                    {
                        "symbol": symbol,
                        "tier": tier,
                        "M": [rows[m][0].filer_name, m_cik],
                        "k": [rows[v[5]][0].filer_name, k_cik],
                        "T": t,
                        "P": str(E["cand"][m]),
                        "C": acc,
                        "C_type": blocks[0].submission_type,
                        "C_filed": str(_fdate(blocks[0])),
                        "C_event": str(doe[acc]),
                        "C_blocks": [
                            [
                                b.reporter_cik,
                                b.reporter_name,
                                str(b.sole_dispositive),
                                str(b.shared_dispositive),
                                str(b.aggregate),
                            ]
                            for b in blocks
                        ],
                        "pie": str(pie),
                        "outstanding": str(out_),
                        "no_data": r.no_data_reason,
                    }
                )
    accounted = st["reached"] + st["error: treated"] + sum(v for k, v in st.items() if k.startswith("no_data"))
    if accounted != st["selected"]:
        fails.append(f"population: {accounted} accounted != {st['selected']} selected")
    for gate in ("error: treated", "error: base", "error: oracle"):
        if st[gate]:
            fails.append(f"{gate} = {st[gate]}")
    # Below 20 samples the p95 is not evaluated (the exclusive estimator can exceed the max there, and a sample's max
    # does not bound the population's p95). That fails a full run; a ``--limit`` run only reports it.
    for name, xs in lat.items():
        if len(xs) < 20:
            msg = f"latency {name}: {len(xs)} samples, p95 not evaluated"
            print(msg)
            if not args.limit:
                fails.append(msg)
            continue
        p95 = statistics.quantiles(xs, n=20)[-1]
        print(f"latency {name}: n {len(xs)} p50 {statistics.median(xs):.2%} p95 {p95:.2%} max {max(xs):.2%}")
        if p95 > 0.10:
            fails.append(f"latency {name}: p95 {p95:.1%} > 10%")
    with open(args.out, "w") as fh:
        json.dump(
            {"head": head, "base": base_sha, "stats": st, "delta": str(delta), "cases": cases, "failures": fails},
            fh,
            indent=1,
            default=str,
        )
    for k, v in sorted(st.items()):
        print(f"{k}: {v}")
    print(
        f"delta {delta:,}; {len(cases)} new duplicates on {len({c['symbol'] for c in cases})} instruments "
        f"(spec census: 73 on 72, -301,408,003); by tier {dict(Counter(c['tier'] for c in cases))}; written {args.out}"
    )
    for c in sorted(cases, key=lambda c: -Decimal(c["T"])):
        print("CASE", json.dumps(c, default=str))
    for f in fails[:40]:
        print("FAIL", f)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
