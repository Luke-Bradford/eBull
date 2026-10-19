"""#3850 premise measurements: ``instrument_valuation``'s count-based columns against the #3841 point-in-time row.

Spec: ``docs/specs/fundamentals/2026-10-19-3850-live-cap-share-basis.md``. Read-only. Database reads share one
REPEATABLE READ snapshot; section 5 additionally runs the CURRENT build reader against the CURRENT
``submissions.zip`` (identities printed), so it is a present reconstruction, not a replay of the printed build.

"The session" of an instrument is its latest stored ``price_daily`` session with a positive close, the bar
``instrument_valuation.priced`` falls back to. "The PIT row" is ``instrument_market_cap_pit`` at exactly that
session. Ratios and thresholds are computed in ``Decimal``.

Prints:

1. every priced view row, cross-tabbed by today's ``market_cap_live`` and the PIT outcome at the session;
2. among PIT-published rows, the view's count against PIT's effective count (``shares_reported × split_factor``);
3. a simulation of the proposed ``market_cap_live`` (the PIT row's ``market_cap``, under the spec's final-reason
   precedence) against today's: kept, gained, lost by reason, and changed by more than 0.5%. The changed rows are
   split by threshold into: counts differ by more than 0.5%, or counts within 0.5% (a mixed change counts as the
   first);
4. named cases with every operand the spec cites, including the view arm, PIT's concept and the anchor window;
5. today's view caps with no PIT row at the session, each with what the current reader observes;
6. the whole ``ads_ratio`` table (with today's view cap) and the FPI/ADR set against the PIT outcome;
7. published PIT rows at the session whose split factor is not 1;
8. over the proposed-published rows, the session against today's ``price_as_of`` (weekdays, days apart).

    PYTHONPATH=. uv run python scripts/measure_3850_premise.py
"""

from __future__ import annotations

import hashlib
import subprocess
import zipfile
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import settings
from app.security.master_key import resolve_data_dir
from app.services.pit_fundamentals import submissions_reader
from app.services.pit_market_cap_build import iter_instrument_builds
from app.services.strategies.validated_universe import load_validated_universe

_ROWS = """
    WITH ses AS (
        SELECT v.instrument_id,
               (SELECT max(p.price_date) FROM price_daily p
                WHERE p.instrument_id = v.instrument_id AND p.close > 0) AS session
        FROM instrument_valuation v
    ),
    fpi_adr AS (
        SELECT instrument_id FROM coverage WHERE filings_status = 'fpi'
        UNION
        SELECT instrument_id FROM instruments WHERE company_name ~* '\\y(ADR|ADS)\\y'
    ),
    dual_class AS (
        SELECT DISTINCT ei.instrument_id
        FROM external_identifiers ei
        JOIN instrument_class_shares_outstanding c ON c.source_cik = lpad(ei.identifier_value, 10, '0')
        WHERE ei.provider = 'sec' AND ei.identifier_type = 'cik' AND ei.is_primary
    )
    SELECT i.instrument_id, i.symbol, v.current_price, v.price_as_of, v.market_cap_live, v.shares_outstanding,
           v.is_complete_ttm, s.session, p.price_date IS NOT NULL AS has_pit, p.market_cap, p.reason, p.close,
           p.split_factor, p.shares_reported, p.shares_concept, p.context_date, p.evidence,
           a.ratio, f.instrument_id IS NOT NULL AS is_fpi_adr, d.instrument_id IS NOT NULL AS is_dual_class
    FROM instrument_valuation v
    JOIN instruments i USING (instrument_id)
    JOIN ses s USING (instrument_id)
    LEFT JOIN instrument_market_cap_pit p ON p.instrument_id = v.instrument_id AND p.price_date = s.session
    LEFT JOIN ads_ratio a ON a.instrument_id = v.instrument_id
    LEFT JOIN fpi_adr f ON f.instrument_id = v.instrument_id
    LEFT JOIN dual_class d ON d.instrument_id = v.instrument_id
    WHERE v.current_price > 0
"""

_ADS = """
    SELECT i.symbol, a.ratio, s.session, p.price_date IS NOT NULL, p.market_cap IS NOT NULL, p.reason,
           v.market_cap_live
    FROM ads_ratio a
    JOIN instruments i USING (instrument_id)
    LEFT JOIN instrument_valuation v ON v.instrument_id = a.instrument_id
    LEFT JOIN LATERAL (SELECT max(price_date) AS session FROM price_daily x
                       WHERE x.instrument_id = a.instrument_id AND x.close > 0) s ON TRUE
    LEFT JOIN instrument_market_cap_pit p ON p.instrument_id = a.instrument_id AND p.price_date = s.session
    ORDER BY i.symbol
"""

_CIK = """
    SELECT instrument_id, identifier_value FROM external_identifiers
    WHERE provider = 'sec' AND identifier_type = 'cik' AND is_primary AND instrument_id = ANY(%(ids)s::bigint[])
"""

_TTM_END = "SELECT instrument_id, ttm_end FROM financial_periods_ttm WHERE instrument_id = ANY(%(ids)s::bigint[])"

_NAMED = ("MNST", "APH", "SPTX", "AMZN", "XOM", "AAPL")
_HALF_PCT = Decimal("0.005")


def _final_reason(r: dict[str, Any]) -> str | None:
    """The proposed view's reason, in the spec's precedence order; None means published."""
    if r["is_dual_class"]:
        return "dual_class"
    if r["ratio"] is not None:
        return "ads_ratio"
    if r["is_fpi_adr"]:
        return "fpi_adr"
    if not r["has_pit"]:
        return "no_pit_row"
    return None if r["market_cap"] is not None else str(r["reason"])


def _effective(r: dict[str, Any]) -> Decimal:
    return Decimal(r["shares_reported"]) * Decimal(str(r["split_factor"]))


def _sha256(path: Any) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
    archive = resolve_data_dir() / "sec" / "bulk" / "submissions.zip"
    with psycopg.connect(settings.database_url) as conn:
        conn.read_only = True
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        with conn.transaction():
            print("as of", datetime.now(UTC).isoformat(timespec="seconds"), "| code", head)
            build = conn.execute(
                "SELECT build_id, build_date, code_sha, submissions_sha256 FROM market_cap_pit_builds "
                "WHERE status = 'complete' ORDER BY build_id DESC LIMIT 1"
            ).fetchone()
            print("latest complete PIT build", build)
            with conn.cursor(row_factory=dict_row) as cur:
                rows = cur.execute(_ROWS).fetchall()
            ttm_end = dict(conn.execute(_TTM_END, {"ids": [r["instrument_id"] for r in rows]}).fetchall())

            print("\n1. priced view rows", len(rows), "| view caps today", sum(1 for r in rows if r["market_cap_live"]))
            cells: Counter[tuple[str, str]] = Counter()
            for r in rows:
                outcome = (
                    "no_pit_row"
                    if not r["has_pit"]
                    else ("published" if r["market_cap"] is not None else str(r["reason"]))
                )
                cells[("view_cap" if r["market_cap_live"] is not None else "no_view_cap", outcome)] += 1
            for key, n in sorted(cells.items()):
                print("  ", key, n)

            pub = [
                r
                for r in rows
                if r["market_cap"] is not None and r["shares_outstanding"] is not None and r["shares_outstanding"] > 0
            ]
            ratios = [Decimal(r["shares_outstanding"]) / _effective(r) for r in pub]
            print(
                "\n2. PIT-published rows with a positive view count:",
                len(pub),
                "| equal",
                sum(x == 1 for x in ratios),
                "| outside [0.9, 1.1]",
                sum(not Decimal("0.9") <= x <= Decimal("1.1") for x in ratios),
                "| below 0.55 or above 1.8",
                sum(x < Decimal("0.55") or x > Decimal("1.8") for x in ratios),
            )

            kept = count_driven = price_only = 0
            gained = 0
            lost: Counter[str] = Counter()
            arm_gained: Counter[str] = Counter()
            for r in rows:
                reason, old = _final_reason(r), r["market_cap_live"]
                if reason is None and old is not None:
                    kept += 1
                    if abs(Decimal(r["market_cap"]) / Decimal(old) - 1) > _HALF_PCT:
                        same_count = (
                            r["shares_outstanding"] is not None
                            and r["shares_outstanding"] > 0
                            and abs(Decimal(r["shares_outstanding"]) / _effective(r) - 1) <= _HALF_PCT
                        )
                        price_only += same_count
                        count_driven += not same_count
                elif reason is None:
                    gained += 1
                    arm_gained["ttm" if r["is_complete_ttm"] else "legacy"] += 1
                elif old is not None:
                    lost[reason] += 1
            print(
                "\n3. proposed vs today: published in both",
                kept,
                "| changed > 0.5%, by threshold: counts differ > 0.5%",
                count_driven,
                "| counts within 0.5%",
                price_only,
                "| cap gained",
                gained,
                dict(arm_gained),
                "| lost",
                sum(lost.values()),
                dict(lost.most_common()),
            )

            print("\n4. named cases")
            for r in rows:
                if r["symbol"] in _NAMED:
                    window = [
                        (w["q"], w["status"], round(w["f_hat"], 4) if w.get("f_hat") else None)
                        for w in ((r["evidence"] or {}).get("basis") or {}).get("window", [])
                    ]
                    print(
                        "  ",
                        r["symbol"],
                        "| arm",
                        "ttm" if r["is_complete_ttm"] else "legacy",
                        "| view count",
                        r["shares_outstanding"],
                        "TTM end",
                        ttm_end.get(r["instrument_id"]),
                        "| price",
                        r["current_price"],
                        "as of",
                        r["price_as_of"],
                        "| view cap",
                        r["market_cap_live"],
                        "| session",
                        r["session"],
                        "close",
                        r["close"],
                        "| PIT",
                        r["shares_concept"],
                        r["shares_reported"],
                        "context",
                        r["context_date"],
                        "factor",
                        r["split_factor"],
                        "reason",
                        r["reason"],
                        "cap",
                        r["market_cap"],
                        "| window",
                        window,
                    )

            missing = [r for r in rows if r["market_cap_live"] is not None and not r["has_pit"]]
            print("\n5. view cap today, no PIT row at the session:", len(missing))
            universe = set(load_validated_universe(conn))
            ciks = dict(conn.execute(_CIK, {"ids": [r["instrument_id"] for r in missing]}).fetchall())
            scope = [
                (r["instrument_id"], str(ciks[r["instrument_id"]]).zfill(10))
                for r in missing
                if r["instrument_id"] in ciks and r["instrument_id"] in universe
            ]
            observed: dict[int, str] = {}
            session_of = {r["instrument_id"]: r["session"] for r in missing}
            if archive.exists() and scope and build:
                print(
                    "   reader: code",
                    head,
                    "| submissions.zip sha256",
                    _sha256(archive),
                    "| build",
                    build[0],
                    "submissions sha256",
                    build[3],
                )
                build_date = build[1] if isinstance(build[1], date) else date.today()
                with zipfile.ZipFile(archive) as z:
                    for batch in iter_instrument_builds(
                        conn, scope=scope, index_of=submissions_reader(z), build_date=build_date
                    ):
                        for res in batch.instruments:
                            if res.refused:
                                observed[res.instrument_id] = f"cik_refused:{res.refused}"
                            elif not res.steps:
                                observed[res.instrument_id] = "no_steps"
                            else:
                                s = session_of[res.instrument_id]
                                covers = any(st.valid_from <= s < st.valid_to for st in res.steps)
                                observed[res.instrument_id] = f"steps_now:{'cover' if covers else 'miss'}_session"
            tally: Counter[str] = Counter()
            for r in sorted(missing, key=lambda x: x["symbol"]):
                where = (
                    "outside_validated_universe"
                    if r["instrument_id"] not in universe
                    else (
                        "no_primary_cik"
                        if r["instrument_id"] not in ciks
                        else observed.get(r["instrument_id"], "not_run")
                    )
                )
                tally[where] += 1
                print("  ", r["symbol"], r["session"], where)
            print("   tally", dict(tally))

            print("\n6. ads_ratio table (symbol, ratio, session, PIT row, PIT published, PIT reason, view cap today)")
            for row in conn.execute(_ADS).fetchall():
                print("  ", row)
            print(
                "   fpi_adr view rows",
                sum(1 for r in rows if r["is_fpi_adr"]),
                "| with a PIT row",
                sum(1 for r in rows if r["is_fpi_adr"] and r["has_pit"]),
                "| published",
                sum(1 for r in rows if r["is_fpi_adr"] and r["market_cap"] is not None),
            )

            published = [r for r in rows if r["market_cap"] is not None]
            print(
                "\n7. published at the session",
                len(published),
                "| split_factor != 1:",
                sum(1 for r in published if r["split_factor"] != 1),
            )

            gaps = Counter(
                (
                    r["session"].strftime("%a"),
                    r["price_as_of"].astimezone(UTC).date().strftime("%a"),
                    (r["price_as_of"].astimezone(UTC).date() - r["session"]).days,
                )
                for r in rows
                if _final_reason(r) is None and r["price_as_of"] is not None
            )
            print(
                "\n8. proposed-published rows: (session weekday, price_as_of UTC weekday, days apart):",
                dict(sorted(gaps.items(), key=lambda kv: -kv[1])),
            )


if __name__ == "__main__":
    main()
