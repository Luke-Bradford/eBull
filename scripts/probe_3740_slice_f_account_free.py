"""Reproduce the account-free source measurements behind #3740 slice F's route note (read-only).

    PYTHONPATH=. uv run python -m scripts.probe_3740_slice_f_account_free

Spec: ``docs/research/2026-10-10-3740-slice-f-forward-capture.md`` §"Route after the no-account rule".

Read-only SQL, two unauthenticated Hugging Face GETs and one GET of eToro's public dividend-calendar web page (no
broker API call). Every count is printed with its
population, because the tables read here are written by live jobs.
"""

from __future__ import annotations

import hashlib
import html
import re
from datetime import date

import httpx
import psycopg

from app.config import settings

# Predicates: XBRL periods ending on or after XBRL_PERIOD_FROM; events whose coalesce(ex, record, declaration) date
# is on or after EVENT_FROM (no upper bound).
XBRL_PERIOD_FROM = date(2025, 7, 1)
EVENT_FROM = date(2025, 10, 1)
# Published split execution dates and ratios (new shares per old share), each the first post-split session.
KNOWN_SPLITS = (
    ("NVDA", date(2024, 6, 7), date(2024, 6, 10), 10),
    ("AVGO", date(2024, 7, 12), date(2024, 7, 15), 10),
    ("CMG", date(2024, 6, 25), date(2024, 6, 26), 50),
    ("WMT", date(2024, 2, 23), date(2024, 2, 26), 3),
)
PWB_DATASET = "https://huggingface.co/api/datasets/paperswithbacktest/Stocks-Daily-Price"
PWB_README = "https://huggingface.co/datasets/paperswithbacktest/Stocks-Daily-Price/raw/main/README.md"
ETORO_DIVIDEND_CALENDAR = "https://www.etoro.com/investing/dividend-calendar/"
BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0"


def main() -> None:
    with psycopg.connect(settings.database_url) as conn:
        # 8-K Item 8.01 dividend parser (app/services/dividend_calendar.py) against XBRL dividend payers.
        row = conn.execute(
            """
            WITH payers AS (
                SELECT DISTINCT instrument_id FROM financial_periods
                WHERE dps_declared > 0 AND period_end_date >= %(xbrl_from)s),
            ev AS (
                SELECT instrument_id, count(*) AS n FROM dividend_events
                WHERE coalesce(ex_date, record_date, declaration_date) >= %(event_from)s
                  AND record_date IS NOT NULL AND dps_declared IS NOT NULL
                GROUP BY 1)
            SELECT (SELECT count(*) FROM payers),
                   (SELECT count(*) FROM payers JOIN ev USING (instrument_id)),
                   (SELECT count(*) FROM payers JOIN ev USING (instrument_id) WHERE ev.n >= 3)
            """,
            {"xbrl_from": XBRL_PERIOD_FROM, "event_from": EVENT_FROM},
        ).fetchone() or (0, 0, 0)
        print(
            f"XBRL dps_declared > 0 payers (period_end >= {XBRL_PERIOD_FROM}): {row[0]}; with >= 1 parsed 8-K "
            f"dividend (record date + amount, from {EVENT_FROM}): {row[1]}; with >= 3: {row[2]}"
        )
        row = conn.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE ex_date IS NOT NULL),
                   count(*) FILTER (WHERE record_date IS NOT NULL)
            FROM dividend_events WHERE coalesce(ex_date, record_date, declaration_date) >= %(event_from)s
            """,
            {"event_from": EVENT_FROM},
        ).fetchone() or (0, 0, 0)
        print(
            f"all dividend_events rows (any instrument, no payer join) with coalesce(ex, record, declaration) >= "
            f"{EVENT_FROM}: {row[0]}; with ex_date: {row[1]}; with record_date: {row[2]}"
        )

        # XBRL per-share declared amounts against XBRL cash dividends paid.
        row = conn.execute(
            """
            WITH fp AS (
                SELECT instrument_id, bool_or(dividends_paid > 0) AS paid, bool_or(dps_declared > 0) AS dps
                FROM financial_periods WHERE period_end_date >= %(xbrl_from)s GROUP BY 1)
            SELECT count(*) FILTER (WHERE paid), count(*) FILTER (WHERE paid AND dps),
                   count(*) FILTER (WHERE paid AND NOT coalesce(dps, false))
            FROM fp
            """,
            {"xbrl_from": XBRL_PERIOD_FROM},
        ).fetchone() or (0, 0, 0)
        print(
            f"XBRL dividends_paid > 0 (period_end >= {XBRL_PERIOD_FROM}): {row[0]}; "
            f"with dps_declared > 0: {row[1]}; without: {row[2]}"
        )

        # eToro's served daily history across published splits: a nominal series would jump by 1/ratio.
        # One current vintage only: compatible with split adjustment, not a measurement of rescale timing.
        for symbol, before, on, ratio in KNOWN_SPLITS:
            rows = conn.execute(
                """
                SELECT p.instrument_id, p.price_date, p.close::float8
                FROM price_daily p JOIN instruments i USING (instrument_id)
                WHERE i.symbol = %(symbol)s AND p.price_date IN (%(before)s, %(on)s)
                """,
                {"symbol": symbol, "before": before, "on": on},
            ).fetchall()
            if len({r[0] for r in rows}) > 1:
                print(f"price_daily {symbol}: {len({r[0] for r in rows})} instruments share the symbol; skipped")
                continue
            closes = {r[1]: r[2] for r in rows}
            if before in closes and on in closes:
                print(
                    f"price_daily {symbol} {before} {closes[before]:.4f} -> {on} {closes[on]:.4f}: "
                    f"ratio {closes[on] / closes[before]:.4f} (nominal series would be ~{1 / ratio:.4f})"
                )
            else:
                print(f"price_daily {symbol}: missing {before} or {on}")

    with httpx.Client(timeout=30, follow_redirects=True) as client:
        meta = client.get(PWB_DATASET)
        meta.raise_for_status()
        info = meta.json()
        print(f"PWB gated={info.get('gated')} sha={info.get('sha')} lastModified={info.get('lastModified')}")
        readme = client.get(PWB_README)
        readme.raise_for_status()
        print(f"PWB README sha256 {hashlib.sha256(readme.content).hexdigest()}")
        for line in readme.text.splitlines():
            if line.startswith("extra_gated_prompt") or "covering **" in line:
                print(f"PWB README: {line}")

        # eToro's public dividend calendar page (marketing page, "indicative and subject to change").
        page = client.get(ETORO_DIVIDEND_CALENDAR, headers={"User-Agent": BROWSER_UA})
        page.raise_for_status()
        print(f"eToro dividend calendar: {len(page.content)} bytes sha256 {hashlib.sha256(page.content).hexdigest()}")
        cells = [
            [html.unescape(re.sub(r"<[^>]+>", " ", c)).split() for c in re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)]
            for r in re.findall(r"<tr[^>]*>(.*?)</tr>", page.text, re.S)
        ]
        cells = [c for c in cells if len(c) >= 6 and c[0] and c[2] and c[3]]
        us = {c[0][0] for c in cells if "." not in c[0][0]}
        ex_dates = sorted(c[2][0] for c in cells)
        pay_dates = sorted(c[3][0] for c in cells)
        if not cells:
            raise SystemExit("eToro dividend calendar: no table rows parsed")
        print(
            f"eToro dividend calendar rows: {len(cells)}; suffix-free symbols: {len(us)}; "
            f"ex-date {ex_dates[0]}..{ex_dates[-1]}; payment {pay_dates[0]}..{pay_dates[-1]}"
        )


if __name__ == "__main__":
    main()
