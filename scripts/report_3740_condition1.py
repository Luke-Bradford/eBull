"""#3740 condition 1: post-publication support for every candidate member, from pinned JKP series.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` §"Condition 1". For each member characteristic, the
long-short factor JKP publish for that characteristic and sign (capped value weight) is read in two windows:

* (i) the US series, every calendar month from the January after the original paper's publication year to 2024-08;
* (ii) the developed-ex-US regional series, every month from its first published month to 2024-08. It counts only
  when the original paper used no developed ex-US data; otherwise (i) alone decides.

A member is supported when every counting window holds exactly one finite value for each of its calendar months,
at least 24 of them, and a positive mean. A family is supported when every member is. Newey-West t and month counts
are printed; significance is not required.

Inputs are archived snapshots in ``reference_data_snapshots``, pinned below by id and sha256; publication years and
original samples are JKP's Table 9 (``Documentation.pdf``, sha256 pinned in ``factor_panel_reference``).

    PYTHONPATH=. uv run python -m scripts.report_3740_condition1           # print
    PYTHONPATH=. uv run python -m scripts.report_3740_condition1 --write   # also write the committed table
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Final, Literal

import psycopg

from app.config import settings
from app.services.factor_panel_reference import (
    JKP_DOCUMENTATION_SHA256,
    JKP_DOCUMENTATION_URL,
    jkp_directions,
    load_table9_signs,
)
from app.services.r6_monthly_trial import Refused, hac_t

REPO_ROOT: Final = Path(__file__).resolve().parents[1]
JSON_PATH: Final = REPO_ROOT / "docs/research/3740-condition1-table.json"
MARKDOWN_PATH: Final = REPO_ROOT / "docs/research/3740-condition1-table.md"
SCHEMA: Final = "3740-condition1-v1"

#: The window end the spec fixes "whatever the vintage".
WINDOW_END: Final = date(2024, 8, 31)
MIN_MONTHS: Final = 24
REGION_DEFINITION: Final = (
    "JKP `developed`: countries with msci_development == 'developed' except USA (ZWE and VEN excluded for data "
    "issues), country factor returns weighted by lagged market cap, at least 3 countries a month "
    "(bkelly-lab/ReplicationCrisis GlobalFactors/portfolios.R at 67174c7f170bf8b952b3c86876f187fd9970b5ba)"
)


@dataclass(frozen=True)
class Pinned:
    dataset_key: str
    location: str
    snapshot_id: int
    sha256: str


US: Final = Pinned(
    "jkp_usa_monthly_vw_cap", "usa", 38, "f766aa2c7db1f97594f79f9063275161b7038e936db9ea960eac6eb7ec27ca80"
)
DEVELOPED: Final = Pinned(
    "jkp_developed_monthly_vw_cap", "developed", 106, "3bf7460655b2a6ba61acffa88cb35958812a58f1d2d5a0669d5626735768715c"
)


@dataclass(frozen=True)
class Member:
    name: str
    family: str
    #: JKP Table 9 "Citation" and "Orig. Sample".
    citation: str
    publication_year: int
    original_sample: str
    #: Whether the original paper used developed ex-US data: "no", "yes", or "undetermined" when the text read does
    #: not settle it. The regional window counts only on "no" (§"Condition 1", Independence).
    uses_developed_ex_us: Literal["no", "yes", "undetermined"]
    #: Where the sample was read, and the sentence that states it.
    markets_evidence: str


_ARCHIVE: Final = "https://web.archive.org/web"
MEMBERS: Final = (
    Member(
        "be_me",
        "value",
        "Rosenberg, Reid, and Lanstein (1985)",
        1985,
        "1973-1984",
        "undetermined",
        "only the first page was readable (publisher preview, "
        f"{_ARCHIVE}/20230518022721id_/https://www.pm-research.com/content/iijpormgmt/11/3/9.extract.jpg): "
        '"approximately 1400 stocks in a prospectively defined universe of large companies, called the HICAP '
        'universe"; the subtitle concludes "prices on the NYSE are inefficient"',
    ),
    Member(
        "ni_me",
        "value",
        "Basu (1983)",
        1983,
        "1963-1979",
        "no",
        f"{_ARCHIVE}/20240716070810id_/http://www-2.rotman.utoronto.ca/~kan/3032/pdf/AssetPricingAnomalies/"
        'Basu_JFE_1983.pdf §2.1: Compustat PDE and CRSP monthly; "a firm was required to have been listed on the New '
        'York Stock Exchange"',
    ),
    Member(
        "ocf_me",
        "value",
        "Desai, Rajgopal, and Venkatachalam (2004)",
        2004,
        "1973-1997",
        "no",
        "published p. 360 §III (faculty.washington.edu/rajgopal/files/drv_tar2004.pdf, via the Wayback Machine): "
        '"the universe of firms listed on the NYSE, Amex, and NASDAQ markets ... on the Compustat and the CRSP '
        'tapes. We exclude closed-end funds, investment trusts, and foreign companies"',
    ),
    Member(
        "gp_at",
        "GP/A",
        "Novy-Marx (2013)",
        2013,
        "1963-2010",
        "yes",
        'https://mysimon.rochester.edu/novy-marx/research/OSoV.pdf (June 2012 draft) §2.4 "International '
        'evidence", Table 5: "stocks from developed markets outside the US ... The data come from Compustat Global. '
        '... covers July 1990 to October 2009"',
    ),
    Member(
        "at_gr1",
        "investment",
        "Cooper, Gulen, and Schill (2008)",
        2008,
        "1968-2003",
        "no",
        f"{_ARCHIVE}/20220209144359id_/http://www.krannert.purdue.edu/faculty/hgulen/asset_growth.pdf §I: "
        '"all NYSE, Amex, and NASDAQ nonfinancial firms ... listed on the CRSP monthly stock return files and the '
        'Compustat annual industrial files"; the abstract says "U.S. stock returns"',
    ),
)


@dataclass(frozen=True)
class WindowResult:
    region: str
    start: str
    end: str
    counting: bool
    months: int
    mean: float | None
    nw_t: float | None
    nw_lag: int | None
    #: ``None`` when the window passes; otherwise why it does not.
    failure: str | None

    @property
    def passes(self) -> bool:
        return self.failure is None


def month_key(day: date) -> tuple[int, int]:
    return day.year, day.month


def calendar_months(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    year, month = start
    while (year, month) <= end:
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def read_series(payload: bytes, location: str) -> dict[str, list[tuple[date, str]]]:
    """Every row per factor as published: ``(date, raw ret)``, NA kept so validity can see it."""
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        (name,) = [member for member in archive.namelist() if not member.endswith("/")]
        text = archive.read(name).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if not {"location", "name", "freq", "weighting", "date", "ret"} <= set(reader.fieldnames or ()):
        raise ValueError(f"the {location} file's header {reader.fieldnames!r} lacks a column this report reads")
    series: dict[str, list[tuple[date, str]]] = {}
    for row in reader:
        # DictReader fills a short row with None and files a long row's extras under the key None.
        if None in row or None in row.values():
            raise ValueError(f"ragged row at line {reader.line_num} of the {location} file")
        if (row["location"], row["freq"], row["weighting"]) != (location, "monthly", "vw_cap"):
            raise ValueError(f"row for {row['location']}/{row['freq']}/{row['weighting']} in the {location} file")
        series.setdefault(row["name"], []).append((date.fromisoformat(row["date"]), row["ret"].strip()))
    return series


def evaluate_window(
    rows: Sequence[tuple[date, str]], *, region: str, start: tuple[int, int], counting: bool
) -> WindowResult:
    """§"Condition 1" data validity, the 24-month floor and the sign of the mean, over ``start``..2024-08."""
    end = month_key(WINDOW_END)
    label = {"region": region, "start": f"{start[0]}-{start[1]:02d}", "end": f"{end[0]}-{end[1]:02d}"}
    months = calendar_months(start, end) if start <= end else []

    def failed(reason: str, count: int = 0) -> WindowResult:
        return WindowResult(**label, counting=counting, months=count, mean=None, nw_t=None, nw_lag=None, failure=reason)

    if not months:
        return failed("the window is empty")
    by_month: dict[tuple[int, int], list[str]] = {}
    for day, raw in rows:
        if start <= month_key(day) <= end:
            by_month.setdefault(month_key(day), []).append(raw)
    values: list[float] = []
    for month in months:
        raws = by_month.get(month, [])
        if len(raws) != 1:
            kind = "missing" if not raws else "duplicated"
            return failed(f"{month[0]}-{month[1]:02d} is {kind}", len(values))
        try:
            value = float(raws[0])
        except ValueError:
            value = math.nan
        if not math.isfinite(value):
            return failed(f"{month[0]}-{month[1]:02d} is not finite ({raws[0]!r})", len(values))
        values.append(value)
    estimate = hac_t(values, ruined=False)
    mean = math.fsum(values) / len(values)
    nw_t, nw_lag = (None, None) if isinstance(estimate, Refused) else (estimate.t_stat, estimate.lag)
    failure = None
    if len(values) < MIN_MONTHS:
        failure = f"{len(values)} months, fewer than {MIN_MONTHS}"
    elif not mean > 0.0:
        failure = "the mean monthly return is not positive"
    return WindowResult(
        **label, counting=counting, months=len(values), mean=mean, nw_t=nw_t, nw_lag=nw_lag, failure=failure
    )


def evaluate_member(
    member: Member, us_rows: Sequence[tuple[date, str]], developed_rows: Sequence[tuple[date, str]]
) -> dict[str, Any]:
    us = evaluate_window(us_rows, region="usa", start=(member.publication_year + 1, 1), counting=True)
    # The first PUBLISHED month, NA rows included: a published NA month is inside the window and fails the
    # "exactly one finite value for every calendar month" validity rule rather than moving the start.
    first = min((month_key(day) for day, _ in developed_rows), default=(WINDOW_END.year + 1, 1))
    developed = evaluate_window(
        developed_rows, region="developed", start=first, counting=member.uses_developed_ex_us == "no"
    )
    windows = [us, developed]
    supported = all(window.passes for window in windows if window.counting)
    return {
        "member": asdict(member),
        "windows": [asdict(window) | {"passes": window.passes} for window in windows],
        "supported": supported,
    }


def evaluate(
    series: Mapping[str, Mapping[str, Sequence[tuple[date, str]]]], members: Sequence[Member] = MEMBERS
) -> dict[str, Any]:
    rows = [evaluate_member(m, series["usa"].get(m.name, ()), series["developed"].get(m.name, ())) for m in members]
    families: dict[str, bool] = {}
    for row in rows:
        family = row["member"]["family"]
        families[family] = families.get(family, True) and row["supported"]
    return {"members": rows, "families": families}


def load_pinned(conn: psycopg.Connection[Any], pinned: Pinned) -> bytes:
    found = conn.execute(
        "SELECT dataset_key, response_sha256, payload, parse_status FROM reference_data_snapshots"
        " WHERE snapshot_id = %(id)s",
        {"id": pinned.snapshot_id},
    ).fetchone()
    if found is None:
        raise SystemExit(f"snapshot {pinned.snapshot_id} is not in this database")
    dataset_key, stored_sha, payload, status = found
    digest = hashlib.sha256(bytes(payload)).hexdigest()
    if (dataset_key, stored_sha, digest, status) != (pinned.dataset_key, pinned.sha256, pinned.sha256, "accepted"):
        raise SystemExit(f"snapshot {pinned.snapshot_id} is not the pinned {pinned.dataset_key} bytes")
    return bytes(payload)


def check_signs(payloads: Mapping[str, bytes], members: Sequence[Member]) -> None:
    """Each series must carry Table 9's sign for its member: JKP's ``ret`` is already signed by ``direction``.

    ``jkp_directions`` refuses a factor whose ``direction`` is not one constant value across its rows.
    """
    signs = load_table9_signs()
    for location, payload in payloads.items():
        directions = jkp_directions(payload)
        for member in members:
            if directions.get(member.name) != signs[member.name]:
                raise SystemExit(
                    f"{location} {member.name}: direction {directions.get(member.name)} ≠ Table 9 sign "
                    f"{signs[member.name]}"
                )


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        "# #3740 condition 1 — post-publication support per member",
        "",
        "Generated by `PYTHONPATH=. uv run python -m scripts.report_3740_condition1 --write`; the JSON beside this "
        "file holds the same figures and the input pins. Rule: `docs/research/2026-10-10-3740-step3-vw-book.md` "
        '§"Condition 1".',
        "",
        f"- US series: snapshot {US.snapshot_id} (`{US.dataset_key}`, sha256 `{US.sha256}`).",
        f"- Developed ex-US series: snapshot {DEVELOPED.snapshot_id} (`{DEVELOPED.dataset_key}`, sha256 "
        f"`{DEVELOPED.sha256}`). {REGION_DEFINITION}.",
        f"- Publication years and original samples: JKP Table 9 (`{JKP_DOCUMENTATION_URL}`, sha256 "
        f"`{JKP_DOCUMENTATION_SHA256}`).",
        f"- Windows end {WINDOW_END:%Y-%m}; a counting window needs one finite value every month, at least "
        f"{MIN_MONTHS} months and a positive mean. Newey-West t uses ⌊4(T/100)^(2/9)⌋ lags and is not gated.",
        "",
        "| member | family | paper (pub. year) | window | counts | months | mean / month | NW t (lag) | passes |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in result["members"]:
        member = row["member"]
        for window in row["windows"]:
            mean = "—" if window["mean"] is None else f"{window['mean'] * 100:.3f}%"
            nw = "—" if window["nw_t"] is None else f"{window['nw_t']:.2f} ({window['nw_lag']})"
            verdict = "yes" if window["passes"] else f"no: {window['failure']}"
            lines.append(
                f"| `{member['name']}` | {member['family']} | {member['citation']} | {window['region']} "
                f"{window['start']}..{window['end']} | {'yes' if window['counting'] else 'no'} | {window['months']} "
                f"| {mean} | {nw} | {verdict} |"
            )
    lines += ["", "| member | supported | developed ex-US data in the original paper (evidence) |", "|---|---|---|"]
    for row in result["members"]:
        member = row["member"]
        uses = member["uses_developed_ex_us"]
        lines.append(
            f"| `{member['name']}` | {'yes' if row['supported'] else 'no'} | {uses}: {member['markets_evidence']} |"
        )
    lines += ["", "| family | supported |", "|---|---|"]
    lines += [f"| {family} | {'yes' if ok else 'no'} |" for family, ok in result["families"].items()]
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="#3740 condition 1: post-publication support per member.")
    parser.add_argument("--write", action="store_true", help="write the committed JSON and Markdown table")
    args = parser.parse_args(argv)
    with psycopg.connect(settings.database_url) as conn:
        payloads = {pinned.location: load_pinned(conn, pinned) for pinned in (US, DEVELOPED)}
    check_signs(payloads, MEMBERS)
    series = {location: read_series(payload, location) for location, payload in payloads.items()}
    result = {
        "schema": SCHEMA,
        "inputs": {
            "us": asdict(US),
            "developed": asdict(DEVELOPED) | {"definition": REGION_DEFINITION},
            "table9": {"url": JKP_DOCUMENTATION_URL, "sha256": JKP_DOCUMENTATION_SHA256},
        },
        "window_end": WINDOW_END.isoformat(),
        "min_months": MIN_MONTHS,
        **evaluate(series),
    }
    markdown = render_markdown(result)
    print(markdown)
    if args.write:
        JSON_PATH.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        MARKDOWN_PATH.write_text(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
