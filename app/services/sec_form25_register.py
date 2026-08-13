"""#2282 stage 2c — the SEC Form 25 / 25-NSE delisting register.

Source rule: **17 CFR 240.12d2-2** (Rule 12d2-2, removal from listing and
registration). The full recipe and every measured trap live in
``.claude/skills/data-sources/sec-edgar.md`` §2.6; this module is that recipe
in code. The traps it must not fall into, all reproduced against the live
2023 cohort:

**Trap 1 — index ROWS are not filings.** EDGAR indexes a 25-NSE under *both*
the exchange CIK and the issuer CIK. 2023 = **2,437 rows → 1,282 accessions**
(reproduced exactly; 1,155 of those accessions appear under more than one CIK).
De-duplicate on accession, never on ``(cik, form)``.

**Trap 2 — a Form 25 is per-SECURITY, not per-issuer.**
``<descriptionClassSecurity>`` names the class struck from the tape, very often
a bond, warrant, unit or preferred. Berkshire filed two 25-NSEs in 2023, both
notes. "CIK appeared in a Form 25 ⇒ delisted" marks Berkshire delisted in
January 2023.

**Trap 3 — filter on ``<ruleProvision>``.** ``(a)(1)`` and ``(a)(2)`` are debt
lifecycle, ~34% of filings, and are NOT delistings. Issuer-filed **Form 25** is
paragraph ``(c)`` and carries no ``notificationOfRemoval`` schema at all, so a
missing provision on ``form in ("25", "25/A")`` is the (c) case and not a parse
failure.

⚠ ``(b)`` and ``(a)(3)`` are different events for a backtest — (b) is a
failure, (a)(3) is an acquisition where shareholders received something. A
vendor's flat "delisted" flag cannot tell them apart. Keeping the provision is
most of this register's value.

**Trap 4 — no ticker, and SEC will not give you one.** ``submissions`` JSON
drops ``tickers`` to ``[]`` on delisting and
``companyconcept/…/dei/TradingSymbol.json`` 404s. The symbol comes from the
cover-page inline XBRL of the last periodic report filed *before* the
delisting.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date

logger = logging.getLogger(__name__)

#: The four exact form types. Matching on a ``25`` prefix also catches ``253G1``
#: / ``253G2`` (Regulation A offering circulars), which are unrelated and would
#: inflate 2023 QTR1 from 622 rows to 807.
FORM25_TYPES = frozenset({"25", "25-NSE", "25/A", "25-NSE/A"})

#: Issuer-filed forms. These are paragraph (c) — voluntary withdrawal — and are
#: a different document shape with no <ruleProvision> element.
_ISSUER_FILED_FORMS = frozenset({"25", "25/A"})

#: Debt lifecycle. A class called for redemption or paid at maturity leaves the
#: tape without the company going anywhere.
DEBT_LIFECYCLE_PROVISIONS = frozenset({"(a)(1)", "(a)(2)"})

#: Delisting-meaning provisions filed by an exchange.
#:   (a)(3) instruments now evidence OTHER securities by operation of law
#:          (merger / reorganisation) — shareholders received something
#:   (a)(4) all rights pertaining to the class extinguished
#:   (b)    exchange-initiated discretionary delisting (non-compliance) — a
#:          failure
DELISTING_PROVISIONS = frozenset({"(a)(3)", "(a)(4)", "(b)"})

PROVISION_CLASSES = ("equity_delisting", "debt_lifecycle", "unknown")

#: ⚠ SECOND, ORTHOGONAL AXIS. The rule provision says what EVENT happened; it
#: says nothing about WHAT came off the tape, because a Form 25 is
#: per-SECURITY (trap 2). Provision-filtering 2023 alone yields 842
#: "delisting-meaning" filings — and 155 of those are warrants, 111 ETFs/funds,
#: 62 units, 56 preferred or depositary shares, 10 NOTES and 3 rights, leaving
#: 317 common equity plus 128 issuer-filed. A warrant expiring
#: worthless alongside its SPAC is not a company delisting, and a cohort built
#: on provision alone silently doubles itself with securities that never had
#: their own price series in our universe.
#:
#: Collapsing the two axes into one is why the #2282 handoff's "578 filings /
#: 443 issuers" could not be reproduced: the provision counts it cites are
#: exactly right (250/190/486/9/219/128 = 1,282, verified) but they classify
#: the EVENT, and the cohort additionally needs the SECURITY.
SECURITY_CLASSES = (
    "common_equity",
    "warrant",
    "unit",
    "preferred",
    "debt",
    "right",
    "fund",
    "unknown",
)

#: Ordered: first match wins. "Units" are usually "one share plus one warrant",
#: so unit must be tested before common; depositary shares represent preferred.
#:
#: ⚠ ``fund`` is LAST on purpose. ETFs and closed-end funds name the PRODUCT
#: where an operating company names the security class — "Invesco DB Gold
#: Fund", "The Cannabis ETF", "Closed End Fund" — so they carry none of the
#: words the classes above look for and fell through to ``unknown`` on the
#: first pass (111 of 2023's delisting filings, nearly all of them funds).
#: Testing it last means a description that DOES state a security class
#: ("Common Stock of XYZ Realty Trust") is classified on the class, and only a
#: bare product name falls through to ``fund``.
#:
#: They matter as their own class rather than as noise: #2289 established that
#: the validated universe is US stocks EX-ETF (6,733 of 7,288, because
#: ``us_equity`` is an exchange class that mixes in 555 ETFs). A fund closing
#: is not a company delisting and must not enter a common-equity cohort.
#:
#: WHY CLASSIFY FREE TEXT AT ALL — the structured source was checked first.
#: There is none. A Form 25's SGML header carries only submission type,
#: conformed name, SIC and file number; the ``notificationOfRemoval`` schema
#: (X0203) exposes the security solely as free-text
#: ``<descriptionClassSecurity>``. So this is the only available source, not a
#: heuristic preferred over a documented rule.
#:
#: AND WHY THE NON-COMMON CLASSES ARE EXCLUDED RATHER THAN JUDGED. Grounded in
#: our own vocabulary, not in taste: ``etoro_instrument_types`` is
#: {1 Forex, 2 Commodity, 3 CFD, 4 Indices, 5 Stocks, 6 ETF, 7 Bonds,
#: 8 TrustFunds, 9 Options, 10 Crypto}, and dev's ``us_equity`` exchange class
#: is 6,740 type-5 + 555 type-6 — exactly #2289's model. eToro has NO warrant,
#: unit, preferred or right type at all. Those securities can therefore never
#: be instruments in our universe, so their delistings cannot contribute
#: survivorship bias to a backtest run on it.
_SECURITY_PATTERNS: tuple[tuple[str, str], ...] = (
    ("warrant", r"warrant"),
    ("unit", r"\bunits?\b"),
    ("preferred", r"preferred|depositary|depository"),
    ("debt", r"\bnotes?\b|debenture|\bbonds?\b"),
    ("right", r"\brights?\b"),
    # ⚠ LEFT word boundaries are load-bearing on every alternative. `shares?\b`
    # without a leading `\b` matches the embedded "Shares" in fund BRAND names
    # — `ConvexityShares`, `iShares`, `WisdomTree ... Shares` — so the fund
    # falls into common_equity before the `fund` rule below ever runs. That is
    # not hypothetical: it put `SPKY` (ConvexityShares 1x SPIKES Futures ETF)
    # into the first draft of the committed cohort, i.e. an ETF inside the
    # ex-ETF cohort, which defeats the filter's entire purpose.
    ("common_equity", r"\bcommon\b|\bordinary\b|class\s+[a-z]\b|\bshares?\b|\bstock\b"),
    ("fund", r"\betfs?\b|\bfunds?\b|closed[- ]end|\btrust\b|\bportfolios?\b"),
)

_INDEX_LINE = re.compile(
    r"^(?P<form>\S+)\s+(?P<company>.*?)\s{2,}(?P<cik>\d+)\s+"
    r"(?P<filed>\d{4}-\d{2}-\d{2})\s+(?P<path>\S+)\s*$"
)

_PROVISION_PARAGRAPH = re.compile(r"12d2-2\s*(\((?:[a-z])\)(?:\(\d+\))?)", re.I)

#: Cover-page inline XBRL, per sec-edgar.md §2.6 trap 4.
TRADING_SYMBOL_RE = re.compile(r'name=["\']dei:TradingSymbol["\'][^>]*>([^<]{1,20})<', re.I)

#: Forms that carry a cover page with dei:TradingSymbol, newest-first priority.
COVER_PAGE_FORMS = ("10-K", "10-Q", "8-K", "20-F", "40-F")

#: A US ticker: 1-6 letters, optionally a class suffix after a dot or hyphen.
_TICKER_RE = re.compile(r"^[A-Z]{1,6}(?:[.\-][A-Z]{1,3})?")


def clean_trading_symbol(raw: str | None) -> str | None:
    """Tagged ``dei:TradingSymbol`` value → the ticker alone.

    ⚠ The footnote marker is INSIDE the tag, put there by the filer. Invacare's
    2023-02-03 8-K (accession 0000742112-23-000014) tags
    ``<ix:nonNumeric name="dei:TradingSymbol">IVC*</ix:nonNumeric>`` — verified
    against the live document, not inferred. NantHealth does the same with
    ``NHIQ*``. Both are companies that delisted in 2023, so the asterisk points
    at a cover-page footnote about the suspension.

    Storing ``IVC*`` would silently fail every downstream symbol join and,
    worse, would make the vendor acceptance test score a miss against a vendor
    that served the name correctly.
    """
    if raw is None:
        return None
    match = _TICKER_RE.match(raw.strip().upper())
    return match.group(0) if match else None


@dataclass(frozen=True)
class Form25IndexRow:
    """One line of EDGAR's quarterly ``form.idx``.

    ``accession_number`` is the de-duplication key: the same filing appears
    under the exchange CIK and the issuer CIK, and ``cik`` here is whichever of
    the two this row happened to be indexed under. It is NOT reliably the
    issuer — read that from the filing.
    """

    form: str
    company: str
    cik: str
    filed_date: date
    path: str

    @property
    def accession_number(self) -> str:
        return self.path.rsplit("/", 1)[-1].removesuffix(".txt")


@dataclass(frozen=True)
class Form25Filing:
    """One parsed filing. ``rule_provision`` is None for issuer-filed (c)."""

    accession_number: str
    form: str
    filed_date: date
    exchange_cik: str | None
    exchange_name: str | None
    issuer_cik: str | None
    issuer_name: str | None
    file_number: str | None
    description_class_security: str | None
    rule_provision: str | None
    signature_date: date | None
    #: Last tradable day, where the filing states one. See ``suspension_date``
    #: in ``parse_suspension_date`` for how often that actually happens.
    suspension_date: date | None

    @property
    def provision_class(self) -> str:
        return classify_provision(self.form, self.rule_provision)

    @property
    def security_class(self) -> str:
        return classify_security(self.description_class_security)


def parse_index_line(line: str) -> Form25IndexRow | None:
    """Parse one ``form.idx`` line, or None if it is not a Form 25 row.

    ``form.idx`` is fixed-width with a space-padded company name, so the
    company column is matched non-greedily up to a two-space run rather than by
    byte offset — offsets have drifted between years.
    """
    match = _INDEX_LINE.match(line)
    if match is None:
        return None
    form = match.group("form")
    if form not in FORM25_TYPES:
        return None
    return Form25IndexRow(
        form=form,
        company=match.group("company").strip(),
        cik=match.group("cik"),
        filed_date=date.fromisoformat(match.group("filed")),
        path=match.group("path"),
    )


def normalise_provision(raw: str | None) -> str | None:
    """``'17 CFR 240.12d2-2(a)(3)'`` → ``'(a)(3)'``.

    The paragraph is what the rule turns on; the citation prefix varies in
    whitespace and in whether the ``17 CFR`` is present.
    """
    if not raw:
        return None
    match = _PROVISION_PARAGRAPH.search(raw)
    return match.group(1).lower() if match else None


def classify_provision(form: str, provision: str | None) -> str:
    """Delisting, debt lifecycle, or genuinely unknown.

    ⚠ Branches on FORM first. An issuer-filed Form 25 is paragraph (c) —
    voluntary withdrawal, a real delisting — and carries no ``<ruleProvision>``
    at all. Treating a missing provision as unparseable would drop 128 of
    2023's filings, all of them delistings.
    """
    if provision in DELISTING_PROVISIONS:
        return "equity_delisting"
    if provision in DEBT_LIFECYCLE_PROVISIONS:
        return "debt_lifecycle"
    if provision is None and form in _ISSUER_FILED_FORMS:
        return "equity_delisting"
    return "unknown"


def classify_security(description: str | None) -> str:
    """``<descriptionClassSecurity>`` → one of ``SECURITY_CLASSES``.

    ⚠ ``unknown`` is a REAL and unavoidable state, not a parse failure. An
    issuer-filed Form 25 (paragraph (c)) carries no
    ``<descriptionClassSecurity>`` at all — it is a different document shape —
    so all 128 of 2023's are ``unknown``. That is a stated limit of the cohort,
    not something to guess away by assuming common stock: assuming would add
    128 unverified names to a register whose whole purpose is being trustworthy
    about what delisted.
    """
    if not description:
        return "unknown"
    text = description.lower()
    for security_class, pattern in _SECURITY_PATTERNS:
        if re.search(pattern, text):
            return security_class
    return "unknown"


def normalise_cik(raw: str | None) -> str | None:
    """CIK as it appears in the filing → 10-digit zero-padded.

    ⚠ SEC's own Form 25 XML does NOT reliably zero-pad. Medtronic's issuer CIK
    in accession 0000876661-23-000234 is ``000064670`` — NINE digits — where
    every CIK column in this schema is ten. Storing it verbatim would create a
    second spelling of CIK 64670 that joins to nothing, which is the identity
    failure the CUSIP/CIK work already guards against elsewhere.

    Padding is the right fix rather than relaxing the CHECK: a CIK is a number,
    the zero-padding is presentational, and the constraint is what surfaced
    this in the first place.
    """
    if raw is None:
        return None
    digits = raw.strip()
    if not digits.isdigit():
        return None
    significant = digits.lstrip("0")
    # An all-zero value is not CIK 0, it is a filer that tagged nothing. The
    # earlier `... .zfill(10) or None` could never return None, because
    # "".zfill(10) is "0000000000" and truthy — dead code masquerading as a
    # guard.
    if not significant:
        return None
    return significant.zfill(10)


def _text(xml: str, tag: str) -> str | None:
    match = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.S | re.I)
    if match is None:
        return None
    value = match.group(1).strip()
    return value or None


def _block(xml: str, tag: str) -> str | None:
    match = re.search(rf"<{tag}>(.*?)</{tag}>", xml, re.S | re.I)
    return match.group(1) if match else None


_SUSPENSION_RE = re.compile(
    r"suspended\s+from\s+trading\s+(?:on|at\s+the\s+opening\s+of\s+business\s+on)\s+"
    r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})",
    re.I,
)

_MONTHS = {
    m: i
    for i, m in enumerate(
        (
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ),
        start=1,
    )
}


def parse_suspension_date(document_text: str) -> date | None:
    """The last tradable day, if the filing actually states one.

    ⚠ sec-edgar.md §2.6 trap 5 is right that a Form 25 can carry three distinct
    dates — but the sentence that distinguishes them lives in the EX-99
    rule-provision exhibit, and most exchange filings attach a stub exhibit
    instead. ``research_price_series.delisting_date`` is CHECK-paired to its
    source precisely so an absent suspension date stays absent rather than
    being back-filled with the filing date, which is a different event and
    would mistruncate every series it touched.
    """
    match = _SUSPENSION_RE.search(document_text)
    if match is None:
        return None
    month_name, day, year = re.split(r"[\s,]+", match.group(1).strip())
    month = _MONTHS.get(month_name.lower())
    if month is None:
        return None
    try:
        return date(int(year), month, int(day))
    except ValueError:
        return None


def parse_submission(
    row: Form25IndexRow,
    submission_text: str,
) -> Form25Filing:
    """Parse the complete ``{accession}.txt`` submission.

    One fetch rather than two: the complete submission carries
    ``primary_doc.xml`` AND every exhibit, so the suspension-date scan costs
    nothing extra. At 1,282 filings against a 10 req/s shared budget that is
    the difference between ~2 and ~4.5 minutes.

    ⚠ The issuer CIK comes from the XML for exchange filings and from the INDEX
    ROW for issuer-filed ones. An issuer-filed Form 25 has no
    ``notificationOfRemoval`` schema and therefore no ``<issuer>`` block at all,
    so reading only the XML leaves all 128 of 2023's with a NULL issuer — an
    entire delisting category that cannot be joined to anything. For those
    forms the FILER IS the issuer, so the index CIK is the right answer rather
    than a guess. The same substitution would be WRONG for a 25-NSE, where the
    index CIK is as often the exchange as the issuer.
    """
    issuer_block = _block(submission_text, "issuer")
    exchange_block = _block(submission_text, "exchange")
    signature_date_raw = _text(submission_text, "signatureDate")

    return Form25Filing(
        accession_number=row.accession_number,
        form=row.form,
        filed_date=row.filed_date,
        exchange_cik=normalise_cik(_text(exchange_block, "cik")) if exchange_block else None,
        exchange_name=_text(exchange_block, "entityName") if exchange_block else None,
        issuer_cik=(
            normalise_cik(_text(issuer_block, "cik"))
            if issuer_block
            else (normalise_cik(row.cik) if row.form in _ISSUER_FILED_FORMS else None)
        ),
        issuer_name=(
            _text(issuer_block, "entityName")
            if issuer_block
            else (row.company if row.form in _ISSUER_FILED_FORMS else None)
        ),
        file_number=_text(issuer_block, "fileNumber") if issuer_block else None,
        description_class_security=_text(submission_text, "descriptionClassSecurity"),
        rule_provision=normalise_provision(_text(submission_text, "ruleProvision")),
        signature_date=(
            date.fromisoformat(signature_date_raw)
            if signature_date_raw and re.fullmatch(r"\d{4}-\d{2}-\d{2}", signature_date_raw)
            else None
        ),
        suspension_date=parse_suspension_date(submission_text),
    )
