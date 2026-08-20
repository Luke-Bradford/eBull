"""#2721 — what a held position realises when its price series terminates.

The survivorship treatment ITSELF: a backtest whose universe contains dead
names must say what happens to a position whose series simply stops. Stamping
``survivorship_free`` while this is undefined would claim handling that does
not exist, which is why the ``universe_basis_not_survivorship_free`` refusal
(#2698) cannot come down without this module.

⚠⚠ SHIPS UNWIRED, ON PURPOSE. No execution path calls this module in #2721
steps 1-2, and ``TERMINATION_RULE_VERSION`` is NOT hashed into any strategy
identity yet. The hash joins the result-producing rule set at the SAME commit
that first wires it (step 3, the ``BACKTEST_UNIVERSE`` parameterisation) —
"inert because nothing calls it" is a stronger invariant than "inert because
the current universe happens to contain no terminating series" (ckpt-1).

Source rule (verified against the papers, 2026-08-15, on-issue evidence pass):

* Shumway, *The Delisting Bias in CRSP Data*, Journal of Finance 52(1) 1997,
  Table V p.336 — where CRSP is missing the delisting return for a
  performance-related delist (NYSE/AMEX), the traced outcomes average **−30%**
  (mean −29.9, median −31.3, 71% of value accounted; the worthless subset is
  −100%).
* Shumway & Warther, *The Delisting Bias in CRSP's Nasdaq Data and Its
  Implications for the Size Effect*, Journal of Finance 54(6) 1999 — the
  corresponding Nasdaq prescription is **−55%**.

The corpus does not record the venue of a dead name, so the ADVERSE anchor of
the two (−55%) is used for every failure-class termination — the same
pessimistic-end construction as ``cost_model.UNKNOWN_NOMINAL_PRICE_BAND``
(absence of evidence prices at the bad end of the measured range, never the
good end).

THE THREE-DATE TRAP (ckpt-1 amendment a). A Form 25 carries up to three dates
— filing, suspension, removal-effective (sec-edgar.md §2.6 trap 5) — and this
module deliberately keys on NONE of them. Termination fires at the series'
**last bar**, and every realisation below is a fraction of the **last close**.
The filing/suspension dates are linkage evidence (which class of termination
this was), never a substitute clock: the last bar's relation to those dates
varies by class ((b) filings trail the OTC death by months; (a)(3) filings
often precede a continuing series) and substituting any of them for the last
bar mistruncates or misprices the position.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

from app.services.strategy_result import AmbiguityArm

#: Shumway & Warther 1999's Nasdaq prescription, as a FRACTION LOST. Applied
#: to the last close: realised = last_close * (1 - SHUMWAY_HAIRCUT). The
#: NYSE/AMEX anchor is −30% (Shumway 1997 Table V); venue is unknown for our
#: dead names, so the adverse anchor binds. Changing this constant is a NEW
#: ``TERMINATION_RULE_VERSION`` by construction (it is hashed below).
SHUMWAY_HAIRCUT: Final[float] = 0.55

_RULE_SET_ID: Final[str] = "series-termination-v1"


def _code_hash() -> str:
    """Hash this module's source, per ``indicator_series._code_hash``'s idiom."""
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


#: ⚠ NOT yet part of any strategy identity — see the module docstring. Once
#: step 3 wires termination into the backtest, this joins the result-producing
#: rule set and every change to this file moves every strategy version.
TERMINATION_RULE_VERSION: Final[str] = f"{_RULE_SET_ID}+{_code_hash()}"


class TerminationClass(StrEnum):
    """The closed set of termination treatments. One per evidence shape.

    Deliberately more classes than treatments: ``EXCHANGE_FAILURE_A4`` and
    ``EXCHANGE_FAILURE`` realise identically today, but "(a)(4) ≈ (b)" is
    ASSERTED, not demonstrated (ckpt-1 amendment c) — the expanded register
    is what verifies it, and collapsing the labels now would make the
    verification unmeasurable later.
    """

    #: Linked Form 25, Rule 12d2-2(b) — exchange-initiated delisting for
    #: non-compliance. The archetypal performance delist: haircut applies.
    EXCHANGE_FAILURE = "exchange_failure"
    #: Linked Form 25, Rule 12d2-2(a)(4). Treated as the failure class, kept
    #: distinct so the expanded register can verify the equivalence.
    EXCHANGE_FAILURE_A4 = "exchange_failure_a4"
    #: Linked Form 25, Rule 12d2-2(a)(3) — the security now evidences other
    #: securities by operation of law (merger, holdco reorg, redomicile).
    #: The position converts at the last print; realise at last close.
    OPERATION_OF_LAW = "operation_of_law"
    #: Linked Form 25 whose <ruleProvision> could not be parsed (the 25-NSE
    #: form omits it by design). Linked-but-unreadable is NOT unknown — the
    #: security was removed — but its failure-vs-conversion split is, so it
    #: takes the two-armed bounds (ckpt-1 amendment d).
    LINKED_UNPARSED = "linked_unparsed_provision"
    #: No Form 25 link, but the vendor symbol carries the ``Q`` bankruptcy
    #: suffix — plausibly a post-petition OTC series that traded to its own
    #: end, so the last close already embeds the collapse. Sound ONLY as a
    #: labelled class, unverified per-name (ckpt-1 amendment f): realise at
    #: last close, and the label is what keeps that assumption auditable.
    Q_SUFFIX_OTC = "q_suffix_otc_unverified"
    #: No usable evidence. Bankruptcy (−100%), acquisition (often a premium),
    #: ticker change and archive gap all look identical — opposite signs for
    #: a held position — so the honest answer is BOTH bounds, reusing the
    #: §3.4 two-armed ambiguity machinery. Honest wide bounds beat a silent
    #: guess.
    UNKNOWN = "unknown_termination"


#: The classes whose realisation depends on the ambiguity arm. Everything
#: else realises the same value under either arm.
TWO_ARMED_CLASSES: Final[frozenset[TerminationClass]] = frozenset(
    {TerminationClass.LINKED_UNPARSED, TerminationClass.UNKNOWN}
)


class UnlinkedStratum(StrEnum):
    """Why an UNKNOWN series has no Form 25 link — absence of linkage is not
    random (ckpt-1 amendment e), and the census that stamps a result must
    report this split, not one pooled "unlinked" count.
    """

    #: The register holds no Form 25 for any candidate spelling of the symbol
    #: — genuinely outside the register's span/venue, or never delisted.
    NO_FORM25 = "no_form25"
    #: A Form 25 exists but its issuer never resolved to a ticker
    #: (closed-end funds file N-CSR, not a cover-page-XBRL 10-K; some
    #: foreign private issuers likewise).
    UNRESOLVED_TICKER = "unresolved_ticker"
    #: Issuer-filed paragraph (c) filings are EXCLUDED from the common-equity
    #: cohort by design (no descriptionClassSecurity to verify the security
    #: class). A series whose only Form 25 is a (c) is unlinked BY THAT
    #: EXCLUSION, and broadening it needs its own labelled stratum, not a
    #: silent widening (ckpt-1 amendment b).
    EXCLUDED_ISSUER_FILED_C = "excluded_issuer_filed_c"
    #: Two filing symbols resolved onto one archive series (RVLP + RVLPQ) —
    #: two delisting events against one price history. The linkage refuses
    #: to tie-break; the series stays unlinked and this stratum says why.
    SYMBOL_COLLISION = "symbol_collision"
    #: A Form 25 exists on the symbol but the series STARTS after it — a
    #: later occupant of the ticker or an unverified relisting. The linkage's
    #: identity gate refused the write (classify_form25_match).
    IDENTITY_UNVERIFIED_REUSE = "identity_unverified_reuse"


@dataclass(frozen=True)
class TerminationEvidence:
    """One terminating series' evidence, as the corpus stores it.

    ``linked`` means ``delisting_source = 'sec_form25'`` on the series row;
    ``provision`` is ``delisting_provision`` (may be NULL on a linked row —
    sql/353); ``q_suffix`` is a property of the VENDOR SYMBOL, not of any
    filing, and the caller must derive it with the SAME rule the linkage's
    candidate ladder strips by (``research_corpus_ingest.
    archive_symbol_candidates``: trailing ``Q`` with more than one letter) —
    a second spelling of that rule here would drift from the first.
    """

    linked: bool
    provision: str | None
    q_suffix: bool


def classify_termination(evidence: TerminationEvidence) -> TerminationClass:
    """Map stored evidence to its termination class. Pure; total.

    Precedence: a Form 25 link outranks the Q-suffix heuristic — the filing
    is a regulator's statement about this security, the suffix a naming
    convention about the symbol. The suffix only speaks where nothing
    stronger does.
    """
    if evidence.linked:
        if evidence.provision == "(b)":
            return TerminationClass.EXCHANGE_FAILURE
        if evidence.provision == "(a)(4)":
            return TerminationClass.EXCHANGE_FAILURE_A4
        if evidence.provision == "(a)(3)":
            return TerminationClass.OPERATION_OF_LAW
        return TerminationClass.LINKED_UNPARSED
    if evidence.q_suffix:
        return TerminationClass.Q_SUFFIX_OTC
    return TerminationClass.UNKNOWN


def terminal_value_fraction(
    termination_class: TerminationClass,
    arm: AmbiguityArm | None,
) -> float:
    """Fraction of the LAST CLOSE a held position realises at termination.

    ``arm`` is required for the two-armed classes and ignored — deliberately,
    so a caller can pass its running arm unconditionally — for the rest.

    Raises on a two-armed class with no arm: silently picking a side there
    would be exactly the unstated survivorship treatment this module exists
    to abolish.
    """
    if termination_class in (
        TerminationClass.EXCHANGE_FAILURE,
        TerminationClass.EXCHANGE_FAILURE_A4,
    ):
        return 1.0 - SHUMWAY_HAIRCUT
    if termination_class in (
        TerminationClass.OPERATION_OF_LAW,
        TerminationClass.Q_SUFFIX_OTC,
    ):
        return 1.0
    # Two-armed: best case the last print was the story (conversion, quiet
    # exit); worst case the name failed and the Shumway haircut binds.
    if arm is None:
        raise ValueError(f"{termination_class} requires an ambiguity arm; refusing to pick a side silently")
    return 1.0 if arm == "best_case" else 1.0 - SHUMWAY_HAIRCUT


__all__ = [
    "SHUMWAY_HAIRCUT",
    "TERMINATION_RULE_VERSION",
    "TWO_ARMED_CLASSES",
    "TerminationClass",
    "TerminationEvidence",
    "UnlinkedStratum",
    "classify_termination",
    "terminal_value_fraction",
]
