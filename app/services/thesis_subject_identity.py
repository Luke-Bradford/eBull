"""Thesis subject identity — does a memo name the company it is about? (#2431)

Extracted from ``app.services.thesis`` by #2436 so the rule can carry a
``RULE_SET_VERSION`` that means something. The repo's established form is
"rule-set id + code hash, not an int" (``app/services/price_quarantine.py:47``):
an integer cannot tell you whether two stored verdicts came from the same code,
a source hash can. Hashing ``thesis.py`` would have been useless — that module
changes for prompt edits, provider plumbing and schema work, and every such edit
would have marked the whole stored corpus stale for no reason. Hashing THIS file
changes the version when, and only when, the rule changes.

The gate itself is #2431's and is unmodified by the extraction. #2436 adds the
STORED side: ``theses.subject_identity_ok`` records the verdict a row was given,
``theses.subject_identity_rule_version`` records which rule gave it, and the
consumers refuse to read a valuation band from a row whose verdict is not TRUE.

⚠ NULL is *not yet checked*, which is not *passed*. Every predicate here is
therefore ``is True``, never ``is not False``.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Final

# ---------------------------------------------------------------------------
# Rule-set version
# ---------------------------------------------------------------------------
RULE_SET_ID: Final = "thesis-subject-identity-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


RULE_SET_VERSION: Final = f"{RULE_SET_ID}+{_code_hash()}"

#: The machine-stable reason a consumer reports when it refuses a thesis. One
#: string, exported once, so the portfolio rationale, the scoring note and the
#: alerts feed all say the same word (``.claude/CLAUDE.md``: single source of
#: truth for constants).
QUARANTINE_REASON: Final = "thesis_quarantined"


#: Rows whose stored rule version is not the current one still need deciding.
#: ``IS DISTINCT FROM`` covers the NULL case (never checked) in the same test.
_STALE_VERDICT_SQL = """
    SELECT t.thesis_id, i.symbol, i.company_name, t.memo_markdown,
           t.subject_identity_ok
    FROM theses t
    JOIN instruments i USING (instrument_id)
    WHERE t.subject_identity_rule_version IS DISTINCT FROM %(ver)s
"""

#: ⚠⚠ ``::boolean`` AT EVERY OCCURRENCE OF ``%(ok)s`` (#2647). psycopg3 dedups
#: the repeated named parameter into one ``$n`` and sends a ``None`` untyped
#: (OID 0); ``CASE WHEN $n IS NULL`` is a NullTest and constrains no type, so
#: the statement cannot be planned and Postgres raises ``AmbiguousParameter``.
#: The assignment ``SET subject_identity_ok = $n`` does NOT rescue it — measured,
#: the UPDATE shape raises exactly as the INSERT one did.
#:
#: ⚠ ``ok=None`` is the boot probe's reset direction — a row whose instrument
#: stopped carrying a checkable name — so untyped, this raised inside the
#: LIFESPAN self-heal, i.e. at application start rather than in a job.
_WRITE_VERDICT_SQL = """
    UPDATE theses
    SET subject_identity_ok           = %(ok)s::boolean,
        subject_identity_rule_version = CASE WHEN %(ok)s::boolean IS NULL THEN NULL ELSE %(ver)s END,
        subject_identity_checked_at   = CASE WHEN %(ok)s::boolean IS NULL THEN NULL ELSE now() END
    WHERE thesis_id = %(id)s
"""


def ensure_subject_identity_verdicts(conn: Any) -> int:
    """Give every stored thesis a verdict under the CURRENT rule. Returns rows written.

    ⚠⚠ THIS RUNS AT LIFESPAN, and that is not tidiness — it is what makes the
    migration safe to deploy. ``sql/332`` can only add NULL columns, and every
    consumer this ticket touched treats NULL as quarantined. So on any database
    that already holds theses, shipping the schema and the code without this
    would silently strip the ENTIRE historical corpus out of portfolio
    decisions, scoring, take-profit and reporting until somebody noticed and
    ran a script by hand. Caught by Codex checkpoint 2; the documented
    "run the backfill after migrating" was not a control, it was a hope.

    Same posture and shape as the ``ensure_*_singleton`` probes beside it in
    ``app/main.py`` — self-healing state that a restore-from-snapshot or a
    rule change would otherwise leave wrong with no programmatic recovery.

    ⚠ Idempotence is VERSION-equivalence, not verdict-equivalence (corrected
    #2647; the prior wording claimed the latter). ``_STALE_VERDICT_SQL`` selects
    on ``subject_identity_rule_version IS DISTINCT FROM`` and the only skip
    below is the both-NULL case, so a steady-state boot writes zero rows — but a
    version bump rewrites every row whose verdict is not NULL, including the
    ones whose verdict is unchanged, disturbing their
    ``subject_identity_checked_at``. That is correct, because the row must
    record WHICH rule decided it and a stale version would be a lie; it is
    called out because the cost is not what the old wording implied.

    ⚠⚠ The hash covers the FILE, so ANY edit to this module — a comment, this
    docstring — bumps the version and triggers that corpus-wide rewrite. #2647
    could not avoid it: the defect being fixed was in ``_WRITE_VERDICT_SQL``
    itself. Price the rewrite before editing here for cosmetic reasons.

    ⚠ NO FALSE-POSITIVE GATE HERE, deliberately, unlike
    ``scripts/backfill_thesis_subject_identity.py``. Refusing to write at boot
    would leave the corpus NULL, which the consumers read as quarantined —
    strictly worse than the verdicts it declined to store. The audit belongs to
    the script, which a human runs and can act on.
    """
    rows = conn.execute(_STALE_VERDICT_SQL, {"ver": RULE_SET_VERSION}).fetchall()
    written = 0
    for thesis_id, symbol, company_name, memo, stored_ok in rows:
        subject = {"symbol": symbol, "company_name": company_name}
        ok = memo_names_subject(str(memo), subject) if subject_is_checkable(subject) else None
        if ok is None and stored_ok is None:
            # Already NULL and still unverdictable — nothing to write. Without
            # this the row is re-read every boot forever (its rule version is
            # NULL by the CHECK, so it always matches _STALE_VERDICT_SQL).
            continue
        conn.execute(_WRITE_VERDICT_SQL, {"id": thesis_id, "ok": ok, "ver": RULE_SET_VERSION})
        written += 1
    return written


def subject_is_checkable(subject: Any) -> bool:
    """Is there enough of a subject for a verdict to mean anything?

    ⚠ An EMPTY dict is not. ``memo_names_subject`` scores ``{}`` as False, and
    storing that would read as "checked and failed" when nothing was checked —
    the same NULL-vs-False confusion ``is_thesis_usable`` exists to keep out of
    the read path.
    """
    if not isinstance(subject, dict):
        return False
    return bool(str(subject.get("symbol") or "").strip() or str(subject.get("company_name") or "").strip())


def is_thesis_usable(row: Any) -> bool:
    """May a consumer read this thesis row's stance, confidence or bands?

    ``row`` is any mapping selected from ``theses`` that includes
    ``subject_identity_ok``; ``None`` (no thesis at all) answers False.

    ⚠⚠ FAIL-CLOSED ON NULL, and the distinction is the whole point. A row
    written before #2436's migration, or inserted with no subject to check
    against, has verdict NULL — *nobody has decided*. Reading a valuation band
    on that basis is exactly the risk this ticket exists to remove:
    ``portfolio.py`` turns ``base_value`` into an EXIT trigger on a real
    position, and 14 such exits had already fired on wrong-company bands when
    this was written. An instrument losing its thesis inputs is a state the
    system already handles; a fabricated exit is not.
    """
    if row is None:
        return False
    return row.get("subject_identity_ok") is True


#: Trailing legal forms stripped before matching a company name in a memo. A
#: correct memo says "Open Text" or "OpenText", never "Open Text Corp".
_CORPORATE_SUFFIXES: Final = (
    "corporation",
    "incorporated",
    "holdings",
    "holding",
    "company",
    "limited",
    "group",
    "corp",
    "inc",
    "ltd",
    "plc",
    "nv",
    "sa",
    "ag",
    "co",
)


def _squash(text: str) -> str:
    """Lowercase, alphanumerics only — so "OpenText" matches "Open Text Corp"."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def memo_names_subject(memo: str, instrument: object) -> bool:
    """Does the memo name the company it is supposed to be about? (#2431)

    ⚠⚠ THE GATE THAT #2235 WANTED AND COULD NOT FIND. That ticket closed the
    PLACEHOLDER half deterministically and left misattribution to prose,
    recording that it lacked "a clean discriminator". This is one, and it is
    verified on the FULL stored corpus rather than argued:

    ```text
    ver      n   REJECTS   reject-rate
     v1     25         0       0.0%
     v2     36         0       0.0%
     v3     71         0       0.0%
     v4    355         0       0.0%      <- 487 known-good memos, ZERO rejected
     v5   2038      1378      67.6%
     v6    127       127     100.0%
    ```

    Reproduce with ``scripts/verify_2431_subject_identity.py``. The load-bearing
    number is the FIRST FOUR ROWS, not the last two: a narrowing gate is only
    safe if it rejects nothing correct, and v1-v4 are the populations where the
    writer was demonstrably naming the right company.

    ⚠ Three spellings are accepted, because a correct memo uses whichever reads
    best and all three are the subject:

      1. the SYMBOL on a word boundary — "OTEX";
      2. the full company name minus its legal suffix, squashed — so
         "OpenText Corporation" matches ``Open Text Corp``;
      3. the LEADING TOKEN when it is >= 5 characters — "Axalta" for
         ``Axalta Coating Systems Ltd``, "Costco" for ``Costco Wholesale
         Corp``. ⚠ Five, not four, and the bound is measured: those two were
         the ONLY false positives across 487 known-good memos before this
         clause, and a 4-character bound would admit generic leads like "Open"
         (``Open Text Corp``), where spelling 2 is the discriminator instead.

    ⚠ Presence, NOT exclusivity. A memo may name peers and the benchmark — the
    system prompt allows exactly that ("versus <peer>"). This asks only whether
    the subject appears at all, which is the failure actually observed: memos
    about Microsoft, Tesla and Apple that never mention their own instrument.
    """
    if not isinstance(instrument, dict):
        return True  # nothing to check against; the schema gates elsewhere

    symbol = str(instrument.get("symbol") or "").strip()
    if symbol:
        # ⚠⚠ CASE-SENSITIVE, and for a SHORT symbol also position-sensitive.
        # 2,186 of 12,696 symbols in the universe are three characters or fewer
        # (17.2%) and many are ordinary words — ON, IT, ALL, KEY, CAR. A
        # case-insensitive bare match would let "Apple is executing on AI" pass
        # as an ON Semiconductor memo, which is precisely the misattribution
        # this gate exists to catch (Codex checkpoint 2). Tickers are written
        # in caps, so requiring caps costs a correct memo nothing; a short one
        # additionally has to appear where a ticker appears — "(ON)",
        # "NASDAQ: ON" — rather than anywhere in the prose.
        if len(symbol) >= 4 and re.search(rf"\b{re.escape(symbol)}\b", memo):
            return True

    raw_name = str(instrument.get("company_name") or "").strip()
    # ⚠⚠ A SUFFIX IS A TOKEN, NOT A TRAILING SUBSTRING (#2434 review). Matching
    # the substring mangles 1,820 of 12,696 names in the universe — "Tesco"
    # loses its "co" and becomes "tes", "Citigroup" becomes "citi" — and a name
    # cut below four characters can never match anything. Splitting on
    # non-alphanumerics and popping whole trailing tokens keeps "Tesco" intact
    # while still reducing "Axalta Coating Systems Ltd" and "Avis Budget Group
    # Inc" to the part a memo actually writes.
    tokens = [token for token in re.split(r"[^A-Za-z0-9]+", raw_name) if token]
    while len(tokens) > 1 and _squash(tokens[-1]) in _CORPORATE_SUFFIXES:
        tokens.pop()
    squashed = _squash("".join(tokens))

    if len(squashed) < 4:
        # ⚠ 97 instruments have BOTH a short symbol and a name too short to
        # carry the check — 3M (MMM), Gap (GAP), NOV, AES, FMC. For those the
        # name offers nothing, so the symbol is allowed to match as a standalone
        # uppercase word rather than only inside ticker punctuation. The
        # relaxation is scoped to the cases with no alternative: ON
        # Semiconductor and Gartner (IT) keep the strict rule, because their
        # names are long enough to do the work.
        # ⚠ ONLY the short symbols reach the regex here: a symbol of 4+ characters
        # already ran this exact search above and failed it, so repeating it is
        # dead work (review round 2).
        if symbol and len(symbol) < 4 and re.search(rf"\b{re.escape(symbol)}\b", memo):
            return True
        # ⚠ CASE-SENSITIVE for a short name, which is what separates the company
        # "Gap" from the English word "gap". A memo writing "Gap Inc. comped
        # positive" names its subject; one writing "the valuation gap widened"
        # does not, and a case-insensitive match could not tell them apart.
        short = " ".join(tokens)
        return bool(short) and re.search(rf"\b{re.escape(short)}\b", memo) is not None

    if symbol and len(symbol) < 4 and re.search(rf"(?:[(:]\s*{re.escape(symbol)}\b|\b{re.escape(symbol)}\s*\))", memo):
        return True

    # ⚠⚠ A PREFIX OF THE NAME, not its leading TOKEN. A correct memo shortens a
    # long name — "Axalta" for ``Axalta Coating Systems Ltd``, "Costco" for
    # ``Costco Wholesale Corp`` — and those two were the only false positives
    # across 487 known-good memos when the full name was required. But matching
    # the leading token alone is far too loose: 3,360 companies (26.5%) share a
    # >= 5-character leading token with a DIFFERENT issuer, so ``Apple
    # Hospitality REIT Inc`` would accept a memo titled "Apple Inc. (AAPL)"
    # (Codex checkpoint 2). A six-character prefix of the squashed name splits
    # them exactly: "axalta" and "costco" still match, while Apple Hospitality
    # needs "appleh", which an Apple Inc memo does not contain.
    return squashed[:6] in _squash(memo)
