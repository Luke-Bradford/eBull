"""Revert-probes for #2408 — inject each defect the new tests guard.

Thirteen injections, one per clause, because the tier has that many distinct ways to be
wrong and a single "delete the feature" probe would only prove the first. Each names the
tests that MUST fail AND the tests that MUST still pass: a probe whose whole file goes red
proves much less than one that moves exactly the cases it targets.

Probes A-I are pure logic. Probes J-M inject into the SQL, so they run against the db tier
and need ``docker compose --profile test up -d postgres-test`` first — a pure-logic suite
cannot revert-probe a column choice or a WHERE clause, and saying "not covered" would have
left Codex's ``is_derivative`` finding (probe M) unpinned.

⚠ Gate on exit code 1. pytest exits 4 on a USAGE error and 2 on an internal error, both
of which look like a pass if the check is ``!= 0`` — that reported a false 4/4 CAUGHT on
#2214. Runs with ``-n 0`` for the same reason (this repo's ``addopts`` carries ``-n``).

⚠⚠ **This script holds an exclusive lock on the checkout.** It writes a mutated copy of
``app/services/ownership_rollup.py`` to disk for the duration of each injection, so ANY
process that reads the tree meanwhile picks up a defect: a ``git commit`` captures it, and
a full-population A/B or smoke run binds it at import. Nothing else starts until this has
printed its restored-suite line. Precedent is this ticket — a commit captured probe E's
clause reordering, and the A/B and panel had to be re-run to prove they had not.

    PYTHONPATH=. uv run python scripts/probe_2408_record_holder_rep.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_MODULE = Path("app/services/ownership_rollup.py")
_TESTS = "tests/test_insider_deemed_chain_collapse.py"

_TIER_CALL = "        candidate = _named_record_holder(cluster, record_holder_evidence)\n"
_UNIQUE = "    if len(named) != 1:\n        return None\n"
_VALUE_KEY = (
    "    for key in {(h.winning_accession, h.shares) for h in insiders}:\n        texts.extend(evidence.get(key, ()))\n"
)
_INSIDER_ONLY = "    insiders = [h for h in cluster if h.winning_source in _INSIDER_GROUP_SOURCES]\n"
_RELEASE = "    if _releases_other_rows(incumbent, cluster, rows_by_identity):\n        return incumbent\n"
_CLAUSE_ORDER = (
    "    candidate: Holder | None = None\n"
    "    if len(attested_direct) == 1 and attested_direct[0].winning_accession != incumbent.winning_accession:\n"
    "        candidate = attested_direct[0]\n"
    "    if candidate is None:\n"
)
# Probes H and I replace the WHOLE match block, not just the ``named`` line: an injection
# that anchors the needle while leaving the blob unpadded is a THIRD rule that neither
# spelling implements, and it then fails a bystander for the wrong reason. Observed —
# probe I read NOT CAUGHT until the ``blobs`` line was included in the swap.
_MATCH = (
    "    blobs = [_normalise_holder_text(t) for t in texts]\n"
    "    named = [h for h in insiders if (n := _normalise_holder_text(h.filer_name)) and any(n in b for b in blobs)]\n"
)

_ALL_2408 = (
    "test_record_holder_text_promotes_the_member_it_names",
    "test_text_naming_two_members_fails_closed",
    "test_text_naming_no_member_keeps_the_incumbent",
    "test_evidence_for_a_different_block_value_is_not_consulted",
    "test_table_i_attestation_still_outranks_the_text",
    "test_named_blockholder_row_cannot_become_the_rep",
    "test_text_tier_swap_is_declined_when_the_incumbent_holds_other_channel_rows",
    "test_person_names_are_matched_verbatim_not_rotated",
    "test_an_abbreviated_conformed_name_still_matches_its_own_footnote",
    "test_a_sibling_fund_substring_declines_rather_than_guessing",
    "test_evidence_is_pooled_across_a_clusters_accessions",
    "test_two_accessions_naming_two_members_fails_closed",
)
# The #2385 cases, which must survive every probe that is not about them. The tier is an
# ADDITION to that rule, so a probe that also breaks the Table I route has changed
# something other than what it claims to.
_2385 = (
    "test_rep_is_the_table_i_direct_holder_not_the_highest_cik",
    "test_role_derived_direct_is_not_a_record_holder",
    "test_two_attested_direct_holders_is_not_a_chain",
)

_PROBES = (
    (
        "A: the text tier is never consulted",
        _TIER_CALL,
        "        candidate = None  # PROBE A — record-holder text ignored\n",
        ("test_record_holder_text_promotes_the_member_it_names",),
        _2385 + ("test_text_naming_two_members_fails_closed",),
    ),
    (
        "B: uniqueness dropped — first named member wins",
        _UNIQUE,
        "    if not named:\n        return None\n",
        (
            "test_text_naming_two_members_fails_closed",
            "test_two_accessions_naming_two_members_fails_closed",
        ),
        _2385 + ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "C: a cluster naming NOBODY invents an answer",
        _UNIQUE,
        "    if len(named) != 1:\n        return insiders[-1] if insiders else None\n",
        ("test_text_naming_no_member_keeps_the_incumbent",),
        ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "D: evidence pooled per ACCESSION, ignoring the block value",
        _VALUE_KEY,
        "    for (acc, _shares), vals in evidence.items():\n"
        "        if acc in {h.winning_accession for h in insiders}:\n"
        "            texts.extend(vals)\n",
        ("test_evidence_for_a_different_block_value_is_not_consulted",),
        _2385 + ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "D2: a member must be named by its OWN filing (pooling removed)",
        _VALUE_KEY,
        "    texts.extend(t for h in insiders for t in evidence.get((h.winning_accession, h.shares), ()))\n"
        "    _own = {\n"
        "        id(h): _normalise_holder_text(' '.join(evidence.get((h.winning_accession, h.shares), ())))\n"
        "        for h in insiders\n"
        "    }\n"
        "    insiders = [h for h in insiders if (n := _normalise_holder_text(h.filer_name)) and n in _own[id(h)]]\n",
        ("test_evidence_is_pooled_across_a_clusters_accessions",),
        _2385 + ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "E: the text tier runs BEFORE the Table I attestation",
        _CLAUSE_ORDER,
        "    candidate: Holder | None = _named_record_holder(cluster, record_holder_evidence)\n"
        "    if candidate is None and len(attested_direct) == 1 "
        "and attested_direct[0].winning_accession != incumbent.winning_accession:\n"
        "        candidate = attested_direct[0]\n"
        "    if candidate is None:\n",
        ("test_table_i_attestation_still_outranks_the_text",),
        ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "F: any source may be the named holder",
        _INSIDER_ONLY,
        "    insiders = list(cluster)  # PROBE F — 13D/G rows eligible as rep\n",
        ("test_named_blockholder_row_cannot_become_the_rep",),
        ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "G: the release guard does not gate the text tier",
        _RELEASE,
        "    if False:  # PROBE G — release exposure ignored\n        return incumbent\n",
        ("test_text_tier_swap_is_declined_when_the_incumbent_holds_other_channel_rows",),
        ("test_record_holder_text_promotes_the_member_it_names",),
    ),
    (
        "H: person names rotated to First-Last as well as verbatim",
        _MATCH,
        "    blobs = [_normalise_holder_text(t) for t in texts]\n"
        "    def _forms(v: str) -> list[str]:\n"
        "        p = _normalise_holder_text(v).split()\n"
        "        return [' '.join(p)] + ([' '.join(p[1:] + p[:1])] if 2 <= len(p) <= 4 else [])\n"
        "    named = [h for h in insiders if any(f in b for b in blobs for f in _forms(h.filer_name))]\n",
        # Rotation breaks BOTH, and that is the finding rather than a widened probe: the
        # TACO footnote names "Harry L. You" alongside the Sponsor, so the rotated form
        # makes the deemed owner matchable, the cluster stops naming exactly one member
        # and the promotion the tier exists for stops happening. The first draft listed
        # the second test as a bystander and read NOT CAUGHT for it.
        (
            "test_person_names_are_matched_verbatim_not_rotated",
            "test_record_holder_text_promotes_the_member_it_names",
        ),
        _2385,
    ),
    (
        "I: name match anchored on token boundaries (the reverted spelling)",
        _MATCH,
        '    blobs = [f" {_normalise_holder_text(t)} " for t in texts]\n'
        "    named = [h for h in insiders "
        'if (n := _normalise_holder_text(h.filer_name)) and any(f" {n} " in b for b in blobs)]\n',
        ("test_an_abbreviated_conformed_name_still_matches_its_own_footnote",),
        _2385 + ("test_record_holder_text_promotes_the_member_it_names",),
    ),
)


# --- SQL probes ---------------------------------------------------------------
# The pure suite cannot reach these: a column choice and a WHERE clause are only
# observable against a real table. They run against the db tier, so this script needs
# ``docker compose --profile test up -d postgres-test`` first.
_DB_TESTS = "tests/test_ownership_rollup_record_holder_db.py"
# Both row queries carry the D/I filter, so the anchor includes the line AFTER it to stay
# unique — the abort fired on a 2-count first, which is the guard doing its job.
_SQL_INDIRECT = (
    "             WHERE accession_number = ANY(%s) AND direct_indirect = 'I'\n"
    "               AND NOT is_derivative\n"
    "               AND post_transaction_shares IS NOT NULL\n"
)
_SQL_DERIVATIVE = "               AND NOT is_derivative\n               AND post_transaction_shares IS NOT NULL\n"
_SQL_AMOUNT = "            key = (str(accession), Decimal(shares))\n"
_SQL_FOOTNOTE_LINK = (
    "    for key, ids in refs.items():\n"
    "        accession, _shares = key\n"
    "        texts.setdefault(key, []).extend(\n"
    "            footnotes[(accession, fid)] for fid in sorted(ids) if (accession, fid) in footnotes\n"
    "        )\n"
)

_SQL_PROBES = (
    (
        "J: the D/I filter dropped — direct lines contribute text",
        _SQL_INDIRECT,
        "             WHERE accession_number = ANY(%s)\n"
        "               AND NOT is_derivative\n"
        "               AND post_transaction_shares IS NOT NULL\n",
        ("test_direct_lines_contribute_nothing",),
        ("test_evidence_is_keyed_on_the_rows_own_amount",),
    ),
    (
        "M: Table II derivative rows contribute their own ownership text (Codex ckpt-2)",
        _SQL_DERIVATIVE,
        "               AND post_transaction_shares IS NOT NULL\n",
        ("test_derivative_rows_contribute_nothing",),
        ("test_evidence_is_keyed_on_the_rows_own_amount",),
    ),
    (
        "K: keyed on the TRANSACTION amount, not the resulting holding",
        _SQL_AMOUNT,
        "            key = (str(accession), Decimal(1))  # PROBE K — wrong amount column\n",
        ("test_evidence_is_keyed_on_the_rows_own_amount",),
        ("test_form_3_holdings_are_read_from_their_own_table",),
    ),
    (
        "L: every footnote on the accession attached, ignoring footnote_refs",
        _SQL_FOOTNOTE_LINK,
        "    for key, ids in refs.items():\n"
        "        accession, _shares = key\n"
        "        texts.setdefault(key, []).extend(\n"
        "            v for (a, _f), v in footnotes.items() if a == accession\n"
        "        )\n",
        ("test_only_the_footnotes_this_row_references_are_attached",),
        ("test_evidence_is_keyed_on_the_rows_own_amount",),
    ),
)


def _pytest(node_ids: tuple[str, ...], target: str = _TESTS) -> int:
    cmd = ["uv", "run", "pytest", target, "-q", "-n", "0", "-p", "no:randomly", "-k", " or ".join(node_ids)]
    return subprocess.run(cmd, capture_output=True, text=True).returncode


def main() -> int:
    original = _MODULE.read_text()
    anchors = (
        ("tier-call", _TIER_CALL),
        ("unique", _UNIQUE),
        ("value-key", _VALUE_KEY),
        ("insider-only", _INSIDER_ONLY),
        ("release", _RELEASE),
        ("clause-order", _CLAUSE_ORDER),
        ("match", _MATCH),
        ("sql-indirect", _SQL_INDIRECT),
        ("sql-derivative", _SQL_DERIVATIVE),
        ("sql-amount", _SQL_AMOUNT),
        ("sql-footnote-link", _SQL_FOOTNOTE_LINK),
    )
    for label, anchor in anchors:
        # A probe that silently matches nothing proves nothing.
        if original.count(anchor) != 1:
            print(f"ABORT: {label} anchor appears {original.count(anchor)} times, expected exactly 1")
            return 2

    caught = 0
    total = len(_PROBES) + len(_SQL_PROBES)
    try:
        for target, probes in ((_TESTS, _PROBES), (_DB_TESTS, _SQL_PROBES)):
            for label, old, new, must_fail, must_pass in probes:
                _MODULE.write_text(original.replace(old, new))
                fail_rc = _pytest(must_fail, target)
                pass_rc = _pytest(must_pass, target)
                ok = fail_rc == 1 and pass_rc == 0
                caught += ok
                print(f"  probe {label}")
                print(
                    f"    targeted rc={fail_rc} (want 1)  bystanders rc={pass_rc} (want 0)  -> "
                    f"{'CAUGHT' if ok else 'NOT CAUGHT'}"
                )
    finally:
        _MODULE.write_text(original)

    restored_rc = _pytest(_ALL_2408 + _2385)
    restored_db_rc = subprocess.run(
        ["uv", "run", "pytest", _DB_TESTS, "-q", "-n", "0", "-p", "no:randomly"], capture_output=True, text=True
    ).returncode
    print(f"\nrestored pure suite rc={restored_rc} (want 0)  db suite rc={restored_db_rc} (want 0)")
    print(f"{caught}/{total} CAUGHT")
    return 0 if caught == total and restored_rc == 0 and restored_db_rc == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
