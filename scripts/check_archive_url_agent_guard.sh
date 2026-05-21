#!/usr/bin/env bash
# scripts/check_archive_url_agent_guard.sh
#
# Defensive lint guard introduced after #1249 + #1250 (PR11 cleanup).
#
# Any manifest-worker parser under app/services/manifest_parsers/ that
# calls _archive_file_url(row.cik, ...) (or any URL construction routed
# through row.cik) MUST ALSO check that row.cik is NOT a known SEC
# filing-agent CIK (Donnelley / EdgarOnline / DFIN / Workiva / etc.)
# BEFORE that call. SEC archives are mounted under the issuer / filer
# CIK, never under agent CIKs — an unguarded call against a stray agent
# CIK would 404 every accession in the cohort.
#
# The lesson surfaced during PR11 (#1233) spec authoring: 13F-HR + N-PORT
# manifest parsers were missing the guard. Empirical prevalence is 0%
# today (curated cohorts exclude agents) — this lint is belt-and-braces
# against future discovery regressions where a new PR might enqueue an
# agent CIK as `row.cik`.
#
# Invariant: every file in app/services/manifest_parsers/sec_*.py that
# matches the literal substring `_archive_file_url(filer_cik` (or
# equivalent row.cik passthrough) MUST also contain the literal
# substring `KNOWN_FILING_AGENT_CIKS` within the SAME file. Whole-file
# grep is sufficient because parsers are small and single-purpose;
# block-scope analysis is unnecessary.
#
# Allow-list: files where row.cik is documented to ALWAYS hold an
# issuer CIK (not a filer CIK) — for those, _archive_file_url(row.cik)
# is correct without an agent check because issuer CIKs never collide
# with the agent set by construction. The allow-list MUST cite the
# subject_type that guarantees the invariant. Maintained inline below.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PARSER_DIR="app/services/manifest_parsers"

# Files where row.cik is guaranteed to be an ISSUER CIK by the manifest
# subject_type contract — agent collision is impossible. Add an entry
# here ONLY when you can cite the subject_type + the discovery path
# that enforces issuer CIK semantics.
ALLOW_LIST=(
    # sec_10k.py        — subject_type='issuer' → row.cik = issuer CIK
    # sec_10q.py        — subject_type='issuer' → row.cik = issuer CIK
    # def14a.py         — subject_type='issuer' → row.cik = issuer CIK
    # eight_k.py        — subject_type='issuer' → row.cik = issuer CIK
    # sec_xbrl_facts.py — subject_type='issuer' → row.cik = issuer CIK
    "$PARSER_DIR/sec_10k.py"
    "$PARSER_DIR/sec_10q.py"
    "$PARSER_DIR/def14a.py"
    "$PARSER_DIR/eight_k.py"
    "$PARSER_DIR/sec_xbrl_facts.py"
    # insider_345.py — does NOT use _archive_file_url(row.cik); fetches
    # by primary_document_url from the row instead. Included to silence
    # any false positive if future code adds a row.cik passthrough.
    "$PARSER_DIR/insider_345.py"
    # sec_n_csr.py — does NOT use _archive_file_url(row.cik); fetches
    # iXBRL companion by primary_document_url manipulation.
    "$PARSER_DIR/sec_n_csr.py"
)

failed=0

for path in "$PARSER_DIR"/sec_*.py; do
    [[ -f "$path" ]] || continue

    # Skip allow-listed files.
    skip=0
    for allowed in "${ALLOW_LIST[@]}"; do
        if [[ "$path" == "$allowed" ]]; then
            skip=1
            break
        fi
    done
    if [[ "$skip" -eq 1 ]]; then
        continue
    fi

    # Does this file call _archive_file_url with a row.cik-derived
    # value? Heuristic: any line containing `_archive_file_url(` whose
    # first argument is a `*cik*` identifier (filer_cik / issuer_cik /
    # subject_cik / etc.). Matches the parser pattern across the repo.
    if ! grep -qE '_archive_file_url\s*\(\s*[a-z_]*cik' "$path"; then
        # Parser uses URL construction through some other path (e.g. the
        # row.primary_document_url passthrough). No guard required.
        continue
    fi

    # Guard required: the file MUST contain an EXECUTABLE membership-check
    # against KNOWN_FILING_AGENT_CIKS (Codex MEDIUM 2026-05-21 — Codex
    # caught that a bare "KNOWN_FILING_AGENT_CIKS" mention in a comment
    # or a dead import would pass the lint without an actual guard). The
    # acceptable shapes are:
    #   if <expr> in KNOWN_FILING_AGENT_CIKS:
    #   if KNOWN_FILING_AGENT_CIKS  (in a conditional)
    #   {... in KNOWN_FILING_AGENT_CIKS ...}  (membership predicate)
    # All three contain the literal `in KNOWN_FILING_AGENT_CIKS` substring,
    # which is the lint's positive test.
    if ! grep -qE 'in[[:space:]]+KNOWN_FILING_AGENT_CIKS\b' "$path"; then
        echo "FAIL: $path calls _archive_file_url(<cik>, ...) but does not"
        echo "      contain an executable 'in KNOWN_FILING_AGENT_CIKS' membership"
        echo "      check (Codex 2026-05-21 lesson: a comment-only reference"
        echo "      or dead import would pass the prior lint without an actual"
        echo "      guard). SEC archives are not mounted under filing-agent"
        echo "      CIKs; an unguarded call would 404 every agent-enqueued"
        echo "      accession. Add a guard mirroring sec_13f_hr.py /"
        echo "      sec_n_port.py (post-#1249/#1250), OR append this file to"
        echo "      ALLOW_LIST in scripts/check_archive_url_agent_guard.sh"
        echo "      with the subject_type invariant that excludes agent CIKs."
        failed=1
    fi
done

if [[ "$failed" -ne 0 ]]; then
    exit 1
fi

echo "check_archive_url_agent_guard: OK"
