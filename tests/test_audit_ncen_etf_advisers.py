"""#2214 — pin the two decisions in the N-CEN ETF-adviser audit script.

``scripts/audit_ncen_etf_advisers.py`` produces every figure quoted in
``.claude/skills/data-sources/sec-edgar.md`` §2.2.1 and in the #2214
prevention-log entry, so its name normalisation is a data-treatment
decision — it decides which N-CEN advisers count as the same entity as a
13F filer — and not a script detail.

Both invariants below shipped broken and were caught after the review bot
had already APPROVEd the file:

* ``_canonical_name`` mapped aliases per WORD while two of its four table
  entries were the multi-token forms ``"L L C"`` / ``"L P"`` — what
  ``L.L.C.`` and ``L.P.`` become once punctuation is stripped. A per-word
  ``dict.get`` can never see a two-word key, so those entries were dead and
  every punctuated filer silently failed to match. Fixing it moved the
  match count 152 → 156 (Codex, checkpoint 2).
* ``_download`` wrote the fetched ZIP straight to its cache path, so an
  interrupted download left a truncated file that passes the caller's
  ``st_size > 0`` cache test and is reused forever (review NITPICK).

⚠ Expectations are hand-written, never derived from the module — a
reference that imports the rule it validates passes whatever the rule does.
"""

from __future__ import annotations

import io
from pathlib import Path
from unittest import mock

import pytest

from scripts.audit_ncen_etf_advisers import _canonical_name, _download


class TestCanonicalName:
    """Punctuation and spelling fold; the legal FORM itself never does."""

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            # The dead-alias class: punctuation splits the form into tokens.
            ("Acme Advisors L.L.C.", "Acme Advisors LLC"),
            ("Acme Partners L.P.", "Acme Partners LP"),
            ("Acme Advisors, L.L.C.", "ACME ADVISORS LLC"),
            # Spelled-out forms, which the per-word table already handled.
            ("Acme Inc", "Acme Incorporated"),
            ("Acme Ltd", "Acme Limited"),
            # Case, ampersand and general punctuation.
            ("Cohen & Steers Capital Management Inc", "COHEN AND STEERS CAPITAL MANAGEMENT INC"),
        ],
    )
    def test_spellings_of_one_entity_fold_together(self, left: str, right: str) -> None:
        assert _canonical_name(left) == _canonical_name(right)

    @pytest.mark.parametrize(
        ("left", "right"),
        [
            # Deliberate: an LLC and a Ltd are different registered entities.
            ("Brown Advisory LLC", "Brown Advisory Ltd"),
            ("Acme Capital LP", "Acme Capital LLC"),
            # The form is preserved, so a bare name is not the same entity.
            ("Acme Capital", "Acme Capital LLC"),
        ],
    )
    def test_different_legal_forms_stay_apart(self, left: str, right: str) -> None:
        assert _canonical_name(left) != _canonical_name(right)

    def test_spaced_form_folds_only_as_whole_tokens(self) -> None:
        """`L P` inside a word must not be folded — only a standalone run."""
        assert _canonical_name("Carl P Smith Advisors") == "CARL P SMITH ADVISORS"

    def test_known_filer_spellings(self) -> None:
        """Hand-transcribed, from the audit's own per-entity output."""
        assert _canonical_name("J.P. Morgan Investment Management Inc.") == "J P MORGAN INVESTMENT MANAGEMENT INC"
        assert _canonical_name("GOLDMAN SACHS ASSET MANAGEMENT, L.P.") == "GOLDMAN SACHS ASSET MANAGEMENT LP"


class TestDownloadIsAtomic:
    """A truncated cache file passes `st_size > 0` and is reused forever."""

    def test_interrupted_write_leaves_no_visible_cache_file(self, tmp_path: Path) -> None:
        """The write dies half-way — disk full, or a kill during the flush.

        ⚠ This test models the failure at the WRITE, not at the socket read.
        A fixture that raises on ``resp.read()`` cannot distinguish the atomic
        form from the direct one: nothing is written either way, so both leave
        the cache clean and a revert-probe reports NOT CAUGHT. The truncation
        the nitpick describes only exists once bytes have landed.
        """
        body = b"PK\x03\x04" + b"x" * 64
        real_write = Path.write_bytes

        def truncating_write(self: Path, data: bytes) -> int:
            real_write(self, data[: len(data) // 2])
            raise OSError("no space left on device")

        with (
            mock.patch("urllib.request.urlopen", return_value=io.BytesIO(body)),
            mock.patch.object(Path, "write_bytes", truncating_write),
            pytest.raises(OSError, match="no space left"),
        ):
            _download("2025q3", tmp_path, "test agent")

        assert not (tmp_path / "2025q3_ncen.zip").exists()
        assert list(tmp_path.iterdir()) == []

    def test_completed_download_lands_at_the_cache_path(self, tmp_path: Path) -> None:
        body = b"PK\x03\x04 pretend zip"

        with mock.patch("urllib.request.urlopen", return_value=io.BytesIO(body)):
            dest = _download("2025q3", tmp_path, "test agent")

        assert dest == tmp_path / "2025q3_ncen.zip"
        assert dest.read_bytes() == body
        assert list(tmp_path.iterdir()) == [dest]

    def test_non_empty_cache_hit_makes_no_request(self, tmp_path: Path) -> None:
        dest = tmp_path / "2025q3_ncen.zip"
        dest.write_bytes(b"cached")

        with mock.patch("urllib.request.urlopen", side_effect=AssertionError("network touched")):
            assert _download("2025q3", tmp_path, "test agent") == dest
