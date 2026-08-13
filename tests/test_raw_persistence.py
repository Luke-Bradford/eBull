"""Unit tests for ``app.services.raw_persistence`` (#268 follow-up PR 1).

Helper-level tests only. Provider call-site migrations land in PR 2;
compaction / sweep / scheduler tests land in PR 3.

Every test monkeypatches ``_DATA_ROOT`` to ``tmp_path`` + uses a real
registered source (``fmp``) so the drift guard fires correctly without
polluting the real ``data/raw/`` tree.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path
from typing import Any

import pytest

from app.services import raw_persistence
from app.services.raw_persistence import _canonicalise_for_hash, persist_raw_if_new

# ---------------------------------------------------------------------
# _canonicalise_for_hash
# ---------------------------------------------------------------------


class TestCanonicalise:
    def test_dict_deterministic(self) -> None:
        """Same dict produces the same bytes across calls."""
        d: dict[str, Any] = {"b": 2, "a": 1, "nested": {"z": 3, "y": 4}}
        assert _canonicalise_for_hash(d) == _canonicalise_for_hash(d)
        assert b'"a":1' in _canonicalise_for_hash(d)  # sort_keys applied

    def test_list_of_dicts_nested_sort(self) -> None:
        """sort_keys recurses into list-of-dicts."""
        payload = [{"y": 1, "x": 2}, {"b": 3, "a": 4}]
        out = _canonicalise_for_hash(payload)
        assert b'"a":4' in out
        assert b'"x":2' in out

    def test_bytes_non_json_passes_through(self) -> None:
        """Non-JSON bytes (e.g. binary response) are returned unchanged."""
        raw = b"\x00\x01\x02not json"
        assert _canonicalise_for_hash(raw) == raw

    def test_bytes_json_canonicalised_matches_dict(self) -> None:
        """Parseable JSON bytes produce identical hash-bytes to the
        equivalent dict (r3-B2 regression — compaction re-canonicalises
        file bytes and must agree with the write path's hash)."""
        d = {"a": 1, "b": [2, 3]}
        raw_json = b'{"b":[2,3],"a":1}'
        assert _canonicalise_for_hash(raw_json) == _canonicalise_for_hash(d)

    def test_str_non_json_utf8_encoded(self) -> None:
        """Non-JSON str (e.g. plaintext error body) UTF-8 encodes."""
        s = "connection refused — not valid json"
        assert _canonicalise_for_hash(s) == s.encode("utf-8")

    def test_str_json_canonicalised_matches_dict(self) -> None:
        """Parseable JSON str produces identical bytes to the
        equivalent dict (r2-B2 — etoro.py persists exc.response.text
        which may be JSON)."""
        d = {"error": "bad_request", "code": 400}
        raw_json_str = '{"code":400,"error":"bad_request"}'
        assert _canonicalise_for_hash(raw_json_str) == _canonicalise_for_hash(d)

    def test_scalar_json_values_accepted(self) -> None:
        """JSON scalars (None, bool, int, float) are accepted — matches
        original per-provider json.dumps(payload) behaviour so upstream
        APIs returning bare JSON null don't crash the sync path."""
        assert _canonicalise_for_hash(None) == b"null"
        assert _canonicalise_for_hash(True) == b"true"
        assert _canonicalise_for_hash(42) == b"42"
        assert _canonicalise_for_hash(3.14) == b"3.14"

    def test_unsupported_type_raises(self) -> None:
        """sets, custom objects, etc. are not supported."""
        with pytest.raises(TypeError, match="unsupported payload type"):
            _canonicalise_for_hash({1, 2, 3})  # type: ignore[arg-type]


# ---------------------------------------------------------------------
# persist_raw_if_new
# ---------------------------------------------------------------------


class TestPersistRawIfNew:
    def test_unknown_source_raises_keyerror(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Drift guard — unknown source raises, never swallowed."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        with pytest.raises(KeyError, match="unknown source"):
            persist_raw_if_new("nonexistent_source", "tag", {"k": "v"})

    def test_first_write_creates_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """First call writes + returns the target path."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", "profile", {"symbol": "AAPL"})
        assert result is not None
        assert result.exists()
        assert result.parent == tmp_path / "fmp"
        assert result.name.startswith("profile_")
        assert result.name.endswith(".json")
        # Hash should be 16 hex chars.
        hash_part = result.name[len("profile_") : -len(".json")]
        assert len(hash_part) == 16
        assert all(c in "0123456789abcdef" for c in hash_part)

    def test_dedup_hit_returns_none(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Second call with identical payload returns None — no duplicate."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        first = persist_raw_if_new("fmp", "profile", {"symbol": "AAPL"})
        second = persist_raw_if_new("fmp", "profile", {"symbol": "AAPL"})
        assert first is not None
        assert second is None
        assert len(list((tmp_path / "fmp").iterdir())) == 1

    def test_str_error_body_persists(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """r2-B2 regression — str payload (etoro exc.response.text)
        writes + returns path (does not raise TypeError)."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("etoro", "quote_error", "HTTP 500 internal server error")
        assert result is not None
        assert result.exists()

    def test_different_payloads_different_files(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Different content → different hash → both persisted."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        a = persist_raw_if_new("fmp", "profile", {"symbol": "AAPL"})
        b = persist_raw_if_new("fmp", "profile", {"symbol": "MSFT"})
        assert a is not None and b is not None
        assert a != b
        assert len(list((tmp_path / "fmp").iterdir())) == 2

    def test_os_error_on_write_returns_none(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Best-effort contract — OSError on any filesystem step
        returns None, never raises. The provider sync path must
        stay intact even under disk-full / permission errors."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)

        def fail_replace(src: str, dst: str) -> None:
            raise OSError("simulated disk full")

        monkeypatch.setattr(os, "replace", fail_replace)
        result = persist_raw_if_new("fmp", "profile", {"symbol": "AAPL"})
        assert result is None

    def test_atomic_write_no_orphan_tmp(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Simulated os.replace failure leaves no .tmp file orphaned
        in the target directory (atomicity invariant)."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)

        def fail_replace(src: str, dst: str) -> None:
            raise OSError("simulated")

        monkeypatch.setattr(os, "replace", fail_replace)
        persist_raw_if_new("fmp", "profile", {"symbol": "AAPL"})
        # No .tmp leftover, no target landed.
        fmp_dir = tmp_path / "fmp"
        if fmp_dir.exists():
            for child in fmp_dir.iterdir():
                assert not child.name.endswith(".tmp"), f"orphan tmp: {child}"
                assert not child.name.startswith(".profile_"), f"orphan prefix: {child}"

    def test_keyerror_runs_before_filesystem(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Drift guard runs before mkdir — no partial directory tree
        created for an invalid source."""
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        with pytest.raises(KeyError):
            persist_raw_if_new("nonexistent_source", "tag", {"k": "v"})
        assert not (tmp_path / "nonexistent_source").exists()


# ---------------------------------------------------------------------
# Tag sanitisation (#249)
# ---------------------------------------------------------------------


class TestTagSanitisation:
    """Provider-derived identifiers (symbol, company_number,
    transaction_id) are interpolated directly into raw filenames.
    Whatever an upstream API returns must NOT be able to escape the
    source directory, smuggle in NUL/control characters, or blow past
    typical filesystem name limits.
    """

    def test_forward_slash_in_tag_replaced(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", "profile/AAPL", {"k": "v"})
        assert result is not None
        # No subdirectory was created; the slash is replaced.
        fmp_dir = tmp_path / "fmp"
        assert result.parent == fmp_dir
        assert "/" not in result.name
        assert "profile_AAPL_" in result.name

    def test_dot_dot_in_tag_replaced(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """``..`` cannot escape the source directory — the regex
        replaces neither dot independently, but leading dots are
        stripped and the containment check would catch a resolved
        path outside the source dir.
        """
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", "../etc/passwd", {"k": "v"})
        assert result is not None
        # ``../`` collapses to ``__`` after sanitisation; the file
        # MUST still live under tmp_path/fmp/.
        assert result.is_relative_to(tmp_path / "fmp")
        assert ".." not in result.name

    def test_backslash_in_tag_replaced(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", r"profile\AAPL", {"k": "v"})
        assert result is not None
        assert "\\" not in result.name
        assert "profile_AAPL_" in result.name

    def test_nul_byte_replaced(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", "profile\x00AAPL", {"k": "v"})
        assert result is not None
        assert "\x00" not in result.name

    def test_unicode_replaced(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", "profile_日本", {"k": "v"})
        assert result is not None
        # Only ASCII alphanumeric + ._- survives.
        assert all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in result.name)

    def test_long_tag_truncated(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        long_tag = "x" * 5000
        result = persist_raw_if_new("fmp", long_tag, {"k": "v"})
        assert result is not None
        # Tag truncated to <=200 chars; full filename stays under
        # typical FS limits.
        assert len(result.name) < 255

    def test_empty_after_sanitisation_uses_underscore(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A tag composed entirely of unsafe chars collapses to the
        single ``_`` placeholder so the filename always has a body
        before the ``_{hash}.json`` suffix.
        """
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        result = persist_raw_if_new("fmp", "/\x00\x01", {"k": "v"})
        assert result is not None
        # All-unsafe tag collapses to a sequence of ``_`` characters
        # (the regex maps each unsafe char to ``_`` and the result
        # becomes the tag prefix). The file MUST land under fmp/ and
        # MUST end with ``_<16hex>.json``.
        assert result.is_relative_to(tmp_path / "fmp")
        # Strip extension, strip 17-char ``_<16hex>`` suffix; what
        # remains is the sanitised tag — must be non-empty.
        stem = result.stem  # filename without .json
        # Stem is ``{tag}_{16hex}`` so split off the last underscore
        # group (the hash):
        head, _, hash_part = stem.rpartition("_")
        assert len(hash_part) == 16
        assert head != ""
        # Every char in the sanitised head is a safe-set member.
        assert all(c in "_" for c in head)

    def test_dedup_robust_across_tag_variants(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two callers passing different but equivalent-after-sanitise
        tags + the same payload still dedup to one file (digest is
        payload-derived, sanitised tag is the same).
        """
        monkeypatch.setattr(raw_persistence, "_DATA_ROOT", tmp_path)
        a = persist_raw_if_new("fmp", "profile/AAPL", {"k": "v"})
        b = persist_raw_if_new("fmp", "profile_AAPL", {"k": "v"})
        # The ``/`` in tag a sanitises to ``_``, so both tags become
        # ``profile_AAPL`` with identical digests → second call dedups.
        assert a is not None
        assert b is None


# ---------------------------------------------------------------------
# Writer-discipline regression guard (#436)
# ---------------------------------------------------------------------


_PROVIDERS_ROOT = Path(__file__).parent.parent / "app" / "providers"


# Bumped with each real provider addition so a misconfigured test env
# (missing directory, wrong cwd, broken glob) can't silently pass with
# zero parametrised cases. Fails LOUD instead — review-prevention
# entry "empty-parametrize silent pass".
_MIN_PROVIDER_FILES = 10


def _iter_provider_files() -> list[Path]:
    """Every ``.py`` under ``app/providers/`` except ``__init__``.

    Recurses so helper subpackages can't escape the guard by hiding
    the forbidden call in a nested module."""
    return sorted(p for p in _PROVIDERS_ROOT.rglob("*.py") if p.name != "__init__.py")


def test_provider_files_sentinel() -> None:
    """Non-parametrised safety net — proves ``_iter_provider_files``
    resolves a non-empty set. If this test is the only one in the
    class to run (zero parametrised cases), the guard below silently
    passes and loses its regression value. This sentinel fails the
    whole file if the glob returns less than the expected minimum,
    so a missing directory / wrong cwd / broken pathing surfaces
    immediately rather than during a real regression."""
    files = _iter_provider_files()
    assert len(files) >= _MIN_PROVIDER_FILES, (
        f"_iter_provider_files() returned {len(files)} files — expected "
        f"at least {_MIN_PROVIDER_FILES}. The writer-discipline guard "
        f"would silently pass with zero parametrised cases. Check "
        f"{_PROVIDERS_ROOT} exists and contains provider modules."
    )


# Attribute-name write shapes. The full regression surface is wider
# (``os.write``, ``open(...).write``, ``from json import dump`` etc.)
# so the guard also flags any bare ``write`` / ``writelines`` /
# ``dump`` attribute call that isn't on a logger — providers have no
# legitimate write path outside ``persist_raw_if_new``.
_FORBIDDEN_WRITE_ATTRS = {
    "write",
    "writelines",
    "write_text",
    "write_bytes",
    "dump",  # json.dump (qualified) + bare dump from ``from json import dump``
}

# Attribute roots that are known safe — writing to these cannot land
# a raw payload on disk. Keeping the allow set short + specific means
# any unexpected call site surfaces as an offender rather than being
# silently swallowed. Extend only with explicit review.
_SAFE_WRITE_ROOTS = {
    "logger",
    "log",
    "sys",  # sys.stdout.write / sys.stderr.write (diagnostic only)
}


def _attribute_root_name(node: ast.AST) -> str | None:
    """Return the leftmost Name in a ``foo.bar.baz`` attribute chain.

    Used to classify ``logger.info(...).write(...)``-style chains as
    safe (root = ``logger``) vs ``open(target).write(...)``-style
    chains (root is a Call, returns None → treated as unsafe)."""
    while isinstance(node, ast.Attribute):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


class TestProviderWriterDiscipline:
    """#436 — providers must route raw persistence through
    ``persist_raw_if_new``. Any other write path is a regression
    surface for the pre-migration double-write pattern (timestamp
    variant + hash variant of the same payload)."""

    @pytest.mark.parametrize(
        "path",
        _iter_provider_files(),
        ids=lambda p: p.relative_to(_PROVIDERS_ROOT).as_posix(),
    )
    def test_no_direct_file_writes(self, path: Path) -> None:
        """No provider makes a filesystem write outside the sanctioned
        ``persist_raw_if_new`` path. Shape-independent: catches
        ``.write``/``.writelines``/``.write_text``/``.write_bytes``/
        ``.dump`` on any value that is not a logging sink. Widened
        from the earlier ``{dump, write_text, write_bytes}`` set per
        Codex checkpoint — ``open(target).write(...)`` and
        ``handle.write(...)`` shapes were previously invisible."""
        tree = ast.parse(path.read_text(encoding="utf-8"))
        offenders: list[str] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute):
                # Bare ``dump(obj, f)`` (``from json import dump``):
                # also a regression surface. Flag Name-form calls to
                # forbidden identifiers.
                if isinstance(func, ast.Name) and func.id in _FORBIDDEN_WRITE_ATTRS:
                    offenders.append(f"line {node.lineno}: {ast.unparse(node)[:120]}")
                continue
            if func.attr not in _FORBIDDEN_WRITE_ATTRS:
                continue
            root = _attribute_root_name(func.value)
            if root in _SAFE_WRITE_ROOTS:
                continue
            offenders.append(f"line {node.lineno}: {ast.unparse(node)[:120]}")
        assert not offenders, (
            f"{path.name} makes a direct filesystem write — providers "
            f"must route through persist_raw_if_new:\n  " + "\n  ".join(offenders)
        )
