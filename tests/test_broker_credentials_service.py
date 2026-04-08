"""Service-level tests for app.services.broker_credentials (issue #99).

Covers input normalisation and the validation rules that do not need a
real database. End-to-end DB round-trip is exercised by the API test
file via mocked service functions; the persistence/audit guarantees are
documented in the service module and reviewed against the migration
schema constraints.
"""

from __future__ import annotations

import base64
import os
from collections.abc import Iterator

import pytest

from app.security import secrets_crypto
from app.services.broker_credentials import (
    CredentialValidationError,
    _normalise_label,
    _normalise_provider,
    _normalise_secret,
)


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(
        secrets_crypto.settings,
        "secrets_key",
        base64.b64encode(os.urandom(32)).decode(),
    )
    secrets_crypto._reset_for_tests()
    yield
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Provider allow-list
# ---------------------------------------------------------------------------


class TestProviderNormalisation:
    def test_etoro_lowercase(self) -> None:
        assert _normalise_provider("etoro") == "etoro"

    def test_etoro_uppercase_normalised(self) -> None:
        assert _normalise_provider("ETORO") == "etoro"

    def test_whitespace_stripped(self) -> None:
        assert _normalise_provider("  etoro  ") == "etoro"

    def test_unsupported_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            _normalise_provider("kraken")

    def test_empty_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            _normalise_provider("   ")


# ---------------------------------------------------------------------------
# Label normalisation
# ---------------------------------------------------------------------------


class TestLabelNormalisation:
    def test_strip(self) -> None:
        assert _normalise_label("  primary  ") == "primary"

    def test_empty_after_strip_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            _normalise_label("   ")

    def test_empty_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            _normalise_label("")


# ---------------------------------------------------------------------------
# Secret normalisation + length floor
# ---------------------------------------------------------------------------


class TestSecretNormalisation:
    def test_strip(self) -> None:
        assert _normalise_secret("  abcd  ") == "abcd"

    def test_empty_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            _normalise_secret("")

    def test_whitespace_only_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            _normalise_secret("    ")

    def test_too_short_rejected(self) -> None:
        # Three chars cannot produce a 4-char last_four preview.
        with pytest.raises(CredentialValidationError):
            _normalise_secret("abc")

    def test_minimum_length_accepted(self) -> None:
        assert _normalise_secret("abcd") == "abcd"
