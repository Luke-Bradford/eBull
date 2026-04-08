"""Service-level tests for app.services.broker_credentials (issue #99).

Covers input normalisation and the validation rules that do not need a
real database. End-to-end DB round-trip is exercised by the API test
file via mocked service functions; the persistence/audit guarantees are
documented in the service module and reviewed against the migration
schema constraints.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from app.security import secrets_crypto
from app.services.broker_credentials import (
    CredentialValidationError,
    normalise_label,
    normalise_provider,
    normalise_secret,
)


@pytest.fixture(autouse=True)
def _key() -> Iterator[None]:
    secrets_crypto.set_active_key(os.urandom(32))
    yield
    secrets_crypto._reset_for_tests()


# ---------------------------------------------------------------------------
# Provider allow-list
# ---------------------------------------------------------------------------


class TestProviderNormalisation:
    def test_etoro_lowercase(self) -> None:
        assert normalise_provider("etoro") == "etoro"

    def test_etoro_uppercase_normalised(self) -> None:
        assert normalise_provider("ETORO") == "etoro"

    def test_whitespace_stripped(self) -> None:
        assert normalise_provider("  etoro  ") == "etoro"

    def test_unsupported_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_provider("kraken")

    def test_empty_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_provider("   ")


# ---------------------------------------------------------------------------
# Label normalisation
# ---------------------------------------------------------------------------


class TestLabelNormalisation:
    def test_strip(self) -> None:
        assert normalise_label("  primary  ") == "primary"

    def test_empty_after_strip_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_label("   ")

    def test_empty_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_label("")


# ---------------------------------------------------------------------------
# Secret normalisation + length floor
# ---------------------------------------------------------------------------


class TestSecretNormalisation:
    def test_strip(self) -> None:
        assert normalise_secret("  abcd  ") == "abcd"

    def test_empty_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_secret("")

    def test_whitespace_only_rejected(self) -> None:
        with pytest.raises(CredentialValidationError):
            normalise_secret("    ")

    def test_too_short_rejected(self) -> None:
        # Three chars cannot produce a 4-char last_four preview.
        with pytest.raises(CredentialValidationError):
            normalise_secret("abc")

    def test_minimum_length_accepted(self) -> None:
        assert normalise_secret("abcd") == "abcd"

    def test_idempotent(self) -> None:
        """A value accepted by normalise_secret must be accepted on
        a second pass with the same result. The lazy-gen path in
        the API layer normalises once and passes the cleaned value
        through to store_credential -- if normalisation were not
        idempotent, the second pass inside store_credential could
        raise on a value the outer pass accepted, triggering
        _rollback_lazy_gen on a non-fatal user-input error
        (review feedback PR #118 round 12).
        """
        for raw in ("abcd", "  abcd  ", "abcd1234", "  long-secret-with-spaces  "):
            once = normalise_secret(raw)
            twice = normalise_secret(once)
            assert once == twice

    def test_idempotent_for_provider_and_label(self) -> None:
        assert normalise_provider(normalise_provider("  ETORO  ")) == "etoro"
        assert normalise_label(normalise_label("  primary  ")) == "primary"
