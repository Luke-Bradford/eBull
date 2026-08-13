"""Unit tests for app.security.passwords -- Argon2id wrapper.

These tests do hit real Argon2id (no mocking) but the default parameters
are fast enough for unit-test speed; if test runtime regresses materially
after a parameter tune, move these to a slower marker.
"""

from __future__ import annotations

from app.security.passwords import hash_password, verify_password


def test_hash_then_verify_roundtrip() -> None:
    h = hash_password("correct horse battery staple")
    assert verify_password("correct horse battery staple", h) is True


def test_verify_rejects_wrong_password() -> None:
    h = hash_password("correct horse battery staple")
    assert verify_password("wrong", h) is False


def test_verify_rejects_malformed_hash() -> None:
    # No exception should escape -- verify_password absorbs all failure
    # modes and returns False, so the HTTP layer can render a single
    # generic 401 regardless of cause.
    assert verify_password("anything", "not-a-real-phc-string") is False


def test_two_hashes_of_same_password_differ() -> None:
    # Argon2id PHC encodes a fresh random salt per call, so two hashes
    # of the same password must differ -- otherwise the salt is broken.
    a = hash_password("samepw")
    b = hash_password("samepw")
    assert a != b
    assert verify_password("samepw", a) is True
    assert verify_password("samepw", b) is True
