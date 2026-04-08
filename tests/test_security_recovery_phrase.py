"""Tests for app.security.recovery_phrase (#114 / ADR-0003)."""

from __future__ import annotations

import os

import pytest

from app.security.recovery_phrase import (
    PHRASE_WORD_COUNT,
    ROOT_SECRET_LEN,
    RecoveryPhraseError,
    decode_phrase,
    encode_phrase,
    wordlist,
)


class TestWordlist:
    def test_has_2048_unique_words(self) -> None:
        words = wordlist()
        assert len(words) == 2048
        assert len(set(words)) == 2048


class TestEncodeDecode:
    def test_round_trip_random(self) -> None:
        for _ in range(50):
            secret = os.urandom(ROOT_SECRET_LEN)
            phrase = encode_phrase(secret)
            assert len(phrase) == PHRASE_WORD_COUNT
            assert decode_phrase(phrase) == secret

    def test_round_trip_zero_secret(self) -> None:
        secret = b"\x00" * ROOT_SECRET_LEN
        assert decode_phrase(encode_phrase(secret)) == secret

    def test_round_trip_max_secret(self) -> None:
        secret = b"\xff" * ROOT_SECRET_LEN
        assert decode_phrase(encode_phrase(secret)) == secret

    def test_string_input_accepted(self) -> None:
        secret = os.urandom(ROOT_SECRET_LEN)
        phrase = " ".join(encode_phrase(secret))
        assert decode_phrase(phrase) == secret

    def test_uppercase_words_accepted(self) -> None:
        secret = os.urandom(ROOT_SECRET_LEN)
        phrase = [w.upper() for w in encode_phrase(secret)]
        assert decode_phrase(phrase) == secret

    def test_list_and_joined_string_equivalent(self) -> None:
        """``decode_phrase`` accepts both ``list[str]`` and ``str``;
        the two branches must produce identical output for the same
        word sequence so the helper's union signature cannot drift
        a future direct caller into a different normalisation path
        (review feedback PR #118 round 16).
        """
        secret = os.urandom(ROOT_SECRET_LEN)
        words = encode_phrase(secret)
        assert decode_phrase(words) == decode_phrase(" ".join(words))


class TestEncodeValidation:
    def test_wrong_length_secret_rejected(self) -> None:
        with pytest.raises(RecoveryPhraseError):
            encode_phrase(b"\x00" * 16)


class TestDecodeValidation:
    def test_wrong_word_count_rejected(self) -> None:
        with pytest.raises(RecoveryPhraseError):
            decode_phrase(["abandon"] * 23)

    def test_unknown_word_rejected(self) -> None:
        secret = os.urandom(ROOT_SECRET_LEN)
        phrase = encode_phrase(secret)
        phrase[5] = "notarealword"
        with pytest.raises(RecoveryPhraseError, match="word 6"):
            decode_phrase(phrase)

    def test_bad_checksum_rejected(self) -> None:
        secret = os.urandom(ROOT_SECRET_LEN)
        phrase = encode_phrase(secret)
        # Swap the last word for a different one from the wordlist --
        # vanishingly unlikely (1/256) to coincidentally satisfy the
        # 8-bit checksum, but try multiple swaps to drive flake risk
        # well below the test budget.
        words = wordlist()
        for candidate in words:
            if candidate == phrase[-1]:
                continue
            mutated = phrase[:-1] + [candidate]
            try:
                decode_phrase(mutated)
            except RecoveryPhraseError as exc:
                if "checksum" in str(exc):
                    return
        pytest.fail("no checksum-failing mutation found in 2047 candidates")
