"""Recovery phrase encoding for the local secret bootstrap (#114 / ADR-0003).

A recovery phrase is a 24-word encoding of a 32-byte root secret. It uses
the BIP39 English wordlist (vendored under ``wordlist_english.txt``) but
this module deliberately does not expose any "BIP39" / "mnemonic" / "seed
phrase" naming -- the operator-facing concept is "recovery phrase" and
nothing else, per ADR-0003.

Encoding:
  * 256 bits of entropy (the root secret) + 8-bit checksum
    (``SHA256(entropy)[0]``) = 264 bits
  * Split into 24 11-bit groups, each indexing the 2048-word list

The checksum lets us reject typos locally before any HKDF/decryption
attempt -- a tampered phrase fails ``decode_phrase`` with a clear error
rather than silently producing garbage entropy.

Pure functions, no I/O beyond loading the vendored wordlist file once at
import time.
"""

from __future__ import annotations

import hashlib
from importlib.resources import files

ROOT_SECRET_LEN = 32
PHRASE_WORD_COUNT = 24
_BITS_PER_WORD = 11
_WORDLIST_LEN = 2048


class RecoveryPhraseError(ValueError):
    """Raised when a recovery phrase fails validation."""


def _load_wordlist() -> tuple[list[str], dict[str, int]]:
    raw = files("app.security").joinpath("wordlist_english.txt").read_text(encoding="utf-8")
    words = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(words) != _WORDLIST_LEN:
        raise RuntimeError(f"recovery_phrase wordlist must be exactly {_WORDLIST_LEN} entries (got {len(words)})")
    return words, {word: idx for idx, word in enumerate(words)}


_WORDLIST, _WORD_INDEX = _load_wordlist()


def encode_phrase(root_secret: bytes) -> list[str]:
    """Encode a 32-byte root secret as a 24-word recovery phrase."""
    if len(root_secret) != ROOT_SECRET_LEN:
        raise RecoveryPhraseError(f"root secret must be exactly {ROOT_SECRET_LEN} bytes (got {len(root_secret)})")
    checksum = hashlib.sha256(root_secret).digest()[0]
    # 256 bits of entropy + 8 bits of checksum = 264 bits, packed big-endian
    bits = int.from_bytes(root_secret, "big") << 8 | checksum
    words: list[str] = []
    for i in range(PHRASE_WORD_COUNT):
        shift = (PHRASE_WORD_COUNT - 1 - i) * _BITS_PER_WORD
        idx = (bits >> shift) & (_WORDLIST_LEN - 1)
        words.append(_WORDLIST[idx])
    return words


def decode_phrase(phrase: list[str] | str) -> bytes:
    """Decode a recovery phrase back to its 32-byte root secret.

    Accepts either a list of 24 words or a single whitespace-separated
    string. Raises :class:`RecoveryPhraseError` on any validation failure
    (wrong length, unknown word, bad checksum).
    """
    if isinstance(phrase, str):
        words = phrase.strip().split()
    else:
        words = [w.strip().lower() for w in phrase]
    if len(words) != PHRASE_WORD_COUNT:
        raise RecoveryPhraseError(f"recovery phrase must be exactly {PHRASE_WORD_COUNT} words (got {len(words)})")
    bits = 0
    for position, word in enumerate(words, start=1):
        idx = _WORD_INDEX.get(word.lower())
        if idx is None:
            raise RecoveryPhraseError(f"word {position} is not recognised")
        bits = (bits << _BITS_PER_WORD) | idx
    # 264 bits total: top 256 are entropy, bottom 8 are the checksum
    checksum = bits & 0xFF
    entropy_int = bits >> 8
    root_secret = entropy_int.to_bytes(ROOT_SECRET_LEN, "big")
    expected = hashlib.sha256(root_secret).digest()[0]
    if checksum != expected:
        raise RecoveryPhraseError("recovery phrase checksum is invalid")
    return root_secret


def wordlist() -> list[str]:
    """Return a copy of the wordlist (used by tests and the frontend port)."""
    return list(_WORDLIST)
