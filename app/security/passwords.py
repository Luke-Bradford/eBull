"""Argon2id password hashing for the operator credential.

Why Argon2id (not bcrypt / scrypt / pbkdf2):
  * Argon2id is the OWASP-recommended default for new password storage.
  * Its memory-hard parameters resist GPU brute force, which is the realistic
    threat for an offline hash leak.
  * The PHC string output encodes the algorithm + parameters + salt + hash
    in one column, so we can re-tune parameters in the future without a
    schema migration.

Tuning policy (per ADR 0001):
  Parameters target ~250-500 ms on the deployment host. We do **not** pin a
  fixed cost in code -- ``argon2-cffi`` defaults are reasonable for typical
  hardware and the parameters are stored inside each hash, so re-hashing on
  next successful login can migrate users to stronger parameters when needed.
  We do not implement an automatic re-hash path in v1; that is tracked as
  out-of-scope optional work.

Generic 401 discipline:
  ``verify_password`` returns a bool. The HTTP layer is responsible for
  returning the same generic 401 for "no such user" and "wrong password" --
  this module never raises a value-leaking exception.
"""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

# Module-level hasher: PasswordHasher is thread-safe and stateless aside
# from its tuning parameters, so a single instance is fine and avoids the
# (small) per-call construction cost.
_hasher = PasswordHasher()


def hash_password(plaintext: str) -> str:
    """Return an Argon2id PHC string for *plaintext*.

    The returned string contains algorithm, parameters, salt, and hash. It
    is safe to store directly in a TEXT column.
    """
    return _hasher.hash(plaintext)


def verify_password(plaintext: str, stored_hash: str) -> bool:
    """Constant-time-ish verify of *plaintext* against *stored_hash*.

    Returns True on match, False on any failure mode -- mismatch, malformed
    hash, or unexpected verification error. Callers must not branch on the
    failure mode for the HTTP response: a missing operator and a wrong
    password must produce the same generic 401.
    """
    try:
        return _hasher.verify(stored_hash, plaintext)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False
