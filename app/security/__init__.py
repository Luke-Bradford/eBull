"""Security primitives for eBull operator auth.

Split into:
  * passwords -- Argon2id hashing / verification helpers.
  * sessions  -- DB-backed opaque session storage and lookup.

These modules are deliberately small and free of HTTP concerns so they can
be unit-tested without spinning up FastAPI.
"""
