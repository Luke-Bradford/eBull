"""CAVEMAN: docstring-only mention of ``with conn.transaction():``.

This file mentions the forbidden phrase inside this module docstring
and inside a function docstring below, but contains ZERO actual
``ast.With`` nodes wrapping ``conn.transaction()``. The AST-based lint
guard MUST ignore this file (docstrings are ``Expr(Constant(str))``
nodes, not ``With`` nodes — grep would false-positive here, which is
precisely why the spec mandates an AST walk).
"""

from __future__ import annotations


def y(conn) -> None:
    """Document the rule without violating it.

    A future maintainer should NOT write ``with conn.transaction():``
    inside this function body. The text appears in this docstring only
    to prove the lint guard ignores prose.
    """
    return None
