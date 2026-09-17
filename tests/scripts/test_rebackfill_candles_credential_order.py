"""#2414 — ``scripts/rebackfill_candles_5y.py`` must load the broker key, and late.

That script is the repo's SOLE ``force_backfill=True`` caller
(``app/services/market_data.py:981``), which makes it the only path that can push
a bar below an instrument's stored minimum — i.e. the only thing that exercises
#2414's ``price_daily_backdated_insert`` writer. Two ordering defects made it
unrunnable, and neither was visible to any existing check because no test imports
it and no scheduled job calls it:

* **I1 — it never loaded the broker-encryption key.** ``load_credential_for_provider_use``
  decrypts, so every invocation raised ``MasterKeyNotLoadedError`` from
  ``secrets_crypto._get_aesgcm`` before doing any work. Dead since #661
  (``c6a80509``) made decryption key-gated.
* **I2 — credentials were loaded BEFORE the ``--apply`` branch**, so the dry run
  the module docstring promises makes "no API calls" still required a decryptable
  key. The mode that exists to be safe on an unconfigured host was the mode that
  could not run there.

Asserted on the AST rather than at runtime: the invariant is an ORDERING inside
``main()``, and exercising it for real would need a DB connection and live
credentials, which is a heavier test for a weaker guarantee.

⚠ ``ensure_broker_key_loaded`` and NOT ``master_key.bootstrap`` — bootstrap runs
``_revoke_stale_ciphertext`` unconditionally, so on a host with no derivable key
it soft-revokes every broker credential on its way to failing. I3 pins that too:
a future edit that "fixes" I1 with bootstrap would reintroduce a mutation into a
script whose dry run is supposed to touch nothing.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "rebackfill_candles_5y.py"


def _main_func() -> ast.FunctionDef:
    tree = ast.parse(SCRIPT.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return node
    raise AssertionError(f"{SCRIPT} has no module-level main()")


def _first_call_line(scope: ast.AST, name: str) -> int | None:
    """Line of the first call to ``name`` anywhere inside ``scope``."""
    lines = [
        node.lineno
        for node in ast.walk(scope)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name
    ]
    return min(lines) if lines else None


def _dry_run_guard_line(main: ast.FunctionDef) -> int:
    """Line of the ``if not args.apply:`` early-return guard."""
    for node in ast.walk(main):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if (
            isinstance(test, ast.UnaryOp)
            and isinstance(test.op, ast.Not)
            and isinstance(test.operand, ast.Attribute)
            and test.operand.attr == "apply"
        ):
            assert any(isinstance(stmt, ast.Return) for stmt in node.body), (
                "the `if not args.apply:` branch must RETURN — a dry run that falls "
                "through would reach the credential load it exists to avoid"
            )
            return node.lineno
    raise AssertionError("no `if not args.apply:` early-return guard found in main()")


def test_i1_key_is_loaded_before_credentials_are_decrypted() -> None:
    main = _main_func()
    ensure = _first_call_line(main, "ensure_broker_key_loaded")
    load = _first_call_line(main, "load_credential_for_provider_use")
    assert ensure is not None, (
        "main() never calls ensure_broker_key_loaded — decryption is key-gated (#661), "
        "so every run raises MasterKeyNotLoadedError before doing any work"
    )
    assert load is not None, "main() no longer loads eToro credentials at all"
    assert ensure < load, (
        f"ensure_broker_key_loaded (line {ensure}) must precede "
        f"load_credential_for_provider_use (line {load})"
    )


def test_i2_dry_run_returns_before_any_credential_load() -> None:
    main = _main_func()
    guard = _dry_run_guard_line(main)
    load = _first_call_line(main, "load_credential_for_provider_use")
    assert load is not None, "main() no longer loads eToro credentials at all"
    assert guard < load, (
        f"the `if not args.apply:` early return (line {guard}) must precede the "
        f"credential load (line {load}); the docstring promises a dry run makes no "
        f"API calls, and decrypting needs a key an unconfigured host does not have"
    )


def test_i3_script_does_not_call_bootstrap() -> None:
    """bootstrap() soft-revokes every credential when no key is derivable."""
    tree = ast.parse(SCRIPT.read_text())
    called = {
        node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    assert "bootstrap" not in called, (
        "master_key.bootstrap() runs _revoke_stale_ciphertext unconditionally; use "
        "ensure_broker_key_loaded, its read-only counterpart (#1265)"
    )
