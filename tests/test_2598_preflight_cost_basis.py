"""``cost_basis`` and its constraint agree (#2598 step 4).

``strategy_entry_preflights.cost_basis`` records WHICH PATH priced
``stressed_cost_amount``. Two independent declarations govern it — the Python
vocabulary the writer binds from, and `sql/342`'s CHECK — and **the CHECK does
not read the constant**. A value added on one side alone fails at INSERT in
production rather than in review, which is the drift this file exists to catch.

⚠ Same shape as #2653's ``test_the_deployment_currency_refusal_and_its_constraint_agree``,
and for the same reason: the safe-looking edit is the one-sided one.

⚠ DB-free. The constraint is read from the migration TEXT, because that file is
the artefact a reviewer changes; an applied database is downstream of it (and
`app/db/migrations.py` hashes applied files, so the text cannot drift silently
from what was applied).
"""

from __future__ import annotations

import pathlib
import re

from app.services.strategy_paper_executor import COST_BASES, COST_BASIS_BROKER_PREFLIGHT

MIGRATION = pathlib.Path(__file__).resolve().parents[1] / "sql" / "342_strategy_entry_preflight_cost_basis.sql"


def _vocabulary_in_the_check() -> set[str]:
    """The literals inside the ``cost_basis IN (...)`` CHECK, from the migration."""
    body = MIGRATION.read_text()
    match = re.search(r"CHECK \(cost_basis IS NULL OR cost_basis IN \(([^)]*)\)\)", body)
    assert match is not None, "the cost_basis vocabulary CHECK is no longer in sql/342 in a readable form"
    return set(re.findall(r"'([^']+)'", match.group(1)))


def test_the_python_vocabulary_and_the_sql_check_are_the_same_set() -> None:
    assert _vocabulary_in_the_check() == set(COST_BASES)


def test_the_writer_binds_a_value_the_check_admits() -> None:
    """The executor binds this literal on every allocated row; a rename on one
    side would otherwise surface as a constraint violation at allocation time."""
    assert COST_BASIS_BROKER_PREFLIGHT in COST_BASES


def test_the_static_band_bound_is_absent_on_purpose() -> None:
    """⚠ THE POINT OF THIS ONE IS THE MESSAGE IT CARRIES, not the assertion.

    #2598's scope text names ``static_band_bound`` as a second basis. It is not
    implemented, and this run's band-stratified census argues against it: the
    worst broker quote observed (ETR, 381.5 bps) is 1.55x the MAXIMUM spread in
    its band's whole calibration snapshot, so no percentile of that snapshot
    bounds what the broker charges. Declaring the value would imply a second
    priced path exists.

    So this failing is not a bug — it is the reminder that adding the value
    means adding the path, the migration and the writer branch together.
    """
    assert "static_band_bound" not in COST_BASES


def test_an_allocated_row_must_carry_a_basis_and_a_rejection_need_not() -> None:
    """A rejection before the cost step priced nothing; recording a basis there
    would record a pricing that never happened."""
    body = MIGRATION.read_text()
    assert "CHECK (verdict <> 'allocated' OR cost_basis IS NOT NULL)" in body
