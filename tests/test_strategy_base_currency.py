"""The declared deployment currency and its normaliser (#2603 item 4)."""

from __future__ import annotations

import pytest

from app.services.strategy_base_currency import (
    DEPLOYMENT_CURRENCY,
    DEPLOYMENT_CURRENCY_UNSUPPORTED,
    SUPPORTED_DEPLOYMENT_CURRENCIES,
    canonical_currency_code,
    normalise_deployment_currency,
)


def test_supported_deployment_currencies_is_usd_only() -> None:
    """Bump-visibility pin: widening the set is a coordinated change, not a one-liner.

    #2603 item 4 says "never a partial lift", which is otherwise a sentence in a
    ticket.  This makes it a gate: adding a currency fails here until whoever adds it
    has read the site list on ``SUPPORTED_DEPLOYMENT_CURRENCIES`` and moved all of it.

    Sites that must move together:

    * ``sql/338_strategy_deployment_currency.sql`` -- two CHECKs, both literal 'USD'.
      A wider Python set with an unmoved constraint fails at INSERT, not at review.
    * ``strategy_control_plane.configure_deployment`` -- normalise + refuse.
    * ``strategy_paper_executor.py`` -- the stored-deployment membership gate.
    * ``app/api/strategies.py`` -- the ``allocation_refusals`` membership gate.
    * ``strategy_paper_executor._eligibility_reason`` / ``._costs`` -- these compare
      for EQUALITY against ``intent.currency`` rather than membership, precisely so a
      wider set cannot let ``_costs`` sum two currencies without FX.  Confirm that is
      still true before widening.
    * Separately locked, NOT bound to this constant, and needing their own decision:
      the paper pool (``sql/290:96`` + ``strategy_control_plane.py:313``) and the core
      mandate (``sql/336:26`` + ``strategy_core_mandate.CORE_MANDATE_BASE_CURRENCY``).

    And the reason the set is one member at all: FX is unmodelled (#2363), so a
    non-USD deployment has no honest cost to charge.
    """
    assert SUPPORTED_DEPLOYMENT_CURRENCIES == frozenset({"USD"})


def test_the_declared_currency_is_the_one_the_default_call_path_uses() -> None:
    """``configure_deployment()`` with no currency argument must not refuse itself.

    ⚠ Deliberately NOT ``DEPLOYMENT_CURRENCY in SUPPORTED_DEPLOYMENT_CURRENCIES``.
    That reads like the invariant ``docs/review-prevention-log.md:2081`` asks for --
    two constants that must agree, bound by a test -- but the set is *derived* from
    the scalar (``frozenset({DEPLOYMENT_CURRENCY})``), so the assertion holds for
    every possible value of either.  A revert-probe setting the scalar to ``"EUR"``
    left it green: a reference that imports the constant it validates is a tautology.

    Construction already guarantees agreement, which is stronger than any test.  What
    is worth pinning is the value the zero-argument path actually persists, and
    ``test_supported_deployment_currencies_is_usd_only`` is what catches a change to
    it -- the probe above turns that one red.
    """
    assert DEPLOYMENT_CURRENCY == "USD"
    assert normalise_deployment_currency(DEPLOYMENT_CURRENCY) == DEPLOYMENT_CURRENCY


def test_the_supported_set_cannot_be_widened_at_runtime() -> None:
    """A mutable set would let any import site silently widen authorisation."""
    assert isinstance(SUPPORTED_DEPLOYMENT_CURRENCIES, frozenset)
    with pytest.raises(AttributeError):
        SUPPORTED_DEPLOYMENT_CURRENCIES.add("GBP")  # type: ignore[attr-defined]


@pytest.mark.parametrize("supplied", ["USD", "usd", "Usd", " USD", "usd ", "  uSd  "])
def test_normalise_canonicalises_case_and_operator_whitespace(supplied: str) -> None:
    """ISO 4217 makes upper case canonical; blank input is rejected upstream.

    The round-trip this protects: both ``configure_deployment`` call sites feed back a
    value read out of the database, and ``is_risk_reducing_deployment_change`` compares
    it to the stored one with ``==``.  Without canonicalisation an operator-supplied
    ``"usd"`` would read as a currency CHANGE against a stored ``"USD"``, silently
    making an otherwise risk-reducing edit non-risk-reducing.
    """
    assert normalise_deployment_currency(supplied) == "USD"


@pytest.mark.parametrize("supplied", ["GBP", "EUR", "gbp", "", "   ", "US", "USDD", "$"])
def test_normalise_returns_none_for_anything_unsupported(supplied: str) -> None:
    assert normalise_deployment_currency(supplied) is None


@pytest.mark.parametrize("supplied", ["USD", "usd", " uSd ", "gbp", " eur "])
def test_the_refusal_path_canonicalises_by_the_same_rule_as_the_admit_path(
    supplied: str,
) -> None:
    """One rule, both paths -- the refusal message cannot canonicalise differently.

    ``normalise_deployment_currency`` answers ``None`` exactly when a caller needs to
    NAME the offending code, so the refusal has no output of its own to reuse and the
    obvious shortcut is a second ``.strip().upper()`` at the message.  That copy is
    free to drift: widen canonicalisation here (say, to fold a full-width ``ＵＳＤ``)
    and the message would go on reporting a code that was never the one compared.

    Pinned by construction rather than by asserting both spellings: ``normalise_...``
    calls ``canonical_currency_code``, so agreement on the ADMIT side is definitional.
    What this adds is the REFUSE side, where nothing structural forces it.
    """
    canonical = canonical_currency_code(supplied)
    assert canonical == supplied.strip().upper()
    if canonical in SUPPORTED_DEPLOYMENT_CURRENCIES:
        assert normalise_deployment_currency(supplied) == canonical
    else:
        assert normalise_deployment_currency(supplied) is None


def test_the_refusal_code_is_the_one_already_rendered_to_the_operator() -> None:
    """Not a new surface -- the name the overview and the executor already emit.

    ``allocation_refusals`` on ``/strategies/overview`` and the executor's stored
    rejection reason both carry this string, and the frontend keys off it.  Renaming
    the constant without renaming those is the drift this pins.
    """
    assert DEPLOYMENT_CURRENCY_UNSUPPORTED == "deployment_currency_unsupported"
