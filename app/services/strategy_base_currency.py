"""The declared deployment currency (#2603 item 4).

One module holding what "which currency may a strategy deployment trade in" means,
so the answer stops being a ``"USD"`` literal repeated at every comparison.

USD only, and deliberately so: #2363 split ``fx_unmodelled`` out as a standing
refusal, so a non-USD deployment has no honest cost to charge.  #2603 item 4 offers
"support across all the hardcode sites, or one explicit refusal if deferred -- never
a partial lift"; this is the refusal, made enforceable.

Standalone rather than a symbol on ``strategy_control_plane`` so that reaching a
two-line constant does not require importing a 1000-line module.
``normalise_deployment_currency`` returns ``None`` rather than raising for the same
reason: each caller raises its own error type, and a shared exception class would
travel across every module that validates a currency.

⚠ This module does NOT govern the paper pool (``strategy_paper_pool_events``,
``CHECK (currency = 'USD')`` at sql/290:96) or the core mandate
(``strategy_core_mandate_events``, sql/336:26).  Those are separate capital
authorities behind their own schema locks, and pointing them here would let this
constant's widening silently drive tables whose CHECKs had not moved.
"""

from __future__ import annotations

DEPLOYMENT_CURRENCY = "USD"

# ⚠ WIDENING THIS IS A COORDINATED CHANGE, NOT A ONE-LINE EDIT.  Every site below
# reads it, and `sql/338`'s CHECK does not -- a wider set with an unmoved constraint
# fails at INSERT rather than at review.  `tests/test_strategy_base_currency.py::
# test_supported_deployment_currencies_is_usd_only` pins the cardinality so this
# list gets read before the set changes:
#
#   sql/338_strategy_deployment_currency.sql       both CHECK (currency = 'USD')
#   strategy_control_plane.configure_deployment    normalise + refuse
#   strategy_paper_executor.py:374                 stored-deployment membership gate
#   strategy_paper_executor.py:555                 broker eligibility currency == intent
#   strategy_paper_executor.py:593                 cost component currency == intent
#   app/api/strategies.py:1129                     allocation_refusals membership gate
#
# The two executor equality sites are equality ON PURPOSE: see `_eligibility_reason`.
# Also revisit, though they are NOT bound to this constant: sql/290:96 +
# strategy_control_plane.py:313 (paper pool), sql/336:26 +
# strategy_core_mandate.CORE_MANDATE_BASE_CURRENCY (core mandate).
SUPPORTED_DEPLOYMENT_CURRENCIES: frozenset[str] = frozenset({DEPLOYMENT_CURRENCY})

# The operator-visible refusal. Already rendered by `allocation_refusals` on
# /strategies/overview and stored as the executor's rejection reason; this names it
# once so the two cannot drift.
DEPLOYMENT_CURRENCY_UNSUPPORTED = "deployment_currency_unsupported"


def canonical_currency_code(value: str) -> str:
    """Canonical form of ``value``, whether or not it is a supported code.

    ISO 4217 makes upper case the canonical form of a currency code, which is the
    whole of what it licenses here.  ``strip()`` is applied because callers reject
    blank input separately (``_require_text``), so whitespace reaching this point is
    operator slop around a real code rather than a distinguishable empty value.

    Split out of ``normalise_deployment_currency`` so a REFUSAL can name the code it
    actually compared: on the refusal path ``normalise_...`` answers ``None``, so
    there is no output for the message to reuse, and re-deriving ``.strip().upper()``
    at the message would be a second copy of this rule free to drift from it.

    ⚠ Not for broker response fields.  What the broker may put in its ``currency``
    field is governed by its contract, not by ISO, and today the executor rejects
    ``" USD "`` -- widening that would be an unsourced behaviour change.
    """
    return value.strip().upper()


def normalise_deployment_currency(value: str) -> str | None:
    """Canonical form of ``value`` if it is a supported deployment currency, else None."""
    code = canonical_currency_code(value)
    return code if code in SUPPORTED_DEPLOYMENT_CURRENCIES else None


__all__ = [
    "DEPLOYMENT_CURRENCY",
    "DEPLOYMENT_CURRENCY_UNSUPPORTED",
    "SUPPORTED_DEPLOYMENT_CURRENCIES",
    "canonical_currency_code",
    "normalise_deployment_currency",
]
