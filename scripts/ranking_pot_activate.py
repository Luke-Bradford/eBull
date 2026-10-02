"""#2842 slice 5c-ii-b — activate (or resume) ranking-pot-v1's executed book (spec §7.3).

Dry run (default) executes the whole transaction and rolls back, printing the capital preview, the ticket minimum and
every refusal:

    PYTHONPATH=. uv run python -m scripts.ranking_pot_activate [--pot-capital <usd>]

⚠ ``--apply`` writes the pot's paper deployment, execution and manager policies, ``POT_CAPITAL`` and the
``executing`` event (or, from a halt, the event alone). It is the supervisor's step in §7.3's sequence (halt v1 →
raise the pool override to ≥ open lifecycles + N → this), run from ``~/Dev/eBull`` at ``origin/main``, never by the
autonomy loop; it refuses from a linked worktree:

    PYTHONPATH=. uv run python -m scripts.ranking_pot_activate --apply --pot-capital <usd> --declared-by <who>

``--pot-capital`` must equal the preview the dry run prints. Exit status: 0 when the step would succeed (dry run),
did (apply) or was already applied (``already_active``); 1 on any refusal.
"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation

import psycopg

from app.config import settings
from app.providers.implementations.etoro_broker import EtoroBrokerProvider
from app.security.unattended_guard import is_linked_worktree
from app.services.ranking_pot_activation import ActivationReport, activate_pot
from app.workers.scheduler import _load_etoro_credentials


def render(report: ActivationReport, *, apply: bool) -> str:
    outcome = "APPLIED" if report.applied else ("dry run (rolled back)" if not apply else "NOT applied")
    lines = [
        f"ranking-pot-v1 {report.mode}: {outcome}",
        f"declaration: {report.declaration_id}  state: {report.state}  POT_CAPITAL: {report.pot_capital}",
    ]
    if report.preview is not None:
        p = report.preview
        lines.append(f"preview: {p.preview} (binding {p.binding_term} = {p.binding_value})")
        lines += [f"  {name}: {value}" for name, value in p.terms.items()]
    if report.minimum is not None:
        lines.append(f"largest broker open minimum over S0: {report.minimum}")
    if report.detail:
        lines.append(f"detail: {report.detail}")
    lines.append(f"refusals: {list(report.refusals) or 'none'}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="commit (supervisor only, from the main checkout)")
    parser.add_argument("--pot-capital", default=None, help="POT_CAPITAL in USD: the dry run's preview")
    parser.add_argument("--declared-by", default="")
    args = parser.parse_args(argv)

    if args.apply and is_linked_worktree():
        print("refused: --apply runs from the main checkout only (this is a linked worktree)")
        return 1
    if args.apply and not args.declared_by.strip():
        print("refused: --apply requires --declared-by")
        return 1
    try:
        pot_capital = None if args.pot_capital is None else Decimal(args.pot_capital)
    except InvalidOperation:
        print(f"refused: --pot-capital {args.pot_capital!r} is not a number")
        return 1
    if settings.etoro_env != "demo":
        print("refused: the ranking pot is demo-only")
        return 1
    creds = _load_etoro_credentials("ranking_pot_activate")
    if creds is None:
        print("refused: eToro demo credentials are missing")
        return 1
    api_key, user_key = creds
    try:
        with (
            EtoroBrokerProvider(api_key=api_key, user_key=user_key, env="demo") as broker,
            psycopg.connect(settings.database_url) as conn,
        ):
            report = activate_pot(
                conn,
                fetch_risk=broker.get_account_risk_snapshot,
                pot_capital=pot_capital,
                declared_by=args.declared_by,
                apply=args.apply,
            )
    except (psycopg.Error, OSError) as exc:
        print(f"activation FAILED: {type(exc).__name__}: {exc}\nRun the dry run before retrying.")
        return 1
    print(render(report, apply=args.apply))
    return 0 if not report.refusals else 1


if __name__ == "__main__":
    raise SystemExit(main())
