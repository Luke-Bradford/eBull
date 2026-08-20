"""The §4 strategy catalogue — one module per strategy, all pure.

Parent spec: ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
§4 (the catalogue), §3.5 (execution semantics), §4.0 (the validated universe).
Registry contract: ``app/services/strategy_registry.py`` (phase 3a).

⚠ EVERY MODULE HERE IS PURE — no database, no clock, no IO. A strategy takes a
``BarSeries`` plus the indicator series over it and returns one verdict per bar.
Loading bars, choosing the corpus and resolving the fill all live outside:
``research_price_structure_store.load_masked_series`` reads,
``signal_ledger.resolve_fills`` fills, ``validated_universe`` says who is in
scope. That split is not tidiness — a strategy that could read a bar is a
strategy that could read bar ``t+1``.
"""
