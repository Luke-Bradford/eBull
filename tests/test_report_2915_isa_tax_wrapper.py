from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.report_2915_isa_tax_wrapper import RateScenario, Scenario, calculate, default_results, render_markdown


def _scenario(*, rate_scenario: RateScenario = "all_higher_rate", turnover: str = "0.25") -> Scenario:
    return Scenario(
        rate_scenario=rate_scenario,
        foreign_purchase_consideration_gbp=Decimal("50000"),
        one_way_turnover=Decimal(turnover),
        gain_fraction_of_sold_proceeds=Decimal("0.20"),
        dividend_yield=Decimal("0.02"),
    )


def test_default_rows_are_exact_and_penny_quantized() -> None:
    rows = default_results()
    assert [
        (
            row.rate_scenario,
            row.one_way_turnover,
            row.assumed_realised_gain_gbp,
            row.hypothetical_gia_cgt_gbp,
            row.hypothetical_gia_dividend_tax_gbp,
            row.isa_initial_fx_gbp,
            row.isa_rebalance_fx_gbp,
            row.tax_minus_first_year_isa_fx_gbp,
            row.tax_minus_rebalance_isa_fx_gbp,
        )
        for row in rows
    ] == [
        (
            "all_lower_rate",
            Decimal("0.25"),
            Decimal("2500.00"),
            Decimal("0.00"),
            Decimal("53.75"),
            Decimal("350.00"),
            Decimal("175.00"),
            Decimal("-471.25"),
            Decimal("-121.25"),
        ),
        (
            "all_higher_rate",
            Decimal("0.25"),
            Decimal("2500.00"),
            Decimal("0.00"),
            Decimal("178.75"),
            Decimal("350.00"),
            Decimal("175.00"),
            Decimal("-346.25"),
            Decimal("3.75"),
        ),
        (
            "all_lower_rate",
            Decimal("1"),
            Decimal("10000.00"),
            Decimal("1260.00"),
            Decimal("53.75"),
            Decimal("350.00"),
            Decimal("700.00"),
            Decimal("263.75"),
            Decimal("613.75"),
        ),
        (
            "all_higher_rate",
            Decimal("1"),
            Decimal("10000.00"),
            Decimal("1680.00"),
            Decimal("178.75"),
            Decimal("350.00"),
            Decimal("700.00"),
            Decimal("808.75"),
            Decimal("1158.75"),
        ),
    ]


def test_markdown_table_in_verdict_is_generated_output() -> None:
    verdict = Path("docs/proposals/ta/2026-08-23-r6-tax-wrapper-verdict.md").read_text()
    assert render_markdown(default_results()) in verdict


def test_turnover_can_exceed_one() -> None:
    assert calculate(_scenario(turnover="2")).isa_rebalance_fx_gbp == Decimal("1400.00")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("foreign_purchase_consideration_gbp", Decimal("-1")),
        ("one_way_turnover", Decimal("NaN")),
        ("gain_fraction_of_sold_proceeds", Decimal("1.01")),
        ("dividend_yield", Decimal("Infinity")),
    ],
)
def test_invalid_inputs_refuse(field: str, value: Decimal) -> None:
    with pytest.raises(ValueError):
        calculate(replace(_scenario(), **{field: value}))


def test_runtime_invalid_rate_scenario_refuses() -> None:
    with pytest.raises(ValueError, match="unknown rate scenario"):
        calculate(replace(_scenario(), rate_scenario="additional"))  # type: ignore[arg-type]
