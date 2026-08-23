"""Reproduce #2915's frozen 2026/27 tax-and-FX sensitivity.

This is not a personal tax estimate. It compares hypothetical GIA tax under
explicit assumptions with the eToro ISA's full FX charge. Other costs are out
of scope and must not be inferred from the difference column.

Default construction: GBP 50,000 foreign-asset purchase consideration; 20%
positive gain as a fraction of sold proceeds; 2% cash dividend yield; and
25%/100% one-way annual turnover. The Personal Allowance is fully consumed by
other income, while the CGT and dividend allowances are wholly unused. The
initial 0.70% FX fee is additional to the purchase consideration.

Usage:
    uv run python -m scripts.report_2915_isa_tax_wrapper
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Final, Literal

RateScenario = Literal["all_lower_rate", "all_higher_rate"]

# Frozen 2026/27 constants. Importing the application's current-year constant
# would let future legislation silently rewrite this dated report.
CGT_ALLOWANCE_2026_27: Final = Decimal("3000")
DIVIDEND_ALLOWANCE_2026_27: Final = Decimal("500")
ISA_FX_RATE_2026_07_29: Final = Decimal("0.007")
CGT_RATES_2026_27: Final[dict[RateScenario, Decimal]] = {
    "all_lower_rate": Decimal("0.18"),
    "all_higher_rate": Decimal("0.24"),
}
DIVIDEND_RATES_2026_27: Final[dict[RateScenario, Decimal]] = {
    "all_lower_rate": Decimal("0.1075"),
    "all_higher_rate": Decimal("0.3575"),
}
PENNY: Final = Decimal("0.01")


@dataclass(frozen=True)
class Scenario:
    rate_scenario: RateScenario
    foreign_purchase_consideration_gbp: Decimal
    one_way_turnover: Decimal
    gain_fraction_of_sold_proceeds: Decimal
    dividend_yield: Decimal


@dataclass(frozen=True)
class Result:
    rate_scenario: RateScenario
    one_way_turnover: Decimal
    sold_notional_gbp: Decimal
    assumed_realised_gain_gbp: Decimal
    assumed_dividends_gbp: Decimal
    hypothetical_gia_cgt_gbp: Decimal
    hypothetical_gia_dividend_tax_gbp: Decimal
    hypothetical_gia_total_tax_gbp: Decimal
    isa_initial_fx_gbp: Decimal
    isa_rebalance_fx_gbp: Decimal
    tax_minus_first_year_isa_fx_gbp: Decimal
    tax_minus_rebalance_isa_fx_gbp: Decimal


def _money(value: Decimal) -> Decimal:
    return value.quantize(PENNY, rounding=ROUND_HALF_UP)


def _finite_nonnegative(name: str, value: Decimal) -> None:
    if not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be finite and non-negative")


def calculate(scenario: Scenario) -> Result:
    """Calculate the explicit sensitivity; do not treat it as a tax return."""
    _finite_nonnegative("foreign purchase consideration", scenario.foreign_purchase_consideration_gbp)
    _finite_nonnegative("turnover", scenario.one_way_turnover)
    for name, value in (
        ("gain fraction", scenario.gain_fraction_of_sold_proceeds),
        ("dividend yield", scenario.dividend_yield),
    ):
        _finite_nonnegative(name, value)
        if value > 1:
            raise ValueError(f"{name} must be in [0, 1]")
    if scenario.rate_scenario not in CGT_RATES_2026_27:
        raise ValueError(f"unknown rate scenario: {scenario.rate_scenario}")

    consideration = scenario.foreign_purchase_consideration_gbp
    sold = consideration * scenario.one_way_turnover
    gain = sold * scenario.gain_fraction_of_sold_proceeds
    dividends = consideration * scenario.dividend_yield
    cgt = max(gain - CGT_ALLOWANCE_2026_27, Decimal(0)) * CGT_RATES_2026_27[scenario.rate_scenario]
    dividend_tax = (
        max(dividends - DIVIDEND_ALLOWANCE_2026_27, Decimal(0)) * DIVIDEND_RATES_2026_27[scenario.rate_scenario]
    )
    gia_tax = cgt + dividend_tax

    initial_fx = consideration * ISA_FX_RATE_2026_07_29
    # One-way turnover is sold notional / mean equity. This frozen all-foreign
    # sensitivity replaces the same gross notional: one sell and one buy.
    rebalance_fx = sold * ISA_FX_RATE_2026_07_29 * 2
    return Result(
        rate_scenario=scenario.rate_scenario,
        one_way_turnover=scenario.one_way_turnover,
        sold_notional_gbp=_money(sold),
        assumed_realised_gain_gbp=_money(gain),
        assumed_dividends_gbp=_money(dividends),
        hypothetical_gia_cgt_gbp=_money(cgt),
        hypothetical_gia_dividend_tax_gbp=_money(dividend_tax),
        hypothetical_gia_total_tax_gbp=_money(gia_tax),
        isa_initial_fx_gbp=_money(initial_fx),
        isa_rebalance_fx_gbp=_money(rebalance_fx),
        tax_minus_first_year_isa_fx_gbp=_money(gia_tax - initial_fx - rebalance_fx),
        tax_minus_rebalance_isa_fx_gbp=_money(gia_tax - rebalance_fx),
    )


def default_results() -> list[Result]:
    return [
        calculate(
            Scenario(
                rate_scenario=rate_scenario,
                foreign_purchase_consideration_gbp=Decimal("50000"),
                one_way_turnover=turnover,
                gain_fraction_of_sold_proceeds=Decimal("0.20"),
                dividend_yield=Decimal("0.02"),
            )
        )
        for turnover in (Decimal("0.25"), Decimal("1"))
        for rate_scenario in ("all_lower_rate", "all_higher_rate")
    ]


def _gbp(value: Decimal) -> str:
    sign = "−" if value < 0 else "+" if value > 0 else ""
    return f"{sign}£{abs(value):,.2f}"


def render_markdown(results: list[Result]) -> str:
    lines = [
        "| rate scenario / turnover | assumed realised gain | hypothetical GIA CGT | "
        "hypothetical GIA dividend tax | ISA initial FX | ISA rebalance FX | "
        "tax minus ISA FX, first year | tax minus ISA FX, no initial purchase |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    labels = {"all_lower_rate": "all lower rate", "all_higher_rate": "all higher rate"}
    for result in results:
        turnover = result.one_way_turnover * 100
        lines.append(
            f"| {labels[result.rate_scenario]} / {turnover:g}% | "
            f"£{result.assumed_realised_gain_gbp:,.2f} | £{result.hypothetical_gia_cgt_gbp:,.2f} | "
            f"£{result.hypothetical_gia_dividend_tax_gbp:,.2f} | £{result.isa_initial_fx_gbp:,.2f} | "
            f"£{result.isa_rebalance_fx_gbp:,.2f} | **{_gbp(result.tax_minus_first_year_isa_fx_gbp)}** | "
            f"**{_gbp(result.tax_minus_rebalance_isa_fx_gbp)}** |"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    print(render_markdown(default_results()))


if __name__ == "__main__":
    main()
