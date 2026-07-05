import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { OfferingBlock } from "@/components/instrument/OfferingBlock";
import type { OfferingSummary } from "@/api/types";

// Live dev fixture: FPS 424B4 0001193125-26-294982 (verified against the
// filed cover — $49.00/share, $2,138,850,000 aggregate).
function pricedOffering(): OfferingSummary {
  return {
    subtype: "424B4",
    is_issuer_offering: true,
    price_per_unit: 49.0,
    unit_label: "Per Share",
    aggregate_offering_amount: 2_138_850_000,
    underwriting_discount: 53_471_250,
    net_proceeds_to_issuer: 695_409_317,
    proceeds_to_selling_holders: 1_389_969_433,
    currency: "USD",
    security_type: "Common Stock",
  };
}

// Live dev fixture: ADT 424B7 0001703056-26-000092 — resale prospectus,
// no priced cover table; every money field is null by contract.
function resaleOffering(): OfferingSummary {
  return {
    subtype: "424B7",
    is_issuer_offering: null,
    price_per_unit: null,
    unit_label: null,
    aggregate_offering_amount: null,
    underwriting_discount: null,
    net_proceeds_to_issuer: null,
    proceeds_to_selling_holders: null,
    currency: "USD",
    security_type: "Common Stock",
  };
}

describe("OfferingBlock", () => {
  it("renders every present money field with label + formatted value", () => {
    render(<OfferingBlock offering={pricedOffering()} />);
    expect(screen.getByText("Common Stock")).toBeInTheDocument();
    expect(screen.getByText("issuer offering")).toBeInTheDocument();
    // price via formatMoney(49, "USD"), magnitudes via formatBigNumber
    expect(screen.getByText("Per Share")).toBeInTheDocument();
    expect(screen.getByText(/\$49\.00/)).toBeInTheDocument();
    expect(screen.getByText("Aggregate")).toBeInTheDocument();
    expect(screen.getByText("2.14B")).toBeInTheDocument();
    expect(screen.getByText("Underwriting discount")).toBeInTheDocument();
    expect(screen.getByText("53.47M")).toBeInTheDocument();
    expect(screen.getByText("Net to issuer")).toBeInTheDocument();
    expect(screen.getByText("695.41M")).toBeInTheDocument();
    expect(screen.getByText("To selling holders")).toBeInTheDocument();
    expect(screen.getByText("1.39B")).toBeInTheDocument();
  });

  it("renders the honest no-priced-cover line when all money fields are null", () => {
    render(<OfferingBlock offering={resaleOffering()} />);
    expect(screen.getByText("Common Stock")).toBeInTheDocument();
    // is_issuer_offering=null → neutral label, not a guessed direction
    expect(screen.getByText("offering")).toBeInTheDocument();
    expect(
      screen.getByText(/No priced cover table in this prospectus/),
    ).toBeInTheDocument();
    expect(screen.queryByText("Aggregate")).not.toBeInTheDocument();
  });

  it("labels a resale explicitly when is_issuer_offering is false", () => {
    render(
      <OfferingBlock
        offering={{ ...resaleOffering(), is_issuer_offering: false }}
      />,
    );
    expect(screen.getByText("resale by holders")).toBeInTheDocument();
  });
});
