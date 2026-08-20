import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { TenderBlock } from "@/components/instrument/TenderBlock";
import type { TenderEventSummary } from "@/api/types";

// Live dev fixture: Oracle's third-party tender for NetSuite, SC TO-T
// 0001193125-16-684804 — $109.00/share, offeror ORACLE CORP.
function oracleNetsuite(): TenderEventSummary {
  return {
    role: "offeror",
    subject_company_name: "NETSUITE INC",
    offeror_names: ["ORACLE CORP"],
    is_third_party_tender: true,
    is_issuer_tender: false,
    is_going_private: false,
    amends_13d: false,
    is_final_amendment: false,
    amendment_no: null,
    offer_price_per_unit: 109.0,
    unit_label: "Share",
    currency: "USD",
    expiration_date: null,
    board_recommendation: null,
  };
}

// Live dev fixture: Healthcare Trust's SC 14D9 0001104659-20-031431 responding to
// the Comrit $8.61 mini-tender — board recommends rejecting; no transaction-type
// checkboxes on a 14D-9 (all null); expires 2020-04-30.
function nhpComrit14d9(): TenderEventSummary {
  return {
    role: "subject",
    subject_company_name: "Healthcare Trust, Inc.",
    offeror_names: null,
    is_third_party_tender: null,
    is_issuer_tender: null,
    is_going_private: null,
    amends_13d: null,
    is_final_amendment: null,
    amendment_no: null,
    offer_price_per_unit: 8.61,
    unit_label: null,
    currency: "USD",
    expiration_date: "2020-04-30",
    board_recommendation: "reject",
  };
}

describe("TenderBlock", () => {
  it("renders a priced third-party offeror tender with parties + price", () => {
    render(<TenderBlock tender={oracleNetsuite()} />);
    expect(screen.getByText(/Third-party tender/)).toBeInTheDocument();
    expect(screen.getByText("offeror")).toBeInTheDocument(); // lowercase muted role
    expect(screen.getByText("Subject")).toBeInTheDocument();
    expect(screen.getByText("NETSUITE INC")).toBeInTheDocument();
    expect(screen.getByText("Offeror")).toBeInTheDocument();
    expect(screen.getByText("ORACLE CORP")).toBeInTheDocument();
    expect(screen.getByText(/109\.00 \/ Share/)).toBeInTheDocument(); // formatMoney(USD)
  });

  it("renders a subject 14D9 with board recommendation + expiry, neutral type, no offeror", () => {
    render(<TenderBlock tender={nhpComrit14d9()} />);
    // all transaction-type checkboxes null → neutral label, never guessed
    expect(screen.getByText("Tender offer")).toBeInTheDocument();
    expect(screen.getByText("subject")).toBeInTheDocument();
    expect(screen.getByText("Healthcare Trust, Inc.")).toBeInTheDocument();
    expect(screen.getByText("Recommends rejecting")).toBeInTheDocument();
    expect(screen.getByText("30 Apr 2020")).toBeInTheDocument();
    expect(screen.getByText(/8\.61/)).toBeInTheDocument();
    expect(screen.queryByText("Offeror")).not.toBeInTheDocument(); // offeror_names null
  });

  it("renders ALL true transaction-type flags — never collapses orthogonal checkboxes", () => {
    render(
      <TenderBlock
        tender={{ ...oracleNetsuite(), is_going_private: true }}
      />,
    );
    // both flags true → both render in the header, not just a priority pick
    expect(screen.getByText(/Third-party tender/)).toBeInTheDocument();
    expect(screen.getByText(/Going-private/)).toBeInTheDocument();
  });

  it("appends a final-amendment status suffix", () => {
    render(
      <TenderBlock
        tender={{ ...oracleNetsuite(), is_final_amendment: true }}
      />,
    );
    expect(screen.getByText(/final amendment/)).toBeInTheDocument();
  });

  it("renders a bare number (no fabricated currency) when currency is null", () => {
    render(<TenderBlock tender={{ ...oracleNetsuite(), currency: null }} />);
    // formatNumber fallback — no "US$"/"£" symbol invented from a null currency
    expect(screen.getByText("109 / Share")).toBeInTheDocument();
  });
});
