/**
 * Unit tests for HistoricalSymbolCallout (#794 frontend finish, Batch
 * 7 of #788).
 */

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { HistoricalSymbolCallout } from "./HistoricalSymbolCallout";

describe("HistoricalSymbolCallout", () => {
  it("renders nothing when there are no historical symbols", () => {
    const { container } = render(
      <HistoricalSymbolCallout currentSymbol="AAPL" historicalSymbols={[]} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when the only symbol matches the current one", () => {
    const { container } = render(
      <HistoricalSymbolCallout
        currentSymbol="AAPL"
        historicalSymbols={[
          {
            symbol: "AAPL",
            effective_from: "2010-01-01",
            effective_to: null,
            source_event: "imported",
          },
        ]}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders the callout when a prior symbol is in the chain", () => {
    render(
      <HistoricalSymbolCallout
        currentSymbol="BBBYQ"
        historicalSymbols={[
          {
            symbol: "BBBY",
            effective_from: "2000-06-01",
            effective_to: "2023-04-01",
            source_event: "delisting",
          },
          {
            symbol: "BBBYQ",
            effective_from: "2023-04-01",
            effective_to: null,
            source_event: "relisting",
          },
        ]}
      />,
    );
    // The component uses ``data-test=`` (the repo's convention) +
    // ``role="note"``; query by role since testing-library's
    // ``getByTestId`` looks for ``data-testid`` and the repo
    // doesn't standardise on that name.
    const callout = screen.getByRole("note");
    expect(callout).toBeInTheDocument();
    expect(callout.textContent).toContain("BBBY");
    expect(callout.textContent).toContain("BBBYQ");
    expect(callout.textContent).toContain("2023-04-01");
  });

  it("treats the current symbol comparison case-insensitively", () => {
    const { container } = render(
      <HistoricalSymbolCallout
        currentSymbol="aapl"
        historicalSymbols={[
          {
            symbol: "AAPL",
            effective_from: "2010-01-01",
            effective_to: null,
            source_event: "imported",
          },
        ]}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});
