import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { OwnershipFreshnessChips } from "./OwnershipFreshnessChips";
import type { SunburstCategory, SunburstRings } from "./ownershipRings";

const TODAY = new Date("2026-05-02T00:00:00Z");

function category(
  key: SunburstCategory["key"],
  as_of_date: string | null,
  shares: number = 100,
): SunburstCategory {
  const labels: Record<SunburstCategory["key"], string> = {
    institutions: "Institutions",
    etfs: "ETFs",
    insiders: "Insiders",
    treasury: "Treasury",
  };
  return {
    key,
    label: labels[key],
    shares,
    reported_total: shares,
    resolved_leaf_shares: shares,
    leaves: [],
    within_category_gap: 0,
    as_of_date,
  };
}

function rings(categories: SunburstCategory[]): SunburstRings {
  return {
    total_shares: 1_000_000_000,
    reported_total: 1_000_000_000,
    categories,
    category_residual: 0,
  };
}

describe("OwnershipFreshnessChips", () => {
  it("renders nothing when there are no categories", () => {
    const { container } = render(
      <OwnershipFreshnessChips rings={rings([])} today={TODAY} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("renders one chip per category in input order", () => {
    render(
      <OwnershipFreshnessChips
        rings={rings([
          category("institutions", "2026-03-31"),
          category("etfs", "2026-03-31"),
          category("insiders", "2026-04-30"),
          category("treasury", "2026-03-28"),
        ])}
        today={TODAY}
      />,
    );
    const chips = screen.getAllByRole("listitem");
    expect(chips).toHaveLength(4);
    expect(chips[0]!.textContent).toMatch(/Institutions/);
    expect(chips[3]!.textContent).toMatch(/Treasury/);
  });

  it("colour-codes by freshness level via data-freshness-level", () => {
    render(
      <OwnershipFreshnessChips
        rings={rings([
          // 32 days → insiders cadence flips to aging (>= 30d).
          category("insiders", "2026-03-31"),
          // 200+ days → treasury cadence stale.
          category("treasury", "2025-10-01"),
          // 30 days → institutions cadence is fresh (well below 135d).
          category("institutions", "2026-04-02"),
        ])}
        today={TODAY}
      />,
    );
    const insiders = screen.getByText("Insiders").closest("[data-freshness-level]");
    const treasury = screen.getByText("Treasury").closest("[data-freshness-level]");
    const institutions = screen
      .getByText("Institutions")
      .closest("[data-freshness-level]");
    expect(insiders!.getAttribute("data-freshness-level")).toBe("aging");
    expect(treasury!.getAttribute("data-freshness-level")).toBe("stale");
    expect(institutions!.getAttribute("data-freshness-level")).toBe("fresh");
  });

  it("renders an em-dash placeholder when as_of_date is null", () => {
    render(
      <OwnershipFreshnessChips
        rings={rings([category("institutions", null)])}
        today={TODAY}
      />,
    );
    const chip = screen.getByText("Institutions").closest("[data-freshness-level]");
    expect(chip!.getAttribute("data-freshness-level")).toBe("unknown");
    expect(chip!.textContent).toContain("—");
  });

  it("includes the as_of_date in the chip title for hover-disclosure", () => {
    render(
      <OwnershipFreshnessChips
        rings={rings([category("treasury", "2026-03-28")])}
        today={TODAY}
      />,
    );
    const chip = screen.getByText("Treasury").closest("[title]");
    expect(chip!.getAttribute("title")).toContain("2026-03-28");
  });

  it("collapses an unparsable as_of_date to 'no date on file' in the title (Codex #767)", () => {
    render(
      <OwnershipFreshnessChips
        rings={rings([category("treasury", "garbage")])}
        today={TODAY}
      />,
    );
    const chip = screen.getByText("Treasury").closest("[title]");
    expect(chip!.getAttribute("title")).toContain("no date on file");
    expect(chip!.getAttribute("title")).not.toContain("garbage");
  });
});
