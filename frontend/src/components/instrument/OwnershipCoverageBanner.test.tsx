/**
 * Per-state rendering tests for the coverage banner (#923).
 *
 * Contract: copy (headline/body) and color (variant) are SERVER-owned
 * and rendered verbatim; the FE adds only the per-state glyph (which
 * disambiguates no_data vs red — both ship variant="error") and the
 * data-banner-state hook. One fixture per state of the shipped
 * 5-state machine (#840; the 6-state design the #923 issue cites was
 * superseded — see settled-decisions).
 */

import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type {
  OwnershipBannerVariant,
  OwnershipCoverageState,
} from "@/api/ownership";

import {
  BANNER_STATE_GLYPH,
  OwnershipCoverageBanner,
} from "./OwnershipCoverageBanner";

interface Fixture {
  readonly state: OwnershipCoverageState;
  readonly variant: OwnershipBannerVariant;
  readonly colorToken: string;
}

// Variants mirror app/services/ownership_rollup.py::_banner_for_state.
const FIXTURES: readonly Fixture[] = [
  { state: "no_data", variant: "error", colorToken: "border-red-200" },
  { state: "red", variant: "error", colorToken: "border-red-200" },
  { state: "unknown_universe", variant: "warning", colorToken: "border-amber-200" },
  { state: "amber", variant: "warning", colorToken: "border-amber-200" },
  { state: "green", variant: "success", colorToken: "border-emerald-200" },
];

function bannerProp(state: OwnershipCoverageState, variant: OwnershipBannerVariant) {
  return {
    state,
    variant,
    headline: `Headline for ${state}`,
    body: `Body copy for ${state}.`,
  };
}

describe("OwnershipCoverageBanner per state", () => {
  it.each(FIXTURES)(
    "$state renders its glyph, server variant color, and verbatim copy",
    ({ state, variant, colorToken }) => {
      const { container } = render(
        <OwnershipCoverageBanner banner={bannerProp(state, variant)} />,
      );
      const region = container.querySelector('[role="status"]');
      expect(region).not.toBeNull();
      expect(region!.getAttribute("data-banner-state")).toBe(state);
      expect(region!.className).toContain(colorToken);
      // Server copy verbatim — FE must not fork it.
      expect(screen.getByText(`Headline for ${state}`)).toBeInTheDocument();
      expect(screen.getByText(`Body copy for ${state}.`)).toBeInTheDocument();
      // Glyph present but aria-hidden: not part of the accessible name.
      const glyph = container.querySelector('span[aria-hidden="true"]');
      expect(glyph?.textContent).toBe(BANNER_STATE_GLYPH[state]);
      expect(glyph?.getAttribute("role")).toBeNull();
      expect(glyph?.getAttribute("title")).toBeNull();
    },
  );

  it("disambiguates no_data from red despite identical variant", () => {
    expect(BANNER_STATE_GLYPH.no_data).not.toBe(BANNER_STATE_GLYPH.red);
  });

  it("renders a mismatched-but-typed payload's variant verbatim (backend-owned invariant)", () => {
    const { container } = render(
      <OwnershipCoverageBanner banner={bannerProp("green", "warning")} />,
    );
    const region = container.querySelector('[role="status"]');
    // Color follows the server variant, glyph follows the state — the
    // FE does not normalize; state↔variant consistency is the
    // backend's contract.
    expect(region!.className).toContain("border-amber-200");
    expect(container.querySelector('span[aria-hidden="true"]')?.textContent).toBe("✓");
  });
});
