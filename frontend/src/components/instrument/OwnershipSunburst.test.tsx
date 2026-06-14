/**
 * Unit tests for the #920 static-polish layer of the ownership
 * sunburst: residual hatching, residual tooltip copy, center
 * known-coverage line, and the legend's residual row.
 *
 * Sector-level DOM (the actual pie arcs) is NOT asserted here —
 * jsdom gives ResponsiveContainer a zero-size box so Recharts
 * renders no sectors. The gap/residual flag placement is covered at
 * the data layer via the exported pure ``buildSunburstChartData``;
 * the copy via the exported pure ``residualTooltipText``. The parts
 * of the component that live OUTSIDE ResponsiveContainer (center
 * label, hatch ``<pattern>`` def, legend) render fine in jsdom and
 * are asserted directly.
 */

import { fireEvent, render, screen } from "@testing-library/react";
import { type ReactElement, cloneElement } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { lightTheme } from "@/lib/chartTheme";

import type { SunburstInputs } from "./ownershipRings";
import {
  OwnershipLegend,
  OwnershipSunburst,
  RESIDUAL_LABEL,
  buildSunburstChartData,
  buildSunburstRings,
  focusedSectorDatum,
  openWedgeSource,
  residualTooltipText,
} from "./OwnershipSunburst";

// jsdom gives ResponsiveContainer a zero-size box, so the real
// component renders no sectors. Recharts computes sector geometry
// mathematically (no layout engine needed) — pinning a fixed size on
// the child chart makes real sector paths render in jsdom, which the
// keyboard test below needs (#921, Codex ckpt-1: a hand-built DOM
// skeleton alone could pass while real Recharts markup breaks).
vi.mock("recharts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("recharts")>();
  return {
    ...actual,
    ResponsiveContainer: ({ children }: { children: ReactElement }) =>
      cloneElement(children, { width: 360, height: 360 }),
  };
});

// vitest 4 reuses spy instances across tests in a file — restore
// per-test or window.open call history leaks (prevention-log:
// "Frontend spy call-history leaks across tests").
afterEach(() => {
  vi.restoreAllMocks();
});

const EDGAR_URL =
  "https://www.sec.gov/Archives/edgar/data/102909/000010290926000123-index.html";

/** 1B outstanding; institutions report 600M with one 400M named
 *  filer → 200M within-category gap; 400M residual. */
function baseInputs(overrides: Partial<SunburstInputs> = {}): SunburstInputs {
  return {
    total_shares: 1_000_000_000,
    holders: [
      {
        key: "0000102909",
        label: "Vanguard Group",
        shares: 400_000_000,
        category: "institutions",
        source_url: EDGAR_URL,
      },
    ],
    institutions_total: 600_000_000,
    etfs_total: null,
    insiders_total: null,
    blockholders_total: null,
    def14a_total: null,
    treasury_shares: null,
    ...overrides,
  };
}

describe("residualTooltipText", () => {
  it("renders the exact spec'd copy", () => {
    expect(residualTooltipText(400_000_000, 0.4)).toBe(
      "Public / unattributed: 40.00% of outstanding — 400,000,000 shares not attributed to any disclosed filer.",
    );
  });
});

describe("buildSunburstChartData", () => {
  it("marks residual wedges is_residual and within-category gaps not", () => {
    const rings = buildSunburstRings(baseInputs());
    expect(rings).not.toBeNull();
    const { middleData, outerData } = buildSunburstChartData(rings!, lightTheme);

    const middleResidual = middleData.find((d) => d.id === "middle-residual");
    expect(middleResidual).toMatchObject({
      name: RESIDUAL_LABEL,
      shares: 400_000_000,
      is_gap: true,
      is_residual: true,
      target: null,
    });

    const outerResidual = outerData.find((d) => d.id === "outer-residual");
    expect(outerResidual).toMatchObject({
      name: RESIDUAL_LABEL,
      is_gap: true,
      is_residual: true,
      target: null,
    });

    const categoryGap = outerData.find((d) => d.id === "institutions-gap");
    expect(categoryGap).toMatchObject({
      shares: 200_000_000,
      is_gap: true,
      is_residual: false,
      target: null,
    });

    const known = middleData.find((d) => d.id === "cat-institutions");
    expect(known).toMatchObject({ is_gap: false, is_residual: false });
  });

  it("threads source_url onto the leaf wedge's click target (#921)", () => {
    const rings = buildSunburstRings(baseInputs());
    const { outerData } = buildSunburstChartData(rings!, lightTheme);
    const leaf = outerData.find((d) => d.id === "leaf-institutions-0000102909");
    expect(leaf?.target).toEqual({
      kind: "leaf",
      category_key: "institutions",
      leaf_key: "0000102909",
      source_url: EDGAR_URL,
    });
  });

  it("emits no residual datums when categories cover the denominator", () => {
    const rings = buildSunburstRings(
      baseInputs({ institutions_total: 1_000_000_000 }),
    );
    const { middleData, outerData } = buildSunburstChartData(rings!, lightTheme);
    expect(middleData.find((d) => d.id === "middle-residual")).toBeUndefined();
    expect(outerData.find((d) => d.id === "outer-residual")).toBeUndefined();
  });
});

describe("OwnershipSunburst center label", () => {
  it("shows the known-coverage line under the share count", () => {
    render(<OwnershipSunburst inputs={baseInputs()} />);
    expect(screen.getByText("Total shares")).toBeInTheDocument();
    expect(screen.getByText("1,000,000,000")).toBeInTheDocument();
    // 600M known of 1B outstanding.
    expect(screen.getByText("60.00% known coverage")).toBeInTheDocument();
  });

  it("reads 100% against the bumped denominator when oversubscribed", () => {
    // Category totals (1.5B) oversubscribe reported outstanding (1B):
    // buildSunburstRings bumps the denominator to sum_known, residual
    // clamps to 0, and the label honestly reads 100% — the
    // panel-level OversubscribedWarning carries the diagnostic
    // (spec D5 / Codex ckpt-1 Medium).
    render(
      <OwnershipSunburst
        inputs={baseInputs({ institutions_total: 1_500_000_000 })}
      />,
    );
    expect(screen.getByText("1,500,000,000")).toBeInTheDocument();
    expect(screen.getByText("100.00% known coverage")).toBeInTheDocument();
  });

  it("renders the hatch pattern def", () => {
    const { container } = render(<OwnershipSunburst inputs={baseInputs()} />);
    const pattern = container.querySelector("pattern");
    expect(pattern).not.toBeNull();
    expect(pattern!.id).toMatch(/^residual-hatch-/);
  });
});

describe("OwnershipLegend residual row", () => {
  it("labels the residual row and hatches its swatch", () => {
    const rings = buildSunburstRings(baseInputs());
    const { container } = render(<OwnershipLegend rings={rings!} />);
    expect(screen.getByText(RESIDUAL_LABEL)).toBeInTheDocument();
    const swatches = container.querySelectorAll("li span[aria-hidden]");
    const hatched = Array.from(swatches).filter(
      (s) =>
        s instanceof HTMLElement &&
        s.style.background.includes("repeating-linear-gradient"),
    );
    expect(hatched).toHaveLength(1);
  });

  it("omits the residual row at full coverage", () => {
    const rings = buildSunburstRings(
      baseInputs({ institutions_total: 1_000_000_000 }),
    );
    render(<OwnershipLegend rings={rings!} />);
    expect(screen.queryByText(RESIDUAL_LABEL)).toBeNull();
  });
});

describe("openWedgeSource (#921)", () => {
  it("opens a leaf's sec.gov URL in a new tab, severs opener, returns true", () => {
    // Not the ``noopener`` feature string: per spec that makes
    // window.open return null even on SUCCESS, which would fire the
    // caller's in-app fallback after every successful open (double
    // action — Codex ckpt-2 High). Opener is severed on the handle.
    const handle = { opener: "sentinel" } as unknown as Window;
    const openSpy = vi.spyOn(window, "open").mockReturnValue(handle);
    const opened = openWedgeSource({
      kind: "leaf",
      category_key: "institutions",
      leaf_key: "0000102909",
      source_url: EDGAR_URL,
    });
    expect(opened).toBe(true);
    expect(openSpy).toHaveBeenCalledExactlyOnceWith(EDGAR_URL, "_blank");
    expect(handle.opener).toBeNull();
  });

  it("returns false when the popup is blocked (window.open → null)", () => {
    vi.spyOn(window, "open").mockReturnValue(null);
    expect(
      openWedgeSource({
        kind: "leaf",
        category_key: "institutions",
        leaf_key: "x",
        source_url: EDGAR_URL,
      }),
    ).toBe(false);
  });

  it("fails closed on null, non-sec.gov, category, and center targets", () => {
    const openSpy = vi
      .spyOn(window, "open")
      .mockReturnValue({ opener: null } as unknown as Window);
    expect(
      openWedgeSource({
        kind: "leaf",
        category_key: "etfs",
        leaf_key: "x",
        source_url: null,
      }),
    ).toBe(false);
    expect(
      openWedgeSource({
        kind: "leaf",
        category_key: "etfs",
        leaf_key: "x",
        source_url: "https://evil.example/sec.gov/index.html",
      }),
    ).toBe(false);
    expect(
      openWedgeSource({ kind: "category", category_key: "institutions" }),
    ).toBe(false);
    expect(openWedgeSource({ kind: "center" })).toBe(false);
    expect(openSpy).not.toHaveBeenCalled();
  });
});

describe("focusedSectorDatum (#921)", () => {
  const rings = buildSunburstRings(baseInputs())!;
  const { middleData, outerData } = buildSunburstChartData(rings, lightTheme);

  function sectorPath(ring: string, idx: string): Element {
    const root = document.createElement("div");
    root.innerHTML = `<g class="recharts-pie-sector"><path data-ring="${ring}" data-idx="${idx}"></path></g>`;
    return root.querySelector("path")!;
  }

  it("resolves middle and outer sectors by data attributes", () => {
    expect(focusedSectorDatum(sectorPath("middle", "0"), middleData, outerData)).toBe(
      middleData[0],
    );
    expect(focusedSectorDatum(sectorPath("outer", "1"), middleData, outerData)).toBe(
      outerData[1],
    );
  });

  it("returns null for non-sectors, unknown rings, and bad indices", () => {
    expect(focusedSectorDatum(null, middleData, outerData)).toBeNull();
    expect(
      focusedSectorDatum(document.createElement("div"), middleData, outerData),
    ).toBeNull();
    expect(focusedSectorDatum(sectorPath("inner", "0"), middleData, outerData)).toBeNull();
    expect(focusedSectorDatum(sectorPath("middle", ""), middleData, outerData)).toBeNull();
    expect(
      focusedSectorDatum(sectorPath("outer", "999"), middleData, outerData),
    ).toBeNull();
  });
});

describe("keyboard Enter on a real rendered sector (#921)", () => {
  it("fires the wedge action end-to-end: Enter → leaf target → window.open", () => {
    const openSpy = vi
      .spyOn(window, "open")
      .mockReturnValue({ opener: null } as unknown as Window);
    const { container } = render(
      <OwnershipSunburst
        inputs={baseInputs()}
        onWedgeClick={(target) => {
          openWedgeSource(target);
        }}
      />,
    );
    // Real Recharts markup (fixed-size ResponsiveContainer mock):
    // outer ring idx 0 = Vanguard, the only named leaf.
    const path = container.querySelector('path[data-ring="outer"][data-idx="0"]');
    expect(path).not.toBeNull();
    fireEvent.keyDown(path!, { key: "Enter" });
    expect(openSpy).toHaveBeenCalledExactlyOnceWith(EDGAR_URL, "_blank");
  });

  it("recharts' own Arrow navigation focuses a sector that Enter then activates", () => {
    // Drives the REAL focus chain (Codex ckpt-2 Low): focus the outer
    // pie root, ArrowLeft via recharts' native onkeydown handler
    // moves DOM focus onto a sector <g>, then Enter on it activates
    // the wedge. Proves the wrapper handler composes with recharts'
    // focus management, not just with hand-picked event targets.
    const handler = vi.fn();
    const { container } = render(
      <OwnershipSunburst inputs={baseInputs()} onWedgeClick={handler} />,
    );
    const outerLeafPath = container.querySelector(
      'path[data-ring="outer"][data-idx="0"]',
    );
    // The outer pie root = the .recharts-pie ancestor of an outer cell.
    const outerPieRoot = outerLeafPath!.closest(".recharts-pie");
    expect(outerPieRoot).not.toBeNull();
    // Recharts pre-increments its focus index from 0, so the outer
    // ring's three sectors (leaf, within-category gap, residual)
    // focus in order 1 → 2 → 0; three presses wrap to the leaf.
    // Subsequent presses fire on the focused sector — keydown
    // bubbles to the pie root where recharts attaches its handler,
    // exactly as in a browser.
    fireEvent.keyDown(outerPieRoot!, { key: "ArrowLeft" });
    fireEvent.keyDown(document.activeElement!, { key: "ArrowLeft" });
    fireEvent.keyDown(document.activeElement!, { key: "ArrowLeft" });
    const focused = document.activeElement;
    expect(focused?.classList.contains("recharts-pie-sector")).toBe(true);
    fireEvent.keyDown(focused!, { key: "Enter" });
    expect(handler).toHaveBeenCalledExactlyOnceWith({
      kind: "leaf",
      category_key: "institutions",
      leaf_key: "0000102909",
      source_url: EDGAR_URL,
    });
  });

  it("ignores Enter on gap sectors and non-Enter keys", () => {
    const handler = vi.fn();
    const { container } = render(
      <OwnershipSunburst inputs={baseInputs()} onWedgeClick={handler} />,
    );
    const gap = container.querySelector('path[data-residual="true"]');
    expect(gap).not.toBeNull();
    fireEvent.keyDown(gap!, { key: "Enter" });
    const known = container.querySelector('path[data-ring="outer"][data-idx="0"]');
    fireEvent.keyDown(known!, { key: "a" });
    expect(handler).not.toHaveBeenCalled();
  });
});
