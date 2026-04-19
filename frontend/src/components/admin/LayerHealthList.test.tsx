import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { LayerEntry } from "@/api/types";

import { LayerHealthList } from "./LayerHealthList";


function mk(overrides: Partial<LayerEntry>): LayerEntry {
  return {
    layer: overrides.layer ?? "universe",
    display_name: overrides.display_name ?? "Tradable Universe",
    state: overrides.state ?? "healthy",
    last_updated: overrides.last_updated ?? null,
    plain_language_sla: overrides.plain_language_sla ?? "Refreshed weekly.",
  };
}


describe("LayerHealthList", () => {
  it("renders one row per layer", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "universe", display_name: "Tradable Universe" }),
      mk({ layer: "candles", display_name: "Daily Price Candles" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText("Tradable Universe")).toBeInTheDocument();
    expect(screen.getByText("Daily Price Candles")).toBeInTheDocument();
  });

  it("renders healthy pill as Healthy", () => {
    const layers: LayerEntry[] = [mk({ layer: "universe", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("universe state")).toHaveTextContent(/healthy/i);
  });

  it("renders disabled pill as Disabled", () => {
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "disabled" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("candles state")).toHaveTextContent(/disabled/i);
  });

  it("renders action_needed pill as Needs attention", () => {
    const layers: LayerEntry[] = [mk({ layer: "cik_mapping", state: "action_needed" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("cik_mapping state")).toHaveTextContent(/needs attention/i);
  });

  it("renders secret_missing pill as Needs attention", () => {
    const layers: LayerEntry[] = [mk({ layer: "news", state: "secret_missing" })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByLabelText("news state")).toHaveTextContent(/needs attention/i);
  });

  it("renders running / retrying / cascade_waiting / degraded as Catching up", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "r1", state: "running" }),
      mk({ layer: "r2", state: "retrying" }),
      mk({ layer: "r3", state: "cascade_waiting" }),
      mk({ layer: "r4", state: "degraded" }),
    ];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    for (const name of ["r1", "r2", "r3", "r4"]) {
      expect(screen.getByLabelText(`${name} state`)).toHaveTextContent(/catching up/i);
    }
  });

  it("renders relative last_updated when present", () => {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    const layers: LayerEntry[] = [mk({ layer: "universe", last_updated: oneHourAgo })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/1h ago/i)).toBeInTheDocument();
  });

  it("renders 'never' when last_updated is null", () => {
    const layers: LayerEntry[] = [mk({ layer: "universe", last_updated: null })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/never/i)).toBeInTheDocument();
  });

  it("renders SLA below the row", () => {
    const layers: LayerEntry[] = [mk({ layer: "candles", plain_language_sla: "Refreshed after market close." })];
    render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(screen.getByText(/refreshed after market close/i)).toBeInTheDocument();
  });

  it("mounts each row with id=admin-layer-<name> for deep-linking", () => {
    const layers: LayerEntry[] = [
      mk({ layer: "cik_mapping", display_name: "SEC CIK Mapping" }),
    ];
    const { container } = render(<LayerHealthList layers={layers} onToggle={() => {}} />);
    expect(container.querySelector("#admin-layer-cik_mapping")).not.toBeNull();
  });

  it("toggle button fires onToggle with opposite enabled state", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "healthy" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    fireEvent.click(screen.getByText(/Disable layer/));
    expect(onToggle).toHaveBeenCalledWith("candles", false);
  });

  it("re-enables a disabled layer via the menu", () => {
    const onToggle = vi.fn();
    const layers: LayerEntry[] = [mk({ layer: "candles", state: "disabled" })];
    render(<LayerHealthList layers={layers} onToggle={onToggle} />);
    fireEvent.click(screen.getByLabelText("candles actions"));
    fireEvent.click(screen.getByText(/Enable layer/));
    expect(onToggle).toHaveBeenCalledWith("candles", true);
  });
});
