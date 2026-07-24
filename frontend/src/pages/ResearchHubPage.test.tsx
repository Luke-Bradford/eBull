/**
 * #1917 — Research hub: view presets + old-route redirect shims.
 * The four lens pages are mocked to identifiable stubs; the tests pin the hub's
 * routing/preset behaviour, not each page's internals.
 */
import { describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

import { ResearchHubPage } from "@/pages/ResearchHubPage";
import { PresetRedirect } from "@/pages/PresetRedirect";

vi.mock("@/pages/RankingsPage", () => ({ RankingsPage: () => <div data-testid="lens-ranked" /> }));
vi.mock("@/pages/InstrumentsPage", () => ({ InstrumentsPage: () => <div data-testid="lens-universe" /> }));
vi.mock("@/pages/ThesesPage", () => ({ ThesesPage: () => <div data-testid="lens-theses" /> }));
vi.mock("@/pages/RecommendationsPage", () => ({
  RecommendationsPage: () => <div data-testid="lens-actioned" />,
}));

function LocationProbe() {
  const loc = useLocation();
  return (
    <>
      <div data-testid="path">{loc.pathname}</div>
      <div data-testid="search">{loc.search}</div>
    </>
  );
}

function renderAt(initial: string) {
  return render(
    <MemoryRouter initialEntries={[initial]}>
      <Routes>
        <Route path="/research" element={<ResearchHubPage />} />
        <Route path="/rankings" element={<PresetRedirect view="ranked" />} />
        <Route path="/instruments" element={<PresetRedirect view="universe" />} />
        <Route path="/theses" element={<PresetRedirect view="theses" />} />
        <Route path="/recommendations" element={<PresetRedirect view="actioned" />} />
      </Routes>
      <LocationProbe />
    </MemoryRouter>,
  );
}

describe("ResearchHubPage — presets", () => {
  it("lands on Ranked when no view param", () => {
    renderAt("/research");
    expect(screen.getByTestId("lens-ranked")).toBeInTheDocument();
    expect(screen.queryByTestId("lens-universe")).toBeNull();
  });

  it("renders the lens named by ?view=", () => {
    renderAt("/research?view=theses");
    expect(screen.getByTestId("lens-theses")).toBeInTheDocument();
  });

  it("falls back to Ranked on an unknown view", () => {
    renderAt("/research?view=bogus");
    expect(screen.getByTestId("lens-ranked")).toBeInTheDocument();
  });

  it("switches lens + URL when a preset tab is clicked", async () => {
    const user = userEvent.setup();
    renderAt("/research");
    await user.click(screen.getByRole("tab", { name: "Actioned" }));
    expect(screen.getByTestId("lens-actioned")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByTestId("search").textContent).toBe("?view=actioned"));
  });

  it("shows the disabled Map affordance on a table lens", () => {
    renderAt("/research?view=ranked");
    expect(screen.getByRole("button", { name: "Map" })).toBeDisabled();
  });

  it("hides the Map affordance on a non-mappable lens (Theses)", () => {
    renderAt("/research?view=theses");
    expect(screen.queryByText("Map")).toBeNull();
  });
});

describe("PresetRedirect — old routes forward to the hub, preserving query", () => {
  it("/rankings → /research?view=ranked", async () => {
    renderAt("/rankings");
    await waitFor(() => {
      expect(screen.getByTestId("path").textContent).toBe("/research");
      expect(screen.getByTestId("search").textContent).toBe("?view=ranked");
    });
    expect(screen.getByTestId("lens-ranked")).toBeInTheDocument();
  });

  it("/theses?held=true&stale=true → /research?...&view=theses (query preserved)", async () => {
    renderAt("/theses?held=true&stale=true");
    await waitFor(() => {
      expect(screen.getByTestId("path").textContent).toBe("/research");
      const search = screen.getByTestId("search").textContent ?? "";
      expect(search).toContain("held=true");
      expect(search).toContain("stale=true");
      expect(search).toContain("view=theses");
    });
    expect(screen.getByTestId("lens-theses")).toBeInTheDocument();
  });
});
