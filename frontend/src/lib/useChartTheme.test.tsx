import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { darkTheme, lightTheme } from "./chartTheme";
import { ThemeProvider, useTheme } from "./theme";
import { useChartTheme } from "./useChartTheme";

function ProbeAndToggle() {
  const theme = useChartTheme();
  const { setPreference } = useTheme();
  return (
    <div>
      <span data-testid="bg">{theme.bg}</span>
      <span data-testid="primary-line">{theme.primaryLine}</span>
      <button onClick={() => setPreference("dark")}>force-dark</button>
    </div>
  );
}

beforeEach(() => {
  window.localStorage.clear();
  document.documentElement.classList.remove("dark");
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("useChartTheme", () => {
  it("returns the light palette when the resolved theme is light", () => {
    // Pin to light explicitly so the system-pref fallback does not flip
    // the resolved value on a dark-defaulted CI runner.
    window.localStorage.setItem("ebull.theme", "light");
    render(
      <ThemeProvider>
        <ProbeAndToggle />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("bg")).toHaveTextContent(lightTheme.bg);
    expect(screen.getByTestId("primary-line")).toHaveTextContent(
      lightTheme.primaryLine,
    );
  });

  it("flips to the dark palette when the operator switches preference", async () => {
    window.localStorage.setItem("ebull.theme", "light");
    render(
      <ThemeProvider>
        <ProbeAndToggle />
      </ThemeProvider>,
    );
    await userEvent.click(screen.getByText("force-dark"));
    expect(screen.getByTestId("bg")).toHaveTextContent(darkTheme.bg);
    expect(screen.getByTestId("primary-line")).toHaveTextContent(
      darkTheme.primaryLine,
    );
  });

  it("keeps saturated accents identical across light and dark", () => {
    // Operator color memory: blue line = SMA20 across sessions. The
    // dark-mode design intentionally reuses the light palette for
    // accent / indicator / compare slots; this test pins that contract.
    expect(darkTheme.accent).toBe(lightTheme.accent);
    expect(darkTheme.indicator).toBe(lightTheme.indicator);
    expect(darkTheme.compare).toBe(lightTheme.compare);
    expect(darkTheme.up).toBe(lightTheme.up);
    expect(darkTheme.down).toBe(lightTheme.down);
  });
});
