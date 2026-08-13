import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ThemeProvider, useTheme } from "./theme";

function Probe() {
  const { preference, resolved, setPreference } = useTheme();
  return (
    <div>
      <span data-testid="pref">{preference}</span>
      <span data-testid="resolved">{resolved}</span>
      <button onClick={() => setPreference("dark")}>force-dark</button>
      <button onClick={() => setPreference("light")}>force-light</button>
      <button onClick={() => setPreference("system")}>force-system</button>
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

describe("ThemeProvider", () => {
  it("defaults to system preference when no localStorage entry exists", () => {
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("pref")).toHaveTextContent("system");
  });

  it("setPreference('dark') persists to localStorage and toggles <html class='dark'>", async () => {
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    await userEvent.click(screen.getByText("force-dark"));
    expect(window.localStorage.getItem("ebull.theme")).toBe("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    expect(screen.getByTestId("resolved")).toHaveTextContent("dark");
  });

  it("setPreference('light') strips the dark class", async () => {
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    await userEvent.click(screen.getByText("force-dark"));
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    await userEvent.click(screen.getByText("force-light"));
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    expect(window.localStorage.getItem("ebull.theme")).toBe("light");
  });

  it("reads stored preference on mount and applies it", () => {
    window.localStorage.setItem("ebull.theme", "dark");
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("pref")).toHaveTextContent("dark");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  it("ignores garbage in localStorage and falls back to system", () => {
    window.localStorage.setItem("ebull.theme", "neon");
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    expect(screen.getByTestId("pref")).toHaveTextContent("system");
  });

  it("system preference resolves via prefers-color-scheme media query", () => {
    // jsdom matchMedia stub: declare dark = matches=true.
    const original = window.matchMedia;
    window.matchMedia = vi.fn().mockImplementation((query: string) => ({
      matches: query === "(prefers-color-scheme: dark)",
      media: query,
      onchange: null,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })) as never;
    try {
      render(
        <ThemeProvider>
          <Probe />
        </ThemeProvider>,
      );
      expect(screen.getByTestId("resolved")).toHaveTextContent("dark");
    } finally {
      window.matchMedia = original;
    }
  });

  it("useTheme outside ThemeProvider throws a clear error", () => {
    // Suppress React's expected error log noise during this test.
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(() => render(<Probe />)).toThrow(/ThemeProvider/i);
    } finally {
      spy.mockRestore();
    }
  });

  it("preference change is preserved across toggles", async () => {
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    await userEvent.click(screen.getByText("force-dark"));
    await userEvent.click(screen.getByText("force-system"));
    expect(window.localStorage.getItem("ebull.theme")).toBe("system");
    // act() guard — the system listener may run async on attach.
    await act(async () => {});
  });
});
