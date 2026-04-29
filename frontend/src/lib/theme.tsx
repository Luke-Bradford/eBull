/**
 * ThemeProvider — light / dark / system theme toggle (#690 Phase 1).
 *
 * Operator preference is persisted to localStorage so the choice
 * survives reloads. `system` reads `prefers-color-scheme` and listens
 * for OS-level changes.
 *
 * The provider toggles `<html class="dark">`, which Tailwind's
 * `darkMode: "class"` config keys off for `dark:` utility variants.
 *
 * No backend API yet — see #690 for the full plan that includes
 * persisting to operator settings instead. Phase 1 is local-only.
 *
 * Consumers:
 *   - Components: use Tailwind `dark:` utility variants directly.
 *   - Charts: import `useChartTheme()` from `@/lib/chartTheme` to
 *     get a tone-aware theme object.
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

export type ThemePreference = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

interface ThemeContextValue {
  /** Operator's stored preference (may be "system"). */
  readonly preference: ThemePreference;
  /** Effective theme actually applied (system resolved to light/dark). */
  readonly resolved: ResolvedTheme;
  readonly setPreference: (next: ThemePreference) => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

const STORAGE_KEY = "ebull.theme";

function readStoredPreference(): ThemePreference {
  // Defensive: localStorage can throw in private-mode iframes.
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (raw === "light" || raw === "dark" || raw === "system") return raw;
  } catch {
    /* fall through */
  }
  return "system";
}

function systemTheme(): ResolvedTheme {
  if (typeof window === "undefined" || !window.matchMedia) return "light";
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

function resolve(pref: ThemePreference): ResolvedTheme {
  return pref === "system" ? systemTheme() : pref;
}

function applyToDocument(resolved: ResolvedTheme): void {
  const root = document.documentElement;
  if (resolved === "dark") {
    root.classList.add("dark");
    root.style.colorScheme = "dark";
  } else {
    root.classList.remove("dark");
    root.style.colorScheme = "light";
  }
}

export function ThemeProvider({ children }: { readonly children: ReactNode }) {
  const [preference, setPreferenceState] = useState<ThemePreference>(() =>
    readStoredPreference(),
  );
  const [resolved, setResolved] = useState<ResolvedTheme>(() =>
    resolve(readStoredPreference()),
  );

  // Apply the theme to <html> on mount and whenever the resolved
  // value changes. `colorScheme` style updates the browser's native
  // form-control rendering so disabled inputs / scrollbars match.
  useEffect(() => {
    applyToDocument(resolved);
  }, [resolved]);

  // When preference is "system", listen for OS-level changes so the
  // app re-renders without a manual toggle.
  useEffect(() => {
    if (preference !== "system") return;
    const mql = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => setResolved(systemTheme());
    mql.addEventListener("change", onChange);
    return () => mql.removeEventListener("change", onChange);
  }, [preference]);

  const setPreference = useCallback((next: ThemePreference) => {
    setPreferenceState(next);
    try {
      window.localStorage.setItem(STORAGE_KEY, next);
    } catch {
      /* ignore */
    }
    setResolved(resolve(next));
  }, []);

  const value = useMemo<ThemeContextValue>(
    () => ({ preference, resolved, setPreference }),
    [preference, resolved, setPreference],
  );

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  );
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (ctx === null) {
    throw new Error("useTheme must be used within a ThemeProvider");
  }
  return ctx;
}
