/**
 * Shared `/config` context (#320).
 *
 * Before this: every consumer (DisplayCurrencyContext, DashboardPage,
 * DemoLivePill, …) ran its own `useAsync(fetchConfig)` — duplicate
 * in-flight requests, uncoordinated loading/error state, safety-state
 * pill on one page silently diverging from kill-switch banner on
 * another because each held its own cache.
 *
 * Now: a single `ConfigProvider` at `AppShell` level fetches once and
 * exposes the shared state via `useConfig()`. Any consumer that needs
 * the config uses that hook; nothing else should import `fetchConfig`
 * directly.
 */
import { createContext, useContext, useMemo, type ReactNode } from "react";

import { fetchConfig } from "@/api/config";
import type { ConfigResponse } from "@/api/types";
import { useAsync, type AsyncState } from "@/lib/useAsync";

const ConfigContext = createContext<AsyncState<ConfigResponse> | null>(null);

export function ConfigProvider({ children }: { children: ReactNode }): JSX.Element {
  const state = useAsync(fetchConfig, []);
  // Memoise so consumers that depend on `refetch` / stable identity don't
  // re-render on every parent render. `useAsync` already returns new
  // objects each render, but memoising the context value dedupes identity
  // for consumers that compare references.
  const value = useMemo(
    () => ({
      data: state.data,
      error: state.error,
      loading: state.loading,
      isRevalidating: state.isRevalidating,
      refetch: state.refetch,
    }),
    [state.data, state.error, state.loading, state.isRevalidating, state.refetch],
  );
  return <ConfigContext.Provider value={value}>{children}</ConfigContext.Provider>;
}

/**
 * Consume the shared `/config` state.
 *
 * Must be called inside a `<ConfigProvider>`. Throws outside one so
 * tests that render a consumer without the provider fail loudly rather
 * than silently seeing `data: null`.
 */
export function useConfig(): AsyncState<ConfigResponse> {
  const value = useContext(ConfigContext);
  if (value === null) {
    throw new Error("useConfig() must be used inside a <ConfigProvider>");
  }
  return value;
}

/**
 * Test-only helper that lets a test render consumers of `useConfig`
 * without going through a real fetch. Pass a partial `AsyncState` and
 * sensible defaults fill in the rest.
 */
export function TestConfigProvider({
  value,
  children,
}: {
  value: Partial<AsyncState<ConfigResponse>>;
  children: ReactNode;
}): JSX.Element {
  const merged: AsyncState<ConfigResponse> = {
    data: value.data ?? null,
    error: value.error ?? null,
    loading: value.loading ?? false,
    isRevalidating: value.isRevalidating ?? false,
    refetch: value.refetch ?? (() => {}),
  };
  return <ConfigContext.Provider value={merged}>{children}</ConfigContext.Provider>;
}
