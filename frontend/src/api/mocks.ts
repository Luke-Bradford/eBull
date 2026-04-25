/**
 * Empty-state mock fetchers for the scaffold (#59).
 *
 * Every page in this PR renders the empty state, so each mock returns the
 * minimum well-formed empty payload for its endpoint. When real fetchers land
 * (#60–#65), pages should swap their import from @/api/mocks to a real
 * @/api/<endpoint> module backed by apiFetch.
 */

import type {
  ConfigResponse,
  InstrumentListResponse,
  PortfolioResponse,
  RankingsListResponse,
  SystemStatusResponse,
} from "@/api/types";

export async function fetchInstrumentsMock(): Promise<InstrumentListResponse> {
  return { items: [], total: 0, offset: 0, limit: 0 };
}

export async function fetchRankingsMock(): Promise<RankingsListResponse> {
  return {
    items: [],
    total: 0,
    offset: 0,
    limit: 0,
    model_version: "",
    scored_at: null,
  };
}

export async function fetchPortfolioMock(): Promise<PortfolioResponse> {
  return {
    positions: [],
    mirrors: [],
    position_count: 0,
    total_aum: 0,
    cash_balance: null,
    mirror_equity: 0,
    display_currency: "USD",
    fx_rates_used: {},
    live_quote_instrument_ids: [],
  };
}

export async function fetchConfigMock(): Promise<ConfigResponse> {
  return {
    app_env: "",
    etoro_env: "",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "USD",
      updated_at: new Date(0).toISOString(),
      updated_by: "",
      reason: "",
    },
    kill_switch: {
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    },
  };
}

export async function fetchSystemStatusMock(): Promise<SystemStatusResponse> {
  return {
    checked_at: new Date(0).toISOString(),
    overall_status: "ok",
    layers: [],
    jobs: [],
    kill_switch: {
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    },
  };
}
