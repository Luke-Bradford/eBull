/**
 * Tests for LlmProviderSection (#1919).
 *
 * Scope:
 *   - Current knob values render from the shared config context
 *   - Save disabled until a field changed AND a reason is entered
 *   - Save sends only changed fields (no-op fields omitted)
 *   - Save error surfaces the alert banner
 *   - Config fetch error renders the section retry surface
 *
 * patchConfig is mocked at the module boundary; config state comes from
 * TestConfigProvider (the shared-context test helper).
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { patchConfig } from "@/api/config";
import type { ConfigResponse } from "@/api/types";
import { LlmProviderSection } from "@/components/settings/LlmProviderSection";
import { TestConfigProvider } from "@/lib/ConfigContext";

vi.mock("@/api/config", () => ({
  patchConfig: vi.fn(),
}));

const mockedPatchConfig = vi.mocked(patchConfig);

function configResponse(): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "GBP",
      llm_provider: "openai_compatible",
      llm_base_url: "http://localhost:11434/v1",
      // Distinct values so getByDisplayValue targets one input each.
      llm_model_writer: "qwen3:14b",
      llm_model_critic: "qwen3:8b",
      updated_at: "2026-07-09T10:00:00Z",
      updated_by: "seed",
      reason: "seed",
    },
    kill_switch: {
      active: false,
      activated_at: null,
      activated_by: null,
      reason: null,
    },
  };
}

function renderSection(overrides: { data?: ConfigResponse | null; error?: Error | null; loading?: boolean } = {}) {
  return render(
    <TestConfigProvider
      value={{
        data: "data" in overrides ? (overrides.data ?? null) : configResponse(),
        error: overrides.error ?? null,
        loading: overrides.loading ?? false,
      }}
    >
      <LlmProviderSection />
    </TestConfigProvider>,
  );
}

beforeEach(() => {
  mockedPatchConfig.mockReset();
});

describe("LlmProviderSection", () => {
  it("renders current knob values from config", () => {
    renderSection();
    expect(screen.getByDisplayValue("qwen3:14b")).toBeInTheDocument();
    expect(screen.getByDisplayValue("qwen3:8b")).toBeInTheDocument();
    expect(screen.getByDisplayValue("http://localhost:11434/v1")).toBeInTheDocument();
    expect(screen.getByRole("combobox")).toHaveValue("openai_compatible");
  });

  it("save disabled until a field changed and reason entered", async () => {
    const user = userEvent.setup();
    renderSection();
    const save = screen.getByRole("button", { name: /save llm config/i });
    expect(save).toBeDisabled();

    await user.type(screen.getByPlaceholderText("Why are you changing this?"), "testing");
    expect(save).toBeDisabled(); // reason alone is not a change

    await user.selectOptions(screen.getByRole("combobox"), "anthropic");
    expect(save).toBeEnabled();
  });

  it("sends only changed fields (writer model only)", async () => {
    mockedPatchConfig.mockResolvedValue({
      ...configResponse().runtime,
      llm_model_writer: "deepseek-r1:14b",
    });
    const user = userEvent.setup();
    renderSection();

    const writerInput = screen.getByDisplayValue("qwen3:14b");
    await user.clear(writerInput);
    await user.type(writerInput, "deepseek-r1:14b");
    await user.type(screen.getByPlaceholderText("Why are you changing this?"), "benchmarking");
    await user.click(screen.getByRole("button", { name: /save llm config/i }));

    await waitFor(() => expect(mockedPatchConfig).toHaveBeenCalledOnce());
    expect(mockedPatchConfig).toHaveBeenCalledWith({
      updated_by: "operator",
      reason: "benchmarking",
      llm_provider: undefined,
      llm_base_url: undefined,
      llm_model_writer: "deepseek-r1:14b",
      llm_model_critic: undefined,
    });
  });

  it("sends only changed fields (critic model only)", async () => {
    mockedPatchConfig.mockResolvedValue({
      ...configResponse().runtime,
      llm_model_critic: "qwen3:14b",
    });
    const user = userEvent.setup();
    renderSection();

    const criticInput = screen.getByDisplayValue("qwen3:8b");
    await user.clear(criticInput);
    await user.type(criticInput, "qwen3:14b");
    await user.type(screen.getByPlaceholderText("Why are you changing this?"), "stricter critic");
    await user.click(screen.getByRole("button", { name: /save llm config/i }));

    await waitFor(() => expect(mockedPatchConfig).toHaveBeenCalledOnce());
    expect(mockedPatchConfig).toHaveBeenCalledWith({
      updated_by: "operator",
      reason: "stricter critic",
      llm_provider: undefined,
      llm_base_url: undefined,
      llm_model_writer: undefined,
      llm_model_critic: "qwen3:14b",
    });
  });

  it("drops a pending base-url override when switching provider to anthropic", async () => {
    // Review round 2 WARNING: an edited Base URL must not ride along as a
    // silent llm_base_url audit row once the provider switch disables it.
    mockedPatchConfig.mockResolvedValue({
      ...configResponse().runtime,
      llm_provider: "anthropic",
    });
    const user = userEvent.setup();
    renderSection();

    const urlInput = screen.getByDisplayValue("http://localhost:11434/v1");
    await user.clear(urlInput);
    await user.type(urlInput, "http://other-host:8080/v1");
    await user.selectOptions(screen.getByRole("combobox"), "anthropic");
    await user.type(screen.getByPlaceholderText("Why are you changing this?"), "flip to cloud");
    await user.click(screen.getByRole("button", { name: /save llm config/i }));

    await waitFor(() => expect(mockedPatchConfig).toHaveBeenCalledOnce());
    expect(mockedPatchConfig).toHaveBeenCalledWith({
      updated_by: "operator",
      reason: "flip to cloud",
      llm_provider: "anthropic",
      llm_base_url: undefined,
      llm_model_writer: undefined,
      llm_model_critic: undefined,
    });
  });

  it("surfaces an alert when save fails", async () => {
    mockedPatchConfig.mockRejectedValue(new Error("422"));
    const user = userEvent.setup();
    renderSection();

    await user.selectOptions(screen.getByRole("combobox"), "anthropic");
    await user.type(screen.getByPlaceholderText("Why are you changing this?"), "flip");
    await user.click(screen.getByRole("button", { name: /save llm config/i }));

    expect(await screen.findByRole("alert")).toHaveTextContent(/failed to save llm config/i);
  });

  it("renders the retry surface when config fetch errored", () => {
    renderSection({ data: null, error: new Error("boom") });
    expect(screen.getByRole("button", { name: /retry/i })).toBeInTheDocument();
  });
});
