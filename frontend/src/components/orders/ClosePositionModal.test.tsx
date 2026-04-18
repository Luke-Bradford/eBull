/**
 * Tests for ClosePositionModal (#313).
 *
 * Pins the spec contract:
 *   - Full-close radio -> {units_to_deduct: null} (mode-driven, not float equality)
 *   - Partial-close with fractional value -> {units_to_deduct: 0.5}
 *   - Input > units -> submit disabled
 *   - current_price=null -> preview uses open_rate fallback caption
 *   - valuationSource != "quote" -> amber treatment
 *   - Stale broker position (positionId missing from trades[]) -> fixed error
 *   - 404 surfaced verbatim via error.message
 *   - Unmount-during-submit is safe
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ApiError } from "@/api/client";
import { ClosePositionModal } from "@/components/orders/ClosePositionModal";
import type {
  ConfigResponse,
  InstrumentPositionDetail,
  NativeTradeItem,
  OrderResponse,
} from "@/api/types";

vi.mock("@/api/config", () => ({
  fetchConfig: vi.fn(),
}));
vi.mock("@/api/portfolio", () => ({
  fetchInstrumentPositions: vi.fn(),
}));
vi.mock("@/api/orders", () => ({
  closePosition: vi.fn(),
}));

import { fetchConfig } from "@/api/config";
import { fetchInstrumentPositions } from "@/api/portfolio";
import { closePosition } from "@/api/orders";

const mockedFetchConfig = vi.mocked(fetchConfig);
const mockedFetchDetail = vi.mocked(fetchInstrumentPositions);
const mockedClosePosition = vi.mocked(closePosition);

function demoConfig(): ConfigResponse {
  return {
    app_env: "dev",
    etoro_env: "demo",
    runtime: {
      enable_auto_trading: false,
      enable_live_trading: false,
      display_currency: "GBP",
      updated_at: "2026-04-18T00:00:00Z",
      updated_by: "system",
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

function tradeFor(
  positionId: number,
  overrides: Partial<NativeTradeItem> = {},
): NativeTradeItem {
  return {
    position_id: positionId,
    is_buy: true,
    units: 2,
    amount: 260,
    open_rate: 130,
    open_date_time: "2026-01-01T10:00:00Z",
    current_price: 140,
    market_value: 280,
    unrealized_pnl: 20,
    stop_loss_rate: null,
    take_profit_rate: null,
    is_tsl_enabled: false,
    leverage: 1,
    total_fees: 0,
    ...overrides,
  };
}

function detailWith(trades: NativeTradeItem[]): InstrumentPositionDetail {
  return {
    instrument_id: 7,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    currency: "USD",
    current_price: 140,
    total_units: trades.reduce((s, t) => s + t.units, 0),
    avg_entry: 130,
    total_invested: 260,
    total_value: 280,
    total_pnl: 20,
    trades,
  };
}

function orderFilled(): OrderResponse {
  return {
    order_id: 7,
    status: "filled",
    broker_order_ref: "DEMO-7-EXIT",
    filled_price: 140,
    filled_units: 1,
    fees: 0,
    explanation: "Demo EXIT: price=140 units=1",
  };
}

beforeEach(() => {
  mockedFetchConfig.mockResolvedValue(demoConfig());
  mockedFetchDetail.mockResolvedValue(detailWith([tradeFor(555)]));
  mockedClosePosition.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

function renderModal(
  overrides: Partial<React.ComponentProps<typeof ClosePositionModal>> = {},
) {
  const onFilled = vi.fn();
  const onRequestClose = vi.fn();
  const result = render(
    <ClosePositionModal
      isOpen
      instrumentId={7}
      positionId={555}
      valuationSource="quote"
      onFilled={onFilled}
      onRequestClose={onRequestClose}
      {...overrides}
    />,
  );
  return { onFilled, onRequestClose, ...result };
}

describe("ClosePositionModal", () => {
  it("sends {units_to_deduct: null} for full close and calls onFilled then onRequestClose", async () => {
    mockedClosePosition.mockResolvedValueOnce(orderFilled());
    const user = userEvent.setup();
    const { onFilled, onRequestClose } = renderModal();

    await waitFor(() => {
      expect(screen.getByRole("radio", { name: /Full close/ })).toBeChecked();
    });

    await user.click(screen.getByRole("button", { name: /Close position/ }));

    await waitFor(() => expect(mockedClosePosition).toHaveBeenCalledTimes(1));
    expect(mockedClosePosition).toHaveBeenCalledWith(555, {
      units_to_deduct: null,
    });
    const filledOrder = onFilled.mock.invocationCallOrder[0] ?? 0;
    const closeOrder = onRequestClose.mock.invocationCallOrder[0] ?? 0;
    expect(filledOrder).toBeGreaterThan(0);
    expect(closeOrder).toBeGreaterThan(filledOrder);
  });

  it("sends a numeric units_to_deduct for partial close", async () => {
    mockedClosePosition.mockResolvedValueOnce(orderFilled());
    const user = userEvent.setup();
    renderModal();

    await waitFor(() => {
      expect(screen.getByRole("radio", { name: /Full close/ })).toBeChecked();
    });
    await user.click(screen.getByRole("radio", { name: /Partial close/ }));
    const input = screen.getByLabelText("Units to close") as HTMLInputElement;
    await user.type(input, "0.5");

    await user.click(screen.getByRole("button", { name: /Close position/ }));

    await waitFor(() => expect(mockedClosePosition).toHaveBeenCalled());
    expect(mockedClosePosition).toHaveBeenCalledWith(555, {
      units_to_deduct: 0.5,
    });
  });

  it("disables submit when partial-close value exceeds units", async () => {
    const user = userEvent.setup();
    renderModal();

    await waitFor(() => screen.getByRole("radio", { name: /Full close/ }));
    await user.click(screen.getByRole("radio", { name: /Partial close/ }));

    const input = screen.getByLabelText("Units to close") as HTMLInputElement;
    await user.type(input, "999");

    expect(
      screen.getByRole("button", { name: /Close position/ }),
    ).toBeDisabled();
    expect(screen.getByText(/Exceeds position units/)).toBeInTheDocument();
  });

  it("renders the open-rate fallback caption when current_price is null", async () => {
    mockedFetchDetail.mockReset();
    mockedFetchDetail.mockResolvedValueOnce(
      detailWith([tradeFor(555, { current_price: null })]),
    );
    renderModal();

    await waitFor(() => {
      expect(
        screen.getByText(/using open rate — no quote available/),
      ).toBeInTheDocument();
    });
  });

  it("shows amber treatment and caveat caption when valuation_source is daily_close", async () => {
    renderModal({ valuationSource: "daily_close" });
    await waitFor(() => {
      // daily_close is surfaced in at least two places (info strip
      // source indicator + preview caption); assert on presence via
      // getAllByText so we don't conflict with the caption check below.
      expect(
        screen.getAllByText(/daily_close/).length,
      ).toBeGreaterThanOrEqual(1);
    });
    expect(
      screen.getByText(/may not reflect fill price/i),
    ).toBeInTheDocument();
    // Caption must flag the open-rate fallback risk.
    expect(
      screen.getByText(/backend may fall back to open rate/i),
    ).toBeInTheDocument();
  });

  it("rejects partial-close values below the backend precision floor (1e-6)", async () => {
    const user = userEvent.setup();
    renderModal();

    await waitFor(() => screen.getByRole("radio", { name: /Full close/ }));
    await user.click(screen.getByRole("radio", { name: /Partial close/ }));
    const input = screen.getByLabelText("Units to close") as HTMLInputElement;
    await user.type(input, "0.0000001"); // below 1e-6

    expect(
      screen.getByRole("button", { name: /Close position/ }),
    ).toBeDisabled();
    expect(
      screen.getByText(/Must be at least 0\.000001 units/),
    ).toBeInTheDocument();
  });

  it("shows a fixed error when the target positionId is missing from the response", async () => {
    mockedFetchDetail.mockReset();
    mockedFetchDetail.mockResolvedValueOnce(
      detailWith([tradeFor(999)]),
    );
    renderModal();
    await waitFor(() => {
      expect(
        screen.getByRole("alert"),
      ).toHaveTextContent("This position no longer exists");
    });
    expect(
      screen.getByRole("button", { name: /Close position/ }),
    ).toBeDisabled();
  });

  it("surfaces a 404 body verbatim", async () => {
    mockedClosePosition.mockRejectedValueOnce(
      new ApiError(404, "Position 555 not found or already closed."),
    );
    const user = userEvent.setup();
    renderModal();

    await waitFor(() => screen.getByRole("radio", { name: /Full close/ }));
    await user.click(screen.getByRole("button", { name: /Close position/ }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        "Position 555 not found or already closed.",
      );
    });
  });

  it("surfaces a 403 kill-switch body verbatim", async () => {
    mockedClosePosition.mockRejectedValueOnce(
      new ApiError(403, "Kill switch is active: manual"),
    );
    const user = userEvent.setup();
    renderModal();

    await waitFor(() => screen.getByRole("radio", { name: /Full close/ }));
    await user.click(screen.getByRole("button", { name: /Close position/ }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        "Kill switch is active: manual",
      );
    });
  });

  it("still calls onFilled when unmounted mid-submit if the close actually succeeded", async () => {
    const deferred: { resolve: (v: OrderResponse) => void } = {
      resolve: () => undefined,
    };
    mockedClosePosition.mockImplementationOnce(
      () =>
        new Promise<OrderResponse>((resolve) => {
          deferred.resolve = resolve;
        }),
    );
    const user = userEvent.setup();
    const { unmount, onFilled, onRequestClose } = renderModal();
    await waitFor(() => screen.getByRole("radio", { name: /Full close/ }));
    await user.click(screen.getByRole("button", { name: /Close position/ }));

    unmount();
    await act(async () => {
      deferred.resolve(orderFilled());
      await Promise.resolve();
    });

    expect(onFilled).toHaveBeenCalledTimes(1);
    expect(onRequestClose).not.toHaveBeenCalled();
  });
});
