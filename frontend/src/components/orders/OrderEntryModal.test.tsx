/**
 * Tests for OrderEntryModal (#313).
 *
 * Pins the contract defined in the spec:
 *   - Happy path: valid amount -> onFilled -> onRequestClose (in that order)
 *   - Instrument-detail error: renders retry, submit disabled
 *   - Client validation: Infinity / NaN / negative rejected
 *   - Guard rejection: ApiError.message surfaced verbatim
 *   - Network error: fixed phrase surfaced
 *   - Price-source flag: amber rendering when valuation_source != "quote"
 *   - Unmount-during-submit: no unhandled rejection
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { ApiError } from "@/api/client";
import { OrderEntryModal } from "@/components/orders/OrderEntryModal";
import type {
  ConfigResponse,
  InstrumentPositionDetail,
  OrderResponse,
} from "@/api/types";

vi.mock("@/api/config", () => ({
  fetchConfig: vi.fn(),
}));
vi.mock("@/api/portfolio", () => ({
  fetchInstrumentPositions: vi.fn(),
}));
vi.mock("@/api/orders", () => ({
  placeOrder: vi.fn(),
}));

import { fetchConfig } from "@/api/config";
import { fetchInstrumentPositions } from "@/api/portfolio";
import { placeOrder } from "@/api/orders";

const mockedFetchConfig = vi.mocked(fetchConfig);
const mockedFetchDetail = vi.mocked(fetchInstrumentPositions);
const mockedPlaceOrder = vi.mocked(placeOrder);

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

function detailFor(instrumentId: number): InstrumentPositionDetail {
  return {
    instrument_id: instrumentId,
    symbol: "AAPL",
    company_name: "Apple Inc.",
    currency: "USD",
    current_price: 140,
    total_units: 2,
    avg_entry: 130,
    total_invested: 260,
    total_value: 280,
    total_pnl: 20,
    trades: [
      {
        position_id: 555,
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
      },
    ],
  };
}

function orderFilled(): OrderResponse {
  return {
    order_id: 1,
    status: "filled",
    broker_order_ref: "DEMO-7-ADD",
    filled_price: 140,
    filled_units: 1,
    fees: 0,
    explanation: "Demo ADD: price=140 units=1",
  };
}

beforeEach(() => {
  mockedFetchConfig.mockResolvedValue(demoConfig());
  mockedFetchDetail.mockResolvedValue(detailFor(7));
  mockedPlaceOrder.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

function renderModal(
  overrides: Partial<React.ComponentProps<typeof OrderEntryModal>> = {},
) {
  const onFilled = vi.fn();
  const onRequestClose = vi.fn();
  const result = render(
    <OrderEntryModal
      isOpen
      instrumentId={7}
      symbol="AAPL"
      companyName="Apple Inc."
      valuationSource="quote"
      onFilled={onFilled}
      onRequestClose={onRequestClose}
      {...overrides}
    />,
  );
  return { onFilled, onRequestClose, ...result };
}

describe("OrderEntryModal", () => {
  it("submits with action=ADD + amount and calls onFilled then onRequestClose", async () => {
    mockedPlaceOrder.mockResolvedValueOnce(orderFilled());
    const user = userEvent.setup();
    const { onFilled, onRequestClose } = renderModal();

    await waitFor(() => {
      expect(screen.getByText(/Latest price:/)).toBeInTheDocument();
    });

    const input = screen.getByLabelText(/Notional/) as HTMLInputElement;
    await user.type(input, "250");
    await user.click(screen.getByRole("button", { name: /Place demo order/ }));

    await waitFor(() => {
      expect(mockedPlaceOrder).toHaveBeenCalledTimes(1);
    });
    expect(mockedPlaceOrder).toHaveBeenCalledWith({
      instrument_id: 7,
      action: "ADD",
      amount: 250,
      units: null,
      stop_loss_rate: null,
      take_profit_rate: null,
      is_tsl_enabled: false,
      leverage: 1,
    });
    // onFilled runs before onRequestClose so the refetch fires while
    // the modal is already closed.
    const filledOrder = onFilled.mock.invocationCallOrder[0] ?? 0;
    const closeOrder = onRequestClose.mock.invocationCallOrder[0] ?? 0;
    expect(filledOrder).toBeGreaterThan(0);
    expect(closeOrder).toBeGreaterThan(filledOrder);
  });

  it("submits with action=ADD + units when units mode selected", async () => {
    mockedPlaceOrder.mockResolvedValueOnce(orderFilled());
    const user = userEvent.setup();
    renderModal();

    await waitFor(() => screen.getByText(/Latest price:/));

    await user.click(screen.getByRole("radio", { name: /Units/ }));
    const input = screen.getByLabelText(/Units to buy/) as HTMLInputElement;
    await user.type(input, "1.5");
    await user.click(screen.getByRole("button", { name: /Place demo order/ }));

    await waitFor(() => expect(mockedPlaceOrder).toHaveBeenCalled());
    expect(mockedPlaceOrder).toHaveBeenCalledWith(
      expect.objectContaining({ amount: null, units: 1.5 }),
    );
  });

  it("renders a Retry button and disables submit when price fetch fails", async () => {
    mockedFetchDetail.mockReset();
    mockedFetchDetail.mockRejectedValueOnce(new Error("boom"));
    renderModal();

    await waitFor(() => {
      expect(screen.getByText(/Could not load price context/)).toBeInTheDocument();
    });
    expect(screen.getByRole("button", { name: /Retry/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Place demo order/ })).toBeDisabled();
  });

  it("rejects Infinity input: isFinite guard leaves submit disabled", async () => {
    const user = userEvent.setup();
    renderModal();
    await waitFor(() => screen.getByText(/Latest price:/));

    const input = screen.getByLabelText(/Notional/);
    await user.type(input, "1e309"); // -> Infinity after Number()

    expect(screen.getByRole("button", { name: /Place demo order/ })).toBeDisabled();
    expect(mockedPlaceOrder).not.toHaveBeenCalled();
  });

  it("rejects negative input", async () => {
    const user = userEvent.setup();
    renderModal();
    await waitFor(() => screen.getByText(/Latest price:/));

    const input = screen.getByLabelText(/Notional/);
    await user.type(input, "-10");

    expect(screen.getByRole("button", { name: /Place demo order/ })).toBeDisabled();
  });

  it("surfaces the backend detail verbatim on a 403 kill-switch response", async () => {
    mockedPlaceOrder.mockRejectedValueOnce(
      new ApiError(403, "Kill switch is active: drawdown breach"),
    );
    const user = userEvent.setup();
    renderModal();
    await waitFor(() => screen.getByText(/Latest price:/));

    await user.type(screen.getByLabelText(/Notional/), "250");
    await user.click(screen.getByRole("button", { name: /Place demo order/ }));

    await waitFor(() => {
      expect(
        screen.getByRole("alert"),
      ).toHaveTextContent("Kill switch is active: drawdown breach");
    });
  });

  it("surfaces 422 no-quote detail verbatim", async () => {
    mockedPlaceOrder.mockRejectedValueOnce(
      new ApiError(
        422,
        "No quote available for instrument 7 — cannot fill without a price.",
      ),
    );
    const user = userEvent.setup();
    renderModal();
    await waitFor(() => screen.getByText(/Latest price:/));

    await user.type(screen.getByLabelText(/Notional/), "250");
    await user.click(screen.getByRole("button", { name: /Place demo order/ }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        "No quote available for instrument 7",
      );
    });
  });

  it("surfaces a fixed phrase on non-ApiError network failure", async () => {
    mockedPlaceOrder.mockRejectedValueOnce(new TypeError("network"));
    const user = userEvent.setup();
    renderModal();
    await waitFor(() => screen.getByText(/Latest price:/));

    await user.type(screen.getByLabelText(/Notional/), "100");
    await user.click(screen.getByRole("button", { name: /Place demo order/ }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        "Network error — check connection and try again.",
      );
    });
  });

  it("shows amber treatment when valuation_source is daily_close", async () => {
    renderModal({ valuationSource: "daily_close" });
    await waitFor(() => {
      expect(screen.getByText(/daily_close/)).toBeInTheDocument();
    });
    expect(
      screen.getByText(/may not reflect fill price/i),
    ).toBeInTheDocument();
  });

  it("still calls onFilled when unmounted mid-submit if the order actually filled", async () => {
    // The modal may be unmounted (Escape, parent closes it) between
    // submit and response, but the server-side order already went
    // through. onFilled must fire so the portfolio refetches and
    // the operator sees the truth.
    const deferred: { resolve: (v: OrderResponse) => void } = {
      resolve: () => undefined,
    };
    mockedPlaceOrder.mockImplementationOnce(
      () =>
        new Promise<OrderResponse>((resolve) => {
          deferred.resolve = resolve;
        }),
    );
    const user = userEvent.setup();
    const { unmount, onFilled, onRequestClose } = renderModal();
    await waitFor(() => screen.getByText(/Latest price:/));
    await user.type(screen.getByLabelText(/Notional/), "250");
    await user.click(screen.getByRole("button", { name: /Place demo order/ }));

    // Unmount while the POST is still in flight.
    unmount();
    await act(async () => {
      deferred.resolve(orderFilled());
      await Promise.resolve();
    });

    expect(onFilled).toHaveBeenCalledTimes(1);
    // onRequestClose skipped because we're unmounted.
    expect(onRequestClose).not.toHaveBeenCalled();
  });
});
