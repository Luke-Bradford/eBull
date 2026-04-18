/**
 * Tests for the orders fetchers (issue #313).
 *
 * These are thin wrappers around apiFetch, so the tests mock apiFetch
 * at the module boundary and verify:
 *   - the URL passed is backend-relative and correctly parameterised
 *   - the body is the correct JSON shape
 *   - errors bubble unmodified
 *
 * No response-construction / fetch-API gymnastics — the apiFetch
 * client is already covered by its own contract; here we only assert
 * that these fetchers hand off to it correctly.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError } from "@/api/client";
import { closePosition, placeOrder } from "@/api/orders";
import type {
  ClosePositionRequest,
  OrderResponse,
  PlaceOrderRequest,
} from "@/api/types";

vi.mock("@/api/client", async () => {
  const actual = await vi.importActual<typeof import("@/api/client")>(
    "@/api/client",
  );
  return {
    ...actual,
    apiFetch: vi.fn(),
  };
});

import { apiFetch } from "@/api/client";

const mockedApiFetch = vi.mocked(apiFetch);

function fakeOrderResponse(): OrderResponse {
  return {
    order_id: 42,
    status: "filled",
    broker_order_ref: "DEMO-1-ADD",
    filled_price: 140.12,
    filled_units: 2,
    fees: 0,
    explanation: "Demo ADD: price=140.12 units=2",
  };
}

beforeEach(() => {
  mockedApiFetch.mockReset();
});

describe("placeOrder", () => {
  it("POSTs to /portfolio/orders with the expected JSON body and returns the parsed response", async () => {
    mockedApiFetch.mockResolvedValueOnce(fakeOrderResponse());
    const body: PlaceOrderRequest = {
      instrument_id: 7,
      action: "ADD",
      amount: 250,
      units: null,
      stop_loss_rate: null,
      take_profit_rate: null,
      is_tsl_enabled: false,
      leverage: 1,
    };

    const result = await placeOrder(body);

    expect(result).toEqual(fakeOrderResponse());
    expect(mockedApiFetch).toHaveBeenCalledTimes(1);
    const [path, init] = mockedApiFetch.mock.calls[0]!;
    expect(path).toBe("/portfolio/orders");
    expect(init).toMatchObject({ method: "POST" });
    expect(JSON.parse((init as RequestInit).body as string)).toEqual(body);
  });

  it("bubbles ApiError from apiFetch verbatim", async () => {
    mockedApiFetch.mockRejectedValueOnce(
      new ApiError(403, "Kill switch is active: drawdown breach"),
    );

    await expect(
      placeOrder({
        instrument_id: 7,
        action: "ADD",
        amount: 100,
        units: null,
        stop_loss_rate: null,
        take_profit_rate: null,
        is_tsl_enabled: false,
        leverage: 1,
      }),
    ).rejects.toMatchObject({
      status: 403,
      message: "Kill switch is active: drawdown breach",
    });
  });
});

describe("closePosition", () => {
  it("sends {units_to_deduct: null} for full close and interpolates the positionId", async () => {
    mockedApiFetch.mockResolvedValueOnce(fakeOrderResponse());
    const body: ClosePositionRequest = { units_to_deduct: null };

    await closePosition(99, body);

    const [path, init] = mockedApiFetch.mock.calls[0]!;
    expect(path).toBe("/portfolio/positions/99/close");
    expect(init).toMatchObject({ method: "POST" });
    expect(JSON.parse((init as RequestInit).body as string)).toEqual({
      units_to_deduct: null,
    });
  });

  it("sends the numeric units_to_deduct for partial close", async () => {
    mockedApiFetch.mockResolvedValueOnce(fakeOrderResponse());

    await closePosition(12, { units_to_deduct: 2.5 });

    const [, init] = mockedApiFetch.mock.calls[0]!;
    expect(JSON.parse((init as RequestInit).body as string)).toEqual({
      units_to_deduct: 2.5,
    });
  });

  it("bubbles 404 from the backend verbatim", async () => {
    mockedApiFetch.mockRejectedValueOnce(
      new ApiError(404, "Position 99 not found or already closed."),
    );

    await expect(
      closePosition(99, { units_to_deduct: null }),
    ).rejects.toMatchObject({
      status: 404,
      message: "Position 99 not found or already closed.",
    });
  });
});
