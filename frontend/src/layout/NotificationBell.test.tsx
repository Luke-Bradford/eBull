import { describe, beforeEach, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";

import { NotificationBell } from "./NotificationBell";
import * as alertsApi from "@/api/alerts";

const navigateMock = vi.fn();
vi.mock("react-router-dom", async (importActual) => {
  const actual = (await importActual()) as object;
  return { ...actual, useNavigate: () => navigateMock };
});

describe("NotificationBell (#646)", () => {
  beforeEach(() => {
    navigateMock.mockReset();
    vi.restoreAllMocks();
  });

  it("renders no badge when all three feeds report zero unseen", async () => {
    vi.spyOn(alertsApi, "fetchGuardRejections").mockResolvedValue({
      unseen_count: 0,
      rejections: [],
    } as never);
    vi.spyOn(alertsApi, "fetchPositionAlerts").mockResolvedValue({
      unseen_count: 0,
      alerts: [],
    } as never);
    vi.spyOn(alertsApi, "fetchCoverageStatusDrops").mockResolvedValue({
      unseen_count: 0,
      drops: [],
    } as never);

    render(
      <MemoryRouter>
        <NotificationBell />
      </MemoryRouter>,
    );

    const bell = await screen.findByTestId("notification-bell");
    await waitFor(() => {
      expect(bell.dataset.unseenCount).toBe("0");
    });
    expect(screen.queryByTestId("notification-bell-badge")).not.toBeInTheDocument();
  });

  it("sums unseen_count across all three feeds and renders the badge", async () => {
    vi.spyOn(alertsApi, "fetchGuardRejections").mockResolvedValue({
      unseen_count: 2,
      rejections: [],
    } as never);
    vi.spyOn(alertsApi, "fetchPositionAlerts").mockResolvedValue({
      unseen_count: 5,
      alerts: [],
    } as never);
    vi.spyOn(alertsApi, "fetchCoverageStatusDrops").mockResolvedValue({
      unseen_count: 1,
      drops: [],
    } as never);

    render(
      <MemoryRouter>
        <NotificationBell />
      </MemoryRouter>,
    );

    const badge = await screen.findByTestId("notification-bell-badge");
    expect(badge).toHaveTextContent("8");
    const bell = screen.getByTestId("notification-bell");
    expect(bell.dataset.unseenCount).toBe("8");
  });

  it("treats a single failing feed as zero (best-effort) and still renders the others", async () => {
    vi.spyOn(alertsApi, "fetchGuardRejections").mockRejectedValue(new Error("boom"));
    vi.spyOn(alertsApi, "fetchPositionAlerts").mockResolvedValue({
      unseen_count: 4,
      alerts: [],
    } as never);
    vi.spyOn(alertsApi, "fetchCoverageStatusDrops").mockResolvedValue({
      unseen_count: 1,
      drops: [],
    } as never);

    render(
      <MemoryRouter>
        <NotificationBell />
      </MemoryRouter>,
    );

    const badge = await screen.findByTestId("notification-bell-badge");
    expect(badge).toHaveTextContent("5");
  });

  it("caps the badge display at 99+ for huge counts", async () => {
    vi.spyOn(alertsApi, "fetchGuardRejections").mockResolvedValue({
      unseen_count: 50,
      rejections: [],
    } as never);
    vi.spyOn(alertsApi, "fetchPositionAlerts").mockResolvedValue({
      unseen_count: 50,
      alerts: [],
    } as never);
    vi.spyOn(alertsApi, "fetchCoverageStatusDrops").mockResolvedValue({
      unseen_count: 50,
      drops: [],
    } as never);

    render(
      <MemoryRouter>
        <NotificationBell />
      </MemoryRouter>,
    );

    const badge = await screen.findByTestId("notification-bell-badge");
    expect(badge).toHaveTextContent("99+");
    // Internal data attribute carries the real number for tests /
    // assistive tech parsing.
    const bell = screen.getByTestId("notification-bell");
    expect(bell.dataset.unseenCount).toBe("150");
  });

  it("clicking the bell navigates to / (dashboard root)", async () => {
    vi.spyOn(alertsApi, "fetchGuardRejections").mockResolvedValue({
      unseen_count: 1,
      rejections: [],
    } as never);
    vi.spyOn(alertsApi, "fetchPositionAlerts").mockResolvedValue({
      unseen_count: 0,
      alerts: [],
    } as never);
    vi.spyOn(alertsApi, "fetchCoverageStatusDrops").mockResolvedValue({
      unseen_count: 0,
      drops: [],
    } as never);

    render(
      <MemoryRouter>
        <NotificationBell />
      </MemoryRouter>,
    );

    const bell = await screen.findByTestId("notification-bell");
    await userEvent.click(bell);
    expect(navigateMock).toHaveBeenCalledWith("/");
  });

  it("polls again after the interval — refresh picks up new unseen state", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const guardSpy = vi
        .spyOn(alertsApi, "fetchGuardRejections")
        .mockResolvedValueOnce({ unseen_count: 0, rejections: [] } as never)
        .mockResolvedValueOnce({ unseen_count: 3, rejections: [] } as never);
      vi.spyOn(alertsApi, "fetchPositionAlerts").mockResolvedValue({
        unseen_count: 0,
        alerts: [],
      } as never);
      vi.spyOn(alertsApi, "fetchCoverageStatusDrops").mockResolvedValue({
        unseen_count: 0,
        drops: [],
      } as never);

      render(
        <MemoryRouter>
          <NotificationBell />
        </MemoryRouter>,
      );

      await waitFor(() => {
        expect(guardSpy).toHaveBeenCalledTimes(1);
      });

      // Advance past the 30s poll interval.
      await vi.advanceTimersByTimeAsync(30_000);

      await waitFor(() => {
        expect(guardSpy).toHaveBeenCalledTimes(2);
      });
      const badge = await screen.findByTestId("notification-bell-badge");
      expect(badge).toHaveTextContent("3");
    } finally {
      vi.useRealTimers();
    }
  });
});
