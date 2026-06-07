import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { makeProcessRow } from "@/components/admin/__fixtures__/processes";
import { StaleBanner } from "@/components/admin/StaleBanner";

function renderBanner(
  rows: ReturnType<typeof makeProcessRow>[],
  checkedAt?: Date | null,
) {
  return render(
    <MemoryRouter>
      <StaleBanner rows={rows} checkedAt={checkedAt ?? null} />
    </MemoryRouter>,
  );
}

describe("StaleBanner (#1513 clean-bill header)", () => {
  it("renders the positive all-clear when every row is current/working", () => {
    renderBanner([
      makeProcessRow({ status: "ok", stale_reasons: [] }),
      makeProcessRow({ process_id: "b", status: "running", stale_reasons: [] }),
    ]);
    const text = screen.getByTestId("health-header").textContent ?? "";
    expect(text).toContain("All systems current");
    expect(text).not.toContain("need attention");
    expect(text).not.toContain("self-healing");
  });

  it("renders the attention summary when at least one row needs attention", () => {
    renderBanner([
      makeProcessRow({ process_id: "a", status: "ok", stale_reasons: [] }),
      makeProcessRow({ process_id: "b", stale_reasons: ["watermark_gap"] }),
    ]);
    expect(screen.getByTestId("health-header")).toBeTruthy();
    expect(screen.getByTestId("health-header").textContent).toContain(
      "need attention",
    );
  });

  it("names up to 3 attention process_ids and adds +N more for the rest", () => {
    renderBanner([
      makeProcessRow({ process_id: "p1", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p2", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p3", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p4", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p5", stale_reasons: ["watermark_gap"] }),
    ]);
    const text = screen.getByTestId("health-header").textContent ?? "";
    expect(text).toContain("5 need attention");
    expect(text).toContain("p1");
    expect(text).toContain("p2");
    expect(text).toContain("p3");
    expect(text).not.toContain("p4");
    expect(text).toContain("+2 more");
  });

  it("counts self-healing rows separately from attention", () => {
    renderBanner([
      makeProcessRow({ process_id: "a", stale_reasons: ["queue_stuck"] }),
      makeProcessRow({ process_id: "b", status: "pending_retry", stale_reasons: [] }),
    ]);
    const text = screen.getByTestId("health-header").textContent ?? "";
    expect(text).toContain("1 need attention");
    expect(text).toContain("1 self-healing");
  });

  it("shows the summary (not all-clear) for a self-healing-only snapshot", () => {
    renderBanner([
      makeProcessRow({ process_id: "a", status: "ok", stale_reasons: [] }),
      makeProcessRow({ process_id: "b", status: "pending_retry", stale_reasons: [] }),
    ]);
    const text = screen.getByTestId("health-header").textContent ?? "";
    expect(text).toContain("1 self-healing");
    expect(text).not.toContain("need attention");
    expect(text).not.toContain("All systems current");
  });

  it("View link points to the first attention row's drill-in route", () => {
    renderBanner([
      makeProcessRow({ process_id: "fresh", status: "ok", stale_reasons: [] }),
      makeProcessRow({ process_id: "attn_one", stale_reasons: ["queue_stuck"] }),
    ]);
    const link = screen.getByRole("link", { name: /View/ });
    expect(link.getAttribute("href")).toBe("/admin/processes/attn_one");
  });

  it("renders the checked HH:MM freshness anchor when checkedAt is given", () => {
    const checkedAt = new Date("2026-06-07T14:32:00Z");
    renderBanner([makeProcessRow({ status: "ok", stale_reasons: [] })], checkedAt);
    expect(screen.getByTestId("health-header").textContent).toContain("checked");
  });

  it("omits the checked anchor when checkedAt is null", () => {
    renderBanner([makeProcessRow({ status: "ok", stale_reasons: [] })], null);
    expect(screen.getByTestId("health-header").textContent).not.toContain(
      "checked",
    );
  });
});
