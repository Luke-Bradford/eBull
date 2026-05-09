import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { makeProcessRow } from "@/components/admin/__fixtures__/processes";
import { StaleBanner } from "@/components/admin/StaleBanner";

function renderBanner(rows: ReturnType<typeof makeProcessRow>[]) {
  return render(
    <MemoryRouter>
      <StaleBanner rows={rows} />
    </MemoryRouter>,
  );
}

describe("StaleBanner", () => {
  it("renders nothing when no row has stale_reasons", () => {
    const { container } = renderBanner([
      makeProcessRow({ stale_reasons: [] }),
      makeProcessRow({ process_id: "b", stale_reasons: [] }),
    ]);
    expect(container.querySelector("[data-testid='stale-banner']")).toBeNull();
  });

  it("renders when at least one row is stale", () => {
    renderBanner([
      makeProcessRow({ process_id: "a", stale_reasons: [] }),
      makeProcessRow({ process_id: "b", stale_reasons: ["watermark_gap"] }),
    ]);
    expect(screen.getByTestId("stale-banner")).toBeTruthy();
  });

  it("names up to 3 stale process_ids and adds +N more for the rest", () => {
    renderBanner([
      makeProcessRow({ process_id: "p1", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p2", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p3", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p4", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "p5", stale_reasons: ["watermark_gap"] }),
    ]);
    const banner = screen.getByTestId("stale-banner");
    const text = banner.textContent ?? "";
    expect(text).toContain("p1");
    expect(text).toContain("p2");
    expect(text).toContain("p3");
    expect(text).not.toContain("p4");
    expect(text).toContain("+2 more");
  });

  it("describes single shared cause when all stale rows share one reason", () => {
    renderBanner([
      makeProcessRow({ process_id: "a", stale_reasons: ["queue_stuck"] }),
      makeProcessRow({ process_id: "b", stale_reasons: ["queue_stuck"] }),
    ]);
    const text = screen.getByTestId("stale-banner").textContent ?? "";
    expect(text).toContain("queue stuck");
  });

  it("falls back to 'multiple causes' when stale rows have different reasons", () => {
    renderBanner([
      makeProcessRow({ process_id: "a", stale_reasons: ["watermark_gap"] }),
      makeProcessRow({ process_id: "b", stale_reasons: ["queue_stuck"] }),
    ]);
    const text = screen.getByTestId("stale-banner").textContent ?? "";
    expect(text).toContain("multiple causes");
  });

  it("View link points to the first stale row's drill-in route", () => {
    renderBanner([
      makeProcessRow({ process_id: "fresh", stale_reasons: [] }),
      makeProcessRow({ process_id: "stale_one", stale_reasons: ["queue_stuck"] }),
    ]);
    const link = screen.getByRole("link", { name: /View/ });
    expect(link.getAttribute("href")).toBe(
      "/admin/processes/stale_one",
    );
  });
});
