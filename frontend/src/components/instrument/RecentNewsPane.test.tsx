import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { RecentNewsPane } from "./RecentNewsPane";
import * as newsApi from "@/api/news";

const navigateMock = vi.fn();
vi.mock("react-router-dom", async (importActual) => {
  const actual = (await importActual()) as object;
  return { ...actual, useNavigate: () => navigateMock };
});

beforeEach(() => {
  vi.restoreAllMocks();
  navigateMock.mockReset();
});

function makeItem(id: number) {
  return {
    news_event_id: id,
    event_time: "2026-04-20T00:00:00Z",
    headline: `Headline ${id}`,
    snippet: null,
    category: null,
    sentiment_score: null,
    source: null,
    url: null,
  };
}

describe("RecentNewsPane", () => {
  it("renders up to 5 items when feed has data", async () => {
    vi.spyOn(newsApi, "fetchNews").mockResolvedValueOnce({
      items: Array.from({ length: 7 }, (_, i) => makeItem(i)),
      total: 7,
    } as never);
    render(
      <MemoryRouter>
        <RecentNewsPane instrumentId={1} symbol="X" />
      </MemoryRouter>,
    );
    await waitFor(() => expect(screen.getByText("Headline 0")).toBeInTheDocument());
    // No more than 5 items rendered
    const items = screen.queryAllByText(/^Headline /);
    expect(items.length).toBeLessThanOrEqual(5);
  });

  it("returns null when feed is empty (no card)", async () => {
    vi.spyOn(newsApi, "fetchNews").mockResolvedValueOnce({
      items: [],
      total: 0,
    } as never);
    const { container } = render(
      <MemoryRouter>
        <RecentNewsPane instrumentId={1} symbol="X" />
      </MemoryRouter>,
    );
    await waitFor(() => expect(container.firstChild).toBeNull());
  });
});
