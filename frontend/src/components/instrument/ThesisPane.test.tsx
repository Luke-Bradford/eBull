import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ThesisPane } from "./ThesisPane";
import type { ThesisDetail } from "@/api/types";

const FIXTURE = {
  thesis_id: 1,
  instrument_id: 1,
  thesis_version: 3,
  thesis_type: "turnaround",
  stance: "watch",
  confidence_score: 0.65,
  buy_zone_low: null,
  buy_zone_high: null,
  bear_value: 10,
  base_value: 20,
  bull_value: 30,
  break_conditions_json: ["Lose 50% market share"],
  memo_markdown: "Buy on weakness.",
  critic_json: null,
  created_at: "2026-07-10T12:00:00+00:00",
  prompt_version: "v2",
  model: "qwen3:14b",
  provider: "openai_compatible",
} as unknown as ThesisDetail;

describe("ThesisPane", () => {
  it("renders memo + bear/base/bull when thesis present", () => {
    const { container } = render(<ThesisPane thesis={FIXTURE} errored={false} />);
    expect(screen.getByText("Buy on weakness.")).toBeInTheDocument();
    expect(screen.getByText("Bear")).toBeInTheDocument();
    expect(screen.getByText("Base")).toBeInTheDocument();
    expect(screen.getByText("Bull")).toBeInTheDocument();
    expect(container.querySelector("article")).not.toBeNull();
  });

  it("returns null when thesis is null and not errored (no card)", () => {
    const { container } = render(<ThesisPane thesis={null} errored={false} />);
    expect(container.firstChild).toBeNull();
  });

  it("renders error UI inside Pane when errored", () => {
    render(<ThesisPane thesis={null} errored={true} />);
    expect(screen.getByText(/temporarily unavailable/i)).toBeInTheDocument();
  });

  it("renders the buy zone alongside bear/base/bull (#1902)", () => {
    const thesis = {
      ...FIXTURE,
      stance: "buy",
      buy_zone_low: 15,
      buy_zone_high: 18,
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByText("Buy zone")).toBeInTheDocument();
    expect(screen.getByText("15.00 – 18.00")).toBeInTheDocument();
  });

  it("renders the critic verdict, summary and key risks (#1902)", () => {
    const thesis = {
      ...FIXTURE,
      critic_json: {
        verdict: "Strong challenge",
        summary: "Margins are cyclical, not structural.",
        key_risks: ["Customer concentration", "Refinancing wall in 2027"],
      },
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByText("Critic")).toBeInTheDocument();
    expect(screen.getByText("Strong challenge")).toBeInTheDocument();
    expect(
      screen.getByText("Margins are cyclical, not structural."),
    ).toBeInTheDocument();
    expect(screen.getByText("Customer concentration")).toBeInTheDocument();
  });

  it("says 'no critic' when critic_json exists without a verdict", () => {
    const thesis = {
      ...FIXTURE,
      critic_json: { summary: "partial payload" },
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByText("no critic")).toBeInTheDocument();
  });

  // --- #2000 polish: provenance, price anchor, blind-memo flag ------------

  it("renders stance badge, provenance line and no pre-anchor chip on v2", () => {
    render(<ThesisPane thesis={FIXTURE} errored={false} />);
    expect(screen.getByText("watch")).toBeInTheDocument();
    expect(screen.getByText("turnaround")).toBeInTheDocument();
    expect(screen.getByText(/conf 65%/)).toBeInTheDocument();
    expect(screen.getByText(/v3 · 10 Jul 2026 · qwen3:14b · prompt v2/)).toBeInTheDocument();
    expect(screen.queryByText("pre-anchor memo")).not.toBeInTheDocument();
  });

  it("flags v1 memos as pre-anchor (targets priced blind, #1987)", () => {
    const thesis = { ...FIXTURE, prompt_version: "v1" } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByText("pre-anchor memo")).toBeInTheDocument();
  });

  it("flags unstamped (pre-#1919) memos as pre-anchor too", () => {
    const thesis = { ...FIXTURE, prompt_version: null } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByText("pre-anchor memo")).toBeInTheDocument();
  });

  it("shows current price with currency and implied upside to base", () => {
    render(
      <ThesisPane thesis={FIXTURE} errored={false} currentPrice="16.00" currency="USD" />,
    );
    expect(screen.getByText("Price now")).toBeInTheDocument();
    expect(screen.getByText("16.00 USD")).toBeInTheDocument();
    // base 20 vs price 16 -> +25.0%
    expect(screen.getByText("+25.0%")).toBeInTheDocument();
  });

  it("warns when price sits outside the buy zone on a buy stance", () => {
    const thesis = {
      ...FIXTURE,
      stance: "buy",
      buy_zone_low: 15,
      buy_zone_high: 18,
    } as unknown as ThesisDetail;
    render(
      <ThesisPane thesis={thesis} errored={false} currentPrice="22.50" currency="USD" />,
    );
    expect(screen.getByText(/outside the buy zone/)).toBeInTheDocument();
  });

  it("no outside-zone warning when price is inside the zone", () => {
    const thesis = {
      ...FIXTURE,
      stance: "buy",
      buy_zone_low: 15,
      buy_zone_high: 18,
    } as unknown as ThesisDetail;
    render(
      <ThesisPane thesis={thesis} errored={false} currentPrice="16.20" currency="USD" />,
    );
    expect(screen.queryByText(/outside the buy zone/)).not.toBeInTheDocument();
  });

  it("renders memo headings and bullets typographically (MemoMarkdown)", () => {
    const thesis = {
      ...FIXTURE,
      memo_markdown:
        "### Valuation\nTrading at **fair value** today.\n\n- upside catalyst\n- downside risk",
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByRole("heading", { name: "Valuation" })).toBeInTheDocument();
    expect(screen.getByText("fair value")).toBeInTheDocument();
    expect(screen.getByText("upside catalyst")).toBeInTheDocument();
    // The literal "### Valuation" raw line must NOT appear.
    expect(screen.queryByText(/### Valuation/)).not.toBeInTheDocument();
  });
});
