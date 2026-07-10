import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ThesisPane } from "./ThesisPane";
import type { ThesisDetail } from "@/api/types";

const FIXTURE = {
  thesis_id: 1,
  instrument_id: 1,
  memo_markdown: "Buy on weakness.",
  bear_value: "10",
  base_value: "20",
  bull_value: "30",
  break_conditions_json: ["Lose 50% market share"],
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
      buy_zone_low: 15,
      buy_zone_high: 18,
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.getByText("Buy zone")).toBeInTheDocument();
    expect(screen.getByText("15 – 18")).toBeInTheDocument();
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
});
