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
  // #2306 — the verdict is REQUIRED on the wire, so the fixture carries it
  // rather than leaning on the `as unknown as` cast this file used to end
  // with. A cast here would have hidden the very drift the required type is
  // for, and `undefined` reads as quarantined, so the omission would have
  // silently flipped every assertion below into the refused branch.
  subject_identity_ok: true,
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

  it("does NOT flag future prompt versions (v3+) as pre-anchor", () => {
    // Blind-list is explicit {null, v1}; later versions inherit the anchor.
    const thesis = { ...FIXTURE, prompt_version: "v3" } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.queryByText("pre-anchor memo")).not.toBeInTheDocument();
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

  it("renders the material diff block with summary + expandable detail (#2013)", () => {
    const thesis = {
      ...FIXTURE,
      diff: {
        prev_version: 2,
        curr_version: 3,
        stance: { from_value: "buy", to_value: "watch" },
        thesis_type: null,
        confidence: { from_value: 0.8, to_value: 0.65, delta: -0.15 },
        targets: [],
        break_conditions_added: ["New CEO departs"],
        break_conditions_removed: [],
        memo_sections_added: [],
        memo_sections_removed: [],
        memo_sections_changed: ["Valuation"],
        prompt_version: null,
        model: null,
        material: true,
        summary: "stance buy→watch",
      },
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    const block = screen.getByTestId("thesis-diff");
    expect(block).toHaveTextContent("Δ vs v2:");
    expect(block).toHaveTextContent("stance buy→watch");
    expect(block).toHaveTextContent("conf -15pp");
    expect(block).toHaveTextContent("break condition added: New CEO departs");
    expect(block).toHaveTextContent("memo section revised: Valuation");
  });

  it("omits the diff block on v1 rows (diff null)", () => {
    const thesis = { ...FIXTURE, diff: null } as unknown as ThesisDetail;
    render(<ThesisPane thesis={thesis} errored={false} />);
    expect(screen.queryByTestId("thesis-diff")).not.toBeInTheDocument();
  });
});

describe("ThesisPane — break predicate rows (#2051)", () => {
  const conditions = [
    "Loss of key patents", // prose — no predicate row
    "RSI-14 rises above 70",
    "Altman Z-score crosses into distress (<1.8)",
    "Days-to-cover exceeds 10",
    "Price falls below the 200-day SMA",
  ];
  const predicates = [
    {
      predicate_index: 1,
      metric: "rsi_14",
      op: ">",
      threshold: 70,
      unit: "index",
      baseline_state: "armed",
      baselined_at: "2026-07-16T05:22:00+00:00",
      fired_at: null,
      observed_value: null,
    },
    {
      predicate_index: 2,
      metric: "altman_z",
      op: "<",
      threshold: 1.8,
      unit: "zscore",
      baseline_state: "already_true",
      baselined_at: "2026-07-16T05:22:00+00:00",
      fired_at: null,
      observed_value: null,
    },
    {
      predicate_index: 3,
      metric: "short_interest_days_to_cover",
      op: ">",
      threshold: 10,
      unit: "days",
      baseline_state: "armed",
      baselined_at: "2026-07-16T05:22:00+00:00",
      fired_at: "2026-07-17T05:22:00+00:00",
      observed_value: 12.4,
    },
    {
      predicate_index: 4,
      metric: "price_vs_sma200",
      op: "<",
      threshold: null,
      unit: "regime",
      baseline_state: "pending",
      baselined_at: null,
      fired_at: null,
      observed_value: null,
    },
  ];
  const withPredicates = {
    ...FIXTURE,
    break_conditions_json: conditions,
    break_predicates: predicates,
  } as unknown as ThesisDetail;

  it("chips armed/pending muted, premise with writer-premise tooltip, prose bare", () => {
    render(<ThesisPane thesis={withPredicates} errored={false} />);
    expect(screen.getByText("armed")).toBeInTheDocument();
    expect(screen.getByText("pending")).toBeInTheDocument();
    const premise = screen.getByText("premise");
    expect(premise).toHaveAttribute("title", expect.stringContaining("Writer premise, not a trigger"));
    // Prose condition renders without any chip in its row.
    const prose = screen.getByText(/Loss of key patents/);
    expect(prose.querySelector("span")).toBeNull();
  });

  it("fired predicate renders amber with evidence tooltip", () => {
    render(<ThesisPane thesis={withPredicates} errored={false} />);
    const fired = screen.getByText("fired");
    expect(fired).toHaveAttribute(
      "title",
      expect.stringContaining("observed 12.4 vs threshold > 10"),
    );
    const row = fired.closest("li");
    expect(row).toHaveClass("text-amber-700");
  });

  it("distinguishes already_true_after_gap from already_true", () => {
    const gapped = {
      ...withPredicates,
      break_predicates: [
        { ...predicates[1], baseline_state: "already_true_after_gap" },
      ],
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={gapped} errored={false} />);
    const chip = screen.getByText("premise (gap)");
    expect(chip).toHaveAttribute("title", expect.stringContaining("unobserved gap"));
  });

  it("renders plain conditions when break_predicates is absent (older payloads)", () => {
    const bare = {
      ...FIXTURE,
      break_conditions_json: ["Lose 50% market share"],
    } as unknown as ThesisDetail;
    render(<ThesisPane thesis={bare} errored={false} />);
    expect(screen.getByText("Lose 50% market share")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// #2306 — the subject-identity verdict on the screen.
//
// The deterministic layer has refused these rows since #2431/#2436; until this
// ticket the pane rendered them as fact. On the dev corpus that was 78 of 482
// latest-per-instrument theses.
// ---------------------------------------------------------------------------
describe("ThesisPane subject-identity quarantine (#2306)", () => {
  const BANNER = "thesis-quarantine-banner";

  function withVerdict(ok: boolean | null | undefined): ThesisDetail {
    return { ...FIXTURE, subject_identity_ok: ok } as unknown as ThesisDetail;
  }

  it("shows no banner when the verdict passed", () => {
    render(<ThesisPane thesis={withVerdict(true)} errored={false} />);
    expect(screen.queryByTestId(BANNER)).not.toBeInTheDocument();
    expect(screen.getByTestId("thesis-band")).not.toHaveAttribute("data-refused");
  });

  it.each([
    [false, "unnamed_subject", "never names its own instrument"],
    [null, "unchecked", "has not been checked"],
    [undefined, "unchecked", "has not been checked"],
  ] as const)("banners a %s verdict as %s", (ok, state, copy) => {
    render(<ThesisPane thesis={withVerdict(ok)} errored={false} />);
    const banner = screen.getByTestId(BANNER);
    expect(banner).toHaveAttribute("data-subject-state", state);
    expect(banner).toHaveTextContent(copy);
    // The refusal must name the machine-readable reason the engine logs, so
    // the screen and the logs are greppable by the same word.
    expect(banner).toHaveTextContent("thesis_quarantined");
  });

  it("marks the valuation band refused but still renders the stored targets", () => {
    render(<ThesisPane thesis={withVerdict(false)} errored={false} />);
    // Annotate, do not hide: the band is the writer's actual output and the
    // evidence base for the verdict (docs/settled-decisions.md:147).
    expect(screen.getByTestId("thesis-band")).toHaveAttribute("data-refused", "true");
    expect(screen.getByText("Bear")).toBeInTheDocument();
    expect(screen.getByText("Base")).toBeInTheDocument();
    expect(screen.getByText("Bull")).toBeInTheDocument();
  });

  it("suppresses the upside-to-base conclusion, which is not part of the record", () => {
    // base 20 against a price of 10 is +100%; a quarantined band must not
    // render that as live analysis.
    const { rerender } = render(
      <ThesisPane thesis={withVerdict(true)} errored={false} currentPrice="10" />,
    );
    expect(screen.getByText("+100.0%")).toBeInTheDocument();

    rerender(<ThesisPane thesis={withVerdict(false)} errored={false} currentPrice="10" />);
    expect(screen.queryByText("+100.0%")).not.toBeInTheDocument();
  });

  it("suppresses the outside-buy-zone conclusion", () => {
    const buyable = (ok: boolean): ThesisDetail =>
      ({
        ...FIXTURE,
        stance: "buy",
        buy_zone_low: 100,
        buy_zone_high: 120,
        subject_identity_ok: ok,
      }) as unknown as ThesisDetail;
    const zoneCopy = /entry conditions not met at market/;

    const { rerender } = render(
      <ThesisPane thesis={buyable(true)} errored={false} currentPrice="500" />,
    );
    expect(screen.getByText(zoneCopy)).toBeInTheDocument();

    rerender(<ThesisPane thesis={buyable(false)} errored={false} currentPrice="500" />);
    expect(screen.queryByText(zoneCopy)).not.toBeInTheDocument();
  });

  it("announces assertively — a refusal, not a status (PR #2897 review)", () => {
    // The repo's convention for a safety refusal is role="alert"
    // (KillSwitchSection, ErrorBanner, both order modals). role="status" was
    // copied from OwnershipCoverageBanner, which reports a condition of the
    // DATA rather than a refusal of it.
    render(<ThesisPane thesis={withVerdict(false)} errored={false} />);
    expect(screen.getByTestId(BANNER)).toHaveAttribute("role", "alert");
  });

  it("INVARIANT: never renders the memo without the banner", () => {
    // safety-state-ui.md's failure mode is a banner derived from a refetchable
    // value vanishing while the dangerous content stays. Structurally absent
    // here because both are read off the SAME thesis object and the pane
    // early-returns on a null one — asserted rather than assumed.
    for (const ok of [false, null, undefined] as const) {
      const { unmount } = render(<ThesisPane thesis={withVerdict(ok)} errored={false} />);
      expect(screen.getByText("Buy on weakness.")).toBeInTheDocument();
      expect(screen.getByTestId(BANNER)).toBeInTheDocument();
      unmount();
    }
    // The other half of the invariant: no memo on screen at all when there is
    // no thesis object to carry a verdict.
    const { container } = render(<ThesisPane thesis={null} errored={false} />);
    expect(container).toBeEmptyDOMElement();
  });
});
