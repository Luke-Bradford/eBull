import { describe, expect, it } from "vitest";

import type {
  CoverageStatusDrop,
  GuardRejection,
  PositionAlert,
  RankMove,
} from "@/api/types";

import {
  buildAlertModel,
  guardReasonMeta,
  parseGuardReason,
  type Cursors,
  type CoverageGroupItem,
  type GuardGroupItem,
  type RankMoveItem,
} from "./alertModel";

const NO_CURSORS: Cursors = {
  guard: null,
  position: null,
  coverage: null,
  rank: null,
};

function guard(o: Partial<GuardRejection> = {}): GuardRejection {
  return {
    decision_id: 1,
    decision_time: "2026-07-04T10:00:00Z",
    instrument_id: 1,
    symbol: "AAPL",
    action: "HOLD",
    explanation: "FAIL — kill_switch: kill switch active since 2026-06-28",
    ...o,
  };
}

function position(o: Partial<PositionAlert> = {}): PositionAlert {
  return {
    alert_id: 1,
    alert_type: "sl_breach",
    instrument_id: 2,
    symbol: "MSFT",
    opened_at: "2026-07-04T10:00:00Z",
    resolved_at: null,
    detail: "bid < sl",
    current_bid: "1",
    ...o,
  };
}

function coverage(o: Partial<CoverageStatusDrop> = {}): CoverageStatusDrop {
  return {
    event_id: 1,
    instrument_id: 3,
    symbol: "TSLA",
    changed_at: "2026-07-04T10:00:00Z",
    old_status: "analysable",
    new_status: "insufficient",
    ...o,
  };
}

function rankMove(o: Partial<RankMove> = {}): RankMove {
  return {
    score_id: 1,
    instrument_id: 4,
    symbol: "GME",
    scored_at: "2026-07-04T10:00:00Z",
    rank: 40,
    rank_delta: -30, // moved down 30 places
    ...o,
  };
}

describe("parseGuardReason", () => {
  it.each([
    ["FAIL — kill_switch: active since 2026-06-28", "kill_switch"],
    ["FAIL — auto_trading: enable_auto_trading is False; live_trading off", "auto_trading"],
    ["FAIL — concentration_breach: sector tech 0.42 > 0.30", "concentration_breach"],
    // detail contains a colon — only the leading segment's first colon counts
    ["FAIL — thesis_stale: age=45d; note: regenerate", "thesis_stale"],
    // no FAIL prefix — fall back to leading-token-before-colon
    ["kill_switch: raw", "kill_switch"],
    // no colon at all
    ["mystery_rule", "mystery_rule"],
    ["", "unknown"],
  ])("parses %j → %j", (input, expected) => {
    expect(parseGuardReason(input)).toBe(expected);
  });
});

describe("guardReasonMeta", () => {
  it("returns curated meta for a known code", () => {
    const m = guardReasonMeta("kill_switch");
    expect(m.label).toBe("Kill switch active");
    expect(m.action.to).toBe("/admin");
  });

  it("falls back to a humanized label + triage action for an unknown code", () => {
    const m = guardReasonMeta("some_new_rule");
    expect(m.label).toBe("Some new rule");
    expect(m.action.to).toBe("/recommendations");
  });
});

describe("buildAlertModel — guard grouping", () => {
  it("collapses the dev flood (12 kill_switch emissions × 4 symbols) into one card", () => {
    const symbols = ["BBBY", "IEP", "GME", "VOO"];
    const rejections: GuardRejection[] = [];
    let id = 100;
    for (let day = 0; day < 3; day += 1) {
      for (const s of symbols) {
        rejections.push(guard({ decision_id: id++, symbol: s }));
      }
    }
    const items = buildAlertModel(rejections, [], [], [], NO_CURSORS);
    expect(items).toHaveLength(1);
    const g = items[0] as GuardGroupItem;
    expect(g.kind).toBe("guardGroup");
    expect(g.code).toBe("kill_switch");
    expect(g.count).toBe(12);
    expect(g.symbols).toEqual(["BBBY", "GME", "IEP", "VOO"]); // unique + sorted
    expect(g.maxId).toBe(111); // 100..111
    expect(g.unseen).toBe(true); // cursor null
  });

  it("one card per distinct reason code", () => {
    const items = buildAlertModel(
      [
        guard({ decision_id: 1, explanation: "FAIL — kill_switch: active" }),
        guard({ decision_id: 2, explanation: "FAIL — auto_trading: off" }),
        guard({ decision_id: 3, explanation: "FAIL — kill_switch: active" }),
      ],
      [],
      [],
      [],
      NO_CURSORS,
    );
    expect(items).toHaveLength(2);
    const codes = (items as GuardGroupItem[]).map((i) => i.code).sort();
    expect(codes).toEqual(["auto_trading", "kill_switch"]);
  });

  it("drops null symbols from the summary but still counts the emission", () => {
    const items = buildAlertModel(
      [
        guard({ decision_id: 1, symbol: null }),
        guard({ decision_id: 2, symbol: "GME" }),
      ],
      [],
      [],
      [],
      NO_CURSORS,
    );
    const g = items[0] as GuardGroupItem;
    expect(g.count).toBe(2);
    expect(g.symbols).toEqual(["GME"]);
  });

  it("group is seen when maxId <= cursor", () => {
    const items = buildAlertModel(
      [guard({ decision_id: 400 }), guard({ decision_id: 399 })],
      [],
      [],
      [],
      { ...NO_CURSORS, guard: 400 },
    );
    expect((items[0] as GuardGroupItem).unseen).toBe(false);
  });
});

describe("buildAlertModel — coverage grouping", () => {
  it("groups by transition; null new_status uses a ∅ key and — display, no collision", () => {
    const items = buildAlertModel(
      [],
      [],
      [
        coverage({ event_id: 1, symbol: "TSLA", new_status: "insufficient" }),
        coverage({ event_id: 2, symbol: "NIO", new_status: "insufficient" }),
        coverage({ event_id: 3, symbol: "F", new_status: null }),
      ],
      [],
      NO_CURSORS,
    );
    const cov = items.filter((i) => i.kind === "coverageGroup") as CoverageGroupItem[];
    expect(cov).toHaveLength(2); // insufficient group + null group
    const insufficient = cov.find((c) => c.transition.includes("insufficient"))!;
    expect(insufficient.count).toBe(2);
    expect(insufficient.symbols).toEqual(["NIO", "TSLA"]);
    const nullGroup = cov.find((c) => c.transition.endsWith("—"))!;
    expect(nullGroup.transition).toBe("analysable → —");
    expect(nullGroup.id).toBe("coverage:analysable→∅");
  });
});

describe("buildAlertModel — rank moves", () => {
  it("groups moves by instrument; latest (highest score_id) drives display, rest counted", () => {
    const items = buildAlertModel(
      [],
      [],
      [],
      [
        rankMove({ score_id: 10, instrument_id: 4, symbol: "GME", rank: 40, rank_delta: -30 }),
        rankMove({ score_id: 12, instrument_id: 4, symbol: "GME", rank: 55, rank_delta: -25 }),
        rankMove({ score_id: 11, instrument_id: 7, symbol: "BBBY", rank: 8, rank_delta: 22 }),
      ],
      NO_CURSORS,
    );
    const ranks = items.filter((i) => i.kind === "rankMove") as RankMoveItem[];
    expect(ranks).toHaveLength(2);
    const gme = ranks.find((r) => r.symbol === "GME")!;
    expect(gme.count).toBe(2);
    expect(gme.maxId).toBe(12); // highest score_id
    expect(gme.rank).toBe(55); // latest row's rank
    expect(gme.rankDelta).toBe(-25); // latest row's delta
    expect(gme.tier).toBe("informational");
    expect(gme.id).toBe("rank:4");
    expect(gme.unseen).toBe(true); // cursor null
  });

  it("card is seen when maxId <= rank cursor", () => {
    const items = buildAlertModel(
      [],
      [],
      [],
      [rankMove({ score_id: 500 }), rankMove({ score_id: 499 })],
      { ...NO_CURSORS, rank: 500 },
    );
    expect((items[0] as RankMoveItem).unseen).toBe(false);
  });
});

describe("buildAlertModel — severity ordering", () => {
  it("orders actionable → informational → housekeeping regardless of timestamp", () => {
    const items = buildAlertModel(
      [guard({ decision_time: "2026-07-04T12:00:00Z" })], // newest, but informational
      [position({ opened_at: "2026-07-04T09:00:00Z" })], // oldest, but actionable
      [coverage({ changed_at: "2026-07-04T11:00:00Z" })],
      [],
      NO_CURSORS,
    );
    expect(items.map((i) => i.tier)).toEqual([
      "actionable",
      "informational",
      "housekeeping",
    ]);
  });

  it("within a tier, newest-first by latest emission", () => {
    const items = buildAlertModel(
      [
        guard({ decision_id: 1, explanation: "FAIL — kill_switch: x", decision_time: "2026-07-04T08:00:00Z" }),
        guard({ decision_id: 2, explanation: "FAIL — auto_trading: y", decision_time: "2026-07-04T12:00:00Z" }),
      ],
      [],
      [],
      [],
      NO_CURSORS,
    );
    expect((items as GuardGroupItem[]).map((i) => i.code)).toEqual([
      "auto_trading", // 12:00 newer
      "kill_switch", // 08:00
    ]);
  });
});
