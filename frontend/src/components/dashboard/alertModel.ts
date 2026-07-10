/**
 * alertModel — pure grouping + severity-tiering for the dashboard AlertsStrip (#1898).
 *
 * The three raw feeds emit one row per (subject × emission). For a shared root cause
 * (e.g. kill-switch active since 28 Jun) that means N×M near-identical rows — "18 new,
 * zero information", burying genuinely actionable alerts. This module collapses each feed
 * to root-cause groups and orders them by severity tier so the operator learns something.
 *
 * Grouping is FE-only and source-grounded. Guard `explanation` is built by
 * app/services/execution_guard.py::_build_explanation as
 *   "FAIL — " + "; ".join(f"{rule}: {detail}")
 * A `detail` may itself contain "; ", so only the LEADING rule code (the substring after the
 * "FAIL — " prefix, before the first ":") is unambiguously parseable — that is the group key.
 *
 * Cursor/overflow accounting is NOT done here (see AlertsStrip): the strip keeps operating
 * mark-read/dismiss on the RAW feed arrays by BIGSERIAL id. Groups carry `maxId` only to
 * drive the unseen highlight.
 */
import type {
  CoverageStatusDrop,
  GuardRejection,
  PositionAlert,
  RankMove,
  ThesisStalenessItem,
} from "@/api/types";

export type Tier = "actionable" | "informational" | "housekeeping";

const TIER_ORDER: Record<Tier, number> = {
  actionable: 0,
  informational: 1,
  housekeeping: 2,
};

export type Cursors = {
  guard: number | null;
  position: number | null;
  coverage: number | null;
  rank: number | null;
};

export interface GuardReasonMeta {
  label: string;
  consequence: string;
  action: { label: string; to: string };
}

const ADMIN = { label: "Manage in Admin", to: "/admin" };
const TRIAGE = { label: "Triage", to: "/recommendations" };

/**
 * One entry per execution-guard RuleName (app/services/execution_guard.py:89-107).
 * Config / safety-layer rejections point at /admin (where the operator acts — e.g. the
 * kill switch is deactivated there, NOT from this strip). Data / per-instrument rejections
 * point at /recommendations for triage. Unknown codes fall back to a humanized label.
 */
export const GUARD_REASON_META: Record<string, GuardReasonMeta> = {
  kill_switch: {
    label: "Kill switch active",
    consequence: "All order paths blocked — no trades will place.",
    action: ADMIN,
  },
  kill_switch_config_corrupt: {
    label: "Kill-switch config corrupt",
    consequence: "Guard fails closed — orders blocked until the config is repaired.",
    action: ADMIN,
  },
  runtime_config_corrupt: {
    label: "Runtime config corrupt",
    consequence: "Guard fails closed — orders blocked until the config is repaired.",
    action: ADMIN,
  },
  auto_trading: {
    label: "Auto-trading disabled",
    consequence: "Orders are not placed automatically (manual approval required).",
    action: ADMIN,
  },
  live_trading: {
    label: "Live trading disabled",
    consequence: "Live order path off — running in demo / blocked mode.",
    action: ADMIN,
  },
  coverage_not_tier1: {
    label: "Coverage below Tier 1",
    consequence: "Instrument not analysable enough to trade.",
    action: TRIAGE,
  },
  no_coverage_row: {
    label: "No coverage data",
    consequence: "Instrument has no coverage row — cannot assess.",
    action: TRIAGE,
  },
  thesis_stale: {
    label: "Thesis stale",
    consequence: "Thesis too old to act on — regenerate before trading.",
    action: TRIAGE,
  },
  no_thesis: {
    label: "No thesis",
    consequence: "No thesis on file — nothing to justify a trade.",
    action: TRIAGE,
  },
  spread_wide: {
    label: "Spread too wide",
    consequence: "Bid/ask spread exceeds the limit — execution deferred.",
    action: TRIAGE,
  },
  spread_unavailable: {
    label: "Spread unavailable",
    consequence: "No live quote — spread cannot be checked.",
    action: TRIAGE,
  },
  transaction_cost_prohibitive: {
    label: "Transaction cost prohibitive",
    consequence: "Estimated cost exceeds the limit — trade skipped.",
    action: TRIAGE,
  },
  budget_available: {
    label: "Insufficient budget",
    consequence: "Not enough allocatable capital for this order.",
    action: TRIAGE,
  },
  instrument_missing: {
    label: "Instrument missing",
    consequence: "Instrument not in the universe — cannot trade.",
    action: TRIAGE,
  },
  sector_missing: {
    label: "Sector missing",
    consequence: "No sector classification — concentration checks blocked.",
    action: TRIAGE,
  },
  concentration_breach: {
    label: "Concentration limit breach",
    consequence: "Would exceed the sector / position concentration cap.",
    action: TRIAGE,
  },
  safety_layers_enabled: {
    label: "Safety layers enabled",
    consequence: "A required safety layer blocked the order.",
    action: ADMIN,
  },
};

// Must match the literal the backend emits: `app/services/execution_guard.py:593`
// `return "FAIL — " + "; ".join(parts)` (note the em-dash, U+2014). If that
// format ever changes, `parseGuardReason` still degrades gracefully — it just
// skips the strip and reads the code before the first ":".
const GUARD_PREFIX = "FAIL — ";

/**
 * Leading execution-guard rule code from an `explanation`. Strips the literal "FAIL — "
 * prefix then takes the substring before the first ":". Robust to a detail that itself
 * contains ":" or "; " (only the first ":" of the leading segment is consulted).
 */
export function parseGuardReason(explanation: string): string {
  let s = explanation.trim();
  if (s.startsWith(GUARD_PREFIX)) s = s.slice(GUARD_PREFIX.length);
  const colon = s.indexOf(":");
  const code = (colon === -1 ? s : s.slice(0, colon)).trim();
  return code || "unknown";
}

function humanize(code: string): string {
  if (!code || code === "unknown") return "Guard rejection";
  return code.replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());
}

export function guardReasonMeta(code: string): GuardReasonMeta {
  return (
    GUARD_REASON_META[code] ?? {
      label: humanize(code),
      consequence: "Order blocked by the execution guard.",
      action: TRIAGE,
    }
  );
}

export interface GuardGroupItem {
  kind: "guardGroup";
  tier: Tier;
  id: string;
  code: string;
  label: string;
  consequence: string;
  action: { label: string; to: string };
  symbols: string[];
  count: number;
  latestTs: string;
  sortKey: number;
  maxId: number;
  unseen: boolean;
}

export interface PositionItem {
  kind: "position";
  tier: Tier;
  id: string;
  sortKey: number;
  unseen: boolean;
  row: PositionAlert;
}

export interface CoverageGroupItem {
  kind: "coverageGroup";
  tier: Tier;
  id: string;
  transition: string;
  symbols: string[];
  count: number;
  latestTs: string;
  sortKey: number;
  maxId: number;
  unseen: boolean;
}

export interface RankMoveItem {
  kind: "rankMove";
  tier: Tier;
  id: string;
  symbol: string;
  instrumentId: number;
  rank: number;
  rankDelta: number; // latest move; positive = moved up
  count: number;
  latestTs: string;
  sortKey: number;
  maxId: number;
  unseen: boolean;
}

/**
 * Thesis-staleness (#1902) — ONE grouped card for all held instruments whose
 * thesis is stale. Standing condition, not an event: there is no cursor, no
 * unseen highlight and no dismiss — the card clears when theses regenerate
 * (thesis_refresh drains at ≤5/hour, or per-row force from /theses).
 */
export interface ThesisStaleItem {
  kind: "thesisStale";
  tier: Tier;
  id: string;
  symbols: string[];
  count: number;
  sortKey: number;
  unseen: boolean; // always false — excluded from unseen/dismiss accounting
}

export type AlertItem =
  | GuardGroupItem
  | PositionItem
  | CoverageGroupItem
  | RankMoveItem
  | ThesisStaleItem;

function uniqueSorted(values: (string | null)[]): string[] {
  return Array.from(new Set(values.filter((v): v is string => !!v))).sort();
}

/**
 * Collapse the three raw feeds into severity-tiered, root-cause-grouped display items.
 * Ordering: tier ASC (actionable → informational → housekeeping), then latest emission DESC.
 */
export function buildAlertModel(
  rejections: GuardRejection[],
  positions: PositionAlert[],
  drops: CoverageStatusDrop[],
  moves: RankMove[],
  cursors: Cursors,
  staleTheses: ThesisStalenessItem[] = [],
): AlertItem[] {
  const items: AlertItem[] = [];

  // Guard rejections → one card per leading rule code (informational tier).
  const guardGroups = new Map<string, GuardRejection[]>();
  for (const r of rejections) {
    const code = parseGuardReason(r.explanation);
    const arr = guardGroups.get(code);
    if (arr) arr.push(r);
    else guardGroups.set(code, [r]);
  }
  for (const [code, members] of guardGroups) {
    const meta = guardReasonMeta(code);
    const maxId = Math.max(...members.map((m) => m.decision_id));
    const latest = members.reduce((a, b) =>
      Date.parse(b.decision_time) > Date.parse(a.decision_time) ? b : a,
    );
    items.push({
      kind: "guardGroup",
      tier: "informational",
      id: `guard:${code}`,
      code,
      label: meta.label,
      consequence: meta.consequence,
      action: meta.action,
      symbols: uniqueSorted(members.map((m) => m.symbol)),
      count: members.length,
      latestTs: latest.decision_time,
      sortKey: Date.parse(latest.decision_time),
      maxId,
      unseen: cursors.guard === null || maxId > cursors.guard,
    });
  }

  // Position alerts → per-instrument, kept individual (actionable tier — never bury these).
  for (const a of positions) {
    items.push({
      kind: "position",
      tier: "actionable",
      id: `position:${a.alert_id}`,
      sortKey: Date.parse(a.opened_at),
      unseen: cursors.position === null || a.alert_id > cursors.position,
      row: a,
    });
  }

  // Coverage drops → one card per transition (housekeeping tier). A `∅` sentinel keeps a
  // SQL NULL new_status from colliding with a literal status string in the group key.
  const coverageGroups = new Map<string, CoverageStatusDrop[]>();
  for (const d of drops) {
    const key = `${d.old_status}→${d.new_status ?? "∅"}`;
    const arr = coverageGroups.get(key);
    if (arr) arr.push(d);
    else coverageGroups.set(key, [d]);
  }
  for (const [key, members] of coverageGroups) {
    const maxId = Math.max(...members.map((m) => m.event_id));
    const latest = members.reduce((a, b) =>
      Date.parse(b.changed_at) > Date.parse(a.changed_at) ? b : a,
    );
    items.push({
      kind: "coverageGroup",
      tier: "housekeeping",
      id: `coverage:${key}`,
      transition: `${latest.old_status} → ${latest.new_status ?? "—"}`,
      symbols: uniqueSorted(members.map((m) => m.symbol)),
      count: members.length,
      latestTs: latest.changed_at,
      sortKey: Date.parse(latest.changed_at),
      maxId,
      unseen: cursors.coverage === null || maxId > cursors.coverage,
    });
  }

  // Rank moves → one card per held instrument (informational tier). A single
  // instrument may have several in-window move rows (one per re-score); show
  // the latest (highest score_id) and count the rest. maxId drives the unseen
  // highlight, consistent with the BIGSERIAL cursor accounting.
  const rankGroups = new Map<number, RankMove[]>();
  for (const m of moves) {
    const arr = rankGroups.get(m.instrument_id);
    if (arr) arr.push(m);
    else rankGroups.set(m.instrument_id, [m]);
  }
  for (const [instrumentId, members] of rankGroups) {
    const maxId = Math.max(...members.map((m) => m.score_id));
    const latest = members.reduce((a, b) => (b.score_id > a.score_id ? b : a));
    items.push({
      kind: "rankMove",
      tier: "informational",
      id: `rank:${instrumentId}`,
      symbol: latest.symbol,
      instrumentId,
      rank: latest.rank,
      rankDelta: latest.rank_delta,
      count: members.length,
      latestTs: latest.scored_at,
      sortKey: Date.parse(latest.scored_at),
      maxId,
      unseen: cursors.rank === null || maxId > cursors.rank,
    });
  }

  // Thesis staleness (#1902) → ONE card for all stale held instruments
  // (informational tier — research hygiene, not an immediate trade action).
  // sortKey 0: with no event timestamp of its own, a standing condition
  // sorts below fresh events within its tier rather than pinning to top.
  if (staleTheses.length > 0) {
    items.push({
      kind: "thesisStale",
      tier: "informational",
      id: "thesisStale",
      symbols: uniqueSorted(staleTheses.map((t) => t.symbol)),
      count: staleTheses.length,
      sortKey: 0,
      unseen: false,
    });
  }

  items.sort(
    (a, b) => TIER_ORDER[a.tier] - TIER_ORDER[b.tier] || b.sortKey - a.sortKey,
  );
  return items;
}
