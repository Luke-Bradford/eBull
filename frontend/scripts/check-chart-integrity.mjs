#!/usr/bin/env node
/**
 * Chart-integrity gate (#2185, scope widened in #2190).
 *
 * Two rules, both of which shipped as live defects on the fundamentals drill
 * page and neither of which any existing gate could see:
 *
 *   A. Cubic interpolation on a discrete financial series. Recharts'
 *      `monotone` curve draws a smooth path BETWEEN reported values, so the
 *      rendered line passes through magnitudes the issuer never reported and
 *      can overshoot a local extreme. Quarterly / annual financials are
 *      discrete observations — the connecting line is a reading aid, and the
 *      only honest one is straight. Use the linear curve type instead.
 *
 *   B. A raw palette (`lightTheme.` / `darkTheme.`) referenced outside
 *      `lib/chartTheme.ts`. `design-system.md` §"Chart theming — ONE source":
 *      *"a new chart imports the theme; it does not re-pick colors."*
 *      `fundamentalsCharts.tsx` bound `useChartTheme()` to `theme` and then
 *      reached past it for `lightTheme.accent[...]` 23 times — hardcoding the
 *      LIGHT palette into a dark-capable page. The dark gate
 *      (`check-dark-classes.mjs`) cannot catch this: it inspects Tailwind
 *      bg/border/hover class pairs, and these are inline hex values from a JS
 *      module (prevention log → "a lint gate's file-glob is part of its
 *      contract").
 *
 * SCOPE: `components/` + `pages/`, tree-wide within them. #2185 shipped this
 * gate scoped to `components/fundamentals/` alone — correct at the time, per
 * the spec's §4 "reduced chart infrastructure" decision and Codex's advice not
 * to build chart infrastructure before the grammar settled. #2190 measured the
 * cost of that scope: 11 cubic-curve series across 7 files, plus 19 raw-palette
 * text matches across 8 — of which 3 turned out to be DOC COMMENTS (two of them
 * warning against the very pattern they quoted), rewritten to prose here, so 16
 * are live reads across 5 files. Among them
 * `components/reports/PerformanceChart.tsx`, an operator-facing statement
 * chart, on the same surface #2178 had just worked. 27 real violations total.
 *
 * `lib/chartTheme.ts` — rule B's definition site, and the one file that MUST
 * name the raw palettes — stays out of scope BY PATH: it lives under `lib/`,
 * which is neither `components/` nor `pages/`. That is deliberate. Encoding it
 * as an exemption branch instead would leave dead, untested logic that reads
 * as protection. If the scope ever widens to `src/`, rule B needs a real
 * early-return for it at that point, not before.
 *
 * THE RATCHET — and why it is NOT the skip-list #987 killed
 * --------------------------------------------------------
 * Widening the scope in one step would fail every developer's pre-push on all
 * 27 pre-existing violations, and a gate that is red on day one gets disabled
 * rather than obeyed. So the pre-existing debt is capped, not exempted, by
 * `RATCHET` below.
 *
 * `check-dark-classes.mjs` carries the instruction "do NOT reintroduce a
 * skip-list — fix the file instead", from #987, which drained its Check F
 * skip-list to empty. That instruction is about not re-adding debt to an
 * already-clean gate, and it stands. This is a different mechanism at a
 * different stage — the same stage `dark:check` was at BEFORE #987, widening
 * over debt that already exists:
 *
 *   - A skip-list is binary. A listed file is not checked, so debt inside it
 *     can GROW invisibly. That is what made #987's drain necessary.
 *   - This ratchet is a COUNT, per file and per rule, and the match is EXACT.
 *     A listed file at its cap still fails on violation N+1, so debt cannot
 *     grow. It also fails when the actual count drops BELOW the cap, which
 *     forces a burn-down PR to lower the number in the same commit — without
 *     that half, every fix silently leaves headroom for the debt to return and
 *     the "temporary" mechanism becomes permanent.
 *   - An unlisted file fails on its first violation, so new charts are guarded
 *     immediately. That is the durable guarantee, and it lands now rather than
 *     after all 29 fixes.
 *
 * Burn the entries down one file at a time, then DELETE the `RATCHET` map and
 * the code that reads it. An empty ratchet is a bug, not a milestone: the gate
 * refuses to run with one, so the mechanism cannot outlive the debt.
 *
 * Burn-down progress (#2197 tracks the drain): `PerformanceChart.tsx` (2 curve)
 * done in #2190 — it was first because it renders on an operator-facing
 * statement. `dividendsCharts.tsx` (3 curve, 7 palette) then
 * `FundamentalsPane.tsx` (1 curve, 4 palette) and `riskCharts.tsx` (2 curve)
 * done in #2197, largest entries first. 8 violations remain across 6 files —
 * all of them singles.
 *
 * Note the forbidden strings are assembled from fragments below: these gates
 * are textual and line-based, so writing the banned literal in this file — even
 * inside a comment — would trip the check against itself (the same reason
 * `check-hand-rolled-pills.mjs` builds its class fragments). The same
 * constraint applies to the scanned tree: describe these props and palettes in
 * prose inside doc comments, never by quoting the literal (prevention log,
 * #1908 PR-2). `Sparkline.tsx` was rewritten that way in #2190 rather than
 * given a ratchet entry — a doc comment has no violation to burn down, so it
 * would have sat in the map forever.
 *
 * Exits non-zero with file:line:reason for each violation.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../src", import.meta.url));

/**
 * Path prefixes of the scanned areas, relative to `src`, so the check is
 * checkout-independent. Each must match at least one file — see the empty-scope
 * guard below.
 */
const SCOPES = ["components", "pages"];

// Assembled from fragments — see the note in the module docstring.
const CURVE_PROP = "type=" + '"' + "mono" + "tone" + '"';
const RAW_PALETTES = ["light" + "Theme.", "dark" + "Theme."];

const RULE_CURVE = "curve";
const RULE_PALETTE = "palette";

/**
 * Pre-existing debt at the moment the scope widened (#2190), as
 * `src`-relative POSIX path → per-rule count. Counts are EXACT, not ceilings:
 * see the ratchet note in the module docstring. Lower a number in the same
 * commit that fixes the violations; delete the entry at zero; delete this map
 * and its reader once it is empty.
 *
 * Measured on `origin/main` at d5853233.
 */
const RATCHET = {
  "components/filings/filingsAnalyticsCharts.tsx": { curve: 1, palette: 0 },
  "components/insider/InsiderByOfficer.tsx": { curve: 0, palette: 2 },
  "components/insider/InsiderNetByMonth.tsx": { curve: 0, palette: 1 },
  "components/instrument/OwnershipHistoryChart.tsx": { curve: 1, palette: 0 },
  "components/news/newsAnalyticsCharts.tsx": { curve: 1, palette: 0 },
  "pages/components/ChartWorkspaceCanvas.tsx": { curve: 0, palette: 2 },
};

/**
 * Collect every `.ts` / `.tsx` under `dir`.
 *
 * `.ts` is walked as well as `.tsx`: a series-colour table or a formatter
 * extracted out of a chart component into a plain module is a normal refactor,
 * and an extension filter covering only where the pattern first appeared
 * leaves the rest structurally unguarded (#1908 PR-2 / prevention log).
 */
function collect(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      out.push(...collect(full));
    } else if (entry.endsWith(".ts") || entry.endsWith(".tsx")) {
      out.push(full);
    }
  }
  return out;
}

/** `src`-relative POSIX key for a collected absolute path. */
function keyFor(file) {
  return relative(ROOT, file).split(sep).join("/");
}

/**
 * Per-rule violations in one file's source, as `{ rule, line }` records.
 *
 * Not exported: `vitest` collects `src/**` only, and neither sibling gate
 * (`check-dark-classes.mjs`, `check-hand-rolled-pills.mjs`) is unit-tested.
 * These scripts are verified by running them — see the PR for this gate
 * exercised against all four failure modes.
 */
function scanSource(source) {
  const found = [];
  source.split("\n").forEach((line, i) => {
    if (line.includes(CURVE_PROP)) {
      found.push({ rule: RULE_CURVE, line: i + 1 });
    }
    for (const palette of RAW_PALETTES) {
      if (line.includes(palette)) {
        found.push({ rule: RULE_PALETTE, line: i + 1 });
      }
    }
  });
  return found;
}

const REASONS = {
  [RULE_CURVE]:
    "cubic interpolation on a discrete financial series — use the linear curve type",
  [RULE_PALETTE]:
    "raw palette outside lib/chartTheme.ts — read the resolved palette via useChartTheme()",
};

/**
 * Compare measured counts against `RATCHET`, returning human-readable failures.
 *
 * Both directions fail, and the `stale` direction is the load-bearing one —
 * see the ratchet note in the module docstring.
 */
function checkRatchet(measured, ratchet) {
  const failures = [];
  const rules = [RULE_CURVE, RULE_PALETTE];

  for (const [path, allowed] of Object.entries(ratchet)) {
    for (const rule of rules) {
      const cap = allowed[rule] ?? 0;
      const actual = measured[path]?.[rule] ?? 0;
      if (actual > cap) {
        failures.push(
          `${path}: ${rule} violations rose ${cap} -> ${actual}. ` +
            "Ratchet entries cap pre-existing debt; they do not license more of it.",
        );
      } else if (actual < cap) {
        failures.push(
          `${path}: ${rule} violations fell ${cap} -> ${actual} but the ratchet ` +
            `still says ${cap}. Lower it to ${actual} (or delete the entry at zero) ` +
            "in this commit — a stale cap is headroom for the debt to return.",
        );
      }
    }
  }

  for (const [path, counts] of Object.entries(measured)) {
    if (path in ratchet) continue;
    for (const rule of rules) {
      // Report EVERY offending line, not just the first: a developer fixing a
      // new chart should see the whole list in one run rather than rediscover
      // it one pre-push at a time. `*Lines` is optional so this stays callable
      // with bare `{ curve, palette }` counts (the unit tests do exactly that).
      const lines = counts[`${rule}Lines`] ?? [];
      if (lines.length > 0) {
        for (const line of lines) {
          failures.push(`${path}:${line}: ${REASONS[rule]}`);
        }
      } else if ((counts[rule] ?? 0) > 0) {
        failures.push(`${path}: ${counts[rule]}x ${REASONS[rule]}`);
      }
    }
  }

  return failures;
}

const files = SCOPES.flatMap((scope) => collect(join(ROOT, scope)));

// A file-glob that matches nothing must FAIL, not pass quietly. Renaming or
// moving a scanned directory would otherwise leave the gate printing
// "0 files, no violations" and exiting 0 forever — the exact shape of the
// prevention-log lesson this script cites ("a lint gate's file-glob is part of
// its contract"). Checked PER SCOPE, because a combined count stays healthy
// while one half of the scope silently disappears.
for (const scope of SCOPES) {
  const matched = files.filter((f) => keyFor(f).startsWith(scope + "/"));
  if (matched.length === 0) {
    console.error(
      `x chart-integrity gate matched NO files under ${scope} — the scope moved ` +
        "or was renamed. Update SCOPES in this script; do not leave it empty.",
    );
    process.exit(1);
  }
}

// An empty ratchet means the debt is drained: delete the mechanism rather than
// leaving a dormant one for the next person to add to.
if (Object.keys(RATCHET).length === 0) {
  console.error(
    "x chart-integrity ratchet is empty — the debt is drained. Delete RATCHET, " +
      "checkRatchet() and this guard; the gate is now a plain tree-wide check.",
  );
  process.exit(1);
}

const measured = {};
for (const file of files) {
  const found = scanSource(readFileSync(file, "utf8"));
  if (found.length === 0) continue;
  const path = keyFor(file);
  measured[path] = {
    curve: found.filter((f) => f.rule === RULE_CURVE).length,
    palette: found.filter((f) => f.rule === RULE_PALETTE).length,
    curveLines: found.filter((f) => f.rule === RULE_CURVE).map((f) => f.line),
    paletteLines: found
      .filter((f) => f.rule === RULE_PALETTE)
      .map((f) => f.line),
  };
}

// A ratchet entry for a file that no longer exists is stale bookkeeping: it
// would keep the map alive past the debt it describes.
const orphans = Object.keys(RATCHET).filter(
  (path) => !files.some((f) => keyFor(f) === path),
);

// An orphan is already fully explained by its own message; leaving it in the
// ratchet passed to `checkRatchet` would ALSO report it as a stale cap
// ("fell 2 -> 0"), which is the same fact stated twice and sends the reader
// looking for a file that is gone.
const liveRatchet = Object.fromEntries(
  Object.entries(RATCHET).filter(([path]) => !orphans.includes(path)),
);

const failures = [
  ...orphans.map(
    (path) =>
      `${path}: ratchet entry for a file that no longer exists — delete the entry.`,
  ),
  ...checkRatchet(measured, liveRatchet),
];

if (failures.length > 0) {
  console.error(`x ${failures.length} chart-integrity violation(s):\n`);
  for (const f of failures) console.error(`  ${f}`);
  console.error(
    "\nSee .claude/skills/frontend/design-system.md " +
      '§"Chart theming — ONE source". Do NOT widen a ratchet entry to pass.',
  );
  process.exit(1);
}

const capped = Object.values(RATCHET).reduce(
  (sum, c) => sum + c.curve + c.palette,
  0,
);
console.log(
  `OK chart-integrity gate: ${files.length} files, no new violations ` +
    `(${capped} pre-existing capped across ${Object.keys(RATCHET).length} files — #2190 burn-down)`,
);
