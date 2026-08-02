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
 * THE RATCHET — drained and removed (#2197)
 * -----------------------------------------
 * This gate shipped over 27 pre-existing violations. Failing every pre-push on
 * day one would have got it disabled rather than obeyed, so the debt was
 * CAPPED, not exempted, by a `RATCHET` map: a per-file, per-rule, EXACT count.
 * `actual > cap` caught regression; `actual < cap` caught a burn-down that
 * forgot to lower its number, which is the half that stops every fix leaving
 * headroom for the debt to return. Unlisted files failed on their first
 * violation, so new charts were guarded from the start rather than after the
 * last fix. That is why it was not the skip-list #987 killed — a skip-list is
 * binary and lets debt grow invisibly inside a listed file; a counted ratchet
 * cannot.
 *
 * It is now GONE, drained one file at a time across #2190 and #2197
 * (`PerformanceChart` · `dividendsCharts` · `FundamentalsPane` · `riskCharts` ·
 * `filingsAnalyticsCharts` · `OwnershipHistoryChart` · `newsAnalyticsCharts` ·
 * `InsiderByOfficer` · `InsiderNetByMonth` · `ChartWorkspaceCanvas`). The gate
 * refused to run on an empty map by design, so the final fix and the removal of
 * the mechanism had to land in the SAME commit — the mechanism could not
 * outlive the debt. Do not reintroduce it: this is now a plain tree-wide check,
 * and a new violation is simply a failure to fix before pushing.
 *
 * Two things learned draining it, worth keeping:
 *   - The counts measured offending LINES, not offending references. The
 *     scanner records at most one violation per palette name per line, so the
 *     same defect cost 2 in one file and 1 in another purely because of where
 *     the formatter put a line break.
 *   - The last entry was the one the mechanism could not have fixed early:
 *     module-scope colour constants, where no hook can run. A shortcut taken
 *     because "the palettes are identical anyway" is self-reinforcing — module
 *     scope is exactly where the correct read is unavailable.
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
 * Every measured violation as a `path:line: reason` failure string.
 *
 * Reports EVERY offending line, not just the first per file: a developer
 * fixing a chart should see the whole list in one run rather than rediscover
 * it one pre-push at a time.
 */
function reportViolations(measured) {
  const failures = [];
  for (const [path, counts] of Object.entries(measured)) {
    for (const rule of [RULE_CURVE, RULE_PALETTE]) {
      for (const line of counts[`${rule}Lines`]) {
        failures.push(`${path}:${line}: ${REASONS[rule]}`);
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

const measured = {};
for (const file of files) {
  const found = scanSource(readFileSync(file, "utf8"));
  if (found.length === 0) continue;
  const path = keyFor(file);
  measured[path] = {
    curveLines: found.filter((f) => f.rule === RULE_CURVE).map((f) => f.line),
    paletteLines: found
      .filter((f) => f.rule === RULE_PALETTE)
      .map((f) => f.line),
  };
}

const failures = reportViolations(measured);

if (failures.length > 0) {
  console.error(`x ${failures.length} chart-integrity violation(s):\n`);
  for (const f of failures) console.error(`  ${f}`);
  console.error(
    "\nSee .claude/skills/frontend/design-system.md " +
      '§"Chart theming — ONE source". Fix the file; do NOT reintroduce a ' +
      "ratchet or a skip-list to pass.",
  );
  process.exit(1);
}

console.log(
  `OK chart-integrity gate: ${files.length} files, no violations ` +
    "(ratchet drained and removed — #2197)",
);
