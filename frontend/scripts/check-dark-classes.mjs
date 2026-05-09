#!/usr/bin/env node
/**
 * Dark-mode class hygiene gate (#708).
 *
 * Six checks run line-by-line over every .tsx in frontend/src:
 *
 *   A. Duplicate Tailwind variant utility on the same line. Catches
 *      the PR #707 case where two independent sweeps (#706 + #703)
 *      both appended `dark:text-slate-100` to the same className.
 *
 *   B. `border-slate-200` / `border-slate-300` without a
 *      `dark:border-` partner on the same line.
 *
 *   C. `hover:bg-slate-50` / `hover:bg-slate-100` without a
 *      `dark:hover:bg-` partner on the same line.
 *
 *   D. `dark:bg-<X>` (non-hover) on a line that has no light base
 *      `bg-<Y>` partner. Catches the PR #711 case where a regex
 *      sweep over `bg-slate-100` matched inside `hover:bg-slate-100`
 *      and added an always-on `dark:bg-slate-800` to elements that
 *      had no light base bg, making every nav link look permanently
 *      selected in dark mode.
 *
 *   E. Dead `dark:hover:bg-X` — the same color as the element's
 *      `dark:bg-X` base, making hover a no-op in dark mode. Catches
 *      the PR #711 round-2 ChartPage case where toggle buttons had
 *      `dark:bg-slate-800 dark:hover:bg-slate-800` (light pair was
 *      `bg-slate-50 hover:bg-slate-100`, dark should be `bg-slate-900
 *      hover:bg-slate-800`).
 *
 *   F. Tinted `bg-<color>-(50|100|200)` (semantic accent colors,
 *      not slate) without a `dark:bg-` partner on the same line.
 *      Catches the #970 BootstrapProgress case where pale tinted
 *      backgrounds rendered light-on-light in dark mode (washed
 *      out, near-unreadable).
 *
 * Exits non-zero with file:line:reason for each violation.
 *
 * SKIP_FILES below records pre-existing violators that are queued
 * for a separate sweep PR. Each entry should reference the tracking
 * issue. New files MUST NOT be added to this list — fix the
 * violation in the same PR that introduces it.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../src", import.meta.url));
const SKIP_DIRS = new Set(["test", "__mocks__"]);

/**
 * Files exempt from check F only — they carry pre-existing tinted-bg
 * violations being drained in a separate sweep ticket.
 *
 * Tracking: #987 (sweep). When that ticket lands the entire set
 * should empty out and this constant can be removed.
 *
 * Do NOT add new files here. Fix the violation in the same PR.
 */
const CHECK_F_SKIP_FILES = new Set([
  "src/components/broker/ValidationResultDisplay.tsx",
  "src/components/dashboard/AlertsStrip.tsx",
  "src/components/dashboard/RecentRecommendations.tsx",
  "src/components/instrument/CrossRefPopover.tsx",
  "src/components/instrument/EightKEventsPanel.tsx",
  "src/components/instrument/InsiderActivityPanel.tsx",
  "src/components/instrument/KeyStatsPane.tsx",
  "src/components/instrument/RightRail.tsx",
  "src/components/instrument/SummaryStrip.tsx",
  "src/components/instrument/dividendsShared.tsx",
  "src/components/orders/ClosePositionModal.tsx",
  "src/components/orders/DemoLivePill.tsx",
  "src/components/orders/OrderEntryModal.tsx",
  "src/components/rankings/RankingsTable.tsx",
  "src/components/recommendations/AuditTrail.tsx",
  "src/components/recommendations/RecommendationsTable.tsx",
  "src/components/settings/BudgetConfigSection.tsx",
  "src/components/settings/DisplayCurrencySection.tsx",
  "src/components/states/ErrorBanner.tsx",
  "src/components/ui/Pagination.tsx",
  "src/pages/AdminPage.tsx",
  "src/pages/ChartPage.tsx",
  "src/pages/CopyTradingPage.tsx",
  "src/pages/DashboardPage.tsx",
  "src/pages/EightKListPage.tsx",
  "src/pages/InstrumentPage.tsx",
  "src/pages/InstrumentsPage.tsx",
  "src/pages/LoginPage.tsx",
  "src/pages/OperatorsPage.tsx",
  "src/pages/ReportsPage.tsx",
  "src/pages/SettingsPage.tsx",
  "src/pages/SetupPage.tsx",
]);

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const st = statSync(full);
    if (st.isDirectory()) {
      if (SKIP_DIRS.has(entry)) continue;
      out.push(...walk(full));
    } else if (entry.endsWith(".tsx")) {
      out.push(full);
    }
  }
  return out;
}

const VARIANT_RE = /(?:dark|sm|md|lg|xl|2xl):(?:hover:|focus:|disabled:|placeholder:|active:|group-hover:|aria-[a-z-]+:)?[a-z][\w-]*(?:-\d+(?:\/\d+)?)?(?![\w/])/g;

function findDuplicateVariants(line) {
  const seen = new Map();
  const dups = new Set();
  let m;
  VARIANT_RE.lastIndex = 0;
  while ((m = VARIANT_RE.exec(line)) !== null) {
    const tok = m[0];
    if (seen.has(tok)) {
      dups.add(tok);
    } else {
      seen.set(tok, m.index);
    }
  }
  return [...dups];
}

function findMissingBorderPartner(line) {
  const hasLight = /(?<![\w-])border-slate-(?:200|300)(?![0-9])/.test(line);
  if (!hasLight) return null;
  if (/dark:border-/.test(line)) return null;
  return "border-slate-200|300 missing dark:border partner";
}

function findMissingHoverPartner(line) {
  const hasLight = /(?<![\w-])hover:bg-slate-(?:50|100)(?![0-9])/.test(line);
  if (!hasLight) return null;
  if (/dark:hover:bg-/.test(line)) return null;
  return "hover:bg-slate-50|100 missing dark:hover:bg- partner";
}

/** Check E: `dark:bg-X` and `dark:hover:bg-X` resolve to the same
 *  token — the hover is dead in dark mode (the ChartPage dead-hover
 *  bug from PR #711 round 2).
 *
 * Compares the body of the two tokens (after the `dark:` / `dark:hover:`
 * prefix) and flags when they match. Same-line scope is intentional —
 * the comparison only makes sense within one className.
 */
function findDeadDarkHover(line) {
  const baseMatch = line.match(
    /(?<![\w:-])dark:bg-([\w/-]+)/,
  );
  const hoverMatch = line.match(
    /(?<![\w:-])dark:hover:bg-([\w/-]+)/,
  );
  if (!baseMatch || !hoverMatch) return null;
  if (baseMatch[1] !== hoverMatch[1]) return null;
  return `dark:bg-${baseMatch[1]} and dark:hover:bg-${hoverMatch[1]} are identical — hover is a no-op in dark mode`;
}

/** Check F: tinted (non-slate) `bg-<color>-(50|100|200)` without a
 *  `dark:bg-` partner on the same line. Catches washed-out
 *  light-on-light banners in dark mode (the BootstrapProgress
 *  bug from #970).
 *
 * `bg-` must not be preceded by another utility prefix
 * (`hover:`, `dark:`, etc.) — those are handled by checks C / D / E.
 * Opacity suffix is accepted in either numeric (`/40`) or arbitrary
 * (`/[.35]`) form so the lookahead doesn't reject the base class
 * just because an opacity modifier follows.
 *
 * Slate is excluded — slate tints are the neutral surface and
 * already covered by checks B / C / D / E. Same-line scope matches
 * checks A-E; multi-element-on-one-line is a separate hygiene
 * concern not addressed by this gate.
 */
const TINT_COLORS_F =
  "blue|emerald|amber|rose|sky|cyan|orange|purple|pink|lime|teal|indigo|violet|fuchsia|yellow|red|green";
const TINTED_BG_RE = new RegExp(
  `(?<![\\w:-])bg-(?:${TINT_COLORS_F})-(?:50|100|200)(?:\\/(?:\\d+|\\[[^\\]]*\\]))?(?![\\w/-])`,
);
function findMissingTintedBgPartner(line) {
  if (!TINTED_BG_RE.test(line)) return null;
  if (/dark:bg-/.test(line)) return null;
  return "tinted bg-<color>-(50|100|200) missing dark:bg- partner";
}

/** Check D: dark:bg-* base added to an element whose only light bg
 *  was a hover state — produces an always-on background in dark mode
 *  (the Sidebar permanent-hover bug from PR #711).
 *
 * Trigger: `hover:bg-slate-...` (light hover) AND `dark:bg-...`
 * (non-state) on the same line, AND no light base `bg-...` (without
 * a state prefix) on the same line.
 *
 * Inputs / selects without any light bg utility (default user-agent
 * white) intentionally use `dark:bg-slate-900` to override only in
 * dark mode — those don't trigger because they have no light hover
 * either.
 */
function findOrphanDarkBg(line) {
  const lightHover = /(?<![\w-])hover:bg-(?:slate|gray|white|red|emerald|sky|amber|rose|orange|cyan|blue|purple|pink|lime)/.test(
    line,
  );
  if (!lightHover) return null;
  const stripped = line
    .replace(/dark:(?:hover|focus|active):bg-[\w/-]+/g, "")
    .replace(/(?:hover|focus|active|group-hover|aria-[a-z-]+):dark:bg-[\w/-]+/g, "");
  const darkBg = stripped.match(/(?<![\w:-])dark:bg-[\w/-]+/);
  if (!darkBg) return null;
  const baseRe =
    /(?:^|[\s'"`{}])bg-(?:white|black|transparent|current|inherit|slate-\d+|gray-\d+|red-\d+|emerald-\d+|sky-\d+|amber-\d+|rose-\d+|orange-\d+|cyan-\d+|blue-\d+|purple-\d+|pink-\d+|lime-\d+|teal-\d+|indigo-\d+|violet-\d+|fuchsia-\d+|yellow-\d+)(?:\/\d+)?(?![\w-])/;
  if (baseRe.test(line)) return null;
  return `${darkBg[0]} on a hover-only element produces always-on dark bg (PR #711 sidebar bug)`;
}

const violations = [];
const files = walk(ROOT);
for (const file of files) {
  const lines = readFileSync(file, "utf8").split("\n");
  const rel = relative(ROOT, file).split(sep).join("/");
  const relFromSrc = `src/${rel}`;
  const skipCheckF = CHECK_F_SKIP_FILES.has(relFromSrc);
  lines.forEach((line, i) => {
    const lineNo = i + 1;
    const dups = findDuplicateVariants(line);
    if (dups.length > 0) {
      violations.push({
        file,
        line: lineNo,
        reason: `duplicate Tailwind variant utility: ${dups.join(", ")}`,
      });
    }
    const borderMiss = findMissingBorderPartner(line);
    if (borderMiss) {
      violations.push({ file, line: lineNo, reason: borderMiss });
    }
    const hoverMiss = findMissingHoverPartner(line);
    if (hoverMiss) {
      violations.push({ file, line: lineNo, reason: hoverMiss });
    }
    const orphanDark = findOrphanDarkBg(line);
    if (orphanDark) {
      violations.push({ file, line: lineNo, reason: orphanDark });
    }
    const deadHover = findDeadDarkHover(line);
    if (deadHover) {
      violations.push({ file, line: lineNo, reason: deadHover });
    }
    if (!skipCheckF) {
      const tintedMiss = findMissingTintedBgPartner(line);
      if (tintedMiss) {
        violations.push({ file, line: lineNo, reason: tintedMiss });
      }
    }
  });
}

if (violations.length > 0) {
  console.error(`x ${violations.length} dark-mode class violation(s):\n`);
  for (const v of violations) {
    const rel = relative(process.cwd(), v.file).split(sep).join("/");
    console.error(`  ${rel}:${v.line}: ${v.reason}`);
  }
  console.error(
    "\nFix: add the missing dark: partner OR remove the duplicate utility.",
  );
  process.exit(1);
}

console.log(
  `OK dark-mode class gate: ${files.length} files, no violations`,
);
