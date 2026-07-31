#!/usr/bin/env node
/**
 * Chart-integrity gate (#2185).
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
 * SCOPE: `components/fundamentals/` only, per the spec's §4 "reduced chart
 * infrastructure" decision — the other chart surfaces have not been swept yet,
 * and a gate that fails on day one for pre-existing debt gets disabled rather
 * than obeyed. Widen the scope in the PR that drains the next surface; do NOT
 * add a per-file skip-list (the #987 dark-gate precedent).
 *
 * Rule B's one structural exclusion is the definition site itself. That is
 * what makes it a chokepoint, not an exemption.
 *
 * Note the forbidden strings are assembled from fragments below: these gates
 * are textual and line-based, so writing the banned literal in this file — even
 * inside a comment — would trip the check against itself (the same reason
 * `check-hand-rolled-pills.mjs` builds its class fragments).
 *
 * Exits non-zero with file:line:reason for each violation.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../src", import.meta.url));

/** Path suffix of the scanned area, so the check is checkout-independent. */
const SCOPE = join("components", "fundamentals");

/** The one module allowed to name the raw palettes. */
const THEME_DEFINITION_SITE = join("lib", "chartTheme.ts");

// Assembled from fragments — see the note in the module docstring.
const CURVE_PROP = "type=" + '"' + "mono" + "tone" + '"';
const RAW_PALETTES = ["light" + "Theme.", "dark" + "Theme."];

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

const files = collect(ROOT).filter((f) => f.includes(SCOPE + sep));
const violations = [];

for (const file of files) {
  const lines = readFileSync(file, "utf8").split("\n");
  lines.forEach((line, i) => {
    if (line.includes(CURVE_PROP)) {
      violations.push({
        file,
        line: i + 1,
        reason:
          "cubic interpolation on a discrete financial series — use the linear curve type",
      });
    }
    if (file.endsWith(THEME_DEFINITION_SITE)) return;
    for (const palette of RAW_PALETTES) {
      if (line.includes(palette)) {
        violations.push({
          file,
          line: i + 1,
          reason: `raw palette \`${palette}\` outside lib/chartTheme.ts — read the resolved palette via useChartTheme()`,
        });
      }
    }
  });
}

if (violations.length > 0) {
  console.error(`x ${violations.length} chart-integrity violation(s):\n`);
  for (const v of violations) {
    const rel = relative(process.cwd(), v.file).split(sep).join("/");
    console.error(`  ${rel}:${v.line}: ${v.reason}`);
  }
  console.error(
    "\nSee .claude/skills/frontend/design-system.md " +
      '§"Chart theming — ONE source". Do NOT add a skip-list.',
  );
  process.exit(1);
}

console.log(`OK chart-integrity gate: ${files.length} files, no violations`);
