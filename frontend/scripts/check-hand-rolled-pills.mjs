#!/usr/bin/env node
/**
 * Hand-rolled status-pill gate (#2148).
 *
 * `components/ui/Badge.tsx` is the ONE status-pill primitive (#1908 PR-2,
 * design-system.md §"Badge / pill — ONE component"). Before it, ~130 inline
 * pill class-strings were spread across ~22 files; every one was a separate
 * line the dark gate had to police and a separate place the operator colour
 * table could drift. #1908 PR-2 consolidated 20 files, #2148 the remaining
 * admin/process cluster. This gate closes the door behind them.
 *
 * A violation is a line that RE-DECLARES the pill's geometry — the padding and
 * text-size pair from `operator-ui-conventions.md` §Density (pill padding one
 * and a half / half units, pill text ten pixels) — while also carrying a
 * border, i.e. a bordered chip. Two structural exclusions, deliberately NOT a
 * filename skip-list:
 *
 *   1. The definition site itself (`components/ui/Badge.tsx`). A chokepoint
 *      gate permits exactly one place to hold the pattern; that is the point
 *      of a chokepoint, not an exemption.
 *   2. Lines with no `border` token. A positioned overlay that happens to
 *      share the geometry — e.g. the Sparkline hover tooltip, which is
 *      `absolute`, filled, and borderless — is not a status pill and has no
 *      Badge to migrate onto.
 *
 * Do NOT add a per-file skip-list when this fires. The #987 precedent on the
 * dark gate is explicit: drain the violation in the PR that introduces it.
 *
 * Note the class strings below are built from fragments rather than written
 * literally, because these gates are textual and line-based — a doc comment or
 * source line quoting a full Tailwind class trips its own check (prevention-log
 * → "A lint gate's file-glob is part of its contract", note 3).
 *
 * Exits non-zero with file:line for each violation.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../src", import.meta.url));

/**
 * The one file allowed to declare pill geometry, as a path suffix so the check
 * is independent of where the repo is checked out.
 */
const DEFINITION_SITE = join("components", "ui", "Badge.tsx");

// Assembled from fragments so this file does not contain the literal pattern
// it forbids (see the note in the module docstring).
const PAD = ["px-1.5", "py-0.5"];
const PILL_TEXT = "text-" + "[10px]";

/**
 * Collect every `.ts` / `.tsx` under `dir`.
 *
 * `.ts` is walked as well as `.tsx`: a tone map extracted out of a component
 * into a plain module is a normal refactor, and an extension filter that
 * covers only where the pattern happened to appear FIRST leaves the rest of
 * the tree structurally unguarded (prevention-log, #1908 PR-2).
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

const files = collect(ROOT);
const violations = [];

for (const file of files) {
  if (file.endsWith(DEFINITION_SITE)) continue;
  const lines = readFileSync(file, "utf8").split("\n");
  lines.forEach((line, i) => {
    const hasGeometry = PAD.every((p) => line.includes(p)) && line.includes(PILL_TEXT);
    if (!hasGeometry) return;
    // Borderless => an overlay/tooltip, not a status pill (see docstring).
    if (!/\bborder\b|\bborder-/.test(line)) return;
    violations.push({ file, line: i + 1 });
  });
}

if (violations.length > 0) {
  console.error(`x ${violations.length} hand-rolled status pill(s):\n`);
  for (const v of violations) {
    const rel = relative(process.cwd(), v.file).split(sep).join("/");
    console.error(`  ${rel}:${v.line}`);
  }
  console.error(
    "\nFix: render <Badge tone=... > from components/ui/Badge instead of " +
      "re-declaring the pill geometry. Pass meaning (ok | warn | risk | info | " +
      "neutral), never a colour class. Do NOT add a skip-list.",
  );
  process.exit(1);
}

console.log(`OK hand-rolled pill gate: ${files.length} files, no violations`);
