#!/usr/bin/env node
/**
 * Dark-mode class hygiene gate (#708).
 *
 * Four checks run line-by-line over every .tsx in frontend/src:
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
 * Exits non-zero with file:line:reason for each violation.
 */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../src", import.meta.url));
const SKIP_DIRS = new Set(["test", "__mocks__"]);

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
