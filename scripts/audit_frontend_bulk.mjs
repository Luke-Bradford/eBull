#!/usr/bin/env node
// audit_frontend_bulk.mjs — #2038: `pnpm audit` replacement.
//
// On 2026-07-15 the npm registry retired BOTH classic audit endpoints
// (/-/npm/v1/security/audits/quick and /audits -> 410 Gone) and pnpm
// (verified on 10.33.0 / 11.0.6 / 11.13.0) has no bulk-endpoint support,
// so `pnpm audit` fails on every CI run. This script POSTs the resolved
// package set to the registry's bulk advisory endpoint instead. The
// endpoint does SERVER-SIDE version filtering (verified empirically:
// lodash@4.17.20 returns the <4.17.21 high advisory, 4.17.21 does not),
// so no client-side semver matching is needed.
//
// Usage (CI):
//   pnpm --dir frontend list --prod --depth Infinity --json > prod-tree.json
//   pnpm --dir frontend list --depth Infinity --json \
//     | node scripts/audit_frontend_bulk.mjs --audit-level high --prod-tree prod-tree.json
//
// Exit codes: 0 = no advisory at/above the floor; 1 = advisories found;
// 2 = harness failure (empty package set, endpoint error) — fail-closed,
// a broken audit must not pass as a clean one.
//
// Revert path: when pnpm ships bulk-advisory audit support, swap the CI
// step back to `pnpm --dir frontend audit --audit-level high`.
//
// Accepted advisories (#3594): an advisory with NO patched version cannot be
// cleared by an upgrade, so without an exception it fails every PR. An entry
// below is honoured only while (a) today is before its `until` date — after
// that the gate fails again and the entry must be re-justified or removed —
// and (b) the package is NOT reachable from production dependencies,
// read on every run from the `--prod-tree` listing (`pnpm list --prod`). The
// combined listing cannot answer (b): pnpm prints a shared subtree once and
// every later occurrence as a childless `deduped` node, so a package first
// expanded under a dev root looks dev-only (Codex ckpt-2, #3594). Within the
// prod-only listing every deduped node is expanded elsewhere in that same
// listing, which the script checks; no `--prod-tree` means no exception.
// Accepted findings are printed, never hidden.

import { readFileSync } from "node:fs";

const BULK_URL = "https://registry.npmjs.org/-/npm/v1/security/advisories/bulk";
const SEVERITY_ORDER = { low: 0, moderate: 1, high: 2, critical: 3 };
// Observed constraint, not an assumption: the endpoint accepted 100-name
// chunks for the real 327-package frontend tree (4 POSTs, all 2xx) — and
// npm's own audit client batches its bulk requests similarly. Not a
// documented registry limit; if a future tree trips a 413, lower this.
const CHUNK_SIZE = 100;
const RETRY_DELAY_MS = 2000;
// Fail fast into the documented exit(2) instead of hanging the CI job on a
// stalled TCP connection (PR #2037 review WARNING).
const FETCH_TIMEOUT_MS = 30_000;

const ACCEPTED_ADVISORIES = [
  {
    // braces <= 3.0.3 stack-exhaustion DoS; first_patched_version null and
    // 3.0.3 is the latest release. Reached only via tailwindcss (dev) ->
    // chokidar/micromatch: build-time expansion of our own config globs.
    url: "https://github.com/advisories/GHSA-vfj7-8cjw-p6xm",
    name: "braces",
    issue: "#3594",
    // Re-check date, by construction 30 days from acceptance (2026-10-03).
    until: "2026-11-02",
  },
];

function parseArgs(argv) {
  const i = argv.indexOf("--audit-level");
  const level = i >= 0 ? argv[i + 1] : "high";
  if (!(level in SEVERITY_ORDER)) {
    console.error(`unknown --audit-level ${level}; expected one of ${Object.keys(SEVERITY_ORDER)}`);
    process.exit(2);
  }
  const j = argv.indexOf("--prod-tree");
  return { level, prodTree: j >= 0 ? argv[j + 1] : null };
}

// The registry package name: pnpm keys an aliased dependency
// (`alias: npm:real@x`) by its alias and puts the real name in `from` (Codex
// ckpt-2, #3594). Auditing or proving reachability by alias would miss it.
function packageName(key, info) {
  return typeof info.from === "string" && info.from.length > 0 ? info.from : key;
}

// Every package name reachable from production roots, from a `pnpm list
// --prod` listing, plus the listing's root keys (checked against the combined
// listing by the caller). Exit 2 (harness failure) on anything that would make
// the closure incomplete — an empty listing, a node without a version, a
// deduped name never expanded: an incomplete production set would read as
// "dev-only" and fail OPEN (Codex ckpt-2, #3594).
function productionNames(path) {
  let projects;
  try {
    projects = JSON.parse(readFileSync(path, "utf8"));
  } catch (err) {
    console.error(`--prod-tree ${path} unreadable: ${err.message}`);
    process.exit(2);
  }
  const names = new Set();
  const roots = new Set();
  const expanded = new Set();
  const deduped = new Set();
  const visit = (deps) => {
    if (!deps) return;
    for (const [key, info] of Object.entries(deps)) {
      if (!info || typeof info.version !== "string") {
        console.error(`--prod-tree ${path}: dependency ${key} has no version`);
        process.exit(2);
      }
      const name = packageName(key, info);
      names.add(name);
      if (info.deduped) deduped.add(name);
      else expanded.add(name);
      visit(info.dependencies);
    }
  };
  const list = Array.isArray(projects) ? projects : [projects];
  if (list.length === 0) {
    console.error(`--prod-tree ${path} lists no project`);
    process.exit(2);
  }
  for (const project of list) {
    for (const k of [...Object.keys(project.dependencies ?? {}), ...Object.keys(project.optionalDependencies ?? {})]) {
      roots.add(k);
    }
    if (project.devDependencies && Object.keys(project.devDependencies).length > 0) {
      console.error(`--prod-tree ${path} lists devDependencies; expected \`pnpm list --prod\` output`);
      process.exit(2);
    }
    visit(project.dependencies);
    visit(project.optionalDependencies);
  }
  const unexpanded = [...deduped].filter((n) => !expanded.has(n));
  if (unexpanded.length > 0) {
    console.error(`--prod-tree ${path}: deduped but never expanded: ${unexpanded.join(", ")}`);
    process.exit(2);
  }
  if (names.size === 0) {
    console.error(`--prod-tree ${path}: no production packages collected`);
    process.exit(2);
  }
  return { names, roots };
}

// The prod listing must describe the same production roots as the combined
// listing on stdin; otherwise it is a different or truncated tree.
function sameRoots(prodRoots, projects) {
  const combined = new Set();
  for (const project of projects) {
    for (const k of [...Object.keys(project.dependencies ?? {}), ...Object.keys(project.optionalDependencies ?? {})]) {
      combined.add(k);
    }
  }
  return combined.size === prodRoots.size && [...combined].every((k) => prodRoots.has(k));
}

async function readStdin() {
  let data = "";
  for await (const chunk of process.stdin) data += chunk;
  return data;
}

// Walk `pnpm list --json` output: an array of projects, each with
// dependencies/devDependencies/optionalDependencies maps of
// name -> {version, dependencies?: <nested same shape>}.
function collectPackages(projects) {
  const versions = new Map(); // name -> Set(version)
  const visit = (deps) => {
    if (!deps) return;
    for (const [key, info] of Object.entries(deps)) {
      if (!info || typeof info.version !== "string") continue;
      const name = packageName(key, info);
      if (!versions.has(name)) versions.set(name, new Set());
      versions.get(name).add(info.version);
      visit(info.dependencies);
    }
  };
  for (const project of projects) {
    visit(project.dependencies);
    visit(project.devDependencies);
    visit(project.optionalDependencies);
  }
  return versions;
}

// Why an at/above-floor finding is accepted, or null when it blocks. `prod`
// is null when no --prod-tree was given: no exception can then be honoured.
function acceptance(finding, prod, today) {
  const entry = ACCEPTED_ADVISORIES.find((a) => a.url === finding.url && a.name === finding.name);
  if (!entry) return null;
  if (today >= entry.until) return { blocked: `exception ${entry.issue} expired ${entry.until}` };
  if (prod === null) return { blocked: `exception ${entry.issue} needs --prod-tree to prove the package dev-only` };
  if (prod.has(finding.name)) return { blocked: `exception ${entry.issue} void: reachable from production dependencies` };
  return { accepted: `dev-only, no patch; ${entry.issue} until ${entry.until}` };
}

async function postChunk(chunk, attempt = 1) {
  let res;
  try {
    res = await fetch(BULK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(chunk),
      signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
    });
  } catch (err) {
    // Network-level failure (DNS blip, connection reset, timeout abort):
    // one retry with backoff, then fail closed via the caller's exit(2).
    if (attempt === 1) {
      await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
      return postChunk(chunk, 2);
    }
    throw new Error(`bulk advisory request failed at network level: ${err.message}`);
  }
  if (!res.ok) {
    // Retry 5xx and 429 (rate limit) once with a short backoff — don't
    // hammer a flaky registry, don't fail the audit on one transient.
    if ((res.status >= 500 || res.status === 429) && attempt === 1) {
      await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
      return postChunk(chunk, 2);
    }
    throw new Error(`bulk advisory endpoint returned ${res.status}`);
  }
  return res.json();
}

const { level: floor, prodTree } = parseArgs(process.argv.slice(2));
const prodListing = prodTree === null ? null : productionNames(prodTree);
const raw = await readStdin();
let projects;
try {
  projects = JSON.parse(raw);
} catch {
  console.error("stdin was not valid JSON — expected `pnpm list --depth Infinity --json` output");
  process.exit(2);
}
const projectList = Array.isArray(projects) ? projects : [projects];
const versions = collectPackages(projectList);
if (prodListing !== null && !sameRoots(prodListing.roots, projectList)) {
  console.error(`--prod-tree ${prodTree}: its production roots differ from the combined listing's`);
  process.exit(2);
}
const prod = prodListing === null ? null : prodListing.names;
if (versions.size === 0) {
  // Fail-closed: an empty set means the list step broke, not a clean tree.
  console.error("no packages collected from stdin — refusing to pass an empty audit");
  process.exit(2);
}

const names = [...versions.keys()];
const findings = [];
for (let i = 0; i < names.length; i += CHUNK_SIZE) {
  const chunk = Object.fromEntries(names.slice(i, i + CHUNK_SIZE).map((n) => [n, [...versions.get(n)]]));
  let body;
  try {
    body = await postChunk(chunk);
  } catch (err) {
    console.error(`bulk advisory request failed: ${err.message}`);
    process.exit(2);
  }
  for (const [name, advisories] of Object.entries(body)) {
    for (const adv of advisories) {
      findings.push({
        name,
        installed: [...versions.get(name)].join(", "),
        severity: adv.severity,
        title: adv.title,
        url: adv.url,
        range: adv.vulnerable_versions,
      });
    }
  }
}

// Fail-closed on an unrecognized severity: a registry schema change must be
// a harness failure, not a silently-ignored advisory (Codex review, #2038).
const unknown = findings.filter((f) => !(f.severity in SEVERITY_ORDER));
if (unknown.length > 0) {
  console.error(`unrecognized severity values from the bulk endpoint: ${unknown.map((f) => `${f.name}:${f.severity}`).join(", ")}`);
  process.exit(2);
}
const today = new Date().toISOString().slice(0, 10);
const atOrAbove = findings.filter((f) => SEVERITY_ORDER[f.severity] >= SEVERITY_ORDER[floor]);
const below = findings.length - atOrAbove.length;
const blocking = [];
console.log(`audited ${versions.size} packages against the npm bulk advisory endpoint`);
if (below > 0) console.log(`${below} advisories below the '${floor}' floor (ignored)`);
for (const f of atOrAbove) {
  const verdict = acceptance(f, prod, today);
  if (verdict?.accepted) {
    console.log(`ACCEPTED [${f.severity}] ${f.name} (installed: ${f.installed}) — ${f.url} — ${verdict.accepted}`);
  } else {
    blocking.push({ ...f, note: verdict?.blocked });
  }
}
if (blocking.length === 0) {
  console.log(`no unaccepted advisories at or above '${floor}'`);
  process.exit(0);
}
console.error(`\n${blocking.length} advisories at or above '${floor}':`);
for (const f of blocking) {
  console.error(`  [${f.severity}] ${f.name} (installed: ${f.installed}; vulnerable: ${f.range})`);
  console.error(`    ${f.title} — ${f.url}`);
  if (f.note) console.error(`    ${f.note}`);
}
process.exit(1);
