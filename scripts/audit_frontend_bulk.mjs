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
//   pnpm --dir frontend list --depth Infinity --json \
//     | node scripts/audit_frontend_bulk.mjs --audit-level high
//
// Exit codes: 0 = no advisory at/above the floor; 1 = advisories found;
// 2 = harness failure (empty package set, endpoint error) — fail-closed,
// a broken audit must not pass as a clean one.
//
// Revert path: when pnpm ships bulk-advisory audit support, swap the CI
// step back to `pnpm --dir frontend audit --audit-level high`.

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

function parseArgs(argv) {
  const i = argv.indexOf("--audit-level");
  const level = i >= 0 ? argv[i + 1] : "high";
  if (!(level in SEVERITY_ORDER)) {
    console.error(`unknown --audit-level ${level}; expected one of ${Object.keys(SEVERITY_ORDER)}`);
    process.exit(2);
  }
  return level;
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
    for (const [name, info] of Object.entries(deps)) {
      if (!info || typeof info.version !== "string") continue;
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

const floor = parseArgs(process.argv.slice(2));
const raw = await readStdin();
let projects;
try {
  projects = JSON.parse(raw);
} catch {
  console.error("stdin was not valid JSON — expected `pnpm list --depth Infinity --json` output");
  process.exit(2);
}
const versions = collectPackages(Array.isArray(projects) ? projects : [projects]);
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
const atOrAbove = findings.filter((f) => SEVERITY_ORDER[f.severity] >= SEVERITY_ORDER[floor]);
const below = findings.length - atOrAbove.length;
console.log(`audited ${versions.size} packages against the npm bulk advisory endpoint`);
if (below > 0) console.log(`${below} advisories below the '${floor}' floor (ignored)`);
if (atOrAbove.length === 0) {
  console.log(`no advisories at or above '${floor}'`);
  process.exit(0);
}
console.error(`\n${atOrAbove.length} advisories at or above '${floor}':`);
for (const f of atOrAbove) {
  console.error(`  [${f.severity}] ${f.name} (installed: ${f.installed}; vulnerable: ${f.range})`);
  console.error(`    ${f.title} — ${f.url}`);
}
process.exit(1);
