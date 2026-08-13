/**
 * Capability-override drift view (#531, PR 3c of #515).
 *
 * Operators can edit ``exchanges.capabilities`` JSONB to adjust which
 * providers a venue uses. Without a drift view, those edits silently
 * accumulate and a future seed-default change can land on top of an
 * operator override unnoticed. This page diffs every exchange's current
 * capabilities against the migration-071 seed default and surfaces only
 * the divergences.
 *
 * Read-only — there is no revert-to-seed action here. Reverting requires
 * a mutating endpoint that does not yet exist (tracked as a #531
 * follow-up); the operator edits via SQL until then. The drift signal
 * itself — the core need in the issue's "Why" — is fully served here.
 */

import { Link } from "react-router-dom";

import { fetchCapabilityOverrides } from "@/api/capabilityOverrides";
import type { ExchangeOverrideRow } from "@/api/types";
import {
  Section,
  SectionError,
  SectionSkeleton,
} from "@/components/dashboard/Section";
import { useAsync } from "@/lib/useAsync";

export function CapabilityOverridesPage() {
  const overrides = useAsync(fetchCapabilityOverrides, []);

  return (
    <div className="space-y-4 pt-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-slate-800 dark:text-slate-100">
          Capability overrides
        </h1>
        <Link to="/admin" className="text-xs text-blue-700 hover:underline">
          ← Back to admin
        </Link>
      </div>

      <Section title="Exchanges diverging from seed defaults">
        {overrides.loading ? (
          <SectionSkeleton rows={5} />
        ) : overrides.error !== null ? (
          <SectionError onRetry={overrides.refetch} />
        ) : overrides.data ? (
          <OverridesTable rows={overrides.data.rows} />
        ) : null}
      </Section>
    </div>
  );
}

function OverridesTable({ rows }: { rows: ExchangeOverrideRow[] }) {
  if (rows.length === 0) {
    return (
      <p className="text-sm text-slate-500">
        Every exchange is at its seed default — no capability overrides in
        effect.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <p className="mb-3 text-xs text-slate-500">
        Each row is one capability whose provider set differs from the
        migration-071 seed default for that exchange&apos;s asset class.
        Providers are compared as a set (ordering is not drift).
      </p>
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="py-2 pr-4">Exchange</th>
            <th className="py-2 pr-4">Asset class</th>
            <th className="py-2 pr-4">Capability</th>
            <th className="py-2 pr-4">Seed default</th>
            <th className="py-2 pr-4">Current</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
          {rows.flatMap((row) =>
            row.diffs.map((diff) => (
              <tr
                key={`${row.exchange_id}:${diff.capability}`}
                className="align-top"
              >
                <td className="py-2 pr-4">
                  <div className="font-medium text-slate-700 dark:text-slate-200">
                    {row.exchange_name ?? "—"}
                  </div>
                  <div className="font-mono text-xs text-slate-500">
                    #{row.exchange_id}
                  </div>
                </td>
                <td className="py-2 pr-4 text-xs text-slate-500">
                  {row.asset_class ?? "—"}
                </td>
                <td className="py-2 pr-4 text-xs font-medium text-slate-700 dark:text-slate-200">
                  {diff.capability}
                </td>
                <td className="py-2 pr-4">
                  <ProviderChips
                    providers={diff.seed_providers}
                    against={diff.current_providers}
                    tone="seed"
                  />
                </td>
                <td className="py-2 pr-4">
                  <ProviderChips
                    providers={diff.current_providers}
                    against={diff.seed_providers}
                    tone="current"
                  />
                </td>
              </tr>
            )),
          )}
        </tbody>
      </table>
    </div>
  );
}

/**
 * Render a provider set as chips. Providers absent from the comparison
 * set are highlighted as the drift: a seed provider missing from current
 * (a removal) and a current provider absent from seed (an addition) both
 * get a tinted chip so the operator can spot the exact change.
 */
function ProviderChips({
  providers,
  against,
  tone,
}: {
  providers: string[];
  against: string[];
  tone: "seed" | "current";
}) {
  if (providers.length === 0) {
    return <span className="text-xs text-slate-400">— (none)</span>;
  }
  const againstSet = new Set(against);
  // seed side: highlight providers missing from current (removed).
  // current side: highlight providers not in seed (added).
  const drifted = tone === "seed" ? "text-red-700 dark:text-red-300" : "text-amber-700 dark:text-amber-300";
  const driftedBg =
    tone === "seed"
      ? "bg-red-50 dark:bg-red-950/40"
      : "bg-amber-50 dark:bg-amber-950/40";
  return (
    <div className="flex flex-wrap gap-1">
      {providers.map((p) => {
        const isDrift = !againstSet.has(p);
        return (
          <span
            key={p}
            className={
              isDrift
                ? `rounded px-1.5 py-0.5 font-mono text-xs ${drifted} ${driftedBg}`
                : "rounded px-1.5 py-0.5 font-mono text-xs text-slate-600 dark:text-slate-300 bg-slate-100 dark:bg-slate-800"
            }
          >
            {p}
          </span>
        );
      })}
    </div>
  );
}
