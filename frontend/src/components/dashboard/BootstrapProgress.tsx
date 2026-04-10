/**
 * First-run bootstrap progress panel.
 *
 * Shown on the dashboard when the system is in first-run state:
 * credentials are saved but the pipeline has not yet populated data.
 *
 * Derives progress from the /system/status layer states and job
 * statuses. Disappears once data layers are no longer all empty.
 */

import type { SystemStatusResponse } from "@/api/types";

type BootstrapStage =
  | "no_credentials"
  | "syncing"
  | "seeding"
  | "loading_data"
  | "ready";

interface StepInfo {
  label: string;
  done: boolean;
  active: boolean;
}

function deriveStage(system: SystemStatusResponse): BootstrapStage {
  const layers = system.layers;
  const jobs = system.jobs;

  // If any layer has data (status != "empty"), the system is past bootstrap.
  const allLayersEmpty = layers.length > 0 && layers.every((l) => l.status === "empty");
  if (!allLayersEmpty) return "ready";

  // Check job states to determine what stage we're in.
  const universeJob = jobs.find((j) => j.name === "nightly_universe_sync");
  const marketJob = jobs.find((j) => j.name === "hourly_market_refresh");

  // If universe sync is currently running, we're syncing.
  if (universeJob?.last_status === "running") return "syncing";

  // If universe sync has completed but market refresh hasn't run yet,
  // we're in the seeding/loading stage.
  if (universeJob?.last_status === "success") {
    if (marketJob?.last_status === "running") return "loading_data";
    return "seeding";
  }

  // No universe sync has run yet — credentials may be missing.
  return "no_credentials";
}

function buildSteps(stage: BootstrapStage): StepInfo[] {
  const stages: BootstrapStage[] = [
    "no_credentials",
    "syncing",
    "seeding",
    "loading_data",
    "ready",
  ];
  const stageIndex = stages.indexOf(stage);

  return [
    {
      label: "Credentials saved",
      done: stageIndex > 0,
      active: stageIndex === 0,
    },
    {
      label: "Universe syncing",
      done: stageIndex > 1,
      active: stageIndex === 1,
    },
    {
      label: "Coverage seeding",
      done: stageIndex > 2,
      active: stageIndex === 2,
    },
    {
      label: "Market data loading",
      done: stageIndex > 3,
      active: stageIndex === 3,
    },
  ];
}

/**
 * Returns true if the system is in bootstrap state (all layers empty),
 * meaning this panel should be shown.
 */
export function isBootstrapping(system: SystemStatusResponse | null): boolean {
  if (system === null) return false;
  return deriveStage(system) !== "ready";
}

export function BootstrapProgress({
  system,
}: {
  system: SystemStatusResponse;
}) {
  const stage = deriveStage(system);
  if (stage === "ready") return null;

  const steps = buildSteps(stage);

  return (
    <div className="rounded-md border border-blue-200 bg-blue-50 p-4">
      <h2 className="text-sm font-semibold text-blue-800">
        Getting started
      </h2>
      <p className="mt-1 text-xs text-blue-700">
        {stage === "no_credentials"
          ? "Save your eToro credentials to start the data pipeline."
          : "The data pipeline is bootstrapping. This usually takes a few minutes."}
      </p>
      <ul className="mt-3 space-y-1.5">
        {steps.map((step) => (
          <li key={step.label} className="flex items-center gap-2 text-sm">
            {step.done ? (
              <span className="text-emerald-600">&#10003;</span>
            ) : step.active ? (
              <span className="inline-block h-3 w-3 animate-pulse rounded-full bg-blue-400" />
            ) : (
              <span className="inline-block h-3 w-3 rounded-full border border-slate-300" />
            )}
            <span
              className={
                step.done
                  ? "text-slate-600"
                  : step.active
                    ? "font-medium text-blue-800"
                    : "text-slate-400"
              }
            >
              {step.label}
              {step.active ? "..." : ""}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
