/**
 * Jobs API client (issue #13 PR B).
 *
 * Surfaces:
 *   - fetchJobsOverview() — declared schedule + last-run summary, polled
 *     by the admin page jobs table. Wraps GET /system/jobs (which has
 *     existed since #57); the alias lives here so the admin page only
 *     imports from one module.
 *   - fetchJobRuns(jobName?, limit?) — recent rows from job_runs,
 *     newest-first; backs the admin page recent-runs panel.
 *   - runJob(jobName) — POST /jobs/{name}/run. Returns void on 202;
 *     ApiError on 404 (unknown job) or 409 (already running). Other
 *     statuses bubble up unchanged.
 */

import { apiFetch } from "@/api/client";
import type { JobRunsListResponse, JobsListResponse } from "@/api/types";

export function fetchJobsOverview(): Promise<JobsListResponse> {
  return apiFetch<JobsListResponse>("/system/jobs");
}

export function fetchJobRuns(
  jobName?: string | null,
  limit: number = 50,
): Promise<JobRunsListResponse> {
  const params = new URLSearchParams();
  // Use ``!= null`` (not falsy) so an empty-string filter is still
  // forwarded to the backend rather than silently dropped. The
  // declared type is ``string | null | undefined``; only the latter
  // two should be treated as "no filter".
  if (jobName != null) params.set("job_name", jobName);
  params.set("limit", String(limit));
  return apiFetch<JobRunsListResponse>(`/jobs/runs?${params.toString()}`);
}

export function runJob(jobName: string): Promise<void> {
  // The 202 path returns no body; apiFetch short-circuits at the
  // status check (see client.ts). 404 / 409 surface as ApiError so
  // the page can render a status-specific message.
  return apiFetch<void>(`/jobs/${encodeURIComponent(jobName)}/run`, {
    method: "POST",
  });
}
