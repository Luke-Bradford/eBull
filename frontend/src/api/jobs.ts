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
 *   - runJob(jobName, body?) — POST /jobs/{name}/run. Returns
 *     {request_id} on 202 (PR1b-2 #1064 added the body); ApiError on
 *     404 (unknown job) or 400 (invalid params / control). Other
 *     statuses bubble up unchanged. PR2 #1064 widens to accept the
 *     {params, control} envelope so the Advanced disclosure form can
 *     submit operator-supplied params.
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

export interface RunJobBody {
  params?: Record<string, unknown>;
  control?: { override_bootstrap_gate?: boolean };
}

export interface RunJobQueuedResponse {
  request_id: number;
}

export function runJob(
  jobName: string,
  body?: RunJobBody,
): Promise<RunJobQueuedResponse | undefined> {
  // PR1b-2 #1064 made the 202 body carry {request_id} so the operator
  // can pivot to the queue row. apiFetch reads JSON when present and
  // falls back to undefined on empty body — defensive on the
  // never-supposed-to-fire case where BE drops the body.
  //
  // Zero-arg calls preserve the pre-PR2 shape: no body sent, BE
  // _safe_read_json_body returns {} which legacy-flat-dict-normalises
  // to {params: {}, control: {}}.
  return apiFetch<RunJobQueuedResponse | undefined>(
    `/jobs/${encodeURIComponent(jobName)}/run`,
    {
      method: "POST",
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    },
  );
}
