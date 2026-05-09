/**
 * Admin control hub /system/processes fetchers (#1076 / #1064).
 *
 * Mirrors app/api/processes.py — see types.ts for the shapes. Drift
 * here silently breaks ProcessesTable; both files update in the same
 * PR per api-shape-and-types.md.
 */

import { apiFetch } from "@/api/client";
import type {
  CancelRequestBody,
  CancelResponse,
  ErrorClassSummaryResponse,
  ProcessListResponse,
  ProcessRowResponse,
  ProcessRunSummaryResponse,
  TriggerRequestBody,
  TriggerResponse,
} from "@/api/types";

export function fetchProcesses(): Promise<ProcessListResponse> {
  return apiFetch<ProcessListResponse>("/system/processes");
}

export function fetchProcess(processId: string): Promise<ProcessRowResponse> {
  return apiFetch<ProcessRowResponse>(
    `/system/processes/${encodeURIComponent(processId)}`,
  );
}

export function fetchProcessRuns(
  processId: string,
  days = 7,
): Promise<ProcessRunSummaryResponse[]> {
  const params = new URLSearchParams({ days: String(days) });
  return apiFetch<ProcessRunSummaryResponse[]>(
    `/system/processes/${encodeURIComponent(processId)}/runs?${params.toString()}`,
  );
}

export function fetchProcessRunErrors(
  processId: string,
  runId: number,
): Promise<ErrorClassSummaryResponse[]> {
  return apiFetch<ErrorClassSummaryResponse[]>(
    `/system/processes/${encodeURIComponent(processId)}/runs/${runId}/errors`,
  );
}

export function triggerProcess(
  processId: string,
  body: TriggerRequestBody,
): Promise<TriggerResponse> {
  return apiFetch<TriggerResponse>(
    `/system/processes/${encodeURIComponent(processId)}/trigger`,
    { method: "POST", body: JSON.stringify(body) },
  );
}

export function cancelProcess(
  processId: string,
  body: CancelRequestBody,
): Promise<CancelResponse> {
  return apiFetch<CancelResponse>(
    `/system/processes/${encodeURIComponent(processId)}/cancel`,
    { method: "POST", body: JSON.stringify(body) },
  );
}
