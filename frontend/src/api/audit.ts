import { apiFetch } from "@/api/client";
import type { AuditDetail, AuditListResponse, AuditPassFail, AuditStage } from "@/api/types";

export interface AuditQuery {
  instrument_id: number | null;
  pass_fail: AuditPassFail | null;
  stage: AuditStage | null;
  date_from: string | null;
  date_to: string | null;
}

export const AUDIT_PAGE_LIMIT = 50;

export function fetchAuditList(
  query: AuditQuery,
  offset = 0,
  limit = AUDIT_PAGE_LIMIT,
): Promise<AuditListResponse> {
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  if (query.instrument_id !== null) params.set("instrument_id", String(query.instrument_id));
  if (query.pass_fail !== null) params.set("pass_fail", query.pass_fail);
  if (query.stage !== null) params.set("stage", query.stage);
  if (query.date_from !== null) params.set("date_from", query.date_from);
  if (query.date_to !== null) params.set("date_to", query.date_to);
  return apiFetch<AuditListResponse>(`/audit?${params.toString()}`);
}

export function fetchAuditDetail(id: number): Promise<AuditDetail> {
  return apiFetch<AuditDetail>(`/audit/${id}`);
}
