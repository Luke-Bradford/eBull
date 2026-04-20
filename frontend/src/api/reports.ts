import { apiFetch } from "@/api/client";

export interface ReportSnapshot {
  snapshot_id: number;
  report_type: "weekly" | "monthly";
  period_start: string;
  period_end: string;
  snapshot_json: Record<string, unknown>;
  computed_at: string;
}

export function fetchWeeklyReports(limit = 10): Promise<ReportSnapshot[]> {
  return apiFetch<ReportSnapshot[]>(`/api/reports/weekly?limit=${limit}`);
}

export function fetchMonthlyReports(limit = 10): Promise<ReportSnapshot[]> {
  return apiFetch<ReportSnapshot[]>(`/api/reports/monthly?limit=${limit}`);
}

export function fetchLatestReport(
  reportType: "weekly" | "monthly",
): Promise<ReportSnapshot | null> {
  return apiFetch<ReportSnapshot | null>(
    `/api/reports/latest?report_type=${reportType}`,
  );
}
