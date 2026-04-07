import { apiFetch } from "@/api/client";
import type { ConfigResponse } from "@/api/types";

export function fetchConfig(): Promise<ConfigResponse> {
  return apiFetch<ConfigResponse>("/config");
}
