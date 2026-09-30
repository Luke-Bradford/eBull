import { apiFetch } from "@/api/client";
import type { AiTrialStatusResponse } from "@/api/types";

/** #3514: the AI trial's readiness, every no-op state named. */
export function fetchAiTrialStatus(): Promise<AiTrialStatusResponse> {
  return apiFetch("/ai-trial/status");
}
