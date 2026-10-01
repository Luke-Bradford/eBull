import { apiFetch } from "@/api/client";
import type { AiTrialStatusResponse } from "@/api/types";

/** The registered trial versions' arm ids (#3515; `ai_trial_version.TRIAL_VERSIONS`). */
export const AI_TRIAL_V1_ARM = "ai-discretionary-v1";
export const AI_TRIAL_FUND_V1_ARM = "ai-discretionary-fund-v1";

/** #3514: the AI trial's readiness, every no-op state named; #3515: per trial version. */
export function fetchAiTrialStatus(arm: string = AI_TRIAL_V1_ARM): Promise<AiTrialStatusResponse> {
  return apiFetch(`/ai-trial/status?arm=${encodeURIComponent(arm)}`);
}
