import type { AuditStage } from "@/api/types";

interface GuardRule {
  rule: string;
  passed: boolean;
  detail: string;
}

function isGuardRules(value: unknown): value is GuardRule[] {
  if (!Array.isArray(value)) return false;
  return value.every(
    (item) =>
      typeof item === "object" &&
      item !== null &&
      "rule" in item &&
      "passed" in item &&
      typeof (item as Record<string, unknown>).passed === "boolean" &&
      "detail" in item,
  );
}

export function EvidencePanel({
  stage,
  evidence,
}: {
  stage: AuditStage;
  evidence: Record<string, unknown> | Record<string, unknown>[] | null;
}) {
  if (evidence === null) {
    return <p className="text-xs text-slate-500">No evidence recorded.</p>;
  }

  if (stage === "execution_guard" && isGuardRules(evidence)) {
    return <GuardChecklist rules={evidence} />;
  }

  return <GenericEvidence data={evidence} />;
}

function GuardChecklist({ rules }: { rules: GuardRule[] }) {
  return (
    <ul className="space-y-1">
      {rules.map((r) => (
        <li key={r.rule} className="flex items-start gap-2 text-xs">
          <span
            className={`mt-0.5 shrink-0 text-sm ${r.passed ? "text-emerald-600" : "text-red-600"}`}
            aria-label={r.passed ? "Passed" : "Failed"}
          >
            {r.passed ? "✓" : "✗"}
          </span>
          <div>
            <span className="font-medium text-slate-700">{r.rule}</span>
            {r.detail ? (
              <span className="ml-1 text-slate-500">— {r.detail}</span>
            ) : null}
          </div>
        </li>
      ))}
    </ul>
  );
}

function GenericEvidence({ data }: { data: Record<string, unknown> | Record<string, unknown>[] }) {
  return (
    <pre className="max-h-48 overflow-auto rounded bg-slate-100 p-2 text-xs text-slate-700">
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}
