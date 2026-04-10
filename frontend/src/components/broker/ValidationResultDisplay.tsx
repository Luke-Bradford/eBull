import type { ValidateCredentialResponse } from "@/api/brokerCredentials";

export function ValidationResultDisplay({
  result,
  error,
}: {
  result: ValidateCredentialResponse | null;
  error: string | null;
}): JSX.Element | null {
  if (error !== null) {
    return (
      <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
        {error}
      </div>
    );
  }
  if (result === null) return null;

  if (!result.auth_valid) {
    return (
      <div role="alert" className="rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700">
        Authentication failed — check your API key and user key.
      </div>
    );
  }

  if (!result.env_valid) {
    return (
      <div className="space-y-1">
        <div className="rounded bg-amber-50 px-2 py-1.5 text-xs text-amber-700">
          Authenticated, but environment check failed: {result.env_check}
        </div>
        {result.note && (
          <p className="text-xs text-slate-400">{result.note}</p>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-1">
      <div className="rounded bg-emerald-50 px-2 py-1.5 text-xs text-emerald-700">
        Connection verified
        {result.identity?.gcid != null && (
          <span className="ml-1 text-emerald-600">
            (account {result.identity.gcid})
          </span>
        )}
      </div>
      {result.note && (
        <p className="text-xs text-slate-400">{result.note}</p>
      )}
    </div>
  );
}
