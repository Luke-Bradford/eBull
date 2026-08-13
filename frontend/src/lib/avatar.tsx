/**
 * eToro-style initials avatar — one source for the tone hash + circle markup
 * that PositionsTable (dashboard), PortfolioPage (workstation) and
 * CopyTradingPage each previously re-declared (#1901 PR-2).
 *
 * The tone is a deterministic function of the username so the same trader
 * always gets the same colour across every surface.
 */

/** Deterministic avatar background, keyed on the username string. */
const AVATAR_TONES = [
  "bg-blue-600",
  "bg-emerald-600",
  "bg-amber-600",
  "bg-rose-600",
  "bg-violet-600",
  "bg-cyan-600",
] as const;

/** Stable colour for a username — same input always yields the same tone. */
export function avatarTone(name: string): string {
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) | 0;
  return AVATAR_TONES[Math.abs(hash) % AVATAR_TONES.length] ?? "bg-blue-600";
}

/** Circle diameter + text size per call site (behaviour-preserving mapping):
 *  sm = PortfolioPage row, md = Dashboard row, lg = CopyTradingPage header. */
const AVATAR_SIZES = {
  sm: "h-6 w-6 text-[10px]",
  md: "h-7 w-7 text-xs",
  lg: "h-8 w-8 text-sm",
} as const;

export function Avatar({
  username,
  size = "md",
}: {
  username: string;
  size?: keyof typeof AVATAR_SIZES;
}) {
  return (
    <span
      className={`inline-flex shrink-0 items-center justify-center rounded-full font-semibold text-white ${AVATAR_SIZES[size]} ${avatarTone(username)}`}
    >
      {username.charAt(0).toUpperCase()}
    </span>
  );
}
