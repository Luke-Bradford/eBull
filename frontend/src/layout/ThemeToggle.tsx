/**
 * ThemeToggle — header-mounted light / dark / system selector.
 *
 * Three-way segmented switch backed by `useTheme()` (#690 Phase 1).
 * Stays small enough to fit beside the sign-out button without
 * crowding the operator-name + status-dot row.
 */
import { useTheme, type ThemePreference } from "@/lib/theme";

const OPTIONS: ReadonlyArray<{
  readonly value: ThemePreference;
  readonly label: string;
  readonly title: string;
}> = [
  { value: "light", label: "L", title: "Light theme" },
  { value: "system", label: "S", title: "Match operating-system theme" },
  { value: "dark", label: "D", title: "Dark theme" },
];

export function ThemeToggle(): JSX.Element {
  const { preference, setPreference } = useTheme();
  return (
    <div
      role="group"
      aria-label="Theme"
      className="flex items-center gap-0 rounded border border-slate-200 bg-slate-50 p-0.5 dark:border-slate-700 dark:bg-slate-800"
    >
      {OPTIONS.map((opt) => {
        const active = preference === opt.value;
        return (
          <button
            key={opt.value}
            type="button"
            onClick={() => setPreference(opt.value)}
            title={opt.title}
            aria-pressed={active}
            className={[
              "rounded px-1.5 py-0.5 text-[11px] font-semibold transition-colors",
              active
                ? "bg-white text-slate-900 shadow-sm dark:bg-slate-950 dark:text-slate-100"
                : "text-slate-500 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200",
            ].join(" ")}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}
