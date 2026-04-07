import { EmptyState } from "@/components/states/EmptyState";

export function SettingsPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Settings</h1>
      <EmptyState
        title="No runtime config yet"
        description="Runtime flags and kill switch controls will appear here once #65 wires up GET /config and PATCH /config."
      />
    </div>
  );
}
