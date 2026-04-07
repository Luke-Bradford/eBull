import { EmptyState } from "@/components/states/EmptyState";

export function AdminPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">System health</h1>
      <EmptyState
        title="No health data yet"
        description="Layer freshness, job health, and kill switch state will appear here once #64 wires up GET /system/status."
      />
    </div>
  );
}
