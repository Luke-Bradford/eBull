import { EmptyState } from "@/components/states/EmptyState";

export function DashboardPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Dashboard</h1>
      <EmptyState
        title="No portfolio data yet"
        description="Portfolio overview will appear here once #60 wires up GET /portfolio."
      />
    </div>
  );
}
