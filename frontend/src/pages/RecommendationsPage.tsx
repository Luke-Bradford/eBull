import { EmptyState } from "@/components/states/EmptyState";

export function RecommendationsPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Recommendations</h1>
      <EmptyState
        title="No recommendations yet"
        description="Pending execution decisions will appear here once #63 wires up the recommendations endpoint."
      />
    </div>
  );
}
