import { EmptyState } from "@/components/states/EmptyState";

export function RankingsPage() {
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Rankings</h1>
      <EmptyState
        title="No rankings yet"
        description="Candidate rankings will appear here once #61 wires up GET /rankings."
      />
    </div>
  );
}
