import { useParams } from "react-router-dom";
import { EmptyState } from "@/components/states/EmptyState";

export function InstrumentDetailPage() {
  const { instrumentId } = useParams<{ instrumentId: string }>();
  return (
    <div className="space-y-4">
      <h1 className="text-xl font-semibold">Instrument {instrumentId}</h1>
      <EmptyState
        title="No instrument detail yet"
        description="Instrument details will appear here once #62 wires up GET /instruments/{id}."
      />
    </div>
  );
}
