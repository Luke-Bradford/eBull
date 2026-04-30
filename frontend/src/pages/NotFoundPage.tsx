import { EmptyState } from "@/components/states/EmptyState";

export function NotFoundPage() {
  return (
    <div className="space-y-4 pt-6">
      <EmptyState title="Not found" description="That route does not exist." />
    </div>
  );
}
