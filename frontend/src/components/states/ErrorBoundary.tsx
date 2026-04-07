import { Component, type ErrorInfo, type ReactNode } from "react";
import { ErrorBanner } from "@/components/states/ErrorBanner";

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Console only for the scaffold; real telemetry sink is out of scope.
    console.error("ErrorBoundary caught:", error, info);
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div className="p-6">
          <ErrorBanner
            message={`Something went wrong: ${this.state.error.message}`}
          />
        </div>
      );
    }
    return this.props.children;
  }
}
