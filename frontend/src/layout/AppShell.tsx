import { Outlet } from "react-router-dom";
import { Sidebar } from "@/layout/Sidebar";
import { Header } from "@/layout/Header";

export function AppShell() {
  return (
    <div className="flex h-screen w-screen overflow-hidden">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <Header />
        {/* No top padding: pages with sticky headers (e.g. SummaryStrip
            on the instrument page) must be able to flush with the
            <Header> bar above. Pages that need top breathing room add
            their own `pt-6` to the root container. */}
        <main className="flex-1 overflow-auto px-6 pb-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
