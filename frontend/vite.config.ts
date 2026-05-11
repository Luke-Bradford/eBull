import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// The FastAPI backend (see app/main.py) mounts routers at the root, e.g. /config,
// /system/status, /instruments, /portfolio, /rankings. To avoid coupling the
// frontend to those bare paths, we proxy /api/* and strip the prefix on the way
// through. Frontend code therefore always calls /api/<router>/...
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    // Fail fast instead of silent port-hop to 5174/5175/... When 5173 is
    // held by a zombie vite instance the operator notices immediately
    // and the VS Code task pre-kill (.vscode/tasks.json) gets a chance
    // to reap before the next launch. Without this, orphaned vite
    // processes accumulate across sessions (each session takes the next
    // free port) until lsof -iTCP:517[3-9] is a wall of zombies.
    strictPort: true,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ""),
      },
    },
  },
});
