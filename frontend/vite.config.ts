import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// The FastAPI backend (see app/main.py) mounts routers at the root, e.g. /config,
// /system/status, /instruments, /portfolio, /rankings. To avoid coupling the
// frontend to those bare paths, we proxy /api/* and strip the prefix on the way
// through. Frontend code therefore always calls /api/<router>/...
export default defineConfig({
  plugins: [react()],
  // es2022: esbuild >=0.28 (pinned via pnpm.overrides for the
  // GHSA on esbuild <0.28.1) removed down-transforms to vite 6's
  // legacy default targets (chrome87/safari14/es2020). eBull is a
  // single-operator dashboard on a current browser — raising the
  // build target is free; lowering it back requires esbuild <0.28.
  build: { target: "es2022" },
  // Dev dep pre-bundling (optimizeDeps) runs esbuild too, and it does NOT
  // inherit build.target — left unset it falls back to vite 6's legacy default
  // (chrome87/safari14/es2020). esbuild >=0.28 (the #1606 GHSA pin) dropped the
  // down-transforms for those targets, so `vite` (dev) crashes optimizing deps
  // that ship modern syntax (e.g. @remix-run/router destructuring). Mirror the
  // build target so dev and build agree.
  optimizeDeps: { esbuildOptions: { target: "es2022" } },
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
