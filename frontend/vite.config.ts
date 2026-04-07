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
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ""),
      },
    },
  },
});
