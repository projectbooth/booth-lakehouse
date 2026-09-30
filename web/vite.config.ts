/// <reference types="vitest/config" />
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Two personalities, one config (ADR 0030) — the same arrangement as booth-catalog/booth-storage:
//   - `vite` (serve): the dev harness — src/main.tsx wraps LakehouseApp in a mock shell
//     (src/devshell/DevShell.tsx), since booth-design's real shell doesn't run here.
//   - `vite build`: the publishable library (@projectbooth/lakehouse-ui) from src/index.ts, with
//     react/react-dom external so booth-design's own copies are used ("Invalid hook call" otherwise).
export default defineConfig(({ command }) => ({
  plugins: [react()],
  build:
    command === "build"
      ? {
          lib: {
            entry: fileURLToPath(new URL("./src/index.ts", import.meta.url)),
            formats: ["es"],
            fileName: "index",
          },
          rollupOptions: {
            external: ["react", "react-dom", "react/jsx-runtime"],
          },
        }
      : undefined,
  server: {
    proxy: {
      // Local dev only: mimics booth-core's gateway — strips /modules/lakehouse and forwards the
      // browser's X-Workspace as X-Booth-Workspace (ADR 0025), pointed at a locally running API.
      "/modules/lakehouse": {
        target: process.env.BOOTH_LAKEHOUSE_DEV_BACKEND ?? "http://localhost:8080",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/modules\/lakehouse/, ""),
        configure: (proxy) => {
          proxy.on("proxyReq", (proxyReq, req) => {
            const ws = req.headers["x-workspace"];
            if (typeof ws === "string" && ws !== "") proxyReq.setHeader("X-Booth-Workspace", ws);
          });
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/setupTests.ts"],
  },
}));
