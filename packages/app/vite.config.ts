import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { TanStackRouterVite } from "@tanstack/router-plugin/vite";
import { resolve } from "path";
import { parse } from "smol-toml";
import { readFileSync } from "fs";

const pyproject = parse(readFileSync(resolve(__dirname, "pyproject.toml"), "utf-8"));
const apxMeta = (pyproject as Record<string, unknown>)?.tool as Record<string, unknown>;
const apx = (apxMeta?.apx as Record<string, unknown>) ?? {};
const metadata = (apx?.metadata as Record<string, string>) ?? {};
const uiConfig = (apx?.ui as Record<string, string>) ?? {};

const appName = metadata["app-name"] ?? "spend-app";
const apiPrefix = metadata["api-prefix"] ?? "/api";
const uiRoot = resolve(__dirname, uiConfig.root ?? "src/spend_app/ui");

export default defineConfig({
  root: uiRoot,
  plugins: [
    TanStackRouterVite({
      routesDirectory: resolve(uiRoot, "routes"),
      generatedRouteTree: resolve(uiRoot, "types/routeTree.gen.ts"),
    }),
    react(),
    tailwindcss(),
  ],
  define: {
    __APP_NAME__: JSON.stringify(appName),
  },
  resolve: {
    alias: {
      "@": uiRoot,
    },
  },
  server: {
    proxy: {
      [apiPrefix]: {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: resolve(__dirname, "src/spend_app/__dist__"),
    emptyOutDir: true,
  },
});
