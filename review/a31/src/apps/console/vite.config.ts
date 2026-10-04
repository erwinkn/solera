import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const api = process.env.SOLERA_SERVER_URL ?? "http://127.0.0.1:8000";

export default defineConfig(({ command }) => ({
  // The Python app serves the bundle under /static/; client routes live at /.
  base: command === "build" ? "/static/" : "/",
  resolve: { tsconfigPaths: true },
  server: {
    proxy: { "/api": api, "/healthz": api },
  },
  build: { outDir: "dist", emptyOutDir: true, chunkSizeWarningLimit: 400 },
  plugins: [tailwindcss(), react()],
}));
