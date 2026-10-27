import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// `decdn` embeds exactly three files (crates/cli/build.rs) and `decdn
// dashboard` serves them at `/`, `/_dashboard/app.js` and
// `/_dashboard/app.css`, so the build
// emits one script and one stylesheet under fixed names, with every asset
// inlined.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  base: "/_dashboard/",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    assetsDir: "",
    cssCodeSplit: false,
    modulePreload: { polyfill: false },
    assetsInlineLimit: 1 << 20,
    rollupOptions: {
      output: {
        inlineDynamicImports: true,
        entryFileNames: "app.js",
        assetFileNames: "app[extname]",
      },
    },
  },
  server: {
    // `decdn dashboard` on its default address answers the API. It refuses
    // a `Host` other than its own, so the proxy rewrites it.
    proxy: {
      "/_status": { target: "http://127.0.0.1:8485", changeOrigin: true },
    },
  },
  test: {
    include: ["src/**/*.test.ts"],
  },
});
