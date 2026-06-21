import { defineConfig } from "@playwright/test";

export default defineConfig({
  outputDir: ".playwright-test-results",
  testMatch: ["web/*.playwright.spec.mjs"],
});
