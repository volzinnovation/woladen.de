import { expect, test } from "@playwright/test";
import { spawn } from "node:child_process";
import net from "node:net";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const ROOT_DIR = path.resolve(fileURLToPath(new URL("../", import.meta.url)));
const STATION_ID = "DE:884bd7b49ef38349";

let server;
let baseUrl;

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const address = probe.address();
      probe.close(() => resolve(address.port));
    });
  });
}

async function waitForServer(url) {
  for (let attempt = 0; attempt < 60; attempt += 1) {
    if (server.exitCode !== null) {
      throw new Error(`Static server exited with code ${server.exitCode}`);
    }
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until the static server accepts connections.
    }
    await delay(100);
  }
  throw new Error(`Static server did not start at ${url}`);
}

test.beforeAll(async () => {
  const port = await getFreePort();
  baseUrl = `http://127.0.0.1:${port}`;
  server = spawn(
    "python3",
    ["-m", "http.server", String(port), "--bind", "127.0.0.1", "--directory", "site"],
    {
      cwd: ROOT_DIR,
      stdio: "ignore",
    },
  );
  await waitForServer(baseUrl);
});

test.afterAll(() => {
  server?.kill();
});

test("favoriting from detail shows a star on the main list card", async ({ page }) => {
  await page.addInitScript(() => {
    localStorage.removeItem("woladen_favs");
  });

  await page.goto(`${baseUrl}/?station=${encodeURIComponent(STATION_ID)}&lang=de`);

  const detailModal = page.locator("#modal-detail");
  await expect(detailModal).not.toHaveClass(/hidden/);

  const favoriteButton = page.locator("#btn-toggle-fav");
  await expect(favoriteButton).toHaveAttribute("aria-pressed", "false");
  await favoriteButton.click();
  await expect(favoriteButton).toHaveAttribute("aria-pressed", "true");
  await expect(page.locator("#map .station-map-marker-favorite-icon")).toHaveCount(1);

  await page.locator('[data-close="modal-detail"]').click();
  await expect(detailModal).toHaveClass(/hidden/);

  const mainCard = page.locator(`.station-card[data-station-id="${STATION_ID}"]`).first();
  await expect(mainCard).toBeVisible();
  await expect(mainCard.locator(".favorite-station-star")).toHaveText("★");
  await expect(mainCard.locator(".amenity-dot")).toHaveCount(0);
  await expect(mainCard.locator("button")).toHaveCount(0);
});
