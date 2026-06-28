import test from "node:test";
import assert from "node:assert/strict";

import { chunkItems, runLookupBatches } from "./batch-fetch.mjs";

test("chunkItems preserves order and clamps invalid batch sizes", () => {
  assert.deepEqual(chunkItems([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(chunkItems([1, 2, 3], 0), [[1], [2], [3]]);
});

test("runLookupBatches keeps result order while running bounded concurrent lookups", async () => {
  let active = 0;
  let maxActive = 0;
  const seen = [];

  const { results, errors } = await runLookupBatches(["a", "b", "c", "d", "e"], {
    batchSize: 2,
    concurrency: 2,
    lookup: async (batch, index) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      seen.push(batch.join(""));
      await new Promise((resolve) => setTimeout(resolve, index === 0 ? 10 : 0));
      active -= 1;
      return batch.join(":");
    },
  });

  assert.equal(maxActive, 2);
  assert.deepEqual(seen.sort(), ["ab", "cd", "e"]);
  assert.deepEqual(errors, []);
  assert.deepEqual(results.map((result) => result.value), ["a:b", "c:d", "e"]);
});

test("runLookupBatches reports failed batches without dropping successful batches", async () => {
  const { results, errors } = await runLookupBatches(["a", "b", "c"], {
    batchSize: 1,
    concurrency: 2,
    lookup: async (batch) => {
      if (batch[0] === "b") {
        throw new Error("upstream failed");
      }
      return batch[0].toUpperCase();
    },
  });

  assert.equal(errors.length, 1);
  assert.equal(errors[0].batch[0], "b");
  assert.equal(results[0].value, "A");
  assert.equal(results[1].ok, false);
  assert.equal(results[2].value, "C");
});
