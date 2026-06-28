export function chunkItems(items, batchSize) {
  const size = Number.isFinite(Number(batchSize)) ? Math.max(1, Math.trunc(Number(batchSize))) : 1;
  const chunks = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

export async function runLookupBatches(items, options) {
  const batchSize = options?.batchSize || 1;
  const concurrency = Math.max(1, Math.trunc(Number(options?.concurrency || 1)));
  const lookup = options?.lookup;
  if (typeof lookup !== "function") {
    throw new TypeError("lookup must be a function");
  }

  const batches = chunkItems(items, batchSize);
  const results = Array(batches.length);
  const errors = [];
  let nextBatchIndex = 0;

  async function worker() {
    while (nextBatchIndex < batches.length) {
      const batchIndex = nextBatchIndex;
      nextBatchIndex += 1;
      const batch = batches[batchIndex];
      try {
        results[batchIndex] = {
          ok: true,
          batch,
          value: await lookup(batch, batchIndex),
        };
      } catch (error) {
        const failed = { ok: false, batch, error };
        results[batchIndex] = failed;
        errors.push(failed);
      }
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, batches.length) }, () => worker());
  await Promise.all(workers);
  return { results, errors };
}
