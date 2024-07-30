import {
  setupAnvil,
  setupCommon,
  setupDatabaseServices,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { beforeEach, expect, test } from "vitest";
import { createHistoricalSync } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

test("createHistoricalSync()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    chain: context.networks[0].chain,
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  expect(historicalSync).toBeDefined();

  await cleanup();
});

test("sync() with log filter", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    chain: context.networks[0].chain,
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 4]);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(2);

  const intervals = await database.syncDb
    .selectFrom("interval")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(1);

  await cleanup();
});

test("sync() with cache hit", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  let historicalSync = await createHistoricalSync({
    common: context.common,
    chain: context.networks[0].chain,
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });
  await historicalSync.sync([0, 4]);

  // re-instantiate `historicalSync` to reset the cached intervals

  historicalSync = await createHistoricalSync({
    common: context.common,
    chain: context.networks[0].chain,
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });
  await historicalSync.sync([0, 4]);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(2);

  const intervals = await database.syncDb
    .selectFrom("interval")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(1);

  await cleanup();
});
