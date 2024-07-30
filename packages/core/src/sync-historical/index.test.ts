import {
  setupAnvil,
  setupCommon,
  setupDatabaseServices,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { publicClient } from "@/_test/utils.js";
import type { SyncBlock } from "@/sync/index.js";
import { beforeEach, expect, test, vi } from "vitest";
import { createHistoricalSync } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

test("createHistoricalSync()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
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
    network: context.networks[0],
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

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

test("sync() with block filter", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [context.sources[2]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(3);

  const intervals = await database.syncDb
    .selectFrom("interval")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(1);

  await cleanup();
});

test("sync() with log address filter", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [context.sources[1]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  const intervals = await database.syncDb
    .selectFrom("interval")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(2);

  const addresses = await database.syncDb
    .selectFrom("address")
    .select("address")
    .execute();

  expect(addresses).toHaveLength(1);

  await cleanup();
});

test("sync() with many filters", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: context.sources,
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(6);

  const intervals = await database.syncDb
    .selectFrom("interval")
    .selectAll()
    .execute();

  expect(intervals).toHaveLength(4);

  await cleanup();
});

test("sync() with cache hit", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  let historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });
  await historicalSync.sync([0, 5]);

  // re-instantiate `historicalSync` to reset the cached intervals

  const spy = vi.spyOn(syncStore, "populateEvents");

  historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [context.sources[0]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });
  await historicalSync.sync([0, 5]);

  expect(spy).toHaveBeenCalledTimes(0);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(2);

  await cleanup();
});

test("syncAddress() with cache hit", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabaseServices(context);

  let historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [context.sources[1]],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

  // re-instantiate `historicalSync` to reset the cached intervals

  const spy = vi.spyOn(syncStore, "insertInterval");

  // change `filter` but not `filter.address`
  const f = context.sources[1].filter;
  f.topics = [];

  historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [{ ...context.sources[1], filter: f }],
    syncStore,
    requestQueue: context.requestQueues[0],
  });
  await historicalSync.sync([0, 5]);

  expect(spy).toHaveBeenCalledTimes(1);

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(2);

  await cleanup();
});

test("initializeMetrics()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: context.sources,
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  const finalizeBlock = await publicClient.request({
    method: "eth_getBlockByNumber",
    params: ["latest", false],
  });

  historicalSync.initializeMetrics(finalizeBlock as SyncBlock);

  const totalBlocksMetric = (
    await context.common.metrics.ponder_historical_total_blocks.get()
  ).values;
  const cachedBlocksMetric = (
    await context.common.metrics.ponder_historical_cached_blocks.get()
  ).values;

  expect(totalBlocksMetric).toEqual(
    expect.arrayContaining([
      {
        labels: { network: "mainnet", source: "Erc20", type: "log" },
        value: 6,
      },
      // {
      //   labels: { network: "mainnet", source: "Pair_factory", type: "log" },
      //   value: 6,
      // },
      { labels: { network: "mainnet", source: "Pair", type: "log" }, value: 6 },
      {
        labels: { network: "mainnet", source: "OddBlocks", type: "block" },
        value: 5,
      },
      // {
      //   labels: { network: "mainnet", source: "Factory", type: "trace" },
      //   value: 6,
      // },
    ]),
  );

  expect(cachedBlocksMetric).toEqual(
    expect.arrayContaining([
      {
        labels: { network: "mainnet", source: "Erc20", type: "log" },
        value: 0,
      },
      // {
      //   labels: { network: "mainnet", source: "Pair_factory", type: "log" },
      //   value: 0,
      // },
      { labels: { network: "mainnet", source: "Pair", type: "log" }, value: 0 },
      {
        labels: { network: "mainnet", source: "OddBlocks", type: "block" },
        value: 0,
      },
      // {
      //   labels: { network: "mainnet", source: "Factory", type: "trace" },
      //   value: 0,
      // },
    ]),
  );

  await cleanup();
});

test("initializeMetrics() with cache hit", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  let historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: context.sources,
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

  historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: context.sources,
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  const finalizeBlock = await publicClient.request({
    method: "eth_getBlockByNumber",
    params: ["latest", false],
  });

  historicalSync.initializeMetrics(finalizeBlock as SyncBlock);

  const totalBlocksMetric = (
    await context.common.metrics.ponder_historical_total_blocks.get()
  ).values;
  const cachedBlocksMetric = (
    await context.common.metrics.ponder_historical_cached_blocks.get()
  ).values;

  expect(totalBlocksMetric).toEqual(
    expect.arrayContaining([
      {
        labels: { network: "mainnet", source: "Erc20", type: "log" },
        value: 6,
      },
      // {
      //   labels: { network: "mainnet", source: "Pair_factory", type: "log" },
      //   value: 6,
      // },
      { labels: { network: "mainnet", source: "Pair", type: "log" }, value: 6 },
      {
        labels: { network: "mainnet", source: "OddBlocks", type: "block" },
        value: 5,
      },
      // {
      //   labels: { network: "mainnet", source: "Factory", type: "trace" },
      //   value: 6,
      // },
    ]),
  );

  expect(cachedBlocksMetric).toEqual(
    expect.arrayContaining([
      {
        labels: { network: "mainnet", source: "Erc20", type: "log" },
        value: 6,
      },
      // {
      //   labels: { network: "mainnet", source: "Pair_factory", type: "log" },
      //   value: 0,
      // },
      { labels: { network: "mainnet", source: "Pair", type: "log" }, value: 6 },
      {
        labels: { network: "mainnet", source: "OddBlocks", type: "block" },
        value: 5,
      },
      // {
      //   labels: { network: "mainnet", source: "Factory", type: "trace" },
      //   value: 0,
      // },
    ]),
  );

  await cleanup();
});

test("sync() updates metrics", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: context.sources,
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync([0, 5]);

  const completedBlocksMetric = (
    await context.common.metrics.ponder_historical_completed_blocks.get()
  ).values;

  expect(completedBlocksMetric).toEqual(
    expect.arrayContaining([
      {
        labels: { network: "mainnet", source: "Erc20", type: "log" },
        value: 6,
      },
      // {
      //   labels: { network: "mainnet", source: "Pair_factory", type: "log" },
      //   value: 0,
      // },
      { labels: { network: "mainnet", source: "Pair", type: "log" }, value: 6 },
      {
        labels: { network: "mainnet", source: "OddBlocks", type: "block" },
        value: 5,
      },
      // {
      //   labels: { network: "mainnet", source: "Factory", type: "trace" },
      //   value: 0,
      // },
    ]),
  );

  await cleanup();
});

test("syncBlock() with cache", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  // block 2 and 4 will be requested
  const blockFilter = context.sources[2].filter;
  blockFilter.offset = 0;

  const historicalSync = await createHistoricalSync({
    common: context.common,
    network: context.networks[0],
    sources: [
      context.sources[0],
      { ...context.sources[2], filter: blockFilter },
    ],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  const spy = vi.spyOn(context.requestQueues[0], "request");

  await historicalSync.sync([0, 5]);

  // 1 call to `syncBlock()` will be cached because
  // each source in `sources` matches block 2
  expect(spy).toHaveBeenCalledTimes(3);

  await cleanup();
});
