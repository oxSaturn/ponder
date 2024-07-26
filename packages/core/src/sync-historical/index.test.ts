import {
  setupAnvil,
  setupCommon,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { PostgresDatabaseService } from "@/database/postgres/service.js";
import { SqliteDatabaseService } from "@/database/sqlite/service.js";
import { createSchema } from "@/schema/schema.js";
import { createSyncStore } from "@/sync-store/index.js";
import type { LogFilter } from "@/sync/filter.js";
import { type TestContext, beforeEach, expect, test } from "vitest";
import { createHistoricalSync } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

const defaultDatabaseServiceSetup = {
  buildId: "test",
  schema: createSchema(() => ({})),
  indexing: "historical",
};

const setupDatabase = async (context: TestContext) => {
  let database: SqliteDatabaseService | PostgresDatabaseService;

  if (context.databaseConfig.kind === "sqlite") {
    database = new SqliteDatabaseService({
      common: context.common,
      directory: context.databaseConfig.directory,
    });
  } else {
    database = new PostgresDatabaseService({
      common: context.common,
      poolConfig: context.databaseConfig.poolConfig,
      userNamespace: context.databaseConfig.schema,
    });
  }

  const result = await database.setup(defaultDatabaseServiceSetup);
  await database.migrateSyncStore();

  const cleanup = () => database.kill();

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  return {
    syncStore,
    database,
    namespaceInfo: result.namespaceInfo,
    cleanup,
  };
};

test("createHistoricalSync()", async (context) => {
  const { cleanup, syncStore } = await setupDatabase(context);

  const filter = {
    type: "log",
    address: context.sources[0].criteria.address,
    topics: context.sources[0].criteria.topics,
  } satisfies LogFilter;

  const historicalSync = await createHistoricalSync({
    chain: context.networks[0].chain,
    filters: [filter],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  expect(historicalSync).toBeDefined();

  await cleanup();
});

test("sync() with log filter", async (context) => {
  const { cleanup, syncStore, database } = await setupDatabase(context);

  const filter = {
    type: "log",
    address: context.sources[0].criteria.address,
    topics: context.sources[0].criteria.topics,
  } satisfies LogFilter;

  const historicalSync = await createHistoricalSync({
    chain: context.networks[0].chain,
    filters: [filter],
    syncStore,
    requestQueue: context.requestQueues[0],
  });

  await historicalSync.sync(0n, 4n);

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
