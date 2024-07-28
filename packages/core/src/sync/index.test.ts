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
import { drainAsyncGenerator } from "@/utils/drainAsyncGenerator.js";
import { type TestContext, beforeEach, expect, test } from "vitest";
import { createHistoricalSync } from "./index.js";
import { createSync } from "./index2.js";

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

test("createSync()", async (context) => {
  const { cleanup, syncStore } = await setupDatabase(context);

  const sync = await createSync({
    syncStore,
    // @ts-ignore
    sources: [context.sources[0]],
    common: context.common,
    networks: context.networks,
  });

  expect(sync).toBeDefined();

  await cleanup();
});

test("getEvents() returns events", async (context) => {
  const { cleanup, syncStore } = await setupDatabase(context);

  const sync = await createSync({
    syncStore,
    // @ts-ignore
    sources: [context.sources[0]],
    common: context.common,
    networks: context.networks,
  });

  const events = await drainAsyncGenerator(sync.getEvents());

  expect(events).toBeDefined();

  await cleanup();
});

test.todo("getEvents() with cache");

test.todo("getEvents() with end block");
