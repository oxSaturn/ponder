import {
  setupAnvil,
  setupCommon,
  setupDatabaseServices,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { drainAsyncGenerator } from "@/utils/drainAsyncGenerator.js";
import { beforeEach, expect, test } from "vitest";
import { createSync } from "./index.js";
import { createLocalSync } from "./local.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

test("createLocalSync()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const sync = await createLocalSync({
    syncStore,
    sources: [context.sources[0]],
    common: context.common,
    network: context.networks[0],
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
