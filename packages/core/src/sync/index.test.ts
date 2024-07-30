import {
  setupAnvil,
  setupCommon,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { drainAsyncGenerator } from "@/utils/drainAsyncGenerator.js";
import { beforeEach, expect, test } from "vitest";
import { createSync } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

test("createSync()", async (context) => {
  const { cleanup, syncStore } = await setupDatabase(context);

  const sync = await createSync({
    syncStore,

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
