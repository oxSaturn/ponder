import { ALICE } from "@/_test/constants.js";
import { erc20ABI } from "@/_test/generated.js";
import {
  setupAnvil,
  setupCommon,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { getRawRPCData } from "@/_test/utils.js";
import { PostgresDatabaseService } from "@/database/postgres/service.js";
import { SqliteDatabaseService } from "@/database/sqlite/service.js";
import { createSchema } from "@/schema/schema.js";
import type { LogFilter } from "@/sync/filter.js";
import {
  encodeCheckpoint,
  maxCheckpoint,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import { toLowerCase } from "@/utils/lowercase.js";
import {
  type Hex,
  getAbiItem,
  getEventSelector,
  hexToBigInt,
  padHex,
  zeroAddress,
} from "viem";
import { type TestContext, beforeEach, expect, test } from "vitest";
import { createSyncStore } from "./index.js";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

const defaultDatabaseServiceSetup = {
  buildId: "test",
  schema: createSchema(() => ({})),
  indexing: "historical",
};

const setupDatabase = async (context: TestContext) => {
  if (context.databaseConfig.kind === "sqlite") {
    const database = new SqliteDatabaseService({
      common: context.common,
      directory: context.databaseConfig.directory,
    });

    const result = await database.setup(defaultDatabaseServiceSetup);
    await database.migrateSyncStore();

    const cleanup = () => database.kill();

    return {
      database,
      namespaceInfo: result.namespaceInfo,
      cleanup,
    };
  } else {
    const database = new PostgresDatabaseService({
      common: context.common,
      poolConfig: context.databaseConfig.poolConfig,
      userNamespace: context.databaseConfig.schema,
    });
    await database.migrateSyncStore();

    const result = await database.setup(defaultDatabaseServiceSetup);

    await database.migrateSyncStore();

    const cleanup = () => database.kill();

    return {
      database,
      namespaceInfo: result.namespaceInfo,
      cleanup,
    };
  }
};

test("setup creates tables", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const tables = await database.syncDb.introspection.getTables();
  const tableNames = tables.map((t) => t.name);

  expect(tableNames).toContain("block");
  expect(tableNames).toContain("log");
  expect(tableNames).toContain("transaction");
  // expect(tableNames).toContain("call_trace");
  expect(tableNames).toContain("transaction_receipt");

  expect(tableNames).toContain("interval");
  expect(tableNames).toContain("address");

  expect(tableNames).toContain("event");

  expect(tableNames).toContain("rpcRequestResults");
  await cleanup();
});

test("createSyncStore()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);

  createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await cleanup();
});

test("getAddresses()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  const addressFilter = {
    type: "log",
    address: zeroAddress,
    eventSelector: "0xeventselector",
    childAddressLocation: "topic1",
  } as const;

  await syncStore.insertAddress(addressFilter, "0xa", 0n, 1);

  const addresses = await syncStore.getAddresses(addressFilter, 1);

  expect(addresses).toHaveLength(1);
  expect(addresses[0]).toBe("0xa");

  cleanup();
});

test("getAddressess() with no matches", async (context) => {
  const { cleanup, database } = await setupDatabase(context);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  const addressFilter = {
    type: "log",
    address: zeroAddress,
    eventSelector: "0xeventselector",
    childAddressLocation: "topic1",
  } as const;

  await syncStore.insertAddress(addressFilter, "0xa", 0n, 1);

  const addresses = await syncStore.getAddresses(
    { ...addressFilter, childAddressLocation: "topic2" },
    1,
  );

  expect(addresses).toHaveLength(0);

  cleanup();
});

test.todo("getInterval() merges intervals");

test.todo("getInterval() finds adjacent filters");

test("insertLogs()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertLogs(rpcData.block3.logs, 1);

  const logs = await database.syncDb.selectFrom("log").selectAll().execute();
  expect(logs).toHaveLength(1);

  cleanup();
});

test("insertBlock()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertBlock(rpcData.block3.block, 1);

  const blocks = await database.syncDb
    .selectFrom("block")
    .selectAll()
    .execute();
  expect(blocks).toHaveLength(1);

  cleanup();
});

test("hasBlock()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertBlock(rpcData.block3.block, 1);
  let block = await syncStore.hasBlock(rpcData.block3.block.hash, 1);
  expect(block).toBe(true);

  block = await syncStore.hasBlock(rpcData.block2.block.hash, 1);
  expect(block).toBe(false);

  block = await syncStore.hasBlock(rpcData.block3.block.hash, 2);
  expect(block).toBe(false);

  cleanup();
});

test("insertTransaction()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);

  const transactions = await database.syncDb
    .selectFrom("transaction")
    .selectAll()
    .execute();
  expect(transactions).toHaveLength(1);

  cleanup();
});

test("hasTransaction()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);
  let transaction = await syncStore.hasTransaction(
    rpcData.block3.transactions[0].hash,
    1,
  );
  expect(transaction).toBe(true);

  transaction = await syncStore.hasTransaction(
    rpcData.block2.transactions[0].hash,
    1,
  );
  expect(transaction).toBe(false);

  transaction = await syncStore.hasTransaction(
    rpcData.block3.transactions[0].hash,
    2,
  );
  expect(transaction).toBe(false);

  cleanup();
});

test("insertTransactionReceipt()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertTransactionReceipt(
    rpcData.block3.transactionReceipts[0],
    1,
  );

  const transactionReceipts = await database.syncDb
    .selectFrom("transaction_receipt")
    .selectAll()
    .execute();
  expect(transactionReceipts).toHaveLength(1);

  cleanup();
});

test("hasTransactionReceipt()", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertTransactionReceipt(
    rpcData.block3.transactionReceipts[0],
    1,
  );
  let transaction = await syncStore.hasTransactionReceipt(
    rpcData.block3.transactionReceipts[0].transactionHash,
    1,
  );
  expect(transaction).toBe(true);

  transaction = await syncStore.hasTransactionReceipt(
    rpcData.block2.transactionReceipts[0].transactionHash,
    1,
  );
  expect(transaction).toBe(false);

  transaction = await syncStore.hasTransactionReceipt(
    rpcData.block3.transactionReceipts[0].transactionHash,
    2,
  );
  expect(transaction).toBe(false);

  cleanup();
});

test("populateEvents() creates events", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertLogs(rpcData.block3.logs, 1);
  await syncStore.insertBlock(rpcData.block3.block, 1);
  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);

  const filter = { type: "log" } satisfies LogFilter;

  await syncStore.populateEvents({
    filters: [filter],
    chainId: 1,
    fromBlock: hexToBigInt(rpcData.block3.block.number),
    toBlock: hexToBigInt(rpcData.block3.block.number),
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("populateEvents() handles log filter logic", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertLogs(rpcData.block2.logs, 1);
  await syncStore.insertBlock(rpcData.block2.block, 1);
  await syncStore.insertTransaction(rpcData.block2.transactions[0], 1);
  await syncStore.insertTransaction(rpcData.block2.transactions[1], 1);

  await syncStore.insertLogs(rpcData.block3.logs, 1);
  await syncStore.insertBlock(rpcData.block3.block, 1);
  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);

  const transferSelector = getEventSelector(
    getAbiItem({ abi: erc20ABI, name: "Transfer" }),
  );

  const filter = {
    type: "log",
    topics: [transferSelector, toLowerCase(padHex(ALICE)), null, null],
  } satisfies LogFilter;

  await syncStore.populateEvents({
    filters: [filter],
    chainId: 1,
    fromBlock: hexToBigInt(rpcData.block2.block.number),
    toBlock: hexToBigInt(rpcData.block3.block.number),
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("populateEvents() handles block bounds", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertLogs(rpcData.block2.logs, 1);
  await syncStore.insertBlock(rpcData.block2.block, 1);
  await syncStore.insertTransaction(rpcData.block2.transactions[0], 1);
  await syncStore.insertTransaction(rpcData.block2.transactions[1], 1);

  await syncStore.insertLogs(rpcData.block3.logs, 1);
  await syncStore.insertBlock(rpcData.block3.block, 1);
  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);

  const filter = {
    type: "log",
  } satisfies LogFilter;

  await syncStore.populateEvents({
    filters: [filter],
    chainId: 1,
    fromBlock: hexToBigInt(rpcData.block3.block.number),
    toBlock: hexToBigInt(rpcData.block3.block.number),
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test.todo("populateEvents() handles log address filters");

test("populateEvents() handles conflicts", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertLogs(rpcData.block3.logs, 1);
  await syncStore.insertBlock(rpcData.block3.block, 1);
  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);

  const filter = { type: "log" } satisfies LogFilter;

  await syncStore.populateEvents({
    filters: [filter],
    chainId: 1,
    fromBlock: hexToBigInt(rpcData.block3.block.number),
    toBlock: hexToBigInt(rpcData.block3.block.number),
  });

  await syncStore.populateEvents({
    filters: [filter],
    chainId: 1,
    fromBlock: hexToBigInt(rpcData.block3.block.number),
    toBlock: hexToBigInt(rpcData.block3.block.number),
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test.only("getEvents() returns events", async (context) => {
  const { cleanup, database } = await setupDatabase(context);
  const rpcData = await getRawRPCData(context.sources);

  const syncStore = createSyncStore({
    common: context.common,
    db: database.syncDb,
    sql: database.kind,
  });

  await syncStore.insertLogs(rpcData.block3.logs, 1);
  await syncStore.insertBlock(rpcData.block3.block, 1);
  await syncStore.insertTransaction(rpcData.block3.transactions[0], 1);

  const filter = { type: "log" } satisfies LogFilter;

  await syncStore.populateEvents({
    filters: [filter],
    chainId: 1,
    fromBlock: hexToBigInt(rpcData.block3.block.number),
    toBlock: hexToBigInt(rpcData.block3.block.number),
  });

  const events = await syncStore.getEvents({
    filters: [filter],
    chainId: 1,
    before: encodeCheckpoint(maxCheckpoint),
    after: encodeCheckpoint(zeroCheckpoint),
    limit: 10,
  });

  expect(events.events).toHaveLength(1);

  cleanup();
});

test.todo("getEvents() pagination");
