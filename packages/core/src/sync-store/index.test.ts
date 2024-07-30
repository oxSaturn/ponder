import {
  setupAnvil,
  setupCommon,
  setupDatabaseServices,
  setupIsolatedDatabase,
} from "@/_test/setup.js";
import { getRawRPCData } from "@/_test/utils.js";
import type { AddressFilter, LogFilter } from "@/sync/filter.js";
import {
  decodeCheckpoint,
  encodeCheckpoint,
  maxCheckpoint,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import { hexToNumber } from "viem";
import { beforeEach, expect, test } from "vitest";

beforeEach(setupCommon);
beforeEach(setupAnvil);
beforeEach(setupIsolatedDatabase);

test("setup creates tables", async (context) => {
  const { cleanup, database } = await setupDatabaseServices(context);
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

test("getAddresses()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  await syncStore.insertAddresses({
    filter: context.sources[1].filter.address as AddressFilter,
    addresses: [{ address: "0xa", blockNumber: 0 }],
  });

  const addresses = await syncStore.getAddresses({
    filter: context.sources[1].filter.address as AddressFilter,
  });

  expect(addresses).toHaveLength(1);
  expect(addresses[0]).toBe("0xa");

  cleanup();
});

test("getAddressess() empty", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const addresses = await syncStore.getAddresses({
    filter: context.sources[1].filter.address as AddressFilter,
  });

  expect(addresses).toHaveLength(0);

  cleanup();
});

test.todo("getInterval() empty");

test.todo("getInterval() merges intervals");

test("insertLogs()", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });

  const logs = await database.syncDb.selectFrom("log").selectAll().execute();
  expect(logs).toHaveLength(1);

  cleanup();
});

test("insertLogs() with duplicates", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });

  const logs = await database.syncDb.selectFrom("log").selectAll().execute();
  expect(logs).toHaveLength(1);

  cleanup();
});

test("insertBlock()", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });

  const blocks = await database.syncDb
    .selectFrom("block")
    .selectAll()
    .execute();
  expect(blocks).toHaveLength(1);

  cleanup();
});

test("insertBlock() with duplicates", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });

  const blocks = await database.syncDb
    .selectFrom("block")
    .selectAll()
    .execute();
  expect(blocks).toHaveLength(1);

  cleanup();
});

test("hasBlock()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  let block = await syncStore.hasBlock({
    hash: rpcData.block3.block.hash,
    chainId: 1,
  });
  expect(block).toBe(true);

  block = await syncStore.hasBlock({
    hash: rpcData.block2.block.hash,
    chainId: 1,
  });
  expect(block).toBe(false);

  block = await syncStore.hasBlock({
    hash: rpcData.block3.block.hash,
    chainId: 2,
  });
  expect(block).toBe(false);

  cleanup();
});

test("insertTransaction()", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const transactions = await database.syncDb
    .selectFrom("transaction")
    .selectAll()
    .execute();
  expect(transactions).toHaveLength(1);

  cleanup();
});

test("insertTransaction() with duplicates", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const transactions = await database.syncDb
    .selectFrom("transaction")
    .selectAll()
    .execute();
  expect(transactions).toHaveLength(1);

  cleanup();
});

test("hasTransaction()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });
  let transaction = await syncStore.hasTransaction({
    hash: rpcData.block3.transactions[0].hash,
    chainId: 1,
  });
  expect(transaction).toBe(true);

  transaction = await syncStore.hasTransaction({
    hash: rpcData.block2.transactions[0].hash,
    chainId: 1,
  });
  expect(transaction).toBe(false);

  transaction = await syncStore.hasTransaction({
    hash: rpcData.block3.transactions[0].hash,
    chainId: 2,
  });
  expect(transaction).toBe(false);

  cleanup();
});

test("insertTransactionReceipt()", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertTransactionReceipt({
    transactionReceipt: rpcData.block3.transactionReceipts[0],
    chainId: 1,
  });

  const transactionReceipts = await database.syncDb
    .selectFrom("transaction_receipt")
    .selectAll()
    .execute();
  expect(transactionReceipts).toHaveLength(1);

  cleanup();
});

test("insertTransactionReceipt() with duplicates", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertTransactionReceipt({
    transactionReceipt: rpcData.block3.transactionReceipts[0],
    chainId: 1,
  });
  await syncStore.insertTransactionReceipt({
    transactionReceipt: rpcData.block3.transactionReceipts[0],
    chainId: 1,
  });

  const transactionReceipts = await database.syncDb
    .selectFrom("transaction_receipt")
    .selectAll()
    .execute();
  expect(transactionReceipts).toHaveLength(1);

  cleanup();
});

test("hasTransactionReceipt()", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertTransactionReceipt({
    transactionReceipt: rpcData.block3.transactionReceipts[0],
    chainId: 1,
  });
  let transaction = await syncStore.hasTransactionReceipt({
    hash: rpcData.block3.transactionReceipts[0].transactionHash,
    chainId: 1,
  });
  expect(transaction).toBe(true);

  transaction = await syncStore.hasTransactionReceipt({
    hash: rpcData.block2.transactionReceipts[0].transactionHash,
    chainId: 1,
  });
  expect(transaction).toBe(false);

  transaction = await syncStore.hasTransactionReceipt({
    hash: rpcData.block3.transactionReceipts[0].transactionHash,
    chainId: 2,
  });
  expect(transaction).toBe(false);

  cleanup();
});

test("populateEvents() creates events", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;
  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("populateEvents() handles log filter logic", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block2.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block2.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block2.transactions[0],
    chainId: 1,
  });
  await syncStore.insertTransaction({
    transaction: rpcData.block2.transactions[1],
    chainId: 1,
  });

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  await syncStore.populateEvents({
    filter: context.sources[0].filter,
    interval: [2, 2],
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(2);

  cleanup();
});

test("populateEvents() handles block bounds", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block2.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block2.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block2.transactions[0],
    chainId: 1,
  });
  await syncStore.insertTransaction({
    transaction: rpcData.block2.transactions[1],
    chainId: 1,
  });

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;

  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("populateEvents() creates log filter checkpoint", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;

  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const { checkpoint } = await database.syncDb
    .selectFrom("event")
    .select("checkpoint")
    .executeTakeFirstOrThrow();

  expect(decodeCheckpoint(checkpoint).blockTimestamp).toBe(
    hexToNumber(rpcData.block3.block.timestamp),
  );
  expect(decodeCheckpoint(checkpoint).chainId).toBe(1n);
  expect(decodeCheckpoint(checkpoint).blockNumber).toBe(3n);
  expect(decodeCheckpoint(checkpoint).transactionIndex).toBe(0n);
  expect(decodeCheckpoint(checkpoint).eventType).toBe(5);
  expect(decodeCheckpoint(checkpoint).eventIndex).toBe(0n);

  cleanup();
});

test("populateEvents() creates log filter data", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;

  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const { data } = await database.syncDb
    .selectFrom("event")
    .select("data")
    .executeTakeFirstOrThrow();

  expect(JSON.parse(data as string).data).toBe(rpcData.block3.logs[0].data);

  cleanup();
});

test("populateEvents() handles log address filters", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertLogs({ logs: rpcData.block4.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block4.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block4.transactions[0],
    chainId: 1,
  });

  await syncStore.populateEvents({
    filter: context.sources[1].filter,
    interval: [4, 4],
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("populateEvents() handles block filter logic", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertBlock({ block: rpcData.block2.block, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block4.block, chainId: 1 });

  await syncStore.populateEvents({
    filter: context.sources[2].filter,
    interval: [2, 4],
  });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("populateEvents() handles conflicts", async (context) => {
  const { cleanup, database, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;

  await syncStore.populateEvents({ filter, interval: [3, 3] });
  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const events = await database.syncDb
    .selectFrom("event")
    .selectAll()
    .execute();

  expect(events).toHaveLength(1);

  cleanup();
});

test("getEvents() returns events", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;
  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const events = await syncStore.getEvents({
    filters: [filter],
    from: encodeCheckpoint(zeroCheckpoint),
    to: encodeCheckpoint(maxCheckpoint),
    limit: 10,
  });

  expect(events.events).toHaveLength(1);

  cleanup();
});

test("getEventCount empty", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;
  const count = await syncStore.getEventCount({ filters: [filter] });

  expect(count).toBe(0);

  cleanup();
});

test("getEventCount", async (context) => {
  const { cleanup, syncStore } = await setupDatabaseServices(context);
  const rpcData = await getRawRPCData();

  await syncStore.insertLogs({ logs: rpcData.block3.logs, chainId: 1 });
  await syncStore.insertBlock({ block: rpcData.block3.block, chainId: 1 });
  await syncStore.insertTransaction({
    transaction: rpcData.block3.transactions[0],
    chainId: 1,
  });

  const filter = { type: "log", chainId: 1, fromBlock: 0 } satisfies LogFilter;
  await syncStore.populateEvents({ filter, interval: [3, 3] });

  const count = await syncStore.getEventCount({ filters: [filter] });

  expect(count).toBe(1);

  cleanup();
});

test.todo("getEvents() pagination");
