import type { Common } from "@/common/common.js";
import type { HeadlessKysely } from "@/database/kysely.js";
import type { RawEvent } from "@/sync/events.js";
import {
  type AddressFilter,
  type Filter,
  type LogFilter,
  getFilterId,
} from "@/sync/filter.js";
import { encodeAsText } from "@/utils/encoding.js";
import { type Interval, intervalUnion } from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import type {
  SyncBlock,
  SyncLog,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/utils/rpc.js";
import { type SelectQueryBuilder, sql as ksql } from "kysely";
import type { Address, Hash } from "viem";
import {
  type PonderSyncSchema,
  encodeBlock,
  encodeLog,
  encodeTransaction,
  encodeTransactionReceipt,
} from "./encoding.js";

export type SyncStore = {
  /** Insert an address that matches `filter`. */
  insertAddresses(args: {
    filter: AddressFilter;
    addresses: { address: Address; blockNumber: number }[];
  }): Promise<void>;
  /**
   * Return all addresses that match `filter`.
   * TODO(kyle) should `blockNumber` be used as an upper bound here?
   */
  getAddresses(args: { filter: AddressFilter }): Promise<Address[]>;
  insertInterval<type extends "event" | "address">(args: {
    filterType: type;
    filter: type extends "address" ? AddressFilter : Filter;
    interval: Interval;
  }): Promise<void>;
  getIntervals<type extends "event" | "address">(args: {
    filterType: type;
    filter: type extends "address" ? AddressFilter : Filter;
  }): Promise<Interval[]>;
  insertLogs(args: { logs: SyncLog[]; chainId: number }): Promise<void>;
  insertBlock(args: { block: SyncBlock; chainId: number }): Promise<void>;
  /** Return true if the block receipt is present in the database. */
  hasBlock(args: { hash: Hash; chainId: number }): Promise<boolean>;
  insertTransaction(args: {
    transaction: SyncTransaction;
    chainId: number;
  }): Promise<void>;
  /** Return true if the transaction is present in the database. */
  hasTransaction(args: { hash: Hash; chainId: number }): Promise<boolean>;
  insertTransactionReceipt(args: {
    transactionReceipt: SyncTransactionReceipt;
    chainId: number;
  }): Promise<void>;
  /** Return true if the transaction receipt is present in the database. */
  hasTransactionReceipt(args: {
    hash: Hash;
    chainId: number;
  }): Promise<boolean>;
  /**
   * Use the "block", "log",  "transaction", and "transaction_receipt" data to fill in the event
   * table for the given `filters` and `interval`. It is assumed that all underlying data is
   * present in the database before this is called. Upon successful completion, a row should be
   * inserted into the "interval" table.
   */
  populateEvents(args: { filter: Filter; interval: Interval }): Promise<void>;
  /** Returns an ordered list of events based on the `filters` and pagination arguments. */
  getEvents(args: {
    filters: Filter[];
    from: string;
    to: string;
    limit: number;
  }): Promise<{ events: RawEvent[]; cursor: string }>;
  /** Returns the total number of events for the `filters`. */
  getEventCount(args: { filters: Filter[] }): Promise<number>;
  insertRpcRequestResult(args: {
    request: string;
    blockNumber: number;
    chainId: number;
    result: string;
  }): Promise<void>;
  getRpcRequestResult(args: {
    request: string;
    blockNumber: number;
    chainId: number;
  }): Promise<string | null>;
  // pruneByBlock,
  // pruneBySource,
  // pruneByChain,
};

export const createSyncStore = ({
  db,
  sql,
}: {
  common: Common;
  sql: "sqlite" | "postgres";
  db: HeadlessKysely<PonderSyncSchema>;
}): SyncStore => ({
  insertAddresses: async ({ filter, addresses }) =>
    db.wrap({ method: "insertAddress" }, async () => {
      await db
        .insertInto("address")
        .values(
          addresses.map(({ address, blockNumber }) => ({
            filter_id: getFilterId("address", filter),
            chain_id: filter.chainId,
            block_number:
              sql === "sqlite"
                ? encodeAsText(blockNumber)
                : BigInt(blockNumber),
            address: address,
          })),
        )
        .execute();
    }),
  getAddresses: async ({ filter }) =>
    db.wrap({ method: "getAddresses" }, async () => {
      return await db
        .selectFrom("address")
        .select("address")
        .where("chain_id", "=", filter.chainId)
        .where("filter_id", "=", getFilterId("address", filter))
        .execute()
        .then((result) => result.map(({ address }) => address));
    }),
  insertInterval: async ({ filterType, filter, interval }) =>
    db.wrap({ method: "insertInterval" }, async () => {
      await db
        .insertInto("interval")
        .values({
          filter_id: getFilterId(filterType, filter),
          chain_id: filter.chainId,
          from: BigInt(interval[0]),
          to: BigInt(interval[1]),
        })
        .execute();
    }),
  getIntervals: async ({ filterType, filter }) =>
    db.wrap({ method: "getIntervals" }, () =>
      db.transaction().execute(async (tx) => {
        const filterId = getFilterId(filterType, filter);

        const existingIntervals = await tx
          .deleteFrom("interval")
          .where("filter_id", "=", filterId)
          .returning(["from", "to"])
          .execute();

        const mergedIntervals = intervalUnion(
          existingIntervals.map((i) => [Number(i.from), Number(i.to)]),
        );

        if (mergedIntervals.length === 0) return [];

        const mergedIntervalRows = mergedIntervals.map(([from, to]) => ({
          chain_id: filter.chainId,
          filter_id: filterId,
          from: BigInt(from),
          to: BigInt(to),
        }));

        await tx.insertInto("interval").values(mergedIntervalRows).execute();

        return mergedIntervals;
      }),
    ),
  insertLogs: async ({ logs, chainId }) =>
    db.wrap({ method: "insertLogs" }, async () => {
      await db
        .insertInto("log")
        .values(logs.map((log) => encodeLog(log, chainId, sql)))
        .onConflict((oc) =>
          oc.columns(["block_hash", "log_index", "chain_id"]).doNothing(),
        )
        .execute();
    }),
  insertBlock: async ({ block, chainId }) =>
    db.wrap({ method: "insertBlock" }, async () => {
      await db
        .insertInto("block")
        .values(encodeBlock(block, chainId, sql))
        .onConflict((oc) => oc.columns(["hash", "chain_id"]).doNothing())
        .execute();
    }),
  hasBlock: async ({ hash, chainId }) =>
    db.wrap({ method: "hasBlock" }, async () => {
      return await db
        .selectFrom("block")
        .select("hash")
        .where("chain_id", "=", chainId)
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertTransaction: async ({ transaction, chainId }) =>
    db.wrap({ method: "insertTransaction" }, async () => {
      await db
        .insertInto("transaction")
        .values(encodeTransaction(transaction, chainId, sql))
        .onConflict((oc) => oc.columns(["hash", "chain_id"]).doNothing())
        .execute();
    }),
  hasTransaction: async ({ hash, chainId }) =>
    db.wrap({ method: "hasTransaction" }, async () => {
      return await db
        .selectFrom("transaction")
        .select("hash")
        .where("chain_id", "=", chainId)
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertTransactionReceipt: async ({ transactionReceipt, chainId }) =>
    db.wrap({ method: "insertTransactionReceipt" }, async () => {
      await db
        .insertInto("transaction_receipt")
        .values(encodeTransactionReceipt(transactionReceipt, chainId, sql))
        .onConflict((oc) => oc.columns(["hash", "chain_id"]).doNothing())
        .execute();
    }),
  hasTransactionReceipt: async ({ hash, chainId }) =>
    db.wrap({ method: "hasTransactionReceipt" }, async () => {
      return await db
        .selectFrom("transaction_receipt")
        .select("hash")
        .where("chain_id", "=", chainId)
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  populateEvents: async ({ filter, interval }) => {
    const addressSQL = <
      T extends SelectQueryBuilder<PonderSyncSchema, "log", {}>,
    >(
      qb: T,
      address: LogFilter["address"],
    ): T => {
      if (typeof address === "string")
        return qb.where("address", "=", address) as T;
      if (Array.isArray(address))
        return qb.where("address", "in", address) as T;
      if (address?.type === "log") {
        // log address filter
        return qb.where(
          "address",
          "in",
          db
            .selectFrom("address")
            .select("address")
            .where("chain_id", "=", address.chainId)
            .where("filter_id", "=", getFilterId("address", address)),
        ) as T;
      }
      return qb;
    };

    const blockTimestampQuery = db
      .selectFrom("block")
      .select("block.timestamp")
      .where("hash", "=", ksql.ref("log.block_hash"))
      .where("chain_id", "=", ksql.ref("log.chain_id"))
      .compile().sql;

    const transactionIndexQuery = db
      .selectFrom("transaction")
      .select("transaction.transaction_index")
      .where("hash", "=", ksql.ref("log.transaction_hash"))
      .where("chain_id", "=", ksql.ref("log.chain_id"))
      .compile().sql;

    const logDataSQite = `
json_object(
  'data', data, 
  'topic0', topic0, 
  'topic1', topic1,
  'topic2', topic2,
  'topic3', topic3
)`;

    const logDataPostgres = `
jsonb_build_object(
  'data', data, 
  'topic0', topic0, 
  'topic1', topic1,
  'topic2', topic2,
  'topic3', topic3
)`;

    const logCheckpointSQLite = `
substr((${blockTimestampQuery}), -10, 10) ||
substr('0000000000000000' || chain_id, -16, 16) ||
substr(block_number, -16, 16) ||
substr('0000000000000000' || (${transactionIndexQuery}), -16, 16) ||
'5' ||
substr('0000000000000000' || log_index, -16, 16)`;

    const logCheckpointPostgres = `
lpad((${blockTimestampQuery})::text, 10, '0') ||
lpad(chain_id::text, 16, '0') ||
lpad(block_number::text, 16, '0') ||
lpad((${transactionIndexQuery})::text, 16, '0') ||
'5' ||
lpad(log_index::text, 16, '0')`;

    const blockCheckpointSQLite = `
substr(timestamp, -10, 10) ||
substr('0000000000000000' || chain_id, -16, 16) ||
substr(number, -16, 16) ||
'9999999999999999' ||
'5' ||
'0000000000000000'`;

    const blockCheckpointPostgres = `
lpad(timestamp::text, 10, '0') ||
lpad(chain_id::text, 16, '0') ||
lpad(number::text, 16, '0') ||
'9999999999999999' ||
'5' ||
'0000000000000000'`;

    await db.wrap({ method: "populateEvents" }, async () => {
      switch (filter.type) {
        case "log":
          {
            const subquery = db
              .selectFrom("log")
              .select([
                ksql.raw(`'${getFilterId("event", filter)}'`).as("filter_id"),
                ksql
                  .raw(
                    sql === "sqlite"
                      ? logCheckpointSQLite
                      : logCheckpointPostgres,
                  )
                  .as("checkpoint"),
                "chain_id",
                ksql
                  .raw(sql === "sqlite" ? logDataSQite : logDataPostgres)
                  .as("data"),
                "block_number",
                "block_hash",
                "log_index",
                "transaction_hash",
              ])
              .where("chain_id", "=", filter.chainId)
              .$if(filter.topics !== undefined, (qb) => {
                for (const idx_ of [0, 1, 2, 3]) {
                  const idx = idx_ as 0 | 1 | 2 | 3;
                  // If it's an array of length 1, collapse it.
                  const raw = filter.topics![idx] ?? null;
                  if (raw === null) continue;
                  const topic =
                    Array.isArray(raw) && raw.length === 1 ? raw[0]! : raw;
                  if (Array.isArray(topic)) {
                    qb = qb.where((eb) =>
                      eb.or(topic.map((t) => eb(`log.topic${idx}`, "=", t))),
                    );
                  } else {
                    qb = qb.where(`log.topic${idx}`, "=", topic);
                  }
                }
                return qb;
              })
              .$call((qb) => addressSQL(qb, filter.address))
              .$if(filter.fromBlock !== undefined, (qb) =>
                qb.where(
                  "block_number",
                  ">=",
                  sql === "sqlite"
                    ? encodeAsText(filter.fromBlock!)
                    : BigInt(filter.fromBlock!),
                ),
              )
              .$if(filter.toBlock !== undefined, (qb) =>
                qb.where(
                  "block_number",
                  "<=",
                  sql === "sqlite"
                    ? encodeAsText(filter.toBlock!)
                    : BigInt(filter.toBlock!),
                ),
              )
              .where(
                "block_number",
                ">=",
                sql === "sqlite"
                  ? encodeAsText(interval[0])
                  : BigInt(interval[0]),
              )
              .where(
                "block_number",
                "<=",
                sql === "sqlite"
                  ? encodeAsText(interval[1])
                  : BigInt(interval[1]),
              );

            await db
              .insertInto("event")
              .columns([
                "filter_id",
                "checkpoint",
                "chain_id",
                "data",
                "block_number",
                "block_hash",
                "log_index",
                "transaction_hash",
              ])
              .expression(subquery)
              .onConflict((oc) =>
                oc.columns(["filter_id", "checkpoint"]).doNothing(),
              )
              .execute();
          }
          break;

        case "block":
          {
            const subquery = db
              .selectFrom("block")
              .select([
                ksql.raw(`'${getFilterId("event", filter)}'`).as("filter_id"),
                ksql
                  .raw(
                    sql === "sqlite"
                      ? blockCheckpointSQLite
                      : blockCheckpointPostgres,
                  )
                  .as("checkpoint"),
                "chain_id",
                "number as block_number",
                "hash as block_hash",
              ])
              .where("chain_id", "=", filter.chainId)
              .$if(
                filter !== undefined && filter.interval !== undefined,
                (qb) =>
                  qb.where(
                    ksql`(number - ${filter.offset}) % ${filter.interval} = 0`,
                  ),
              )
              .$if(filter.fromBlock !== undefined, (qb) =>
                qb.where(
                  "number",
                  ">=",
                  sql === "sqlite"
                    ? encodeAsText(filter.fromBlock!)
                    : BigInt(filter.fromBlock!),
                ),
              )
              .$if(filter.toBlock !== undefined, (qb) =>
                qb.where(
                  "number",
                  "<=",
                  sql === "sqlite"
                    ? encodeAsText(filter.toBlock!)
                    : BigInt(filter.toBlock!),
                ),
              )
              .where(
                "number",
                ">=",
                sql === "sqlite"
                  ? encodeAsText(interval[0])
                  : BigInt(interval[0]),
              )
              .where(
                "number",
                "<=",
                sql === "sqlite"
                  ? encodeAsText(interval[1])
                  : BigInt(interval[1]),
              );

            await db
              .insertInto("event")
              .columns([
                "filter_id",
                "checkpoint",
                "chain_id",
                "block_number",
                "block_hash",
              ])
              .expression(subquery)
              .onConflict((oc) =>
                oc.columns(["filter_id", "checkpoint"]).doNothing(),
              )
              .execute();
          }
          break;

        default:
          never(filter);
      }
    });
  },
  getEvents: async ({ filters, from, to, limit }) => {
    const events = await db.wrap({ method: "getEvents" }, async () => {
      return await db
        .selectFrom("event")
        .selectAll()
        .where(
          "event.filter_id",
          "in",
          filters.map((filter) => getFilterId("event", filter)),
        )
        .where("event.checkpoint", ">", from)
        .where("event.checkpoint", "<=", to)
        .orderBy("event.checkpoint", "asc")
        .orderBy("event.filter_id", "asc")
        .limit(limit)
        .execute();
    });

    if (sql === "sqlite") {
      for (let i = 0; i < events.length; i++) {
        if (events[i]!.data !== null) {
          events[i]!.data = JSON.parse(events[i]!.data!);
        }
      }
    }

    let cursor: string;
    if (events.length !== limit) {
      cursor = to;
    } else {
      cursor = events[events.length - 1]!.checkpoint;
    }

    return { events: events as RawEvent[], cursor };
  },
  getEventCount: async ({ filters }) =>
    db.wrap({ method: "getEventCount" }, async () => {
      return await db
        .selectFrom("event")
        .select(ksql<number>`count(*)`.as("count"))
        .where(
          "event.filter_id",
          "in",
          filters.map((filter) => getFilterId("event", filter)),
        )
        .executeTakeFirst()
        .then((result) => result?.count ?? 0);
    }),
  insertRpcRequestResult: async ({ request, blockNumber, chainId, result }) =>
    db.wrap({ method: "insertRpcRequestResult" }, async () => {
      await db
        .insertInto("rpcRequestResults")
        .values({
          request,
          blockNumber:
            sql === "sqlite" ? encodeAsText(blockNumber) : BigInt(blockNumber),
          chainId,
          result,
        })
        .onConflict((oc) =>
          oc.columns(["request", "chainId", "blockNumber"]).doNothing(),
        )
        .execute();
    }),
  getRpcRequestResult: async ({ request, blockNumber, chainId }) =>
    db.wrap({ method: "getRpcRequestResult" }, async () => {
      const result = await db
        .selectFrom("rpcRequestResults")
        .select("result")
        .where("request", "=", request)
        .where("chainId", "=", chainId)
        .where(
          "blockNumber",
          "=",
          sql === "sqlite" ? encodeAsText(blockNumber) : BigInt(blockNumber),
        )
        .executeTakeFirst();

      return result?.result ?? null;
    }),
});
