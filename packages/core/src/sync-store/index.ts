import type { Common } from "@/common/common.js";
import type { HeadlessKysely } from "@/database/kysely.js";
import type { RawEvent } from "@/sync/events.js";
import {
  type AddressFilter,
  type Filter,
  type LogAddressFilter,
  type LogFilter,
  getFilterId,
} from "@/sync/filter.js";
import type {
  SyncBlock,
  SyncLog,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/sync/index.js";
import { encodeAsText } from "@/utils/encoding.js";
import { type Interval, intervalUnion } from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import { type SelectQueryBuilder, sql as ksql } from "kysely";
import type { Address, Hash, Hex } from "viem";
import {
  type PonderSyncSchema,
  encodeBlock,
  encodeLog,
  encodeTransaction,
  encodeTransactionReceipt,
} from "./encoding.js";

// TODO(kyle) do nothing on conflict
// TODO(kyle) handle empty

export type SyncStore = {
  insertAddress(
    filter: AddressFilter,
    address: Address,
    blockNumber: bigint,
  ): Promise<void>;
  getAddresses(filter: AddressFilter): Promise<Address[]>;
  insertInterval<type extends "event" | "address">(
    filterType: type,
    filter: type extends "address" ? AddressFilter : Filter,
    interval: Interval,
  ): Promise<void>;
  getIntervals<type extends "event" | "address">(
    filterType: type,
    filter: type extends "address" ? AddressFilter : Filter,
  ): Promise<Interval[]>;
  insertLogs(logs: SyncLog[], chainId: number): Promise<void>;
  insertBlock(block: SyncBlock, chainId: number): Promise<void>;
  hasBlock(hash: Hash, chainId: number): Promise<boolean>;
  insertTransaction(
    transaction: SyncTransaction,
    chainId: number,
  ): Promise<void>;
  hasTransaction(hash: Hash, chainId: number): Promise<boolean>;
  insertTransactionReceipt(
    transactionReceipt: SyncTransactionReceipt,
    chainId: number,
  ): Promise<void>;
  hasTransactionReceipt(hash: Hash, chainId: number): Promise<boolean>;
  populateEvents(filter: Filter, interval: Interval): Promise<void>;
  /** Returns an ordered list of events based on the provided sources and pagination arguments. */
  getEvents(args: {
    filters: Filter[];
    from: string;
    to: string;
    limit: number;
  }): Promise<{ events: RawEvent[]; cursor: string }>;
  getEventCount(args: { filters: Filter[] }): Promise<number>;
  deleteSync(fromBlock: bigint, chainId: number): Promise<void>;
  // insertRpcRequestResult
  // getRpcRequestResult;
};

export const createSyncStore = ({
  db,
  sql,
}: {
  common: Common;
  sql: "sqlite" | "postgres";
  db: HeadlessKysely<PonderSyncSchema>;
}): SyncStore => ({
  insertAddress: async (filter, address, blockNumber) =>
    db.wrap({ method: "insertAddress" }, async () => {
      await db
        .insertInto("address")
        .values({
          filter_id: getFilterId("address", filter),
          chain_id: filter.chainId,
          block_number:
            sql === "sqlite" ? encodeAsText(blockNumber) : blockNumber,
          address,
        })
        .execute();
    }),
  getAddresses: async (filter) =>
    db.wrap({ method: "getAddresses" }, async () => {
      return await db
        .selectFrom("address")
        .select("address")
        .where("chain_id", "=", filter.chainId)
        .where("filter_id", "=", getFilterId("address", filter))
        .execute()
        .then((result) => result.map(({ address }) => address));
    }),
  insertInterval: async (type, filter, interval) =>
    db.wrap({ method: "insertInterval" }, async () => {
      await db
        .insertInto("interval")
        .values({
          filter_id: getFilterId(type, filter),
          chain_id: filter.chainId,
          from: BigInt(interval[0]),
          to: BigInt(interval[1]),
        })
        .execute();
    }),
  getIntervals: async (type, filter) =>
    db.wrap({ method: "getIntervals" }, () =>
      db.transaction().execute(async (tx) => {
        const filterId = getFilterId(type, filter);

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
  insertLogs: async (logs, chainId) =>
    db.wrap({ method: "insertLogs" }, async () => {
      if (logs.length === 0) return;
      await db
        .insertInto("log")
        .values(logs.map((log) => encodeLog(log, chainId, sql)))
        .onConflict((oc) =>
          oc.columns(["block_hash", "log_index", "chain_id"]).doNothing(),
        )
        .execute();
    }),
  insertBlock: async (block, chainId) =>
    db.wrap({ method: "insertBlock" }, async () => {
      await db
        .insertInto("block")
        .values(encodeBlock(block, chainId, sql))
        .onConflict((oc) => oc.columns(["hash", "chain_id"]).doNothing())
        .execute();
    }),
  hasBlock: async (hash, chainId) =>
    db.wrap({ method: "hasBlock" }, async () => {
      return await db
        .selectFrom("block")
        .select("hash")
        .where("chain_id", "=", chainId)
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertTransaction: async (transaction, chainId) =>
    db.wrap({ method: "insertTransaction" }, async () => {
      await db
        .insertInto("transaction")
        .values(encodeTransaction(transaction, chainId, sql))
        .onConflict((oc) => oc.columns(["hash", "chain_id"]).doNothing())
        .execute();
    }),
  hasTransaction: async (hash, chainId) =>
    db.wrap({ method: "hasTransaction" }, async () => {
      return await db
        .selectFrom("transaction")
        .select("hash")
        .where("chain_id", "=", chainId)
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  insertTransactionReceipt: async (transactionReceipt, chainId) =>
    db.wrap({ method: "insertTransactionReceipt" }, async () => {
      await db
        .insertInto("transaction_receipt")
        .values(encodeTransactionReceipt(transactionReceipt, chainId, sql))
        .execute();
    }),
  hasTransactionReceipt: async (hash, chainId) =>
    db.wrap({ method: "hasTransactionReceipt" }, async () => {
      return await db
        .selectFrom("transaction_receipt")
        .select("hash")
        .where("chain_id", "=", chainId)
        .where("hash", "=", hash)
        .executeTakeFirst()
        .then((result) => result !== undefined);
    }),
  populateEvents: async (filter, interval) => {
    const childAddressSQL = (
      childAddressLocation: LogAddressFilter["childAddressLocation"],
    ) => {
      if (childAddressLocation.startsWith("offset")) {
        const childAddressOffset = Number(childAddressLocation.substring(6));
        const start = 2 + 12 * 2 + childAddressOffset * 2 + 1;
        const length = 20 * 2;
        return ksql<Hex>`'0x' || substring(data, ${start}, ${length})`;
      } else {
        const start = 2 + 12 * 2 + 1;
        const length = 20 * 2;
        return ksql<Hex>`'0x' || substring(${ksql.ref(childAddressLocation)}, ${start}, ${length})`;
      }
    };

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
            .selectFrom("log")
            .select(
              childAddressSQL(address.childAddressLocation).as("childAddress"),
            )
            .where("chain_id", "=", address.chainId)
            .where("topic0", "=", address.eventSelector)
            .$call((_qb) => addressSQL(_qb, address.address)),
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

    await db.wrap({ method: "populateEvents" }, async () => {
      switch (filter.type) {
        case "log":
          {
            const subquery = db
              .selectFrom("log")
              .select([
                // TODO(kyle) postgres uses ::[type] operator
                ksql
                  .raw(`'${getFilterId("event", filter)}'`)
                  .as("filter_id"),
                ksql
                  .raw(
                    sql === "sqlite"
                      ? logCheckpointSQLite
                      : logCheckpointPostgres,
                  )
                  .as("checkpoint"),
                "chain_id",
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
                "block_number",
                "block_hash",
                "log_index",
                "transaction_hash",
              ])
              .expression(subquery)
              .onConflict((oc) =>
                oc.columns(["filter_id", "checkpoint", "chain_id"]).doNothing(),
              )
              .execute();
          }
          break;

        case "block":
          {
            const subquery = db
              .selectFrom("block")
              .select([
                // TODO(kyle) postgres uses ::[type] operator
                ksql
                  .raw(`'${getFilterId("event", filter)}'`)
                  .as("filter_id"),
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
                oc.columns(["filter_id", "checkpoint", "chain_id"]).doNothing(),
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
    const start = performance.now();
    const events = await db.wrap({ method: "getEvents" }, async () => {
      return await db
        .selectFrom("event")
        .innerJoin("block", (join) =>
          join.on((eb) =>
            eb.and([
              eb("event.block_hash", "=", ksql.ref("block.hash")),
              eb("event.chain_id", "=", ksql.ref("block.chain_id")),
            ]),
          ),
        )
        .leftJoin("log", (join) =>
          join.on((eb) =>
            eb.and([
              eb("event.block_hash", "=", ksql.ref("log.block_hash")),
              eb("event.log_index", "=", ksql.ref("log.log_index")),
              eb("event.chain_id", "=", ksql.ref("log.chain_id")),
            ]),
          ),
        )
        .leftJoin("transaction", (join) =>
          join.on((eb) =>
            eb.and([
              eb("event.transaction_hash", "=", ksql.ref("transaction.hash")),
              eb("event.chain_id", "=", ksql.ref("transaction.chain_id")),
            ]),
          ),
        )
        // .leftJoin(
        //   "transaction_receipt",
        //   "transaction_receipt.hash",
        //   "event.transaction_hash",
        // )
        .select([
          "event.checkpoint",
          "event.filter_id",
          // "block.body as block",
          // "log.body as log",
          // "transaction.body as transaction",
          // "transaction_receipt.body as transaction_receipt",
        ])
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

    console.log(performance.now() - start);

    let cursor: string;
    if (events.length !== limit) {
      cursor = to;
    } else {
      cursor = events[events.length - 1]!.checkpoint;
    }

    return { events, cursor };
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
  // pruneByBlock,
  // pruneBySource,
  // pruneByChain,
});

const blockCheckpointSQLite = `
substr(timestamp, -10, 10) ||
substr('0000000000000000' || chain_id, -16, 16) ||
substr(number, -16, 16) ||
'9999999999999999' ||
'5' ||
'0000000000000000'`;

const blockCheckpointPostgres = `
substr(timestamp, -10, 10) ||
substr('0000000000000000' || chain_id, -16, 16) ||
substr(number, -16, 16) ||
'9999999999999999' ||
'5' ||
'0000000000000000'`;
