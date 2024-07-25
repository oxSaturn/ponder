import type { Common } from "@/common/common.js";
import type { HeadlessKysely } from "@/database/kysely.js";
import type { RawEvent } from "@/sync/events.js";
import {
  type AddressFilter,
  type Filter,
  type LogAddressFilter,
  type LogFilter,
  getFilterId,
  isBlockFilter,
  isLogFilter,
} from "@/sync/filter.js";
import type {
  SyncBlock,
  SyncLog,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/sync/index.js";
import { encodeAsText } from "@/utils/encoding.js";
import { type Interval, intervalUnion } from "@/utils/interval.js";
import { type SelectQueryBuilder, sql as ksql } from "kysely";
import type { Address, Hash, Hex } from "viem";
import {
  type PonderSyncSchema,
  formatBlock,
  formatHex,
  formatLog,
  formatTransaction,
  formatTransactionReceipt,
} from "./encoding.js";

export type SyncStore = {
  insertAddress(
    filter: AddressFilter,
    address: Address,
    blockNumber: bigint,
    chainId: number,
  ): Promise<void>;
  getAddresses(filter: AddressFilter, chainId: number): Promise<Address[]>;
  insertInterval<type extends "raw" | "event" | "address">(
    filterType: type,
    filter: type extends "address" ? AddressFilter : Filter,
    interval: Interval,
    chainId: number,
  ): Promise<void>;
  getIntervals<type extends "raw" | "event" | "address">(
    filterType: type,
    filter: type extends "address" ? AddressFilter : Filter,
    chainId: number,
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
  populateEvents(args: {
    filters: (Filter | AddressFilter)[];
    chainId: number;
    fromBlock: bigint;
    toBlock: bigint;
  }): Promise<void>;
  /** Returns an ordered list of events based on the provided sources and pagination arguments. */
  getEvents(args: {
    filters: (Filter | AddressFilter)[];
    chainId: number;
    after: string;
    before: string;
    limit: number;
  }): Promise<{ events: RawEvent[]; cursor: string }>;
  deleteSync(fromBlock: bigint, chainId: number): Promise<void>;
  // insertRpcRequestResult
  // getRpcRequestResult;
  // TODO(kyle) prune
};

export const createSyncStore = ({
  db,
  sql,
}: {
  common: Common;
  sql: "sqlite" | "postgres";
  db: HeadlessKysely<PonderSyncSchema>;
}): SyncStore => ({
  insertAddress: async (filter, address, blockNumber, chainId) =>
    db.wrap({ method: "insertAddress" }, async () => {
      await db
        .insertInto("address")
        .values({
          filter_id: getFilterId("address", filter),
          chain_id: chainId,
          block_number:
            sql === "sqlite" ? encodeAsText(blockNumber) : blockNumber,
          address,
        })
        .execute();
    }),
  getAddresses: async (filter, chainId) =>
    db.wrap({ method: "getAddresses" }, async () => {
      return await db
        .selectFrom("address")
        .select("address")
        .where("chain_id", "=", chainId)
        .where("filter_id", "=", getFilterId("address", filter))
        .execute()
        .then((result) => result.map(({ address }) => address));
    }),
  insertInterval: async (type, filter, interval, chainId) =>
    db.wrap({ method: "insertInterval" }, async () => {
      await db
        .insertInto("interval")
        .values({
          filter_id: getFilterId(type, filter),
          chain_id: chainId,
          from: BigInt(interval[0]),
          to: BigInt(interval[1]),
        })
        .execute();
    }),
  // TODO(kyle) handle adjacent intervals
  getIntervals: async (type, filter, chainId) =>
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

        const mergedIntervalRows = mergedIntervals.map(([from, to]) => ({
          chain_id: chainId,
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
      await db
        .insertInto("log")
        .values(logs.map((log) => formatLog(log, chainId, sql)))
        .execute();
    }),
  insertBlock: async (block, chainId) =>
    db.wrap({ method: "insertBlock" }, async () => {
      await db
        .insertInto("block")
        .values(formatBlock(block, chainId, sql))
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
        .values(formatTransaction(transaction, chainId, sql))
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
        .values(formatTransactionReceipt(transactionReceipt, chainId, sql))
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
  populateEvents: async ({ filters, chainId, fromBlock, toBlock }) => {
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
            .where("chain_id", "=", chainId)
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
      .compile().sql;

    const transactionIndexQuery = db
      .selectFrom("transaction")
      .select("transaction.transaction_index")
      .where("hash", "=", ksql.ref("log.transaction_hash"))
      .compile().sql;

    for (const logFilter of filters.filter(isLogFilter)) {
      await db.wrap({ method: "populateEvents" }, async () => {
        const subquery = db
          .selectFrom("log")
          .select([
            // TODO(kyle) postgres uses ::[type] operator
            ksql
              .raw(`'${getFilterId("event", logFilter)}'`)
              .as("filter_id"),
            ksql
              .raw(`
substr((${blockTimestampQuery}), -10, 10) ||
substr('0000000000000000' || chain_id, -16, 16) ||
substr(block_number, -16, 16) ||
substr(('0000000000000000' || (${transactionIndexQuery})), -16, 16) ||
'5' ||
substr('0000000000000000' || log_index, -16, 16)`)
              .as("checkpoint"),
            "chain_id",
            "block_number",
            "block_hash",
            "log_index",
            "transaction_hash",
          ])
          .where("chain_id", "=", chainId)
          .$if(logFilter.topics !== undefined, (qb) => {
            for (const idx_ of [0, 1, 2, 3]) {
              const idx = idx_ as 0 | 1 | 2 | 3;
              // If it's an array of length 1, collapse it.
              const raw = logFilter.topics![idx] ?? null;
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
          .$call((qb) => addressSQL(qb, logFilter.address))
          .$if(logFilter.fromBlock !== undefined, (qb) =>
            qb.where(
              "block_number",
              ">=",
              formatHex(sql, logFilter.fromBlock!),
            ),
          )
          .$if(logFilter.toBlock !== undefined, (qb) =>
            qb.where("block_number", "<=", formatHex(sql, logFilter.toBlock!)),
          )
          .where(
            "block_number",
            ">=",
            sql === "sqlite" ? encodeAsText(fromBlock) : fromBlock,
          )
          .where(
            "block_number",
            "<=",
            sql === "sqlite" ? encodeAsText(toBlock) : toBlock,
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
      });
    }

    for (const blockFilter of filters.filter(isBlockFilter)) {
      const subquery = db
        .selectFrom("block")
        .select([
          // TODO(kyle) postgres uses ::[type] operator
          ksql
            .raw(`'${getFilterId("event", blockFilter)}'`)
            .as("filter_id"),
          ksql
            .raw(`
substr(timestamp, -10, 10) ||
substr('0000000000000000' || chain_id, -16, 16) ||
substr(number, -16, 16) ||
'9999999999999999' ||
'5' ||
'0000000000000000'`)
            .as("checkpoint"),
          "chain_id",
          "number as block_number",
          "hash as block_hash",
        ])
        .where("chain_id", "=", chainId)
        .$if(
          blockFilter !== undefined && blockFilter.interval !== undefined,
          (qb) =>
            qb.where(
              ksql`(number - ${blockFilter.offset}) % ${blockFilter.interval} = 0`,
            ),
        )
        .$if(blockFilter.fromBlock !== undefined, (qb) =>
          qb.where("number", ">=", formatHex(sql, blockFilter.fromBlock!)),
        )
        .$if(blockFilter.toBlock !== undefined, (qb) =>
          qb.where("number", "<=", formatHex(sql, blockFilter.toBlock!)),
        )
        .where(
          "number",
          ">=",
          sql === "sqlite" ? encodeAsText(fromBlock) : fromBlock,
        )
        .where(
          "number",
          "<=",
          sql === "sqlite" ? encodeAsText(toBlock) : toBlock,
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
  },
  getEvents: async ({ filters, chainId, after, before, limit }) => {
    const events = await db.wrap({ method: "getEvents" }, async () => {
      return await db
        .selectFrom("event")
        .innerJoin("block", "block.hash", "event.block_hash")
        .leftJoin("log", (join) =>
          join.on((eb) =>
            eb.and([
              eb("event.block_hash", "=", ksql.ref("log.block_hash")),
              eb("event.log_index", "=", ksql.ref("log.log_index")),
            ]),
          ),
        )
        .leftJoin("transaction", "transaction.hash", "event.transaction_hash")
        .leftJoin(
          "transaction_receipt",
          "transaction_receipt.hash",
          "event.transaction_hash",
        )
        .select([
          "event.checkpoint",
          "event.filter_id",
          "block.body as block",
          "log.body as log",
          "transaction.body as transaction",
          "transaction_receipt.body as transaction_receipt",
        ])
        .where(
          "event.filter_id",
          "in",
          filters.map((filter) => getFilterId("event", filter)),
        )
        .where("event.chain_id", "=", chainId)
        .where("event.checkpoint", ">", after)
        .where("event.checkpoint", "<=", before)
        .orderBy("event.checkpoint", "asc")
        .orderBy("event.filter_id", "asc")
        .limit(limit)
        .execute();
    });

    let cursor: string;
    if (events.length !== limit) {
      cursor = before;
    } else {
      cursor = events[events.length - 1]!.checkpoint;
    }

    return { events, cursor };
  },
  deleteSync: async () => {},
});
