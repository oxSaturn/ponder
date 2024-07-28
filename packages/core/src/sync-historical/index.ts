import type { SyncStore } from "@/sync-store/index.js";
import type {
  BlockFilter,
  Filter,
  LogAddressFilter,
  LogFilter,
} from "@/sync/filter.js";
import {
  type SyncBlock,
  _eth_getBlockByHash,
  _eth_getBlockByNumber,
  _eth_getLogs,
} from "@/sync/index.js";
import { type Interval, intervalDifference } from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import type { RequestQueue } from "@/utils/requestQueue.js";
import { dedupe } from "@ponder/common";
import { type Chain, type Hash, hexToBigInt, toHex } from "viem";

export type HistoricalSync = {
  /** Closest-to-tip block that is synced. */
  latestBlock: SyncBlock | undefined;
  sync(interval: Interval): Promise<void>;
};

type CreateHistoricaSyncParameters = {
  filters: Filter[];
  syncStore: SyncStore;
  chain: Chain;
  requestQueue: RequestQueue;
};

export const createHistoricalSync = async (
  args: CreateHistoricaSyncParameters,
): Promise<HistoricalSync> => {
  const blockCache = new Map<bigint, Promise<SyncBlock>>();
  // ...
  // Note: `intervalsCache` is not updated after a new interval is synced.
  const intervalsCache: Map<Filter, Interval[]> = new Map();

  // ...
  for (const filter of args.filters) {
    const intervals = await args.syncStore.getIntervals("event", filter);
    intervalsCache.set(filter, intervals);
  }

  let latestBlock: SyncBlock | undefined;

  // Helper functions for specific sync tasks

  const syncLogFilter = async (filter: LogFilter, interval: Interval) => {
    // TODO(kyle) fetch last block of interval at same time as log, for the purpose
    // of potentially ordering in omnichain environment.

    // TODO(kyle) chunk interval based on filter metadata

    // TODO(kyle) log address filter
    const logs = await _eth_getLogs(args, {
      address: filter.address as any,
      topics: filter.topics,
      fromBlock: interval[0],
      toBlock: interval[1],
    });

    await args.syncStore.insertLogs(logs, args.chain.id);

    const dedupedBlockNumbers = dedupe(logs.map((l) => l.blockNumber));
    const transactionHashes = new Set(logs.map((l) => l.transactionHash));

    await Promise.all(
      dedupedBlockNumbers.map((number) =>
        syncBlock(hexToBigInt(number), transactionHashes),
      ),
    );
  };

  const syncBlockFilter = async (filter: BlockFilter, interval: Interval) => {};

  const syncLogAddressFilter = async (
    filter: LogAddressFilter,
    interval: Interval,
  ) => {};

  const syncBlock = async (number: bigint, transactionHashes?: Set<Hash>) => {
    let block: SyncBlock;

    if (blockCache.has(number)) {
      block = await blockCache.get(number)!;
    } else {
      // TODO(kyle) syncStore.hasBlock();
      const _block = _eth_getBlockByNumber(args, {
        blockNumber: toHex(number),
      });
      blockCache.set(number, _block);
      block = await _block;
      await args.syncStore.insertBlock(block, args.chain.id);
    }

    if (hexToBigInt(block.number) > hexToBigInt(latestBlock?.number ?? "0x0")) {
      latestBlock = block;
    }

    if (transactionHashes !== undefined) {
      for (const transaction of block.transactions) {
        if (transactionHashes.has(transaction.hash)) {
          await args.syncStore.insertTransaction(transaction, args.chain.id);
        }
      }
    }
  };

  // TODO(kyle) use filter metadata for recommended "eth_getLogs" ranges

  return {
    get latestBlock() {
      return latestBlock;
    },
    sync: async (_interval) => {
      // TODO(kyle) concurrency
      for (const filter of args.filters) {
        // Compute the required interval to sync, accounting for cached
        // intervals and start + end block.

        // Skip sync if the interval is after the `toBlock`.
        if (filter.toBlock && filter.toBlock < _interval[0]) continue;
        const interval: Interval = [
          Math.max(filter.fromBlock ?? 0, _interval[0]),
          Math.min(filter.toBlock ?? Number.POSITIVE_INFINITY, _interval[1]),
        ];
        const completedIntervals = intervalsCache.get(filter)!;
        const requiredIntervals = intervalDifference(
          [interval],
          completedIntervals,
        );
        // Skip sync if the interval is already complete.
        if (requiredIntervals.length === 0) continue;

        // sync required intervals
        for (const interval of requiredIntervals) {
          switch (filter.type) {
            case "log":
              await syncLogFilter(filter, interval);
              break;

            case "block":
              await syncBlockFilter(filter, interval);
              break;

            default:
              never(filter);
          }

          // Add newly synced data to the "event" table, and then
          // add to the "interval" table.
          await args.syncStore.populateEvents(filter, interval);
          await args.syncStore.insertInterval("event", filter, interval);
        }
      }
      blockCache.clear();
    },
  };
};
