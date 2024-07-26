import type { SyncStore } from "@/sync-store/index.js";
import type {
  BlockFilter,
  Filter,
  LogAddressFilter,
  LogFilter,
} from "@/sync/filter.js";
import { _eth_getBlockByHash, _eth_getLogs } from "@/sync/index.js";
import { type Interval, intervalDifference } from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import type { RequestQueue } from "@/utils/requestQueue.js";
import { dedupe } from "@ponder/common";
import type { Chain } from "viem";

export type HistoricalSync = {
  sync(interval: Interval): Promise<void>;
};

type CreateHistoricaSyncParameters = {
  filters: Filter[];
  syncStore: SyncStore;
  chain: Chain;
  requestQueue: RequestQueue;
};

// TODO(kyle) save most recently completed block
// type BlockCache = {};

// const createBlockCache = (): BlockCache => {};

export const createHistoricalSync = async (
  args: CreateHistoricaSyncParameters,
): Promise<HistoricalSync> => {
  // ...
  // Note: `intervalsCache` is not updated after a new interval is synced.
  const intervalsCache: Map<Filter, Interval[]> = new Map();

  // ...
  for (const filter of args.filters) {
    const intervals = await args.syncStore.getIntervals(
      "event",
      filter,
      args.chain.id,
    );
    intervalsCache.set(filter, intervals);
  }

  // Helper functions for specific sync tasks

  const syncLogFilter = async (filter: LogFilter, interval: Interval) => {
    // TODO(kyle) fetch last block of interval at same time as log, for the purpose
    // of potentially ordering in omnichain environment.

    // TODO(kyle) chunk interval based on filter metadata

    // TODO(kyle) log address filter
    const logs = await _eth_getLogs(args, {
      address: filter.address as Exclude<
        LogFilter["address"],
        LogAddressFilter
      >,
      topics: filter.topics,
      fromBlock: interval[0],
      toBlock: interval[1],
    });

    await args.syncStore.insertLogs(logs, args.chain.id);

    const dedupedBlockHashes = dedupe(logs.map((l) => l.blockHash));
    const transactionHashes = new Set(logs.map((l) => l.transactionHash));

    await Promise.all(
      dedupedBlockHashes.map(async (hash) => {
        const block = await _eth_getBlockByHash(args, { hash });

        await args.syncStore.insertBlock(block, args.chain.id);
        for (const transaction of block.transactions) {
          if (transactionHashes.has(transaction.hash)) {
            await args.syncStore.insertTransaction(transaction, args.chain.id);
          }
        }
      }),
    );
  };

  const syncBlockFilter = async (filter: BlockFilter, interval: Interval) => {};

  const syncLogAddressFilter = async (
    filter: LogAddressFilter,
    interval: Interval,
  ) => {};

  // const blockCache = createBlockCache();
  // TODO(kyle) filter metadata for recommended "eth_getLogs" ranges

  return {
    sync: async (interval) => {
      // TODO(kyle) concurrency
      for (const filter of args.filters) {
        const completedIntervals = intervalsCache.get(filter)!;

        const requiredIntervals = intervalDifference(
          [interval],
          completedIntervals,
        );

        // skip sync if the interval is already complete
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

          await args.syncStore.populateEvents({
            filters: [filter],
            chainId: args.chain.id,
            fromBlock: BigInt(interval[0]),
            toBlock: BigInt(interval[1]),
          });

          await args.syncStore.insertInterval(
            "event",
            filter,
            interval,
            args.chain.id,
          );
        }
      }
    },
  };
};
