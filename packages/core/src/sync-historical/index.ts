import type { Common } from "@/common/common.js";
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
import type { Source } from "@/sync/source.js";
import { formatPercentage } from "@/utils/format.js";
import {
  type Interval,
  intervalDifference,
  intervalSum,
} from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import type { RequestQueue } from "@/utils/requestQueue.js";
import { dedupe } from "@ponder/common";
import { type Chain, type Hash, hexToBigInt, hexToNumber, toHex } from "viem";

export type HistoricalSync = {
  /** Closest-to-tip block that is synced. */
  latestBlock: SyncBlock | undefined;
  sync(interval: Interval): Promise<void>;
  initializeMetrics(finalizedBlock: SyncBlock): void;
};

type CreateHistoricaSyncParameters = {
  common: Common;
  sources: Source[];
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
  for (const { filter } of args.sources) {
    const intervals = await args.syncStore.getIntervals({
      filterType: "event",
      filter,
    });
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

    await args.syncStore.insertLogs({ logs, chainId: args.chain.id });

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
      await args.syncStore.insertBlock({ block, chainId: args.chain.id });
    }

    if (hexToBigInt(block.number) > hexToBigInt(latestBlock?.number ?? "0x0")) {
      latestBlock = block;
    }

    if (transactionHashes !== undefined) {
      for (const transaction of block.transactions) {
        if (transactionHashes.has(transaction.hash)) {
          await args.syncStore.insertTransaction({
            transaction,
            chainId: args.chain.id,
          });
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
      for (const { filter, ...source } of args.sources) {
        // Compute the required interval to sync, accounting for cached
        // intervals and start + end block.

        // Skip sync if the interval is after the `toBlock`.
        if (filter.toBlock && filter.toBlock < _interval[0]) continue;
        const interval: Interval = [
          Math.max(filter.fromBlock, _interval[0]),
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
          // insert into the "interval" table.
          await args.syncStore.populateEvents({ filter, interval });
          await args.syncStore.insertInterval({
            filterType: "event",
            filter,
            interval,
          });
        }

        args.common.metrics.ponder_historical_completed_blocks.inc(
          {
            network: source.networkName,
            source: source.name,
            type: filter.type,
          },
          interval[1] - interval[0] + 1,
        );
      }
      blockCache.clear();
    },
    initializeMetrics(finalizedBlock) {
      for (const source of args.sources) {
        const label = {
          network: source.networkName,
          source: source.name,
          type: source.filter.type,
        };

        if (source.filter.fromBlock > hexToNumber(finalizedBlock.number)) {
          args.common.metrics.ponder_historical_total_blocks.set(label, 0);

          args.common.logger.warn({
            service: "historical",
            msg: `Skipped syncing '${source.networkName}' logs for '${source.name}' because the start block is not finalized`,
          });
        } else {
          const interval = [
            source.filter.fromBlock,
            source.filter.toBlock ?? hexToNumber(finalizedBlock.number),
          ] satisfies Interval;

          const requiredIntervals = intervalDifference(
            [interval],
            intervalsCache.get(source.filter)!,
          );

          const totalBlocks = interval[1] - interval[0] + 1;
          const cachedBlocks = totalBlocks - intervalSum(requiredIntervals);

          args.common.metrics.ponder_historical_total_blocks.set(
            label,
            totalBlocks,
          );

          args.common.metrics.ponder_historical_cached_blocks.set(
            label,
            cachedBlocks,
          );

          // TODO(kyle) different message for full cache?
          args.common.logger.info({
            service: "historical",
            msg: `Started syncing '${source.networkName}' for '${
              source.name
            }' with ${formatPercentage(
              Math.min(1, cachedBlocks / (totalBlocks || 1)),
            )} cached`,
          });
        }
      }
    },
  };
};
