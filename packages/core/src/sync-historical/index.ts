import type { Common } from "@/common/common.js";
import { getHistoricalSyncProgress } from "@/common/metrics.js";
import type { Network } from "@/config/networks.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  type AddressFilter,
  type BlockFilter,
  type Filter,
  type LogAddressFilter,
  type LogFilter,
  isAddressFilter,
} from "@/sync/filter.js";
import {
  type SyncBlock,
  type SyncLog,
  _eth_getBlockByHash,
  _eth_getBlockByNumber,
  _eth_getLogs,
} from "@/sync/index.js";
import type { Source } from "@/sync/source.js";
import { formatEta, formatPercentage } from "@/utils/format.js";
import {
  type Interval,
  intervalDifference,
  intervalSum,
} from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import type { RequestQueue } from "@/utils/requestQueue.js";
import { dedupe } from "@ponder/common";
import { type Address, type Hash, hexToBigInt, hexToNumber, toHex } from "viem";

export type HistoricalSync = {
  /** Closest-to-tip block that is synced. */
  latestBlock: SyncBlock | undefined;
  /** Extract raw data for `interval`. */
  sync(interval: Interval): Promise<void>;
  initializeMetrics(finalizedBlock: SyncBlock): void;
  kill(): void;
};

type CreateHistoricaSyncParameters = {
  common: Common;
  sources: Source[];
  syncStore: SyncStore;
  network: Network;
  requestQueue: RequestQueue;
};

export const createHistoricalSync = async (
  args: CreateHistoricaSyncParameters,
): Promise<HistoricalSync> => {
  /**
   * Blocks that have already been extracted.
   * Note: All entries are deleted at the end of each call to `sync()`.
   */
  const blockCache = new Map<bigint, Promise<SyncBlock>>();

  /** Intervals that have been completed for all "event" and "address" filters
   * in `args.sources`.
   *
   * Note: `intervalsCache` is not updated after a new interval is synced.
   */
  const intervalsCache: Map<Filter | AddressFilter, Interval[]> = new Map();

  // Populate `intervalsCache` by querying the sync-store.
  for (const { filter } of args.sources) {
    const getAddressInterval = async (filter: AddressFilter) => {
      const intervals = await args.syncStore.getIntervals({
        filterType: "address",
        filter,
      });
      intervalsCache.set(filter, intervals);

      const _address = filter.address;
      if (isAddressFilter(_address)) await getAddressInterval(_address);
    };

    // "event" intervals
    const intervals = await args.syncStore.getIntervals({
      filterType: "event",
      filter,
    });
    intervalsCache.set(filter, intervals);

    // "address" intervals
    if (filter.type === "log") {
      const _address = filter.address;
      if (isAddressFilter(_address)) await getAddressInterval(_address);
    }
  }

  // Closest-to-tip block that has been fully injested.
  let latestBlock: SyncBlock | undefined;

  ////////
  // Helper functions for specific sync tasks
  ////////

  const syncLogFilter = async (filter: LogFilter, interval: Interval) => {
    // TODO(kyle) fetch last block of interval at same time as log, for the purpose
    // of potentially ordering in omnichain environment.

    // Resolve `filter.address`
    const _address = filter.address;
    const address = isAddressFilter(_address)
      ? await syncAddress(_address, interval)
      : _address;

    const logs = await _eth_getLogs(args, {
      address,
      topics: filter.topics,
      fromBlock: interval[0],
      toBlock: interval[1],
    });

    await args.syncStore.insertLogs({ logs, chainId: args.network.chainId });

    const dedupedBlockNumbers = dedupe(logs.map((l) => l.blockNumber));
    const transactionHashes = new Set(logs.map((l) => l.transactionHash));

    await Promise.all(
      dedupedBlockNumbers.map((b) =>
        syncBlock(hexToBigInt(b), transactionHashes),
      ),
    );
  };

  const syncBlockFilter = async (filter: BlockFilter, interval: Interval) => {
    const baseOffset = (interval[0] - filter.offset) % filter.interval;
    const offset = baseOffset === 0 ? 0 : filter.interval - baseOffset;

    // Determine which blocks are matched by the block filter.
    const requiredBlocks: number[] = [];
    for (let b = interval[0] + offset; b <= interval[1]; b += filter.interval) {
      requiredBlocks.push(b);
    }

    await Promise.all(requiredBlocks.map((b) => syncBlock(BigInt(b))));
  };

  /** Extract and insert the log-based addresses that match `filter` + `interval`. */
  const syncLogAddressFilter = async (
    filter: LogAddressFilter,
    interval: Interval,
  ) => {
    // Resolve `filter.address`
    const _address = filter.address;
    const address = isAddressFilter(_address)
      ? await syncAddress(_address, interval)
      : _address;

    const logs = await _eth_getLogs(args, {
      address,
      topics: [filter.eventSelector],
      fromBlock: interval[0],
      toBlock: interval[1],
    });

    const getLogAddressFilterAddress = (log: SyncLog): Address => {
      if (filter.childAddressLocation.startsWith("offset")) {
        const childAddressOffset = Number(
          filter.childAddressLocation.substring(6),
        );
        const start = 2 + 12 * 2 + childAddressOffset * 2;
        const length = 20 * 2;
        return `0x${log.data.substring(start, start + length)}`;
      } else {
        const start = 2 + 12 * 2;
        const length = 20 * 2;

        return `0x${log.topics[
          Number(
            filter.childAddressLocation.charAt(
              filter.childAddressLocation.length - 1,
            ),
          )
        ]!.substring(start, start + length)}`;
      }
    };

    const addresses = logs.map((l) => ({
      address: getLogAddressFilterAddress(l),
      blockNumber: hexToNumber(l.blockNumber),
    }));

    // Insert the addresses into the sync-store
    await args.syncStore.insertAddresses({ filter, addresses });
  };

  /**
   * Extract block, using `blockCache` to avoid fetching
   * the same block twice. Also, update `latestBlock`.
   *
   * @param number Block to be extracted
   * @param transactionHashes Hashes to be inserted into the sync-store
   *
   * Note: This function could more accurately skip network requests by taking
   * advantage of `syncStore.hasBlock` and `syncStore.hasTransaction`.
   */
  const syncBlock = async (number: bigint, transactionHashes?: Set<Hash>) => {
    let block: SyncBlock;

    /**
     * `blockCache` contains all blocks that have been extracted during the
     * current call to `sync()`. If `number` is present in `blockCache` use it,
     * otherwise, request the block and add it to `blockCache` and the sync-store.
     */

    if (blockCache.has(number)) {
      block = await blockCache.get(number)!;
    } else {
      const _block = _eth_getBlockByNumber(args, {
        blockNumber: toHex(number),
      });
      blockCache.set(number, _block);
      block = await _block;
      await args.syncStore.insertBlock({
        block,
        chainId: args.network.chainId,
      });

      // Update `latestBlock` if `block` is closer to tip.
      if (
        hexToBigInt(block.number) > hexToBigInt(latestBlock?.number ?? "0x0")
      ) {
        latestBlock = block;
      }
    }

    // Add `transactionHashes` to the sync-store.
    if (transactionHashes !== undefined) {
      for (const transaction of block.transactions) {
        if (transactionHashes.has(transaction.hash)) {
          await args.syncStore.insertTransaction({
            transaction,
            chainId: args.network.chainId,
          });
        }
      }
    }
  };

  /**
   * Return all addresses that match `filter` after extracting addresses
   * that match `filter` and `interval`.
   */
  const syncAddress = async (
    filter: AddressFilter,
    interval: Interval,
  ): Promise<Address[]> => {
    const completedIntervals = intervalsCache.get(filter)!;
    const requiredIntervals = intervalDifference(
      [interval],
      completedIntervals,
    );

    // Skip sync if the interval is already complete.
    if (requiredIntervals.length !== 0) {
      // sync required intervals
      for (const interval of requiredIntervals) {
        await syncLogAddressFilter(filter, interval);
      }

      // mark `interval` for `filter` as completed in the sync-store
      await args.syncStore.insertInterval({
        filterType: "address",
        filter,
        interval,
      });
    }

    // TODO(kyle) should "address" metrics be tracked?

    // Query the sync-store for all addresses that match `filter`.
    return await args.syncStore.getAddresses({ filter });
  };

  // TODO(kyle) use filter metadata for recommended "eth_getLogs" ranges

  // Emit status update logs on an interval for each source.
  const interval = setInterval(async () => {
    const historical = await getHistoricalSyncProgress(args.common.metrics);

    for (const {
      networkName,
      sourceName,
      progress,
      eta,
    } of historical.sources) {
      // TODO(kyle) check `requiredIntervals`.
      if (progress === 1 || networkName !== args.network.name) return;
      args.common.logger.info({
        service: "historical",
        msg: `Syncing '${networkName}' for '${sourceName}' with ${formatPercentage(
          progress ?? 0,
        )} complete${eta !== undefined ? ` and ~${formatEta(eta)} remaining` : ""}`,
      });
    }
  }, 1_000);

  return {
    get latestBlock() {
      return latestBlock;
    },
    async sync(_interval) {
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
        }

        // Add newly synced data to the "event" table, and then
        // mark `interval` for `filter` as completed in the sync-store.
        await args.syncStore.populateEvents({ filter, interval });
        await args.syncStore.insertInterval({
          filterType: "event",
          filter,
          interval,
        });

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
            msg: `Skipped syncing '${source.networkName}' for '${source.name}' because the start block is not finalized`,
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
    kill() {
      clearInterval(interval);
    },
  };
};
