import type { Common } from "@/common/common.js";
import type { Network } from "@/config/networks.js";
import { createHistoricalSync } from "@/sync-historical/index.js";
import type { SyncStore } from "@/sync-store/index.js";
import type { SyncBlock } from "@/types/sync.js";
import type { Interval } from "@/utils/interval.js";
import { type RequestQueue, createRequestQueue } from "@/utils/requestQueue.js";
import { hexToNumber } from "viem";
import { _eth_getBlockByNumber } from "../utils/rpc.js";
import type { Source } from "./source.js";

export type LocalSync = {
  requestQueue: RequestQueue;
  startBlock: SyncBlock;
  endBlock: SyncBlock | undefined;
  latestBlock: SyncBlock | undefined;
  finalizedBlock: SyncBlock;
  sync(): Promise<void>;
  /** Returns true when `finalizedBlock` is closer to tip than `endBlock` */
  isComplete(): boolean;
  kill(): void;
};

type CreateLocalSyncParameters = {
  common: Common;
  syncStore: SyncStore;
  sources: Source[];
  network: Network;
};

export const createLocalSync = async (
  args: CreateLocalSyncParameters,
): Promise<LocalSync> => {
  const requestQueue = createRequestQueue({
    network: args.network,
    common: args.common,
  });

  /** Earliest `startBlock` among all `filters` */
  const start = Math.min(
    ...args.sources.map(({ filter }) => filter.fromBlock ?? 0),
  );
  /**
   * Latest `endBlock` among all filters. `undefined` if at least one
   * of the filters doesn't have an `endBlock`.
   */
  const end = args.sources.some(({ filter }) => filter.toBlock === undefined)
    ? undefined
    : Math.min(...args.sources.map(({ filter }) => filter.toBlock!));

  const [remoteChainId, startBlock, endBlock, latestBlock] = await Promise.all([
    requestQueue.request({ method: "eth_chainId" }),
    _eth_getBlockByNumber(requestQueue, { blockNumber: start }),
    end === undefined
      ? undefined
      : _eth_getBlockByNumber(requestQueue, { blockNumber: end }),
    _eth_getBlockByNumber(requestQueue, { blockTag: "latest" }),
  ]);

  // Warn if the config has a different chainId than the remote.
  if (hexToNumber(remoteChainId) !== args.network.chainId) {
    args.common.logger.warn({
      service: "sync",
      msg: `Remote chain ID (${remoteChainId}) does not match configured chain ID (${args.network.chainId}) for network "${args.network.name}"`,
    });
  }

  const finalizedBlockNumber = Math.max(
    0,
    hexToNumber(latestBlock.number) - args.network.finalityBlockCount,
  );

  const finalizedBlock = await _eth_getBlockByNumber(requestQueue, {
    blockNumber: finalizedBlockNumber,
  });

  const historicalSync = await createHistoricalSync({
    common: args.common,
    sources: args.sources,
    syncStore: args.syncStore,
    network: args.network,
    requestQueue,
  });
  historicalSync.initializeMetrics(finalizedBlock);

  /**
   * Estimate the event density, eventually to be used to
   * determine the interval size passed to `historicalSync.sync()`.
   *
   * TODO(kyle) dynamically adjust, use diagnostic run of ~100 blocks.
   */
  const blocksPerEvent = 0.25 / args.sources.length;

  // Cursor to track progress.
  let fromBlock = hexToNumber(startBlock.number);

  // `latestBlock` override. Set during realtime sync
  let _latestBlock: SyncBlock | undefined;

  const localSync = {
    requestQueue,
    startBlock,
    endBlock,
    get latestBlock() {
      if (_latestBlock !== undefined) return _latestBlock;
      /**
       * Use `fromBlock` to determine if the sync is complete. If the sync is
       * complete, return the most relevant block (`endBlock` takes precedence over `finalizedBlock`),
       * otherwise, return the `latestBlock` according to `historicalSync`.
       *
       * This extra complexity is needed to make sure omnichain ordering can happen accurately.
       */
      if (endBlock !== undefined && fromBlock >= hexToNumber(endBlock.number)) {
        return endBlock;
      }
      if (fromBlock >= hexToNumber(finalizedBlock.number)) {
        return finalizedBlock;
      }
      return historicalSync.latestBlock;
    },
    set latestBlock(block) {
      if (block === undefined) return;
      _latestBlock = block;
    },
    finalizedBlock,
    async sync() {
      /**
       * Select a range of blocks to sync bounded by `finalizedBlock`.
       *
       * It is important for devEx that the interval is not too large, because
       * time spent syncing ≈ time before indexing function feedback.
       */
      const interval: Interval = [
        fromBlock,
        Math.min(
          fromBlock + blocksPerEvent * 1_000,
          hexToNumber(finalizedBlock.number),
        ),
      ];
      // Update cursor to record progress
      fromBlock = interval[1];

      await historicalSync.sync(interval);
    },
    isComplete() {
      if (this.endBlock === undefined) return false;
      return (
        hexToNumber(this.finalizedBlock.number) >=
        hexToNumber(this.endBlock.number)
      );
    },
    kill() {
      historicalSync.kill();
    },
  };

  return localSync;
};