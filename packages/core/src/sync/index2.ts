import type { Common } from "@/common/common.js";
import type { Network } from "@/config/networks.js";
import { createHistoricalSync } from "@/sync-historical/index.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  checkpointMin,
  encodeCheckpoint,
  maxCheckpoint,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import type { Interval } from "@/utils/interval.js";
import { type RequestQueue, createRequestQueue } from "@/utils/requestQueue.js";
import { type Chain, hexToBigInt, hexToNumber } from "viem";
import type { RawEvent } from "./events.js";
import type { Filter } from "./filter.js";
import { type SyncBlock, _eth_getBlockByNumber } from "./index.js";
import type { Source } from "./source.js";

export type Sync = {
  getEvents(): AsyncGenerator<RawEvent[]>;
};

type CreateSyncParameters = {
  common: Common;
  syncStore: SyncStore;
  sources: Source[];
  networks: Network[];
};

type LocalSync = {
  startBlock: SyncBlock;
  latestBlock: SyncBlock | undefined;
  finalizedBlock: SyncBlock;
  sync(): Promise<void>;
  requestQueue: RequestQueue;
};

const createLocalSync = async (args: {
  common: Common;
  syncStore: SyncStore;
  filters: Filter[];
  network: Network;
}): Promise<LocalSync> => {
  const requestQueue = createRequestQueue({
    network: args.network,
    common: args.common,
  });

  /** Earliest `startBlock` among all `filters` */
  const start = Math.min(...args.filters.map((f) => f.fromBlock ?? 0));
  // /** `true` if all `filters` have a defined end block */
  // const isEndBlockSet = args.filters.every((f) => f.toBlock !== undefined);
  // /** `true` if all `filters` have a defined end block in the finalized range */
  // const isEndBlockFinalized;

  const [remoteChainId, startBlock, latestBlock] = await Promise.all([
    requestQueue.request({ method: "eth_chainId" }),
    _eth_getBlockByNumber({ requestQueue }, { blockNumber: start }),
    _eth_getBlockByNumber({ requestQueue }, { blockTag: "latest" }),
  ]);

  // ...
  if (hexToNumber(remoteChainId) !== args.network.chain.id) {
    args.common.logger.warn({
      service: "sync",
      msg: `Remote chain ID (${remoteChainId}) does not match configured chain ID (${args.network.chainId}) for network "${args.network.name}"`,
    });
  }

  const finalizedBlockNumber = Math.max(
    0,
    hexToNumber(latestBlock.number) - args.network.finalityBlockCount,
  );

  const finalizedBlock = await _eth_getBlockByNumber(
    { requestQueue },
    {
      blockNumber: finalizedBlockNumber,
    },
  );

  const historicalSync = await createHistoricalSync({
    filters: args.filters,
    syncStore: args.syncStore,
    chain: args.network.chain,
    requestQueue,
  });

  /** ... */
  // TODO(kyle) dynamic, use diagnostics
  const blocksPerEvent = 0.25 / args.filters.length;
  let fromBlock = hexToNumber(startBlock.number);

  return {
    startBlock,
    get latestBlock() {
      // [explain why]
      if (fromBlock === hexToNumber(finalizedBlock.number))
        return finalizedBlock;
      return historicalSync.latestBlock;
    },
    finalizedBlock,
    async sync() {
      const interval: Interval = [
        fromBlock,
        Math.min(
          fromBlock + blocksPerEvent * 1_000,
          hexToNumber(finalizedBlock.number),
        ),
      ];
      fromBlock = interval[1];

      await historicalSync.sync(interval);
    },
    requestQueue,
  };
};

export const createSync = async (args: CreateSyncParameters): Promise<Sync> => {
  const localSyncs = new Map<Chain, LocalSync>();

  await Promise.all(
    args.networks.map(async (network) => {
      const localSync = await createLocalSync({
        common: args.common,
        syncStore: args.syncStore,
        filters: args.sources
          .map(({ filter }) => filter)
          .filter((f) => f.chainId === network.chain.id),
        network,
      });
      localSyncs.set(network.chain, localSync);
    }),
  );

  /**
   * Returns the minimum checkpoint across all chains.
   * Note: `historicalSync.latestBlock` is assumed to be defined if
   * this function is called with `tag` == "latest".
   */
  const getChainsCheckpoint = (
    tag: "start" | "latest" | "finalized",
  ): string => {
    const checkpoints = args.networks.map((network) => {
      const localSync = localSyncs.get(network.chain)!;
      const block = localSync[`${tag}Block`]!;

      return {
        // [inclusivity]
        ...(tag === "start" ? zeroCheckpoint : maxCheckpoint),
        blockTimestamp: hexToNumber(block.timestamp),
        chainId: BigInt(network.chain.id),
        blockNumber: hexToBigInt(block.number),
      };
    });
    return encodeCheckpoint(checkpointMin(...checkpoints));
  };

  // TODO(kyle) programmatically refetch finalized blocks to avoid exiting too early

  /**
   * Omnichain `getEvents`
   *
   * Retrieve all events across all networks ordered by `checkpoint`
   * up to the minimum finalized checkpoint.
   *
   * Note: `syncStore.getEvents` is used to order between multiple
   * networks. This approach is not future proof.
   */
  async function* getEventsOmni() {
    const start = getChainsCheckpoint("start");
    const end = getChainsCheckpoint("finalized");

    // ...
    let from = start;

    while (true) {
      const _localSyncs = args.networks.map(
        ({ chain }) => localSyncs.get(chain)!,
      );
      // sync each chain
      await Promise.all(_localSyncs.map((l) => l.sync()));
      // TODO(kyle) check against endBlocks
      if (_localSyncs.some((l) => l.latestBlock === undefined)) continue;
      const to = getChainsCheckpoint("latest");

      // TODO(kyle) may be more performant to self-limit `before`
      while (true) {
        if (from === to) break;
        const { events, cursor } = await args.syncStore.getEvents({
          filters: args.sources.map(({ filter }) => filter),
          from,
          to,
          limit: 1_000,
        });

        yield events;
        from = cursor;
      }
      if (to >= end) break;
    }
  }

  /**
   * Multichain `getEvents`
   *
   * Retrieve all events up to the finalized checkpoint for each
   * network ordered by `checkpoint`.
   */
  // async function* getEventsMulti()

  return {
    getEvents: getEventsOmni,
  };
};
