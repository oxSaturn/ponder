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
import { type Chain, type Transport, hexToBigInt, hexToNumber } from "viem";
import type { RawEvent } from "./events.js";
import { type SyncBlock, _eth_getBlockByNumber } from "./index.js";
import type { Source } from "./source.js";
import { cachedTransport } from "./transport.js";

export type Sync = {
  getEvents(): AsyncGenerator<RawEvent[]>;
  getCachedTransport(network: Network): Transport;
};

type CreateSyncParameters = {
  common: Common;
  syncStore: SyncStore;
  sources: Source[];
  networks: Network[];
};

type LocalSync = {
  startBlock: SyncBlock;
  endBlock: SyncBlock | undefined;
  latestBlock: SyncBlock | undefined;
  finalizedBlock: SyncBlock;
  sync(): Promise<void>;
  requestQueue: RequestQueue;
};

const createLocalSync = async (args: {
  common: Common;
  syncStore: SyncStore;
  sources: Source[];
  network: Network;
}): Promise<LocalSync> => {
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
    _eth_getBlockByNumber({ requestQueue }, { blockNumber: start }),
    end === undefined
      ? undefined
      : _eth_getBlockByNumber({ requestQueue }, { blockNumber: end }),
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
    common: args.common,
    sources: args.sources,
    syncStore: args.syncStore,
    network: args.network,
    requestQueue,
  });
  historicalSync.initializeMetrics(finalizedBlock);

  /** ... */
  // TODO(kyle) dynamic, use diagnostics
  const blocksPerEvent = 0.25 / args.sources.length;
  let fromBlock = hexToNumber(startBlock.number);

  return {
    startBlock,
    endBlock,
    get latestBlock() {
      // ...
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
        sources: args.sources.filter(
          ({ filter }) => filter.chainId === network.chain.id,
        ),
        network,
      });
      localSyncs.set(network.chain, localSync);
    }),
  );

  /**
   * Returns the minimum checkpoint across all chains.
   *
   * Note: `historicalSync.latestBlock` is assumed to be defined if
   * this function is called with `tag`: "latest".
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

    // await args.syncStore
    //   .getEventCount({ filters: args.sources.map(({ filter }) => filter) })
    //   .then(console.log);

    // ...
    let from = start;

    while (true) {
      const _localSyncs = args.networks.map(
        ({ chain }) => localSyncs.get(chain)!,
      );
      await Promise.all(_localSyncs.map((l) => l.sync()));
      if (_localSyncs.some((l) => l.latestBlock === undefined)) continue;
      const to = getChainsCheckpoint("latest");

      // TODO(kyle) may be more performant to self-limit `before`
      while (true) {
        if (from === to) break;
        const { events, cursor } = await args.syncStore.getEvents({
          filters: args.sources.map(({ filter }) => filter),
          from,
          to,
          limit: 10_000,
        });

        yield events;
        from = cursor;
      }
      if (to >= end) break;
      // TODO(kyle) log "finished '[networkName]' historical sync"
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
    getCachedTransport(network) {
      const { requestQueue } = localSyncs.get(network.chain)!;
      return cachedTransport({ requestQueue, syncStore: args.syncStore });
    },
  };
};
