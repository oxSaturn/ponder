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
  getEvents(sources: Source[]): AsyncGenerator<RawEvent[]>;
};

type CreateSyncParameters = {
  common: Common;
  syncStore: SyncStore;
  sources: Source[];
  networks: Network[];
};

type LocalSync = {
  startBlock: SyncBlock;
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
  const start = Math.min(
    ...args.filters.map((f) => (f.fromBlock ? hexToNumber(f.fromBlock) : 0)),
  );

  const [remoteChainId, startBlock, latestBlock] = await Promise.all([
    requestQueue.request({ method: "eth_chainId" }),
    _eth_getBlockByNumber({ requestQueue }, { blockNumber: start }),
    _eth_getBlockByNumber({ requestQueue }, { blockTag: "latest" }),
  ]);

  const finalizedBlock = await _eth_getBlockByNumber(
    { requestQueue },
    {
      blockNumber:
        hexToNumber(latestBlock.number) - args.network.finalityBlockCount,
    },
  );

  // ...
  if (hexToNumber(remoteChainId) !== args.network.chain.id) {
    args.common.logger.warn({
      service: "sync",
      msg: `Remote chain ID (${remoteChainId}) does not match configured chain ID (${args.network.chainId}) for network "${args.network.name}"`,
    });
  }

  const historicalSync = await createHistoricalSync({
    filters: args.filters,
    syncStore: args.syncStore,
    chain: args.network.chain,
    requestQueue,
  });

  /** ... */
  // TODO(kyle) dynamic, use diagnostics
  const blocksPerEvent = 2 * args.filters.length;
  let fromBlock: number;

  return {
    startBlock,
    finalizedBlock,
    async sync() {
      // TODO(kyle) make sure chain doesn't sync passed finalized
      const interval: Interval = [
        fromBlock,
        fromBlock + blocksPerEvent * 10_000,
      ];
      fromBlock = interval[1];

      await historicalSync.sync(interval);
    },
    requestQueue,
  };
};

export const createSync = async (args: CreateSyncParameters): Promise<Sync> => {
  /** Earliest `startBlock` among all `filters` */
  // const start = Math.min(
  //   ...args.filters.map((f) => (f.fromBlock ? hexToNumber(f.fromBlock) : 0)),
  // );

  // /** `true` if all `filters` have a defined end block */
  // const isEndBlockSet = args.filters.every((f) => f.toBlock !== undefined);
  // /** `true` if all `filters` have a defined end block in the finalized range */
  // const isEndBlockFinalized;

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

  /** Returns the minimum checkpoint across all chains. */
  const getChainsCheckpoint = (tag: "start" | "finalized") => {
    const checkpoints = args.networks.map((network) => {
      const block = localSyncs.get(network.chain)![
        tag === "start" ? "startBlock" : "finalizedBlock"
      ];

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
  // TODO(kyle) different implementations depending on onmichain vs multichain
  async function* getEvents() {
    let after = getChainsCheckpoint("start");

    while (true) {
      // sync each chain
      await Promise.all(
        args.networks.map(({ chain }) => localSyncs.get(chain)!.sync()),
      );

      // TODO(kyle) this should use the latest processed block for each network
      const before = getChainsCheckpoint("finalized");

      // TODO(kyle) may be advantageous to self-limit `before`

      while (true) {
        const { events, cursor } = await args.syncStore.getEvents({
          filters: args.sources.map(({ filter }) => filter),
          after,
          before,
          limit: 10_000,
        });

        yield events;

        if (cursor === before) break;
        after = cursor;
      }
    }
  }

  return {
    getEvents,
  };
};
