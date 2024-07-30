import type { Common } from "@/common/common.js";
import type { Network } from "@/config/networks.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  checkpointMin,
  encodeCheckpoint,
  maxCheckpoint,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import { type Transport, hexToBigInt, hexToNumber } from "viem";
import { _eth_getBlockByNumber } from "../utils/rpc.js";
import type { RawEvent } from "./events.js";
import { type LocalSync, createLocalSync } from "./local.js";
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

export const createSync = async (args: CreateSyncParameters): Promise<Sync> => {
  // Network-specific syncs
  const localSyncs = new Map<Network, LocalSync>();

  // Create a `LocalSync` for each network, populating `localSyncs`.
  await Promise.all(
    args.networks.map(async (network) => {
      const localSync = await createLocalSync({
        common: args.common,
        syncStore: args.syncStore,
        sources: args.sources.filter(
          ({ filter }) => filter.chainId === network.chainId,
        ),
        network,
      });
      localSyncs.set(network, localSync);
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
      const localSync = localSyncs.get(network)!;
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
   * Extract all events across `args.networks` ordered by checkpoint.
   * The generator is "completed" when all event have been extracted
   * before the minimum finalized checkpoint.
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
        (network) => localSyncs.get(network)!,
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
      const { requestQueue } = localSyncs.get(network)!;
      return cachedTransport({ requestQueue, syncStore: args.syncStore });
    },
  };
};
