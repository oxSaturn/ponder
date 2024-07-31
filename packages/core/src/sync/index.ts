import type { Common } from "@/common/common.js";
import type { Network } from "@/config/networks.js";
import {
  type RealtimeSyncService,
  createRealtimeSyncService,
} from "@/sync-realtime/index.js";
import type { RealtimeSyncEvent } from "@/sync-realtime/service.js";
import type { SyncStore } from "@/sync-store/index.js";
import {
  checkpointMin,
  encodeCheckpoint,
  maxCheckpoint,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import type { Interval } from "@/utils/interval.js";
import { never } from "@/utils/never.js";
import { type Transport, hexToBigInt, hexToNumber } from "viem";
import { _eth_getBlockByNumber } from "../utils/rpc.js";
import type { RawEvent } from "./events.js";
import { type LocalSync, createLocalSync } from "./local.js";
import type { Source } from "./source.js";
import { cachedTransport } from "./transport.js";

export type Sync = {
  getEvents(): AsyncGenerator<RawEvent[]>;
  startRealtime(): void;
  getCachedTransport(network: Network): Transport;
  kill(): Promise<void>;
};

export type RealtimeEvent =
  | {
      type: "block";
      events: RawEvent[];
    }
  | {
      type: "reorg";
      checkpoint: string;
    }
  | {
      type: "finalize";
      checkpoint: string;
    };

type CreateSyncParameters = {
  common: Common;
  syncStore: SyncStore;
  sources: Source[];
  networks: Network[];
  onRealtimeEvent(event: RealtimeEvent): void;
  onFatalError(error: Error): void;
};

export const createSync = async (args: CreateSyncParameters): Promise<Sync> => {
  // Network-specific syncs
  const localSyncs = new Map<Network, LocalSync>();
  const realtimeSyncs = new Map<Network, RealtimeSyncService>();

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
   * Note: `localSync.latestBlock` is assumed to be defined if
   * this function is called with `tag`: "latest".
   */
  const getChainsCheckpoint = (
    tag: "start" | "latest" | "finalized",
  ): string => {
    const checkpoints = args.networks.map((network) => {
      const localSync = localSyncs.get(network)!;
      const block = localSync[`${tag}Block`]!;

      return {
        // The checkpoint returned by this function is meant to be used in
        // a closed interval (includes endpoints), so "start" should be inclusive.
        ...(tag === "start" ? zeroCheckpoint : maxCheckpoint),
        blockTimestamp: hexToNumber(block.timestamp),
        chainId: BigInt(network.chain.id),
        blockNumber: hexToBigInt(block.number),
      };
    });
    return encodeCheckpoint(checkpointMin(...checkpoints));
  };

  /**
   * Omnichain `getEvents`
   *
   * Extract all events across `args.networks` ordered by checkpoint.
   * The generator is "completed" when all event have been extracted
   * before the minimum finalized checkpoint (supremum).
   *
   * Note: `syncStore.getEvents` is used to order between multiple
   * networks. This approach is not future proof.
   *
   * TODO(kyle) programmatically refetch finalized blocks to avoid exiting too early
   */

  async function* getEventsOmni() {
    const start = getChainsCheckpoint("start");
    const end = getChainsCheckpoint("finalized");

    // Cursor used to track progress.
    let from = start;

    while (true) {
      const _localSyncs = args.networks.map(
        (network) => localSyncs.get(network)!,
      );
      // Sync the next interval of each chain.
      await Promise.all(_localSyncs.map((l) => l.sync()));
      /**
       * `latestBlock` is used to calculate the `to` checkpoint, if any
       * network hasn't yet ingested a block, run another iteration of this loop.
       * It is an invariant that `latestBlock` will eventually be defined. See the
       * implementation of `latestBlock` for more detail.
       */
      if (_localSyncs.some((l) => l.latestBlock === undefined)) continue;
      // Calculate the mininum "latest" checkpoint.
      const to = getChainsCheckpoint("latest");

      /*
       * Extract events with `syncStore.getEvents()`, paginating to
       * avoid loading too many events into memory.
       */
      while (true) {
        if (from === to) break;
        // TODO(kyle) may be more performant to self-limit `to`
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
    }
  }

  /**
   * Omnichain `onRealtimeSyncEvent`
   *
   * Handle callback events across all `args.networks`, and raising these
   * events to `args.onRealtimeEvent` while maintaining checkpoint ordering.
   *
   * Note: "block" events are still being handled by writing and reading from
   * the sync-store. This approach is not future proof and inefficient.
   *
   * TODO(kyle) is async bad?
   * TODO(kyle) handle errors
   */
  const onRealtimeSyncEventOmni =
    (network: Network) => async (event: RealtimeSyncEvent) => {
      const localSync = localSyncs.get(network)!;
      const realtimeSync = realtimeSyncs.get(network)!;
      switch (event.type) {
        /**
         * Handle a new block being ingested.
         */
        case "block":
          {
            /**
             * Newly ingested range.
             *
             * Note: It is an invariant that "block" events are emitted
             * in a continuous order.
             */
            const interval = [
              hexToNumber(event.block.number),
              hexToNumber(event.block.number),
            ] satisfies Interval;

            const filters = args.sources
              .filter(({ filter }) => filter.chainId === network.chainId)
              .map(({ filter }) => filter);

            // Add newly synced data to the "event" table.
            await Promise.all(
              filters.map((filter) =>
                args.syncStore.populateEvents({ filter, interval }),
              ),
            );

            // Update local sync, record checkpoint before and after
            let from = getChainsCheckpoint("latest");
            localSync.latestBlock = event.block;
            const to = getChainsCheckpoint("latest");

            /*
             * Extract events with `syncStore.getEvents()`, paginating to
             * avoid loading too many events into memory.
             */
            while (true) {
              if (from === to) break;
              const { events, cursor } = await args.syncStore.getEvents({
                filters,
                from,
                to,
                limit: 10_000,
              });
              args.onRealtimeEvent({ type: "block", events });
              from = cursor;
            }
          }
          break;
        /**
         * Handle a new block being finalized.
         */
        case "finalize":
          {
            // Newly finalized range
            const interval = [
              hexToNumber(localSync.finalizedBlock.number),
              hexToNumber(event.block.number),
            ] satisfies Interval;

            // Update local sync, record checkpoint before and after
            const prev = getChainsCheckpoint("finalized");
            localSync.finalizedBlock = event.block;
            const checkpoint = getChainsCheckpoint("finalized");

            const filters = args.sources
              .filter(({ filter }) => filter.chainId === network.chainId)
              .map(({ filter }) => filter);
            // Insert an interval for the newly finalized range.
            await Promise.all(
              filters.map((filter) =>
                args.syncStore.insertInterval({
                  filterType: "event",
                  filter,
                  interval,
                }),
              ),
            );

            // Raise event to parent function (runtime)
            if (checkpoint > prev) {
              args.onRealtimeEvent({ type: "finalize", checkpoint });
            }

            /**
             * The realtime service can be killed if `endBlock` is
             * defined has become finalized.
             */
            if (localSync.isComplete()) {
              args.common.logger.info({
                service: "sync",
                msg: `Synced final end block for '${network.name}' (${hexToNumber(localSync.endBlock!.number)}), killing realtime sync service`,
              });
              await realtimeSync.kill();
              // Delete syncs to remove `network` from checkpoint calculations
              localSyncs.delete(network);
              realtimeSyncs.delete(network);
            }
          }
          break;
        /**
         * Handle a reorg with a new common ancestor block being found.
         */
        case "reorg":
          {
            // Update local sync
            localSync.latestBlock = event.block;
            const checkpoint = getChainsCheckpoint("latest");

            // await args.syncStore.pruneByBlock();

            args.onRealtimeEvent({ type: "reorg", checkpoint });
          }
          break;

        default:
          never(event);
      }
    };

  return {
    getEvents: getEventsOmni,
    startRealtime() {
      for (const network of args.networks) {
        const localSync = localSyncs.get(network)!;

        // A `network` doesn't need a realtime sync if `endBlock` is finalized
        if (localSync.isComplete()) {
          // Delete sync to remove from checkpoint calculations
          localSyncs.delete(network);
        } else {
          // Create and start realtime sync
          const realtimeSync = createRealtimeSyncService({
            common: args.common,
            network,
            requestQueue: localSync.requestQueue,
            sources: args.sources.filter(
              ({ filter }) => filter.chainId === network.chainId,
            ),
            finalizedBlock: localSync.finalizedBlock,
            onEvent: onRealtimeSyncEventOmni(network),
            onFatalError: args.onFatalError,
          });
          realtimeSync.start();
          realtimeSyncs.set(network, realtimeSync);
        }
      }
    },
    getCachedTransport(network) {
      const { requestQueue } = localSyncs.get(network)!;
      return cachedTransport({ requestQueue, syncStore: args.syncStore });
    },
    async kill() {
      const promises: Promise<void>[] = [];
      for (const network of args.networks) {
        /**
         * Some or all networks may be undefined, depending
         * on progress and `endBlock` configuration.
         */
        localSyncs.get(network)?.kill();
        const realtimeSync = realtimeSyncs.get(network);
        if (realtimeSync) promises.push(realtimeSync.kill());
      }
      await Promise.all(promises);
    },
  };
};
