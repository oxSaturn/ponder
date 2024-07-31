import type { IndexingBuild } from "@/build/index.js";
import { runCodegen } from "@/common/codegen.js";
import type { Common } from "@/common/common.js";
import { PostgresDatabaseService } from "@/database/postgres/service.js";
import type { DatabaseService, NamespaceInfo } from "@/database/service.js";
import { SqliteDatabaseService } from "@/database/sqlite/service.js";
import { getHistoricalStore } from "@/indexing-store/historical.js";
import { getMetadataStore } from "@/indexing-store/metadata.js";
import { getReadonlyStore } from "@/indexing-store/readonly.js";
import { getRealtimeStore } from "@/indexing-store/realtime.js";
import type { IndexingStore, Status } from "@/indexing-store/store.js";
import { createIndexingService } from "@/indexing/index.js";
import { createSyncStore } from "@/sync-store/index.js";
import { type Event, decodeEvents } from "@/sync/events.js";
import { type RealtimeEvent, createSync } from "@/sync/index.js";
import {
  type Checkpoint,
  isCheckpointEqual,
  zeroCheckpoint,
} from "@/utils/checkpoint.js";
import { never } from "@/utils/never.js";
import { createQueue } from "@ponder/common";

/**
 * Starts the sync and indexing services for the specified build.
 */
export async function run({
  common,
  build,
  onFatalError,
  onReloadableError,
}: {
  common: Common;
  build: IndexingBuild;
  onFatalError: (error: Error) => void;
  onReloadableError: (error: Error) => void;
}) {
  const {
    buildId,
    databaseConfig,
    optionsConfig,
    networks,
    sources,
    graphqlSchema,
    schema,
    indexingFunctions,
  } = build;

  common.options = { ...common.options, ...optionsConfig };

  let database: DatabaseService;
  let namespaceInfo: NamespaceInfo;
  let initialCheckpoint: Checkpoint;

  const status: Status = {};
  for (const network of networks) {
    status[network.name] = {
      ready: false,
      block: null,
    };
  }

  if (databaseConfig.kind === "sqlite") {
    const { directory } = databaseConfig;
    database = new SqliteDatabaseService({ common, directory });
    [namespaceInfo, initialCheckpoint] = await database
      .setup({ schema, buildId })
      .then(({ namespaceInfo, checkpoint }) => [namespaceInfo, checkpoint]);
  } else {
    const { poolConfig, schema: userNamespace, publishSchema } = databaseConfig;
    database = new PostgresDatabaseService({
      common,
      poolConfig,
      userNamespace,
      publishSchema,
    });
    [namespaceInfo, initialCheckpoint] = await database
      .setup({ schema, buildId })
      .then(({ namespaceInfo, checkpoint }) => [namespaceInfo, checkpoint]);
  }

  const syncStore = createSyncStore({
    common,
    db: database.syncDb,
    sql: database.kind,
  });

  const metadataStore = getMetadataStore({
    encoding: database.kind,
    namespaceInfo,
    db: database.indexingDb,
  });

  // This can be a long-running operation, so it's best to do it after
  // starting the server so the app can become responsive more quickly.
  await database.migrateSyncStore();

  runCodegen({ common, graphqlSchema });

  // Note: can throw
  const sync = await createSync({
    common,
    syncStore,
    networks,
    sources,
    onRealtimeEvent: (realtimeEvent) => {
      realtimeQueue.add(realtimeEvent);
    },
    onFatalError,
    // initialCheckpoint,
  });

  const handleEvents = async (events: Event[]) => {
    if (events.length === 0) return;
    const result = await indexingService.processEvents({ events });
    if (result.status === "error") {
      onReloadableError(result.error);
    }
  };

  const handleReorg = async (checkpoint: string) => {
    await database.revert({ checkpoint, namespaceInfo });
  };

  const handleFinalize = async (checkpoint: string) => {
    await database.updateFinalizedCheckpoint({ checkpoint });
  };

  const realtimeQueue = createQueue({
    initialStart: true,
    browser: false,
    concurrency: 1,
    worker: async (event: RealtimeEvent) => {
      switch (event.type) {
        case "block": {
          // Note: statusBlocks should be assigned before any other
          // asynchronous statements in order to prevent race conditions and
          // ensure its correctness.
          // const statusBlocks = syncService.getStatusBlocks(event.toCheckpoint);

          await handleEvents(decodeEvents({ sources, events: event.events }));

          // set status to most recently processed realtime block or end block
          // for each chain.
          // for (const network of networks) {
          //   if (statusBlocks[network.name] !== undefined) {
          //     status[network.name]!.block = statusBlocks[network.name]!;
          //   }
          // }

          // await metadataStore.setStatus(status);

          break;
        }
        case "reorg":
          await handleReorg(event.checkpoint);
          break;

        case "finalize":
          await handleFinalize(event.checkpoint);
          break;

        default:
          never(event);
      }
    },
  });

  const readonlyStore = getReadonlyStore({
    encoding: database.kind,
    schema,
    namespaceInfo,
    db: database.indexingDb,
    common,
  });

  const historicalStore = getHistoricalStore({
    encoding: database.kind,
    schema,
    readonlyStore,
    namespaceInfo,
    db: database.indexingDb,
    common,
    isCacheExhaustive: isCheckpointEqual(zeroCheckpoint, initialCheckpoint),
  });

  let indexingStore: IndexingStore = historicalStore;

  const indexingService = createIndexingService({
    indexingFunctions,
    common,
    indexingStore,
    sources,
    networks,
    sync,
    schema,
  });

  const start = async () => {
    // If the initial checkpoint is zero, we need to run setup events.
    // if (isCheckpointEqual(initialCheckpoint, zeroCheckpoint)) {
    //   const result = await indexingService.processSetupEvents({
    //     sources,
    //     networks,
    //   });
    //   if (result.status === "killed") {
    //     return;
    //   } else if (result.status === "error") {
    //     onReloadableError(result.error);
    //     return;
    //   }
    // }

    // Run historical indexing until complete.
    for await (const events of sync.getEvents()) {
      await handleEvents(decodeEvents({ sources, events }));
    }

    await historicalStore.flush({ isFullFlush: true });

    // Become healthy
    common.logger.info({
      service: "indexing",
      msg: "Completed historical indexing",
    });

    if (database.kind === "postgres") {
      await database.publish();
    }
    // await handleFinalize(sync.finalizedCheckpoint);

    await database.createIndexes({ schema });

    indexingStore = {
      ...readonlyStore,
      ...getRealtimeStore({
        encoding: database.kind,
        schema,
        namespaceInfo,
        db: database.indexingDb,
        common,
      }),
    };

    indexingService.updateIndexingStore({ indexingStore, schema });

    sync.startRealtime();

    // set status to ready and set blocks to most recently processed
    // or end block
    // const statusBlocks = syncService.getStatusBlocks();
    // for (const network of networks) {
    //   status[network.name] = {
    //     ready: true,
    //     block: statusBlocks[network.name] ?? null,
    //   };
    // }

    await metadataStore.setStatus(status);

    common.logger.info({
      service: "server",
      msg: "Started responding as healthy",
    });
  };

  const startPromise = start();

  return async () => {
    indexingService.kill();
    await sync.kill();
    realtimeQueue.pause();
    realtimeQueue.clear();
    await realtimeQueue.onIdle();
    await startPromise;
    await database.kill();
  };
}
