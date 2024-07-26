import type { Filter } from "./filter.js";

type ContractMetadata = any;

type BlockMetadata = any;

// TODO(kyle) sync metadata: local or remote

export type Source = { filter: Filter };
