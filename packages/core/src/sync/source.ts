import type { Abi } from "viem";
import type { BlockFilter, LogFilter } from "./filter.js";

type ContractMetadata = {
  type: "contract";
  abi: Abi;
  name: string;
  networkName: string;
};
type BlockMetadata = {
  type: "block";
  name: string;
  networkName: string;
};

export type ContractSource = { filter: LogFilter } & ContractMetadata;
export type BlockSource = { filter: BlockFilter } & BlockMetadata;
export type Source = ContractSource | BlockSource;
