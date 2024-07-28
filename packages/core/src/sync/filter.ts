import type { Address, Hex, LogTopic } from "viem";

// TODO(kyle) includeTransactionReceipt

export type LogFilter = {
  type: "log";
  chainId: number;
  address?: Address | Address[] | AddressFilter;
  topics?: LogTopic[];
  fromBlock?: number;
  toBlock?: number;
};

export type BlockFilter = {
  type: "block";
  chainId: number;
  interval?: number;
  offset?: number;
  fromBlock?: number;
  toBlock?: number;
};

export type LogAddressFilter = {
  type: "log";
  chainId: number;
  address: Address | Address[] | AddressFilter;
  eventSelector: Hex;
  childAddressLocation: "topic1" | "topic2" | "topic3" | `offset${number}`;
};

export type Filter = LogFilter | BlockFilter;

export type AddressFilter = LogAddressFilter;

// TODO(kyle) normalize filter before stringify
export const getFilterId = <type extends "event" | "address">(
  type: type,
  filter: type extends "address" ? AddressFilter : Filter,
) => `${type}_${JSON.stringify(filter)}`;

// export const isLogFilter = (filter: Filter): filter is LogFilter =>
//   filter.type === "log";

// export const isBlockFilter = (filter: Filter): filter is BlockFilter =>
//   filter.type === "block";
