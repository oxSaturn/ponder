import type { Address, Hex, LogTopic } from "viem";

export type LogFilter = {
  type: "log";
  address?: Address | Address[] | AddressFilter;
  topics?: LogTopic[];
  fromBlock?: Hex;
  toBlock?: Hex;
};

export type BlockFilter = {
  type: "block";
  interval?: number;
  offset?: number;
  fromBlock?: Hex;
  toBlock?: Hex;
};

export type Filter = LogFilter | BlockFilter;

type LogAddressFilter = {
  type: "log";
  address: Address | Address[] | AddressFilter;
  eventSelector: Hex;
  childAddressLocation: "topic1" | "topic2" | "topic3" | `offset${number}`;
};

export type AddressFilter = LogAddressFilter;

export const getFilterId = <type extends "raw" | "event" | "address">(
  type: type,
  filter: type extends "address" ? AddressFilter : Filter,
) => `${type}_${JSON.stringify(filter)}`;

export const getAdjacentFilterIds = <type extends "raw" | "event" | "address">(
  type: type,
  filter: type extends "address" ? AddressFilter : Filter,
) => {
  const ids: string[] = [];

  // for () {}

  return ids;
};

export const isLogFilter = (filter: Filter): filter is LogFilter =>
  filter.type === "log";

export const isBlockFilter = (filter: Filter): filter is BlockFilter =>
  filter.type === "block";
