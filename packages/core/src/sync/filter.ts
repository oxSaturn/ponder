import type { Address, Hex, LogTopic } from "viem";

// TODO(kyle) includeTransactionReceipt

export type LogFilter = {
  type: "log";
  chainId: number;
  address?: Address | Address[] | AddressFilter;
  topics?: LogTopic[];
  fromBlock: number;
  toBlock?: number;
};

export type BlockFilter = {
  type: "block";
  chainId: number;
  interval: number;
  offset: number;
  fromBlock: number;
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

/** Returns true if `address` is an address filter. */
export const isAddressFilter = (
  address: (LogFilter | LogAddressFilter)["address"],
): address is LogAddressFilter => {
  if (address === undefined) return false;
  return typeof address !== "string" && Array.isArray(address) === false;
};
