import type {
  SyncBlock,
  SyncLog,
  SyncTransaction,
  SyncTransactionReceipt,
} from "@/types/sync.js";
import { encodeAsText } from "@/utils/encoding.js";
import { toLowerCase } from "@/utils/lowercase.js";
import { replaceBigInts } from "@ponder/utils";
import type { Generated, Insertable } from "kysely";
import type { Address, Hash, Hex } from "viem";
import {
  formatBlock,
  formatLog,
  formatTransaction,
  formatTransactionReceipt,
  hexToBigInt,
  hexToNumber,
} from "viem";

type BlockTable = {
  hash: Hash;
  chain_id: number;
  number: string | bigint;
  timestamp: string | bigint;
  body: string;
};

const formatHex = (sql: "sqlite" | "postgres", hex: Hex) =>
  sql === "sqlite" ? encodeAsText(hex) : hexToBigInt(hex);

const encodeBody = (body: unknown) =>
  JSON.stringify(replaceBigInts(body, (b) => `#bigint.${String(b)}`));

export const encodeBlock = (
  block: SyncBlock,
  chainId: number,
  sql: "sqlite" | "postgres",
): Insertable<BlockTable> => {
  return {
    hash: block.hash,
    chain_id: chainId,
    number: formatHex(sql, block.number),
    timestamp: formatHex(sql, block.timestamp),
    body: encodeBody(formatBlock(block)),
  };
};

type LogTable = {
  block_hash: Hash;
  log_index: number;
  chain_id: number;
  block_number: string | bigint;
  address: Address;
  topic0: Hex | null;
  topic1: Hex | null;
  topic2: Hex | null;
  topic3: Hex | null;
  data: Hex;
  transaction_hash: Hash;
  body: string;
};

export const encodeLog = (
  log: SyncLog,
  chainId: number,
  sql: "sqlite" | "postgres",
): Insertable<LogTable> => {
  return {
    block_hash: log.blockHash,
    log_index: hexToNumber(log.logIndex),
    chain_id: chainId,
    block_number: formatHex(sql, log.blockNumber),
    address: toLowerCase(log.address),
    topic0: log.topics[0] ?? null,
    topic1: log.topics[1] ?? null,
    topic2: log.topics[2] ?? null,
    topic3: log.topics[3] ?? null,
    data: log.data,
    transaction_hash: log.transactionHash,
    body: encodeBody(formatLog(log)),
  };
};

type TransactionTable = {
  hash: Hash;
  chain_id: number;
  block_number: string | bigint;
  transaction_index: number;
  body: string;
};

export const encodeTransaction = (
  transaction: SyncTransaction,
  chainId: number,
  sql: "sqlite" | "postgres",
): Insertable<TransactionTable> => {
  return {
    hash: transaction.hash,
    chain_id: chainId,
    block_number: formatHex(sql, transaction.blockNumber),
    transaction_index: hexToNumber(transaction.transactionIndex),
    body: encodeBody(formatTransaction(transaction)),
  };
};

type TransactionReceiptTable = {
  hash: Hash;
  chain_id: number;
  block_number: string | bigint;
  body: string;
};

export const encodeTransactionReceipt = (
  transactionReceipt: SyncTransactionReceipt,
  chainId: number,
  sql: "sqlite" | "postgres",
): Insertable<TransactionReceiptTable> => {
  return {
    hash: transactionReceipt.transactionHash,
    chain_id: chainId,
    block_number: formatHex(sql, transactionReceipt.blockNumber),
    body: encodeBody(formatTransactionReceipt(transactionReceipt)),
  };
};

type AddressTable = {
  id: Generated<number>;
  chain_id: number;
  filter_id: string;
  block_number: string | bigint;
  address: Address;
};

type IntervalTable = {
  id: Generated<number>;
  chain_id: number;
  filter_id: string;
  from: string | bigint;
  to: string | bigint;
};

type EventTable = {
  filter_id: string;
  checkpoint: string;
  data: string | null;
  chain_id: number;
  block_number: string | bigint;
  block_hash: Hash;
  log_index: number;
  transaction_hash: Hash;
};

type RpcRequestResultsTable = {
  request: string;
  chainId: number;
  blockNumber: string | bigint;
  result: string;
};

export type PonderSyncSchema = {
  block: BlockTable;
  log: LogTable;
  transaction: TransactionTable;
  // TODO(kyle) call_trace
  transaction_receipt: TransactionReceiptTable;
  address: AddressTable;
  interval: IntervalTable;
  event: EventTable;
  rpcRequestResults: RpcRequestResultsTable;
};
