import { type LogFilter, isAddressFilter } from "@/sync/source.js";
import { type Hex, hexToBytes, keccak256 } from "viem";

export const zeroLogsBloom =
  "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

const BLOOM_SIZE_BYTES = 256;

export const isInBloom = (_bloom: Hex, input: Hex): boolean => {
  const bloom = hexToBytes(_bloom);
  const hash = hexToBytes(keccak256(input));

  for (const i of [0, 2, 4]) {
    const bit = (hash[i + 1]! + (hash[i]! << 8)) & 0x7ff;
    if (
      (bloom[BLOOM_SIZE_BYTES - 1 - Math.floor(bit / 8)]! &
        (1 << (bit % 8))) ===
      0
    )
      return false;
  }

  return true;
};

// TODO(kyle) consider block number
export function isFilterInBloom({
  bloom,
  filter,
}: { bloom: Hex; filter: LogFilter }): boolean {
  if (isAddressFilter(filter.address)) {
    return;
  }

  let isAddressInBloom: boolean;
  let isTopicsInBloom: boolean;

  if (filter.address === undefined) isAddressInBloom = true;
  else if (isAddressFilter(filter.address)) {
    isAddressInBloom = isFilterInBloom({ bloom, filter: filter.address });
  } else if (Array.isArray(filter.address)) {
    if (filter.address.length === 0) {
      isAddressInBloom = true;
    } else {
      isAddressInBloom = filter.address.some((address) =>
        isInBloom(bloom, address),
      );
    }
  } else {
    // single address case
    isAddressInBloom = isInBloom(bloom, filter.address);
  }

  if ("eventSelector" in filter) {
  } else {
    if (filter.topics === undefined || filter.topics.length === 0) {
      isTopicsInBloom = true;
    } else {
      isTopicsInBloom = filter.topics.some((topic) => {
        if (topic === null || topic === undefined) {
          return true;
        } else if (Array.isArray(topic)) {
          return topic.some((t) => isInBloom(bloom, t));
        } else {
          return isInBloom(bloom, topic);
        }
      });
    }
  }

  return isAddressInBloom && isTopicsInBloom;
}
