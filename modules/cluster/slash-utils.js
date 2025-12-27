import { ethers } from 'ethers';
export function currentSlashEpoch(blockTimestamp, epochSec) {
  const ts =
    typeof blockTimestamp === 'bigint' ? Number(blockTimestamp) : Number(blockTimestamp || 0);
  if (!Number.isFinite(ts) || ts <= 0) return 0;
  return Math.floor(ts / epochSec);
}
function deduplicateAndCanonicalizeAddresses(addresses) {
  if (!Array.isArray(addresses) || addresses.length === 0) return [];
  const seen = new Set();
  const unique = [];
  for (const addr of addresses) {
    if (!addr) continue;
    const lower = addr.toLowerCase();
    if (!seen.has(lower)) {
      seen.add(lower);
      unique.push(lower);
    }
  }
  unique.sort((a, b) => a.localeCompare(b));
  return unique;
}
export function selectSlashLeader(activeNodes, targetNode, epoch, salt) {
  const canonicalNodes = deduplicateAndCanonicalizeAddresses(activeNodes);
  if (canonicalNodes.length === 0) return null;
  const seed = ethers.keccak256(
    ethers.solidityPacked(['address', 'uint256', 'bytes32'], [targetNode, epoch, salt]),
  );
  const asBigInt = BigInt(seed);
  const idx = Number(asBigInt % BigInt(canonicalNodes.length));
  return canonicalNodes[idx];
}
