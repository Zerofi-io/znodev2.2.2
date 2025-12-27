import { ethers } from 'ethers';
export function rankCandidatesByEpochSeed(candidatesLower, epochSeed) {
  const seed = epochSeed || ethers.ZeroHash;
  if (!Array.isArray(candidatesLower) || candidatesLower.length === 0) return [];
  const uniq = new Set();
  const normalized = [];
  for (const a of candidatesLower) {
    if (!a) continue;
    try {
      const canonical = ethers.getAddress(a);
      const lower = canonical.toLowerCase();
      if (uniq.has(lower)) continue;
      uniq.add(lower);
      normalized.push(lower);
    } catch {}
  }
  const scored = normalized.map((lower) => ({
    lower,
    score: ethers.keccak256(ethers.solidityPacked(['bytes32', 'address'], [seed, lower])),
  }));
  scored.sort((a, b) => {
    const c = a.score.localeCompare(b.score);
    if (c) return c;
    return a.lower.localeCompare(b.lower);
  });
  return scored.map((x) => x.lower);
}
export function computeDeterministicAssignment({ candidatesLower, epochSeed, selfLower, clusterSize, clusterThreshold }) {
  const sizeRaw = Number(clusterSize);
  const size = Number.isFinite(sizeRaw) && sizeRaw > 0 ? Math.floor(sizeRaw) : 0;
  const thresholdRaw = Number(clusterThreshold);
  const threshold = Number.isFinite(thresholdRaw) && thresholdRaw > 0 ? Math.floor(thresholdRaw) : size;
  const ranked = rankCandidatesByEpochSeed(candidatesLower || [], epochSeed);
  if (!size || ranked.length === 0) {
    return {
      onlineEligibleLower: ranked,
      myIndex: -1,
      groupIndex: -1,
      groupMembersLower: null,
      remainderCount: 0,
    };
  }
  const remainderCount = ranked.length % size;
  const self = selfLower ? String(selfLower).toLowerCase() : '';
  const myIndex = self ? ranked.indexOf(self) : -1;
  if (myIndex < 0) {
    return {
      onlineEligibleLower: ranked,
      myIndex: -1,
      groupIndex: -1,
      groupMembersLower: null,
      remainderCount,
    };
  }
  const groupIndex = Math.floor(myIndex / size);
  const start = groupIndex * size;
  const end = start + size;
  if (end > ranked.length) {
    if (remainderCount >= threshold) {
      const remainderGroup = ranked.slice(start);
      return {
        onlineEligibleLower: ranked,
        myIndex,
        groupIndex,
        groupMembersLower: remainderGroup,
        remainderCount,
      };
    }
    return {
      onlineEligibleLower: ranked,
      myIndex,
      groupIndex,
      groupMembersLower: null,
      remainderCount,
    };
  }
  return {
    onlineEligibleLower: ranked,
    myIndex,
    groupIndex,
    groupMembersLower: ranked.slice(start, end),
    remainderCount,
  };
}
