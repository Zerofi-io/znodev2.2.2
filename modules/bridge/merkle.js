import { ethers } from 'ethers';

function hashLeaf(clusterId, depositId, recipient, amount) {
  return ethers.keccak256(
    ethers.solidityPacked(
      ['string', 'bytes32', 'bytes32', 'address', 'uint256'],
      ['znode-batch-v1', clusterId, depositId, recipient, BigInt(amount)],
    ),
  );
}

function hashPair(a, b) {
  const [left, right] = BigInt(a) <= BigInt(b) ? [a, b] : [b, a];
  return ethers.keccak256(ethers.solidityPacked(['bytes32', 'bytes32'], [left, right]));
}

function buildTree(deposits, clusterId) {
  if (!Array.isArray(deposits) || deposits.length === 0) return null;
  const leaves = deposits.map((d) => ({
    depositId: d.depositId,
    recipient: d.recipient,
    amount: d.amount,
    hash: hashLeaf(clusterId, d.depositId, d.recipient, d.amount),
  }));
  leaves.sort((a, b) => {
    const ha = BigInt(a.hash);
    const hb = BigInt(b.hash);
    if (ha < hb) return -1;
    if (ha > hb) return 1;
    return String(a.depositId).localeCompare(String(b.depositId));
  });
  const leafHashes = leaves.map((l) => l.hash);
  const layers = [leafHashes];
  let current = leafHashes;
  while (current.length > 1) {
    const next = [];
    for (let i = 0; i < current.length; i += 2) {
      if (i + 1 < current.length) {
        next.push(hashPair(current[i], current[i + 1]));
      } else {
        next.push(current[i]);
      }
    }
    layers.push(next);
    current = next;
  }
  return { root: current[0], leaves, layers };
}

function getProof(tree, depositId) {
  if (!tree || !Array.isArray(tree.leaves) || !Array.isArray(tree.layers)) return null;
  const leafIndex = tree.leaves.findIndex((l) => l.depositId === depositId);
  if (leafIndex === -1) return null;
  const proof = [];
  let idx = leafIndex;
  for (let i = 0; i < tree.layers.length - 1; i++) {
    const layer = tree.layers[i];
    const siblingIdx = idx % 2 === 1 ? idx - 1 : idx + 1;
    if (siblingIdx < layer.length) proof.push(layer[siblingIdx]);
    idx = Math.floor(idx / 2);
  }
  return proof;
}

function verifyProof(proof, root, leaf) {
  if (!Array.isArray(proof) || !root || !leaf) return false;
  let hash = leaf;
  for (const sibling of proof) {
    hash = hashPair(hash, sibling);
  }
  return hash === root;
}

function computeLeafHash(clusterId, depositId, recipient, amount) {
  return hashLeaf(clusterId, depositId, recipient, amount);
}

export { buildTree, getProof, verifyProof, computeLeafHash, hashPair };
