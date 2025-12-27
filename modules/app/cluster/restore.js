import fs from 'fs';
import { ethers } from 'ethers';
import { getMoneroWalletDir } from '../monero/wallet-dir.js';
export async function verifyAndRestoreCluster(node) {
  const statePath = node._getClusterStatePath();
  if (!fs.existsSync(statePath)) {
    return await recoverClusterFromChain(node);
  }
  let state;
  try {
    state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
  } catch (e) {
    console.log('[Cluster] Failed to load cluster state:', e.message || String(e));
    return false;
  }
  if (!state.clusterId || !state.members || !state.finalAddress) {
    console.log('[Cluster] Invalid cluster state file');
    node._clearClusterState();
    return false;
  }
  node._sessionId = 'bridge';
  let effectiveClusterId = state.clusterId;
  let onChainAddress;
  let finalized;
  let onChainMembers;
  try {
    try {
      [onChainAddress, , finalized] = await node.registry.clusters(effectiveClusterId);
      onChainMembers = await node.registry.getClusterMembers(effectiveClusterId);
    } catch (primaryErr) {
      try {
        const members = (state.members || []).map((a) => ethers.getAddress(a));
        const sortedMembers = [...members].sort((a, b) => {
          const la = a.toLowerCase();
          const lb = b.toLowerCase();
          if (la < lb) return -1;
          if (la > lb) return 1;
          return 0;
        });
        const canonicalId = ethers.solidityPackedKeccak256(['address[]'], [sortedMembers]);
        if (canonicalId && canonicalId !== effectiveClusterId) {
          const [addr2, , finalized2] = await node.registry.clusters(canonicalId);
          const members2 = await node.registry.getClusterMembers(canonicalId);
          if (finalized2 && addr2 && addr2 === state.finalAddress) {
            console.log('[Cluster] Migrating stored clusterId to canonical on-chain id');
            effectiveClusterId = canonicalId;
            onChainAddress = addr2;
            finalized = finalized2;
            onChainMembers = members2;
          } else {
            throw primaryErr;
          }
        } else {
          throw primaryErr;
        }
      } catch {
        throw primaryErr;
      }
    }
    if (!finalized) {
      console.log('[Cluster] Saved cluster is not finalized on-chain, clearing state');
      node._saveClusterRestoreSnapshot(state);
      node._clearClusterState();
      return false;
    }
    if (!onChainAddress || onChainAddress !== state.finalAddress) {
      console.log('[Cluster] Cluster address mismatch, clearing state');
      node._saveClusterRestoreSnapshot(state);
      node._clearClusterState();
      return false;
    }
    const myAddress = node.wallet.address.toLowerCase();
    const memberAddresses = (onChainMembers || []).map((a) => a.toLowerCase());
    if (!memberAddresses.includes(myAddress)) {
      console.log('[Cluster] Not a member of saved cluster, clearing state');
      node._saveClusterRestoreSnapshot(state);
      node._clearClusterState();
      return false;
    }
    const effectiveMembers =
      Array.isArray(onChainMembers) && onChainMembers.length ? onChainMembers : state.members;
    node.clusterWalletName = state.walletName;
    if (node.clusterState && typeof node.clusterState.update === 'function') {
      node.clusterState.update({
        clusterId: effectiveClusterId,
        members: effectiveMembers,
        finalAddress: onChainAddress,
        finalized: true,
      });
    }
    if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
      node.p2p.setActiveCluster(effectiveClusterId);
    }
    await node._ensureBridgeSessionCluster();
    console.log('[Cluster] Restored finalized cluster from on-chain state');
    console.log('  ClusterId: ' + effectiveClusterId.slice(0, 18) + '...');
    console.log('  Monero address: ' + onChainAddress.slice(0, 24) + '...');
    console.log('  Members: ' + effectiveMembers.length);
    try {
      node.startDepositMonitor();
    } catch (e) {
      console.log('[Bridge] Failed to start deposit monitor:', e.message || String(e));
    }
    try {
      node.startBridgeAPI();
    } catch (e) {
      console.log('[BridgeAPI] Failed to start API server:', e.message || String(e));
    }
    try {
      node.startWithdrawalMonitor();
    } catch (e) {
      console.log('[Withdrawal] Failed to start withdrawal monitor:', e.message || String(e));
    }
    return true;
  } catch (e) {
    console.log('[Cluster] Failed to verify cluster on-chain:', e.message || String(e));
    console.log('[Cluster] Trusting local state file (RPC unavailable)');
    node.clusterWalletName = state.walletName;
    if (node.clusterState && typeof node.clusterState.update === 'function') {
      node.clusterState.update({
        clusterId: state.clusterId,
        members: state.members,
        finalAddress: state.finalAddress,
        finalized: true,
      });
    }
    if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
      node.p2p.setActiveCluster(state.clusterId);
    }
    await node._ensureBridgeSessionCluster();
    console.log('[Cluster] Restored from local state (unverified)');
    try {
      node.startDepositMonitor();
    } catch (e) {
      console.log('[Bridge] Failed to start deposit monitor:', e.message || String(e));
    }
    try {
      node.startBridgeAPI();
    } catch (e) {
      console.log('[BridgeAPI] Failed to start API server:', e.message || String(e));
    }
    try {
      node.startWithdrawalMonitor();
    } catch (e) {
      console.log('[Withdrawal] Failed to start withdrawal monitor:', e.message || String(e));
    }
    return true;
  }
}
export async function recoverClusterFromChain(node) {
  try {
    console.log('[Cluster] No state file found, checking on-chain for existing cluster membership...');
    const myAddress = node.wallet.address;
    const clusterId = await node.registry.nodeToCluster(myAddress);
    if (!clusterId || clusterId === '0x0000000000000000000000000000000000000000000000000000000000000000') {
      console.log('[Cluster] Node is not in any cluster on-chain');
      return false;
    }
    console.log('[Cluster] Found on-chain cluster:', clusterId.slice(0, 18) + '...');
    const isActive = await node.registry.isClusterActive(clusterId);
    if (!isActive) {
      console.log('[Cluster] On-chain cluster is not active');
      return false;
    }
    const members = await node.registry.getClusterMembers(clusterId);
    const [moneroAddress, , finalized] = await node.registry.clusters(clusterId);
    if (!finalized) {
      console.log('[Cluster] On-chain cluster is not finalized');
      return false;
    }
    const walletDir = getMoneroWalletDir();
    const shortEth = myAddress.slice(2, 10);
    let walletName = null;
    try {
      const files = fs.readdirSync(walletDir);
      const pattern = new RegExp(`^znode_att_${shortEth}_[a-fA-F0-9]{8}_\\d{2}\\.keys$`, 'i');
      for (const file of files) {
        if (pattern.test(file)) {
          walletName = file.replace('.keys', '');
          break;
        }
      }
    } catch (e) {
      console.log('[Cluster] Could not scan wallet directory:', e.message || String(e));
    }
    if (!walletName) {
      console.log('[Cluster] No matching wallet file found for recovery');
      return false;
    }
    node.clusterWalletName = walletName;
    if (node.clusterState && typeof node.clusterState.update === 'function') {
      node.clusterState.update({
        clusterId,
        members: Array.from(members),
        finalAddress: moneroAddress,
        finalized: true,
      });
    }
    if (node.p2p && typeof node.p2p.setActiveCluster === 'function') {
      node.p2p.setActiveCluster(clusterId);
    }
    await node._ensureBridgeSessionCluster();
    node._saveClusterState();
    console.log('[Cluster] Recovered finalized cluster from on-chain data');
    console.log('  ClusterId: ' + clusterId.slice(0, 18) + '...');
    console.log('  Monero address: ' + moneroAddress.slice(0, 24) + '...');
    console.log('  Members: ' + members.length);
    console.log('  Wallet: ' + walletName);
    return true;
  } catch (e) {
    console.log('[Cluster] Failed to recover cluster from chain:', e.message || String(e));
    return false;
  }
}
export async function waitForOnChainFinalizationForSelf(node, expectedMoneroAddress, fallbackMembers) {
  if (!node.registry || !node.wallet) {
    return false;
  }
  const timeoutRaw = process.env.FINALIZE_ONCHAIN_TIMEOUT_MS;
  const timeoutParsed = timeoutRaw != null ? Number(timeoutRaw) : NaN;
  const timeoutMs =
    Number.isFinite(timeoutParsed) && timeoutParsed > 0 ? timeoutParsed : 5 * 60 * 1000;
  const pollRaw = process.env.FINALIZE_ONCHAIN_POLL_MS;
  const pollParsed = pollRaw != null ? Number(pollRaw) : NaN;
  const pollMs = Number.isFinite(pollParsed) && pollParsed > 0 ? pollParsed : 10 * 1000;
  const selfAddress = node.wallet.address;
  const zeroCluster =
    '0x0000000000000000000000000000000000000000000000000000000000000000';
  console.log('[Cluster] Waiting for on-chain finalization (direct poll)');
  const startedAt = Date.now();
  const jitterMs = Math.floor(Math.random() * 5000);
  await new Promise((r) => setTimeout(r, jitterMs));
  while (Date.now() - startedAt < timeoutMs) {
    try {
      if (node.p2p && node._activeClusterId) {
        const sessionIds = [];
        if (node._sessionId) sessionIds.push(node._sessionId);
        sessionIds.push(null);
        for (const sid of sessionIds) {
          for (const round of [9998, 9999]) {
            const r = await node.p2p.getRoundData(node._activeClusterId, sid, round);
            for (const payload of Object.values(r || {})) {
              try {
                const data = JSON.parse(payload);
                if (data.type === 'cluster-finalized' && data.clusterId && data.moneroAddress) {
                  if (!expectedMoneroAddress || data.moneroAddress === expectedMoneroAddress) {
                    console.log('[Cluster] Finalization confirmed via P2P broadcast');
                    node._onClusterFinalized(
                      data.clusterId,
                      Array.isArray(data.members) && data.members.length ? data.members : fallbackMembers,
                      data.moneroAddress,
                    );
                    return true;
                  }
                }
              } catch {}
            }
          }
        }
      }
    } catch {}
    try {
      const clusterId = await node.registry.nodeToCluster(selfAddress);
      if (clusterId && clusterId !== zeroCluster) {
        const [moneroAddress, , finalized] = await node.registry.clusters(clusterId);
        if (finalized && moneroAddress) {
          if (expectedMoneroAddress && moneroAddress !== expectedMoneroAddress) {
            console.log('[Cluster] On-chain cluster finalized but Monero address mismatch; continuing to poll');
          } else {
            let membersOnChain = null;
            try {
              membersOnChain = await node.registry.getClusterMembers(clusterId);
            } catch {}
            console.log('[Cluster] Finalization confirmed on-chain (direct poll)');
            node._onClusterFinalized(
              clusterId,
              Array.isArray(membersOnChain) && membersOnChain.length ? membersOnChain : fallbackMembers,
              moneroAddress,
            );
            return true;
          }
        }
      }
    } catch (e) {
      console.log('[Cluster] Finalization poll error:', e.message || String(e));
    }
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
  console.log('[Cluster] Finalization not detected on-chain within timeout window');
  return false;
}
export default {
  verifyAndRestoreCluster,
  recoverClusterFromChain,
  waitForOnChainFinalizationForSelf,
};
