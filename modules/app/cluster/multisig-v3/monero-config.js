import { ethers } from 'ethers';
export async function confirmMoneroConfigConsensus(node, clusterId, attemptScopeId, canonicalMembers, threshold, roundTimeoutPrepare, recordClusterFailure) {
  if (!node.multisigInfo) {
    await node.prepareMultisig();
  }
  let moneroPrimaryAddress = '';
  try {
    moneroPrimaryAddress = await node.monero.getAddress();
  } catch (e) {
    console.log(
      '[ERROR] Failed to get Monero primary address for monero-config:',
      e.message || String(e),
    );
    recordClusterFailure('monero_config_no_address');
    return false;
  }
  try {
    console.log('[MoneroConfig] Local Monero address:', moneroPrimaryAddress);
    console.log(
      '[MoneroConfig] Local wallet for this attempt:',
      node.currentAttemptWallet || node.baseWalletName || '(unset)',
    );
  } catch {}
  const moneroConfigData = JSON.stringify({
    members: canonicalMembers.map((a) => (a || '').toLowerCase()),
    threshold,
  });
  const moneroConfigHash = ethers.id(moneroConfigData);
  console.log('[INFO] PBFT Consensus: confirming monero-config across cluster...');
  let moneroConfigConsensus;
  try {
    moneroConfigConsensus = await node.p2p.runConsensus(
      clusterId,
      attemptScopeId,
      'monero-config',
      moneroConfigHash,
      canonicalMembers,
      roundTimeoutPrepare,
      { requireAll: true },
    );
  } catch (e) {
    console.log('[ERROR] PBFT monero-config consensus error:', e.message || String(e));
    recordClusterFailure('monero_config_pbft_error');
    return false;
  }
  if (!moneroConfigConsensus.success) {
    console.log(
      '[ERROR] PBFT monero-config consensus failed - missing:',
      (moneroConfigConsensus.missing || []).join(', '),
    );
    recordClusterFailure('monero_config_pbft_failed');
    return false;
  }
  console.log('[OK] PBFT monero-config consensus reached');
  return true;
}
