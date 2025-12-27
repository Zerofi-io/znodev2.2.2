import { MoneroHealth } from './health.js';
export async function maybeApplyMoneroHealthOverrideCommand(node) {
  const flag = process.env.MONERO_HEALTH_CLEAR_QUARANTINE;
  if (!flag || flag === '0') {
    return;
  }
  console.log(
    `[MoneroHealth] Clear-quarantine flag detected: MONERO_HEALTH_CLEAR_QUARANTINE=${flag}`,
  );
  const wasQuarantined = node.moneroHealth === MoneroHealth.QUARANTINED;
  node._resetMoneroHealth();
  if (node.moneroErrorCounts) {
    node.moneroErrorCounts.kexRoundMismatch = 0;
    node.moneroErrorCounts.kexTimeout = 0;
    node.moneroErrorCounts.rpcFailure = 0;
  }
  if (wasQuarantined) {
    console.log('[MoneroHealth] Node was quarantined; clearing to HEALTHY via override.');
  } else {
    console.log(`[MoneroHealth] Override applied; health is now ${node.moneroHealth}.`);
  }
  process.env.MONERO_HEALTH_CLEAR_QUARANTINE = '0';
}
