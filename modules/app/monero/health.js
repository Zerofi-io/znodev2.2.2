export const MoneroHealth = {
  HEALTHY: 'HEALTHY',
  NEEDS_ATTEMPT_RESET: 'NEEDS_ATTEMPT_RESET',
  NEEDS_GLOBAL_RESET: 'NEEDS_GLOBAL_RESET',
  QUARANTINED: 'QUARANTINED',
};
export function resetMoneroHealth(node) {
  node.moneroHealth = MoneroHealth.HEALTHY;
}
export function ensureMoneroErrorCounts(node) {
  if (!node.moneroErrorCounts) {
    node.moneroErrorCounts = {
      kexRoundMismatch: 0,
      kexTimeout: 0,
      rpcFailure: 0,
    };
  }
}
export function noteMoneroKexRoundMismatch(node, round, msg) {
  ensureMoneroErrorCounts(node);
  node.moneroErrorCounts.kexRoundMismatch =
    (node.moneroErrorCounts.kexRoundMismatch || 0) + 1;
  const count = node.moneroErrorCounts.kexRoundMismatch;
  const threshold = Number(process.env.MONERO_KEX_MISMATCH_QUARANTINE_THRESHOLD || 3);
  console.log(
    `[MoneroHealth] KEX round mismatch in round ${round}; count=${count}/${threshold}: ${msg}`,
  );
  if (node.moneroHealth !== MoneroHealth.QUARANTINED) {
    node.moneroHealth = MoneroHealth.NEEDS_GLOBAL_RESET;
  }
  if (count >= threshold && node.moneroHealth !== MoneroHealth.QUARANTINED) {
    console.log(
      '[MoneroHealth] KEX mismatch threshold exceeded; node will self-quarantine from new cluster attempts.',
    );
    node.moneroHealth = MoneroHealth.QUARANTINED;
  }
}
export function noteMoneroRpcFailure(node, kind, msg) {
  ensureMoneroErrorCounts(node);
  node.moneroErrorCounts.rpcFailure = (node.moneroErrorCounts.rpcFailure || 0) + 1;
  console.log(`[MoneroHealth] Monero RPC failure (${kind}): ${msg}`);
  if (node.moneroHealth === MoneroHealth.HEALTHY) {
    node.moneroHealth = MoneroHealth.NEEDS_ATTEMPT_RESET;
  }
}
export function noteMoneroKexTimeout(node, round) {
  ensureMoneroErrorCounts(node);
  node.moneroErrorCounts.kexTimeout = (node.moneroErrorCounts.kexTimeout || 0) + 1;
  console.log(
    `[MoneroHealth] KEX timeout at round ${round}; count=${node.moneroErrorCounts.kexTimeout}`,
  );
}
export default {
  MoneroHealth,
  resetMoneroHealth,
  ensureMoneroErrorCounts,
  noteMoneroKexRoundMismatch,
  noteMoneroRpcFailure,
  noteMoneroKexTimeout,
};
