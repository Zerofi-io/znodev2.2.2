import { computeRoundDigest } from '../../cluster/round-utils.js';

const ZERO_BYTES32 = '0x' + '0'.repeat(64);

function isBytes32Hex(value) {
  return typeof value === 'string' && /^0x[0-9a-fA-F]{64}$/.test(value);
}

export async function submitRoundExchangeInfo(
  node,
  clusterId,
  round,
  clusterNodes,
  payloads,
  dryRun,
) {
  if (!node.exchangeCoordinator) return true;
  if (dryRun) {
    console.log(`[RoundBarrier] [DRY_RUN] Skipping submitExchangeInfo for round ${round}`);
    return true;
  }
  const digest = computeRoundDigest(payloads);
  if (!isBytes32Hex(digest) || digest === ZERO_BYTES32) {
    console.log(`[RoundBarrier] Invalid digest for round ${round}`);
    return false;
  }
  const addr = node.wallet && node.wallet.address ? String(node.wallet.address) : '';
  if (!addr) {
    console.log(`[RoundBarrier] Missing wallet address for round ${round}`);
    return false;
  }
  const me = addr.toLowerCase();
  const membersLower = (clusterNodes || []).map((a) => (a || '').toLowerCase());
  if (membersLower.length > 0 && !membersLower.includes(me)) {
    console.log(
      `[RoundBarrier] [WARN]  Local membership check failed: ${addr} not in clusterNodes for R${round}`,
    );
    return false;
  }
  try {
    const prev = await node.exchangeCoordinator.multisigExchangeData(
      clusterId,
      round,
      addr,
    );
    if (isBytes32Hex(prev) && prev !== ZERO_BYTES32) {
      console.log(`[RoundBarrier] Round ${round} exchange digest already present on-chain; continuing`);
      return true;
    }
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.log(`[RoundBarrier] multisigExchangeData read failed for R${round}: ${msg}`);
  }
  try {
    if (typeof node.exchangeCoordinator.submitExchangeInfo.staticCall === 'function') {
      await node.exchangeCoordinator.submitExchangeInfo.staticCall(clusterId, round, digest);
    } else if (
      node.exchangeCoordinator.callStatic &&
      node.exchangeCoordinator.callStatic.submitExchangeInfo
    ) {
      await node.exchangeCoordinator.callStatic.submitExchangeInfo(clusterId, round, digest);
    }
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    if (/already submitted|duplicate|already.*submitted/i.test(msg)) {
      console.log(
        `[RoundBarrier] Round ${round} exchange digest already submitted (preflight); continuing`,
      );
      return true;
    }
    console.log(`[RoundBarrier] Preflight revert for R${round}: ${msg}`);
    return false;
  }
  let gasLimit = null;
  try {
    let est;
    if (
      node.exchangeCoordinator &&
      node.exchangeCoordinator.submitExchangeInfo &&
      node.exchangeCoordinator.submitExchangeInfo.estimateGas
    ) {
      est = await node.exchangeCoordinator.submitExchangeInfo.estimateGas(
        clusterId,
        round,
        digest,
      );
    } else if (
      node.exchangeCoordinator &&
      node.exchangeCoordinator.estimateGas &&
      node.exchangeCoordinator.estimateGas.submitExchangeInfo
    ) {
      est = await node.exchangeCoordinator.estimateGas.submitExchangeInfo(
        clusterId,
        round,
        digest,
      );
    }
    if (est != null) {
      gasLimit = (est * 12n) / 10n;
    }
  } catch (e) {
    const msg = e && e.message ? e.message : String(e);
    console.log(`[RoundBarrier] Gas estimate failed for R${round}: ${msg}`);
  }
  const retries = Number(process.env.ROUND_SUBMIT_RETRIES || 1);
  let attempt = 0;
  let fixedNonce = null;
  while (true) {
    try {
      const baseOverrides = gasLimit ? { gasLimit } : undefined;
      const txOptions = {
        operation: 'submitExchangeInfo',
        retryWithSameNonce: true,
        maxAttempts: 3,
        urgency: 'high',
        overrides: baseOverrides,
      };
      if (fixedNonce != null) {
        txOptions.nonce = fixedNonce;
      }
      const tx = await node.txManager.sendTransaction(
        (overrides) =>
          node.exchangeCoordinator.submitExchangeInfo(
            clusterId,
            round,
            digest,
            overrides,
          ),
        txOptions,
      );
      if (fixedNonce == null && tx.nonce != null) {
        fixedNonce = tx.nonce;
      }
      console.log(`[RoundBarrier] submitExchangeInfo(R${round}) sent: ${tx.hash}`);
      await tx.wait();
      console.log(`[RoundBarrier] submitExchangeInfo(R${round}) confirmed`);
      return true;
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      if (/already submitted|duplicate|already.*submitted/i.test(msg)) {
        console.log(`[RoundBarrier] Round ${round} exchange digest already submitted on-chain; continuing`);
        return true;
      }
      const retryable =
        /timeout|replacement|underpriced|nonce|network|rate|temporar|EHOSTUNREACH|ECONN|ETIMEDOUT/i.test(
          msg,
        );
      if (attempt < retries && retryable) {
        attempt++;
        const backoff = 500 + Math.floor(Math.random() * 500);
        console.log(
          `[RoundBarrier] send retry ${attempt}/${retries} for R${round} after ${backoff}ms: ${msg}`,
        );
        await new Promise((r) => setTimeout(r, backoff));
        continue;
      }
      console.log(`[RoundBarrier] submitExchangeInfo error for round ${round}: ${msg}`);
      return false;
    }
  }
}

export async function waitForOnChainRoundBarrier(
  node,
  clusterId,
  round,
  clusterNodes,
  timeoutMs,
  dryRun,
) {
  if (!node.exchangeCoordinator) {
    return true;
  }
  if (dryRun) {
    console.log(`[RoundBarrier] [DRY_RUN] Skipping on-chain barrier wait for round ${round}`);
    return true;
  }
  const expected = Array.isArray(clusterNodes) ? clusterNodes.length : 0;
  const start = Date.now();
  const noTimeout = !Number.isFinite(timeoutMs) || timeoutMs <= 0;
  const pollMs = 8000;
  console.log(
    `[RoundBarrier] Waiting for on-chain round ${round} completion (0/${expected})... timeoutMs=${noTimeout ? 'infinite' : timeoutMs}`,
  );
  while (true) {
    let complete = false;
    let submitted = 0;
    try {
      const status = await node.getRoundStatus(clusterId, round);
      if (Array.isArray(status)) {
        [complete, submitted] = status;
      } else if (status && typeof status === 'object') {
        complete = !!status.complete;
        submitted = Number(status.submitted || 0);
      }
    } catch (e) {
      console.log(
        `[RoundBarrier] getExchangeRoundStatus error for round ${round}:`,
        e.message || String(e),
      );
      return false;
    }
    if (expected > 0) {
      if (submitted >= expected) {
        console.log(`[RoundBarrier] Round ${round} on-chain complete: ${submitted}/${expected} nodes`);
        return true;
      }
      if (complete) {
        console.log(
          `[RoundBarrier] [WARN]  Coordinator reported complete for round ${round} with only ${submitted}/${expected} submissions; ignoring`,
        );
      }
    } else if (complete) {
      console.log(`[RoundBarrier] Round ${round} on-chain complete: ${submitted}/${expected} nodes`);
      return true;
    }
    const elapsed = Date.now() - start;
    if (!noTimeout && elapsed >= timeoutMs) {
      console.log(`[RoundBarrier] Round ${round} on-chain barrier timeout: ${submitted}/${expected} nodes`);
      try {
        const info = await node.exchangeCoordinator.getExchangeRoundInfo(
          clusterId,
          round,
          clusterNodes,
        );
        const missing = [];
        for (let i = 0; i < clusterNodes.length; i++) {
          const exDigest = info.exchangeDigests
            ? info.exchangeDigests[i]
            : info[1]
              ? info[1][i]
              : ZERO_BYTES32;
          if (!isBytes32Hex(exDigest) || exDigest === ZERO_BYTES32) {
            missing.push(clusterNodes[i]);
          }
        }
        if (missing.length > 0) {
          console.log(`[RoundBarrier] Missing on-chain R${round} from: ${missing.join(', ')}`);
        }
      } catch (e) {
        console.log(`[RoundBarrier] Could not identify missing nodes: ${e.message || e}`);
      }
      return false;
    }
    if (elapsed % 30000 < pollMs) {
      console.log(`[RoundBarrier] Round ${round} on-chain progress: ${submitted}/${expected} nodes`);
    }
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}

export default {
  submitRoundExchangeInfo,
  waitForOnChainRoundBarrier,
};
