import { ethers } from 'ethers';
import { DRY_RUN, TEST_MODE } from '../runtime.js';
export async function checkRequirements(node) {
  console.log('[INFO] Checking requirements...');
  console.log(`  Network: ${node.networkName} (chainId: ${node.chainId})`);
  const ethBalance = await node.provider.getBalance(node.wallet.address);
  if (ethBalance < ethers.parseEther('0.001')) {
    throw new Error('Insufficient ETH for gas (need >= 0.001 ETH)');
  }
  let zfiDecimals;
  try {
    zfiDecimals = await node.zfi.decimals();
    console.log(`  ZFI Decimals: ${zfiDecimals}`);
  } catch (e) {
    if (!TEST_MODE) {
      throw new Error(
        `Failed to read ZFI decimals from contract. This is required in production mode: ${e.message}`,
      );
    }
    console.warn(
      `[WARN]  Could not read ZFI decimals, assuming 18 (TEST_MODE only): ${e.message}`,
    );
    zfiDecimals = 18;
  }
  const zfiBal = await node.zfi.balanceOf(node.wallet.address);
  console.log(`  ZFI Balance: ${ethers.formatUnits(zfiBal, zfiDecimals)}`);
  let stakedAmt = 0n;
  let slashingStage = 0n;
  try {
    const info = await node.staking.getNodeInfo(node.wallet.address);
    if (!Array.isArray(info) || info.length < 7) {
      console.warn('[WARN]  Unexpected getNodeInfo response format, treating as not staked');
      stakedAmt = 0n;
    } else {
      stakedAmt = info[0];
      slashingStage = info[5];
    }
  } catch {
    stakedAmt = 0n;
  }
  console.log(`  ZFI Staked: ${ethers.formatUnits(stakedAmt, zfiDecimals)}`);
  let isBlacklisted = false;
  try {
    isBlacklisted = await node.staking.isBlacklisted(node.wallet.address);
  } catch (e) {
    console.warn(
      '[WARN]  Could not read blacklist status from staking contract:',
      e.message || String(e),
    );
  }
  if (isBlacklisted) {
    throw new Error(
      'This address has been blacklisted due to slashing. It cannot participate as a node.',
    );
  }
  let hasCheckSlashingStatus = false;
  try {
    const fn = node.staking.interface.getFunction('checkSlashingStatus'); if (!fn) throw new Error('not found');
    hasCheckSlashingStatus = true;
  } catch {}
  if (hasCheckSlashingStatus) {
    try {
      const res = await node.staking.checkSlashingStatus(node.wallet.address);
      if (Array.isArray(res) && res.length >= 3) {
        const needsSlash = !!res[0];
        const stage = res[1];
        const hoursOfflinePending = res[2];
        if (needsSlash) {
          console.warn(
            `[WARN]  checkSlashingStatus reports pending downtime slash (stage ${stage}, offline ${hoursOfflinePending} hours). Network participants may submit a slashing tx using signed heartbeats.`,
          );
        }
      }
    } catch (e) {
      console.warn(
        '[WARN]  Could not read slashing status from staking contract:',
        e.message || String(e),
      );
    }
  }
  const required = ethers.parseUnits('1000000', zfiDecimals);
  if (stakedAmt >= required) {
    console.log('  Stake already at or above required amount.');
  } else {
    const stakingAddr = node.staking.target || node.staking.address;
    let amountNeeded;
    let actionLabel;
    if (stakedAmt > 0n) {
      amountNeeded = required - stakedAmt;
      actionLabel = 'top up existing stake';
      console.log(
        `  Detected partial stake. Need to top up by ${ethers.formatUnits(amountNeeded, zfiDecimals)} ZFI to reach 1,000,000.`,
      );
    } else {
      amountNeeded = required;
      actionLabel = 'initial stake';
      console.log('  No existing stake found; will stake full 1,000,000 ZFI.');
    }
    if (zfiBal < amountNeeded) {
      const slashingStageNum = Number(slashingStage || 0n);
      if (slashingStageNum > 0) {
        throw new Error(
          `This node has been partially slashed (stage ${slashingStageNum}) and does not have enough ZFI to restore the full 1,000,000 ZFI stake. Missing ${ethers.formatUnits(amountNeeded, zfiDecimals)} ZFI.`,
        );
      }
      throw new Error(
        `Insufficient ZFI to ${actionLabel} by ${ethers.formatUnits(amountNeeded, zfiDecimals)} ZFI`,
      );
    }
    const allowance = await node.zfi.allowance(node.wallet.address, stakingAddr);
    if (allowance < amountNeeded) {
      const TEST_MODE = process.env.TEST_MODE === '1';
      const approvalMultiplier = TEST_MODE ? Number(process.env.APPROVAL_MULTIPLIER || '1') : 1;
      if (approvalMultiplier !== 1 && !TEST_MODE) {
        console.warn('[WARN]  APPROVAL_MULTIPLIER ignored in production mode (using 1x)');
      }
      const approvalAmount = amountNeeded * BigInt(approvalMultiplier);
      console.log('  Approving ZFI for staking...');
      console.log(
        `  [INFO] approve(${stakingAddr}, ${ethers.formatUnits(approvalAmount, zfiDecimals)} ZFI)`,
      );
      if (approvalMultiplier > 1) {
        console.log(`  [INFO]  TEST_MODE: Approving ${approvalMultiplier}x required amount`);
      }
      if (DRY_RUN) {
        console.log('  [DRY_RUN] Would send approve transaction');
      } else {
        const txA = await node.txManager.sendTransaction(
          (overrides) => node.zfi.approve(stakingAddr, approvalAmount, overrides),
          { operation: 'approve', retryWithSameNonce: true },
        );
        await txA.wait();
        console.log('  [OK] Approved');
      }
    }
    if (stakedAmt === 0n) {
      console.log('  Staking 1,000,000 ZFI...');
      const codeHash = ethers.id('znode-v3');
      console.log(`  [INFO] stake(${codeHash}, '')`);
      if (DRY_RUN) {
        console.log('  [DRY_RUN] Would send stake transaction');
      } else {
        const txS = await node.txManager.sendTransaction(
          (overrides) => node.staking.stake(codeHash, '', overrides),
          { operation: 'stake', retryWithSameNonce: true },
        );
        await txS.wait();
        console.log('  [OK] Staked');
      }
    } else {
      console.log('  Topping up existing stake to 1,000,000 ZFI...');
      console.log('  [INFO] topUpStake()');
      if (DRY_RUN) {
        console.log('  [DRY_RUN] Would send topUpStake transaction');
      } else {
        const txT = await node.txManager.sendTransaction(
          (overrides) => node.staking.topUpStake(overrides),
          { operation: 'topUpStake', retryWithSameNonce: true },
        );
        await txT.wait();
        console.log('  [OK] Top-up complete');
      }
    }
  }
  console.log('[OK] Requirements met\n');
}
export async function ensureRegistered(node) {
  console.log('[INFO] Registering to network (v3)...');
  if (!node.multisigInfo) {
    try {
      await node.prepareMultisig();
    } catch (e) {
      console.log('[ERROR] Failed to prepare multisig during registration:', e.message || String(e));
    }
  }
  let registered = false;
  try {
    const info = await node.registry.nodes(node.wallet.address);
    if (info && info.registered !== undefined) {
      console.log('[Registry] Using named field ABI decoding for nodes()');
      registered = !!info.registered;
    } else if (Array.isArray(info) && info.length >= 2) {
      console.log('[Registry] Using tuple ABI decoding for nodes() (fallback)');
      registered = !!info[1];
    }
  } catch {
    console.log('[Registry] nodes() call failed, assuming not registered');
    registered = false;
  }
  if (registered) {
    console.log('[OK] Already registered\n');
    return;
  }
  const silentIsRegistered = async () => {
    try {
      const info = await node.registry.nodes(node.wallet.address);
      if (info && info.registered !== undefined) {
        return !!info.registered;
      }
      if (Array.isArray(info) && info.length >= 2) {
        return !!info[1];
      }
    } catch {}
    return false;
  };
  const waitForTxWithTimeout = async (tx, timeoutMs) => {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      return tx.wait();
    }
    const hash = tx.hash;
    const startTs = Date.now();
    const pollMs = 5000;
    while (true) {
      const elapsed = Date.now() - startTs;
      if (elapsed >= timeoutMs) {
        return null;
      }
      try {
        const receipt = await node.provider.getTransactionReceipt(hash);
        if (receipt) {
          return receipt;
        }
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  };
  const codeHash = ethers.id('znode-v3');
  console.log(`  [INFO] registerNode(${codeHash})`);
  if (DRY_RUN) {
    console.log('  [DRY_RUN] Would send registerNode transaction');
    return;
  }
  const maxAttemptsRaw = process.env.REGISTER_RETRIES;
  const maxAttemptsParsed = maxAttemptsRaw != null ? Number(maxAttemptsRaw) : NaN;
  const maxAttempts = Number.isFinite(maxAttemptsParsed) && maxAttemptsParsed > 0 ? maxAttemptsParsed : 3;
  const timeoutRaw = process.env.REGISTER_WAIT_TIMEOUT_MS;
  const timeoutParsed = timeoutRaw != null ? Number(timeoutRaw) : NaN;
  const timeoutMs = Number.isFinite(timeoutParsed) && timeoutParsed > 0 ? timeoutParsed : 180_000;
  let fixedNonce = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    console.log(`  [INFO] registerNode attempt ${attempt}/${maxAttempts}`);
    try {
      let overrides = {};
      try {
        if (node.registry && node.registry.registerNode && node.registry.registerNode.estimateGas) {
          const est = await node.registry.registerNode.estimateGas(codeHash);
          const gasLimit = (est * 12n) / 10n;
          overrides.gasLimit = gasLimit;
        }
      } catch (e) {
        console.log(
          '[Registry] Gas estimation for registerNode failed, proceeding without explicit gasLimit:',
          e.message || String(e),
        );
      }
      const txOptions = {
        operation: 'registerNode',
        retryWithSameNonce: true,
        overrides,
        urgency: 'high',
      };
      if (fixedNonce != null) {
        txOptions.nonce = fixedNonce;
      }
      const tx = await node.txManager.sendTransaction(
        (txOverrides) => node.registry.registerNode(codeHash, txOverrides),
        txOptions,
      );
      if (fixedNonce == null && tx.nonce != null) {
        fixedNonce = tx.nonce;
      }
      console.log(`  [INFO] registerNode tx sent: ${tx.hash}`);
      const broadcastTimeoutRaw = process.env.REGISTER_BROADCAST_TIMEOUT_MS;
      const broadcastTimeoutParsed = broadcastTimeoutRaw != null ? Number(broadcastTimeoutRaw) : NaN;
      const broadcastTimeoutMs =
        Number.isFinite(broadcastTimeoutParsed) && broadcastTimeoutParsed > 0
          ? broadcastTimeoutParsed
          : 30_000;
      let broadcastVisible = false;
      try {
        const startBroadcast = Date.now();
        const broadcastPollMs = 3000;
        while (Date.now() - startBroadcast < broadcastTimeoutMs) {
          try {
            const txOnNode = await node.provider.getTransaction(tx.hash);
            if (txOnNode) {
              broadcastVisible = true;
              break;
            }
          } catch {}
          await new Promise((resolve) => setTimeout(resolve, broadcastPollMs));
        }
      } catch {}
      if (!broadcastVisible) {
        console.log(
          `  [WARN] registerNode tx ${tx.hash} not visible on provider after ${broadcastTimeoutMs}ms; treating as broadcast failure`,
        );
        throw new Error('registerNode broadcast not confirmed');
      }
      const receipt = await waitForTxWithTimeout(tx, timeoutMs);
      if (!receipt) {
        console.log(`  [WARN] registerNode tx not confirmed within ${timeoutMs}ms; checking registry state...`);
        const nowRegistered = await silentIsRegistered();
        if (nowRegistered) {
          console.log('[OK] Registered (confirmed via registry after timeout)\n');
          return;
        }
      } else if (receipt.status === 1n || receipt.status === 1) {
        const nowRegistered = await silentIsRegistered();
        if (nowRegistered) {
          console.log('[OK] Registered\n');
          return;
        }
        console.log(
          '  [WARN] registerNode tx confirmed but registry still reports not registered; will retry if attempts remain',
        );
      } else {
        console.log(`  [WARN] registerNode tx failed or reverted (status=${String(receipt.status)})`);
      }
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      console.log(`  [WARN] registerNode attempt ${attempt}/${maxAttempts} error: ${msg}`);
    }
    if (attempt < maxAttempts) {
      const backoff = 3000 + Math.floor(Math.random() * 2000);
      console.log(`  [INFO] Retrying registerNode in ${backoff}ms...`);
      await new Promise((resolve) => setTimeout(resolve, backoff));
      const alreadyRegistered = await silentIsRegistered();
      if (alreadyRegistered) {
        console.log('[OK] Registered during backoff wait\n');
        return;
      }
    }
  }
  throw new Error('Failed to register node after multiple attempts');
}
export default {
  checkRequirements,
  ensureRegistered,
};
