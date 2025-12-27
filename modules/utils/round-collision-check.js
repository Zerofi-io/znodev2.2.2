import {
  MINT_SIGNATURE_ROUND,
  MULTISIG_SYNC_ROUND,
  WITHDRAWAL_UNSIGNED_INFO_ROUND,
  WITHDRAWAL_SIGNED_INFO_ROUND,
  SWEEP_SYNC_ROUND,
} from '../bridge/withdrawals/config.js';

const DEPOSIT_REQUEST_ROUND = Number(process.env.DEPOSIT_REQUEST_ROUND || 9700);
const BATCH_MINT_SIGNATURE_ROUND = Number(process.env.BATCH_MINT_SIGNATURE_ROUND || 9860);
const FRAUD_SLASH_ROUND = Number(process.env.FRAUD_SLASH_ROUND || 9921);
const DOWNTIME_SLASH_SIGN_ROUND = Number(process.env.DOWNTIME_SLASH_SIGN_ROUND || 9932);
const WITHDRAWAL_SIGN_STEP_REQUEST_ROUND = 9900;
const WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND = 9901;

const ROUND_ALLOCATIONS = [
  { name: 'DEPOSIT_REQUEST_ROUND', value: DEPOSIT_REQUEST_ROUND, session: 'bridge' },
  { name: 'MINT_SIGNATURE_ROUND', value: MINT_SIGNATURE_ROUND, session: 'bridge' },
  { name: 'MULTISIG_SYNC_ROUND', value: MULTISIG_SYNC_ROUND, session: 'bridge' },
  { name: 'SWEEP_SYNC_ROUND', value: SWEEP_SYNC_ROUND, session: 'bridge' },
  { name: 'SWEEP_SYNC_ROUND+1', value: SWEEP_SYNC_ROUND + 1, session: 'bridge' },
  { name: 'SWEEP_SYNC_ROUND+2', value: SWEEP_SYNC_ROUND + 2, session: 'bridge' },
  { name: 'SWEEP_SYNC_ROUND+3', value: SWEEP_SYNC_ROUND + 3, session: 'bridge' },
  { name: 'BATCH_MINT_SIGNATURE_ROUND', value: BATCH_MINT_SIGNATURE_ROUND, session: 'bridge' },
  { name: 'WITHDRAWAL_UNSIGNED_INFO_ROUND', value: WITHDRAWAL_UNSIGNED_INFO_ROUND, session: 'bridge' },
  { name: 'WITHDRAWAL_SIGN_STEP_REQUEST_ROUND', value: WITHDRAWAL_SIGN_STEP_REQUEST_ROUND, session: 'bridge' },
  { name: 'WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND', value: WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND, session: 'bridge' },
  { name: 'WITHDRAWAL_SIGNED_INFO_ROUND', value: WITHDRAWAL_SIGNED_INFO_ROUND, session: 'bridge' },
  { name: 'FRAUD_SLASH_ROUND', value: FRAUD_SLASH_ROUND, session: 'bridge' },
  { name: 'DOWNTIME_SLASH_SIGN_ROUND', value: DOWNTIME_SLASH_SIGN_ROUND, session: 'slash' },
];

export function checkRoundCollisions() {
  const collisions = [];
  const bySessionAndRound = new Map();
  for (const alloc of ROUND_ALLOCATIONS) {
    const key = `${alloc.session}:${alloc.value}`;
    if (bySessionAndRound.has(key)) {
      const existing = bySessionAndRound.get(key);
      collisions.push({
        round: alloc.value,
        session: alloc.session,
        names: [existing.name, alloc.name],
      });
    } else {
      bySessionAndRound.set(key, alloc);
    }
  }
  return collisions;
}

export function logRoundAllocationSummary() {
  const sorted = [...ROUND_ALLOCATIONS].sort((a, b) => a.value - b.value);
  console.log('[RoundAlloc] P2P Round Allocations:');
  for (const alloc of sorted) {
    console.log(`  ${alloc.value}: ${alloc.name} (session=${alloc.session})`);
  }
  const collisions = checkRoundCollisions();
  if (collisions.length > 0) {
    console.log('[RoundAlloc] WARNING: Round collisions detected!');
    for (const c of collisions) {
      console.log(`  Round ${c.round} (session=${c.session}): ${c.names.join(' vs ')}`);
    }
  } else {
    console.log('[RoundAlloc] No collisions detected');
  }
}

export function validateRoundAllocationsOnStartup() {
  const collisions = checkRoundCollisions();
  if (collisions.length > 0) {
    for (const c of collisions) {
      console.log(`[WARN] P2P Round collision: ${c.round} (session=${c.session}) used by ${c.names.join(' and ')}`);
    }
    return false;
  }
  return true;
}
