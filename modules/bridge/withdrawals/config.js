export function isBridgeEnabled() {
  const v = process.env.BRIDGE_ENABLED;
  return v === undefined || v === '1';
}
export const REQUIRED_WITHDRAWAL_SIGNATURES = 7;
export const MAX_WITHDRAWALS_PER_BATCH = Number(process.env.MAX_WITHDRAWALS_PER_BATCH || 15);
export const MINT_SIGNATURE_ROUND = Number(process.env.MINT_SIGNATURE_ROUND || 9800);
const multisigSyncRoundBase = Number(process.env.MULTISIG_SYNC_ROUND || (MINT_SIGNATURE_ROUND + 10));
export const MULTISIG_SYNC_ROUND =
  multisigSyncRoundBase === MINT_SIGNATURE_ROUND ? MINT_SIGNATURE_ROUND + 10 : multisigSyncRoundBase;
export const ZXMR_DECIMALS = 12;
export const WITHDRAWAL_SIGN_STEP_TIMEOUT_MS = Number(process.env.WITHDRAWAL_SIGN_STEP_TIMEOUT_MS || 20000);
export const WITHDRAWAL_MAX_RETRY_WINDOW_MS = Number(process.env.WITHDRAWAL_MAX_RETRY_WINDOW_MS || 1800000);
export const WITHDRAWAL_RETRY_INTERVAL_MS = Number(process.env.WITHDRAWAL_RETRY_INTERVAL_MS || 300000);
export const WITHDRAWAL_RETRY_FAILED_PERMANENT = process.env.WITHDRAWAL_RETRY_FAILED_PERMANENT === '1';
export const WITHDRAWAL_MAX_ROUND_ATTEMPTS = Number(process.env.WITHDRAWAL_MAX_ROUND_ATTEMPTS || 16);
export const WITHDRAWAL_UNSIGNED_INFO_ROUND = Number(process.env.WITHDRAWAL_UNSIGNED_INFO_ROUND || 9898);
export const WITHDRAWAL_SIGNED_INFO_ROUND = Number(process.env.WITHDRAWAL_SIGNED_INFO_ROUND || 9902);
export const WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND = Number(process.env.WITHDRAWAL_SIGN_STEP_RESPONSE_ROUND || 9901);
export const WITHDRAWAL_SIGNED_INFO_WAIT_MS = Number(process.env.WITHDRAWAL_SIGNED_INFO_WAIT_MS || 300000);
export const WITHDRAWAL_SYNC_WINDOW_MS = Number(process.env.WITHDRAWAL_SYNC_WINDOW_MS || 30000);
export const WITHDRAWAL_BATCH_MANIFEST_PBFT = process.env.WITHDRAWAL_BATCH_MANIFEST_PBFT !== '0';
export const WITHDRAWAL_MANIFEST_WAIT_MS = Number(process.env.WITHDRAWAL_MANIFEST_WAIT_MS || 30000);
export const WITHDRAWAL_MANIFEST_PBFT_TIMEOUT_MS = Number(process.env.WITHDRAWAL_MANIFEST_PBFT_TIMEOUT_MS || 60000);
export const WITHDRAWAL_EVENT_POLL_INTERVAL_MS = Number(process.env.WITHDRAWAL_EVENT_POLL_INTERVAL_MS || 30000);
export const WITHDRAWAL_EVENT_REORG_BUFFER_BLOCKS = Number(process.env.WITHDRAWAL_EVENT_REORG_BUFFER_BLOCKS || 12);
export const WITHDRAWAL_MIN_CONFIRMATIONS = Number(process.env.WITHDRAWAL_MIN_CONFIRMATIONS || 30);
export const WITHDRAWAL_STARTUP_BACKFILL_BLOCKS = Number(process.env.WITHDRAWAL_STARTUP_BACKFILL_BLOCKS || 300);
export const WITHDRAWAL_EVENT_QUERY_CHUNK_BLOCKS = Number(process.env.WITHDRAWAL_EVENT_QUERY_CHUNK_BLOCKS || 500);
export const WITHDRAWAL_PBFT_PHASE_V2 = process.env.WITHDRAWAL_PBFT_PHASE_V2 !== '0';
export const WITHDRAWAL_PBFT_PHASE_V2_TRY_MS = Number(process.env.WITHDRAWAL_PBFT_PHASE_V2_TRY_MS || 15000);
export const WITHDRAWAL_ROUND_V2 = process.env.WITHDRAWAL_ROUND_V2 !== '0';
export const WITHDRAWAL_ROUND_V2_DUALWRITE = process.env.WITHDRAWAL_ROUND_V2_DUALWRITE !== '0';
export const WITHDRAWAL_SECURITY_HARDEN = process.env.WITHDRAWAL_SECURITY_HARDEN !== '0';
export const WITHDRAWAL_STRICT_XMR_ADDRESS = process.env.WITHDRAWAL_STRICT_XMR_ADDRESS !== '0';
export const WITHDRAWAL_VERIFY_BURNS_ONCHAIN = process.env.WITHDRAWAL_VERIFY_BURNS_ONCHAIN !== '0';
export const WITHDRAWAL_VERIFY_TXSET = process.env.WITHDRAWAL_VERIFY_TXSET !== '0';
export const WITHDRAWAL_ROUND_CLEAR_DELAY_MS = Number(process.env.WITHDRAWAL_ROUND_CLEAR_DELAY_MS || 600000);
export const MULTISIG_MAINTENANCE_INTERVAL_MS = Number(process.env.MULTISIG_MAINTENANCE_INTERVAL_MS || 300000);
export const MULTISIG_MAINTENANCE_CLEAR_DELAY_MS = Number(process.env.MULTISIG_MAINTENANCE_CLEAR_DELAY_MS || WITHDRAWAL_ROUND_CLEAR_DELAY_MS);
export const WITHDRAWAL_POSTSUBMIT_SYNC_ENABLED = process.env.WITHDRAWAL_POSTSUBMIT_SYNC_ENABLED !== '0';
export const WITHDRAWAL_POSTSUBMIT_SYNC_TIMEOUT_MS = Number(process.env.WITHDRAWAL_POSTSUBMIT_SYNC_TIMEOUT_MS || 90000);
export const WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS = Number(process.env.WITHDRAWAL_POSTSUBMIT_SYNC_MAX_ATTEMPTS || 4);
export const WITHDRAWAL_MAX_XMR = (() => {
  const raw = process.env.WITHDRAWAL_MAX_XMR;
  if (raw == null || raw === '') return 5000;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0 || !Number.isInteger(n)) return 5000;
  return n;
})();
export const WITHDRAWAL_MAX_XMR_ATOMIC = BigInt(WITHDRAWAL_MAX_XMR) * 1000000000000n;
export const WITHDRAWAL_EPOCH_SECONDS = Number(process.env.WITHDRAWAL_EPOCH_SECONDS || 1800);
export const SWEEP_ENABLED = process.env.SWEEP_ENABLED !== '0';
export const SWEEP_MIN_CONFIRMATIONS = Number(process.env.SWEEP_MIN_CONFIRMATIONS || 10);
export const SWEEP_MIN_AMOUNT_ATOMIC = BigInt(process.env.SWEEP_MIN_AMOUNT_ATOMIC || 100000000);
export const SWEEP_MAX_INPUTS_PER_TX = Number(process.env.SWEEP_MAX_INPUTS_PER_TX || 100);
export const SWEEP_DELAY_AFTER_WITHDRAWAL_MS = Number(process.env.SWEEP_DELAY_AFTER_WITHDRAWAL_MS || 60000);
export const SWEEP_PRIORITY_BY_AMOUNT = process.env.SWEEP_PRIORITY_BY_AMOUNT !== '0';
export const SWEEP_SYNC_ROUND = Number(process.env.SWEEP_SYNC_ROUND || 9850);
const MAINNET_PREFIXES = new Set([18, 19, 42]);
const TESTNET_PREFIXES = new Set([53, 54, 63]);
const STAGENET_PREFIXES = new Set([24, 25, 36]);
function decodeBase58(encoded) {
  const ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  const ALPHABET_MAP = new Map();
  for (let i = 0; i < ALPHABET.length; i++) {
    ALPHABET_MAP.set(ALPHABET[i], BigInt(i));
  }
  let result = 0n;
  for (const char of encoded) {
    const value = ALPHABET_MAP.get(char);
    if (value === undefined) return null;
    result = result * 58n + value;
  }
  const hex = result.toString(16).padStart(2, '0');
  const bytes = [];
  for (let i = 0; i < hex.length; i += 2) {
    bytes.push(parseInt(hex.slice(i, i + 2), 16));
  }
  return new Uint8Array(bytes);
}
export function isValidMoneroAddress(address) {
  if (!address || typeof address !== 'string') return false;
  const trimmed = address.trim();
  if (trimmed.length < 95 || trimmed.length > 106) return false;
  const isStandard = trimmed.length === 95;
  const isIntegrated = trimmed.length === 106;
  const isSubaddress = trimmed.length === 95;
  if (!isStandard && !isIntegrated && !isSubaddress) return false;
  try {
    const decoded = decodeBase58(trimmed);
    if (!decoded || decoded.length < 1) return false;
    const prefix = decoded[0];
    const validMainnet = MAINNET_PREFIXES.has(prefix);
    const validTestnet = TESTNET_PREFIXES.has(prefix);
    const validStagenet = STAGENET_PREFIXES.has(prefix);
    if (!validMainnet && !validTestnet && !validStagenet) return false;
    const networkEnv = process.env.MONERO_NETWORK || 'mainnet';
    if (networkEnv === 'mainnet' && !validMainnet) return false;
    if (networkEnv === 'testnet' && !validTestnet) return false;
    if (networkEnv === 'stagenet' && !validStagenet) return false;
    return true;
  } catch {
    return false;
  }
}
export function validateWithdrawalAddress(address) {
  if (!WITHDRAWAL_STRICT_XMR_ADDRESS) return { valid: true };
  if (!address || typeof address !== 'string') {
    return { valid: false, reason: 'address_required' };
  }
  if (!isValidMoneroAddress(address)) {
    return { valid: false, reason: 'invalid_monero_address' };
  }
  return { valid: true };
}
export const MAX_BRIBE_SLOTS = (() => {
  const fallback = 10;
  const raw = process.env.MAX_BRIBE_SLOTS;
  const n = raw == null || raw == '' ? fallback : Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
    return Math.min(fallback, MAX_WITHDRAWALS_PER_BATCH);
  }
  return Math.min(n, MAX_WITHDRAWALS_PER_BATCH);
})();
export const SLOT_RESERVED_EVENT = 'SlotReserved';
export const BRIBE_BASE_COST = (() => {
  const fallback = 500;
  const raw = process.env.BRIBE_BASE_COST;
  const n = raw == null || raw == '' ? fallback : Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) return fallback;
  return Math.min(n, 1_000_000_000_000);
})();
export const BRIBE_WINDOW_BUFFER = (() => {
  const fallback = 300;
  const raw = process.env.BRIBE_WINDOW_BUFFER;
  const n = raw == null || raw == '' ? fallback : Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) return fallback;
  return Math.min(n, WITHDRAWAL_EPOCH_SECONDS);
})();
export const WITHDRAWAL_PARTIAL_EPOCH_MODE = process.env.WITHDRAWAL_PARTIAL_EPOCH_MODE === '1';
export const WITHDRAWAL_ORPHAN_TTL_MS = (() => {
  const fallback = 86400000;
  const raw = process.env.WITHDRAWAL_ORPHAN_TTL_MS;
  const n = raw == null || raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) return fallback;
  return n;
})();
export const WITHDRAWAL_ORPHAN_SWEEP_INTERVAL_MS = (() => {
  const fallback = 300000;
  const raw = process.env.WITHDRAWAL_ORPHAN_SWEEP_INTERVAL_MS;
  const n = raw == null || raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 60000) return fallback;
  return n;
})();
