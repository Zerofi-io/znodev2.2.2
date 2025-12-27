import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { getSafeFees } from './gas-strategy.js';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function parsePercent(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return n;
}
function parseIntOption(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}
class AsyncMutex {
  constructor() {
    this.locked = false;
    this.queue = [];
  }
  async acquire() {
    if (!this.locked) {
      this.locked = true;
      return () => this.release();
    }
    return new Promise((resolve) => {
      this.queue.push(resolve);
    });
  }
  release() {
    if (this.queue.length > 0) {
      const resolve = this.queue.shift();
      resolve(() => this.release());
    } else {
      this.locked = false;
    }
  }
}
function toBigIntSafe(value) {
  if (typeof value === 'bigint') return value;
  if (value == null) return null;
  try {
    return BigInt(value.toString());
  } catch {
    return null;
  }
}
function compareBigInt(a, b) {
  const aBig = toBigIntSafe(a);
  const bBig = toBigIntSafe(b);
  if (aBig == null || bBig == null) return 0;
  if (aBig < bBig) return -1;
  if (aBig > bBig) return 1;
  return 0;
}
function maxBigInt(a, b) {
  return compareBigInt(a, b) >= 0 ? a : b;
}
function validateNonce(nonce) {
  if (nonce == null) return false;
  const n = Number(nonce);
  return Number.isInteger(n) && n >= 0 && n < Number.MAX_SAFE_INTEGER;
}
export default class TransactionManager {
  constructor(wallet, provider, options = {}) {
    this.wallet = wallet;
    this.provider = provider;
    const defaultStateFile = path.join(__dirname, '..', '.tx-state.json');
    this.stateFile = options.stateFile || process.env.TX_MANAGER_STATE_FILE || defaultStateFile;
    this.gasBumpPercent = parsePercent(
      options.gasBumpPercent ?? process.env.TX_MANAGER_GAS_BUMP_PERCENT,
      20,
    );
    this.maxGasFeeMultiplier = parsePercent(
      options.maxGasFeeMultiplier ?? process.env.TX_MANAGER_MAX_GAS_MULTIPLIER,
      5,
    );
    this.maxPendingAgeMs = parseIntOption(
      options.maxPendingAgeMs ?? process.env.TX_MANAGER_MAX_PENDING_AGE_MS,
      500000,
    );
    const autoClearEnv = process.env.TX_MANAGER_AUTO_CLEAR_STUCK;
    const autoClearDefault = autoClearEnv == null ? true : autoClearEnv !== '0';
    this.autoClearStuck = options.autoClearStuck ?? autoClearDefault;
    this.dryRun = !!options.dryRun;
    this.defaultGasLimit = parseIntOption(
      options.defaultGasLimit ?? process.env.TX_MANAGER_DEFAULT_GAS_LIMIT,
      500000,
    );
    this.alreadyKnownTimeoutMs = parseIntOption(
      options.alreadyKnownTimeoutMs ?? process.env.TX_MANAGER_ALREADY_KNOWN_TIMEOUT_MS,
      120000,
    );
    this.pending = new Map();
    this.lastNonce = null;
    this.address = null;
    this.loaded = false;
    this.nonceMutex = new AsyncMutex();
    this.stateLock = null;
    this.onPendingChange = typeof options.onPendingChange === 'function' ? options.onPendingChange : null;
    this.logger = options.logger || null;
  }
  log(level, message, meta = {}) {
    if (this.logger && typeof this.logger[level] === 'function') {
      this.logger[level](message, meta);
    }
  }
  notifyPendingChange() {
    if (this.onPendingChange) {
      this.onPendingChange(this.pending.size);
    }
  }
  async getAddress() {
    if (this.address) return this.address;
    if (this.wallet && this.wallet.address) {
      this.address = this.wallet.address;
      return this.address;
    }
    if (this.wallet && typeof this.wallet.getAddress === 'function') {
      this.address = await this.wallet.getAddress();
      return this.address;
    }
    throw new Error('TransactionManager wallet has no address');
  }
  async acquireStateLock() {
    if (this.stateLock) return;
    const lockFile = `${this.stateFile}.lock`;
    const maxRetries = 10;
    for (let i = 0; i < maxRetries; i++) {
      try {
        if (!fs.existsSync(lockFile)) {
          fs.writeFileSync(lockFile, String(process.pid), { flag: 'wx', mode: 0o600 });
          this.stateLock = lockFile;
          return;
        }
        try {
          const pid = fs.readFileSync(lockFile, 'utf8').trim();
          try {
            process.kill(Number(pid), 0);
          } catch {
            fs.unlinkSync(lockFile);
            continue;
          }
        } catch {
          fs.unlinkSync(lockFile);
          continue;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      } catch (e) {
        if (e.code !== 'EEXIST') {
          this.log('warn', 'Failed to acquire state lock', { error: e.message });
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  }
  releaseStateLock() {
    if (this.stateLock) {
      try {
        fs.unlinkSync(this.stateLock);
      } catch (e) {
        this.log('warn', 'Failed to release state lock', { error: e.message });
      }
      this.stateLock = null;
    }
  }
  async loadState() {
    if (this.loaded) return;
    await this.acquireStateLock();
    try {
      if (fs.existsSync(this.stateFile)) {
        const raw = fs.readFileSync(this.stateFile, 'utf8');
        if (raw && raw.trim().length > 0) {
          const parsed = JSON.parse(raw);
          const currentAddress = await this.getAddress();
          if (parsed.address && parsed.address !== currentAddress) {
            this.log('warn', 'State address mismatch, resetting state', {
              stateAddress: parsed.address,
              currentAddress,
            });
            this.pending.clear();
            this.lastNonce = null;
          } else {
            if (parsed && Array.isArray(parsed.pending)) {
              for (const entry of parsed.pending) {
                if (entry && validateNonce(entry.nonce)) {
                  this.pending.set(entry.nonce, entry);
                }
              }
            }
            if (parsed && typeof parsed.lastNonce === 'number' && parsed.lastNonce >= 0) {
              this.lastNonce = parsed.lastNonce;
            }
          }
        }
      }
    } catch (e) {
      this.log('error', 'Failed to load state', { error: e.message });
      this.pending.clear();
      this.lastNonce = null;
    } finally {
      this.releaseStateLock();
    }
    this.loaded = true;
    this.notifyPendingChange();
  }
  saveState() {
    if (!this.loaded) return;
    this._acquireStateLockSync();
    try {
      const pending = [];
      for (const [nonce, entry] of this.pending.entries()) {
        pending.push({ ...entry, nonce });
      }
      const state = {
        address: this.address || null,
        lastNonce: this.lastNonce,
        pending,
      };
      const dir = path.dirname(this.stateFile);
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      }
      const tmp = `${this.stateFile}.${Date.now()}.tmp`;
      fs.writeFileSync(
        tmp,
        JSON.stringify(state, (key, value) => (typeof value === 'bigint' ? value.toString() : value)),
        { mode: 0o600 },
      );
      fs.renameSync(tmp, this.stateFile);
    } catch (e) {
      this.log('error', 'Failed to save state', { error: e.message });
    } finally {
      this.releaseStateLock();
    }
  }
  _acquireStateLockSync() {
    if (this.stateLock) return;
    const lockFile = `${this.stateFile}.lock`;
    const maxRetries = 10;
    for (let i = 0; i < maxRetries; i++) {
      try {
        if (!fs.existsSync(lockFile)) {
          fs.writeFileSync(lockFile, String(process.pid), { flag: 'wx', mode: 0o600 });
          this.stateLock = lockFile;
          return;
        }
        try {
          const pid = fs.readFileSync(lockFile, 'utf8').trim();
          try {
            process.kill(Number(pid), 0);
          } catch {
            fs.unlinkSync(lockFile);
            continue;
          }
        } catch {
          fs.unlinkSync(lockFile);
          continue;
        }
        const sleepBuf = new Int32Array(new SharedArrayBuffer(4));
        Atomics.wait(sleepBuf, 0, 0, 10);
      } catch (e) {
        if (e.code !== 'EEXIST') {
          this.log('warn', 'Failed to acquire state lock sync', { error: e.message });
        }
      }
    }
  }
  async getOnChainNonce(tag = 'pending') {
    const address = await this.getAddress();
    const requestedTag = tag === 'latest' ? 'latest' : 'pending';
    let nonce;
    try {
      nonce = await this.provider.getTransactionCount(address, requestedTag);
    } catch (e) {
      if (requestedTag !== 'latest') {
        nonce = await this.provider.getTransactionCount(address, 'latest');
      } else {
        throw e;
      }
    }
    if (typeof nonce === 'bigint') return Number(nonce);
    return Number(nonce);
  }
  async ensureInitialized() {
    await this.loadState();
    if (this.lastNonce == null) {
      const onChain = await this.getOnChainNonce();
      this.lastNonce = onChain;
    }
  }
  async getNextNonce() {
    const release = await this.nonceMutex.acquire();
    try {
      await this.ensureInitialized();
      const chainNonce = await this.getOnChainNonce();
      for (let n = chainNonce; n < (this.lastNonce || chainNonce); n++) {
        if (!this.pending.has(n)) {
          this.log('info', 'Filling nonce gap during allocation', { nonce: n });
          return n;
        }
      }
      const nonce = Math.max(this.lastNonce, chainNonce);
      this.lastNonce = nonce + 1;
      return nonce;
    } finally {
      release();
    }
  }
  bumpBigInt(value, originalValue = null) {
    const n = toBigIntSafe(value);
    if (n == null) return value;
    const mul = BigInt(100 + this.gasBumpPercent);
    const div = 100n;
    const bumped = (n * mul) / div;
    if (originalValue != null) {
      const original = toBigIntSafe(originalValue);
      if (original != null) {
        const maxAllowed = original * BigInt(Math.floor(this.maxGasFeeMultiplier * 100)) / 100n;
        if (bumped > maxAllowed) {
          this.log('warn', 'Gas bump capped at max multiplier', {
            bumped: bumped.toString(),
            maxAllowed: maxAllowed.toString(),
          });
          return maxAllowed;
        }
      }
    }
    return bumped;
  }
  bumpGas(overrides, originalOverrides = null) {
    const next = { ...overrides };
    if (overrides.maxFeePerGas != null) {
      next.maxFeePerGas = this.bumpBigInt(
        overrides.maxFeePerGas,
        originalOverrides?.maxFeePerGas,
      );
    }
    if (overrides.maxPriorityFeePerGas != null) {
      next.maxPriorityFeePerGas = this.bumpBigInt(
        overrides.maxPriorityFeePerGas,
        originalOverrides?.maxPriorityFeePerGas,
      );
    }
    if (overrides.gasPrice != null) {
      next.gasPrice = this.bumpBigInt(overrides.gasPrice, originalOverrides?.gasPrice);
    }
    return next;
  }
  trackPending(nonce, tx, operation, originalGas = null) {
    const entry = {
      nonce,
      hash: tx.hash,
      addedAt: Date.now(),
      operation: operation || null,
      to: tx.to || null,
      data: tx.data || null,
      value: tx.value != null ? tx.value : null,
      maxFeePerGas: tx.maxFeePerGas != null ? tx.maxFeePerGas : null,
      maxPriorityFeePerGas: tx.maxPriorityFeePerGas != null ? tx.maxPriorityFeePerGas : null,
      gasPrice: tx.gasPrice != null ? tx.gasPrice : null,
      originalMaxFeePerGas: originalGas?.maxFeePerGas || tx.maxFeePerGas || null,
      originalMaxPriorityFeePerGas: originalGas?.maxPriorityFeePerGas || tx.maxPriorityFeePerGas || null,
      originalGasPrice: originalGas?.gasPrice || tx.gasPrice || null,
    };
    this.pending.set(nonce, entry);
    if (this.lastNonce == null || this.lastNonce <= nonce) {
      this.lastNonce = nonce + 1;
    }
    this.saveState();
    this.notifyPendingChange();
  }
  async sendTransaction(contractCall, options = {}) {
    if (this.dryRun) {
      throw new Error('TransactionManager cannot send transactions in dry-run mode');
    }
    await this.ensureInitialized();
    const baseOverrides = options.overrides ? { ...options.overrides } : {};
    const retryWithSameNonce = !!options.retryWithSameNonce;
    const maxAttempts = options.maxAttempts && options.maxAttempts > 0 ? options.maxAttempts : 1;
    const operation = options.operation || null;
    const explicitNonce = options.nonce;
    let overrides = baseOverrides;
    if (
      overrides.maxFeePerGas == null &&
      overrides.maxPriorityFeePerGas == null &&
      overrides.gasPrice == null
    ) {
      const urgency = options.urgency || 'normal';
      const fees = await getSafeFees(this.provider, { urgency });
      overrides = {
        ...overrides,
        maxFeePerGas: fees.maxFeePerGas,
        maxPriorityFeePerGas: fees.maxPriorityFeePerGas,
      };
    }
    const originalOverrides = { ...overrides };
    const nonce = explicitNonce != null ? explicitNonce : await this.getNextNonce();
    if (!validateNonce(nonce)) {
      throw new Error(`Invalid nonce: ${nonce}`);
    }
    let lastError = null;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const finalOverrides = { ...overrides, nonce };
      try {
        const tx = await contractCall(finalOverrides);
        this.trackPending(nonce, tx, operation, originalOverrides);
        this.log('info', 'Transaction sent', {
          nonce,
          hash: tx.hash,
          operation,
          attempt,
        });
        return tx;
      } catch (e) {
        lastError = e;
        let msg = e && e.message ? e.message : String(e);
        this.log('warn', 'Transaction send failed', {
          nonce,
          attempt,
          error: msg,
        });
        if (/CALL_EXCEPTION|missing revert data|estimateGas/i.test(msg) && finalOverrides.gasLimit == null) {
          try {
            const tx = await contractCall({ ...finalOverrides, gasLimit: BigInt(this.defaultGasLimit) });
            this.trackPending(nonce, tx, operation, originalOverrides);
            this.log('info', 'Transaction sent', { nonce, hash: tx.hash, operation, attempt });
            return tx;
          } catch (e2) {
            lastError = e2;
            msg = e2 && e2.message ? e2.message : String(e2);
            this.log('warn', 'Transaction send failed', { nonce, attempt, error: msg });
          }
        }
        if (/already known/i.test(msg)) {
          const existing = this.pending.get(nonce);
          if (existing && existing.hash) {
            const hash = existing.hash;
            const manager = this;
            const tx = {
              hash,
              nonce,
              wait(timeoutMs) {
                return manager.waitForConfirmation(hash, timeoutMs || 0);
              },
            };
            this.log('info', 'Transaction already known, returning existing', {
              nonce,
              hash,
            });
            return tx;
          }
          const address = await this.getAddress();
          const manager = this;
          const timeoutMs = this.alreadyKnownTimeoutMs;
          const tx = {
            hash: null,
            nonce,
            wait(waitTimeoutMs) {
              const limit = waitTimeoutMs && waitTimeoutMs > 0 ? waitTimeoutMs : timeoutMs;
              return (async () => {
                const startedAt = Date.now();
                while (true) {
                  const current = await manager.provider.getTransactionCount(address, 'latest');
                  const currentNum = typeof current === 'bigint' ? Number(current) : Number(current);
                  if (currentNum > nonce) {
                    return { status: 1 };
                  }
                  if (limit > 0 && Date.now() - startedAt >= limit) {
                    manager.log('warn', 'Already known transaction wait timeout', { nonce });
                    return null;
                  }
                  await new Promise((resolve) => setTimeout(resolve, 5000));
                }
              })();
            },
          };
          return tx;
        }
        if (!retryWithSameNonce) break;
        if (!/nonce.*too low|replacement.*underpriced|already known/i.test(msg)) {
          this.log('error', 'Non-retryable transaction error', {
            nonce,
            error: msg,
          });
          break;
        }
        overrides = this.bumpGas(overrides, originalOverrides);
        this.log('info', 'Retrying with bumped gas', {
          nonce,
          attempt: attempt + 1,
        });
      }
    }
    this.log('error', 'Transaction send failed after all attempts', {
      nonce,
      maxAttempts,
      error: lastError?.message || 'Unknown error',
    });
    throw lastError || new Error('TransactionManager sendTransaction failed');
  }
  async detectStuckTransactions() {
    await this.ensureInitialized();
    const address = await this.getAddress();
    const chainNonce = await this.getOnChainNonce('latest');
    const now = Date.now();
    const pendingNonces = Array.from(this.pending.keys()).sort((a, b) => a - b);
    const confirmedNonces = [];
    const stuckNonces = [];
    for (const nonce of pendingNonces) {
      const entry = this.pending.get(nonce);
      if (!entry) continue;
      if (nonce < chainNonce) {
        confirmedNonces.push(nonce);
        continue;
      }
      const age = entry.addedAt ? now - entry.addedAt : 0;
      if (age > this.maxPendingAgeMs) {
        stuckNonces.push(nonce);
      }
    }
    const highestNonce = pendingNonces.length > 0 ? pendingNonces[pendingNonces.length - 1] : chainNonce - 1;
    const gapNonces = [];
    for (let n = chainNonce; n <= highestNonce; n += 1) {
      if (!this.pending.has(n)) {
        gapNonces.push(n);
      }
    }
    return {
      address,
      chainNonce,
      pendingNonces,
      confirmedNonces,
      stuckNonces,
      gapNonces,
      highestNonce,
    };
  }
  async clearStuckTransactions() {
    if (this.dryRun) {
      await this.ensureInitialized();
      return;
    }
    const analysis = await this.detectStuckTransactions();
    this.log('info', 'Clearing stuck transactions', {
      confirmed: analysis.confirmedNonces.length,
      stuck: analysis.stuckNonces.length,
      gaps: analysis.gapNonces.length,
    });
    const now = Date.now();
    let safeFees = null;
    try {
      safeFees = await getSafeFees(this.provider, { urgency: 'high' });
    } catch (e) {
      this.log('warn', 'Failed to get safe fees for stuck transaction clearing', {
        error: e.message,
      });
      safeFees = null;
    }
    for (const nonce of analysis.confirmedNonces) {
      this.pending.delete(nonce);
    }
    const resendResults = [];
    for (const nonce of analysis.stuckNonces) {
      const entry = this.pending.get(nonce);
      if (!entry || !entry.to) continue;
      const gasOverrides = {};
      if (entry.maxFeePerGas != null) {
        gasOverrides.maxFeePerGas = this.bumpBigInt(
          entry.maxFeePerGas,
          entry.originalMaxFeePerGas,
        );
      }
      if (entry.maxPriorityFeePerGas != null) {
        gasOverrides.maxPriorityFeePerGas = this.bumpBigInt(
          entry.maxPriorityFeePerGas,
          entry.originalMaxPriorityFeePerGas,
        );
      }
      if (!gasOverrides.maxFeePerGas && entry.gasPrice != null) {
        gasOverrides.gasPrice = this.bumpBigInt(entry.gasPrice, entry.originalGasPrice);
      }
      if (safeFees) {
        if (gasOverrides.maxFeePerGas == null || gasOverrides.maxPriorityFeePerGas == null) {
          gasOverrides.maxFeePerGas = safeFees.maxFeePerGas;
          gasOverrides.maxPriorityFeePerGas = safeFees.maxPriorityFeePerGas;
          delete gasOverrides.gasPrice;
        } else {
          gasOverrides.maxFeePerGas = maxBigInt(gasOverrides.maxFeePerGas, safeFees.maxFeePerGas);
          gasOverrides.maxPriorityFeePerGas = maxBigInt(
            gasOverrides.maxPriorityFeePerGas,
            safeFees.maxPriorityFeePerGas,
          );
        }
      }
      try {
        const tx = await this.wallet.sendTransaction({
          to: entry.to,
          data: entry.data || '0x',
          value: entry.value != null ? entry.value : 0n,
          gasLimit: BigInt(this.defaultGasLimit),
          nonce,
          ...gasOverrides,
        });
        entry.hash = tx.hash;
        entry.addedAt = now;
        entry.maxFeePerGas = tx.maxFeePerGas != null ? tx.maxFeePerGas : entry.maxFeePerGas;
        entry.maxPriorityFeePerGas =
          tx.maxPriorityFeePerGas != null ? tx.maxPriorityFeePerGas : entry.maxPriorityFeePerGas;
        entry.gasPrice = tx.gasPrice != null ? tx.gasPrice : entry.gasPrice;
        this.pending.set(nonce, entry);
        resendResults.push({ nonce, success: true, hash: tx.hash });
        this.log('info', 'Resent stuck transaction', {
          nonce,
          hash: tx.hash,
        });
      } catch (e) {
        const msg = e && e.message ? e.message : String(e);
        resendResults.push({ nonce, success: false, error: msg });
        if (/already known/i.test(msg) && entry && entry.hash) {
          this.log('info', 'Stuck transaction already known', { nonce, hash: entry.hash });
          continue;
        }
        this.log('error', 'Failed to resend stuck transaction', {
          nonce,
          error: msg,
        });
      }
    }
    const activePendingNonces = Array.from(this.pending.keys()).sort((a, b) => a - b);
    const highestPending =
      activePendingNonces.length > 0 ? activePendingNonces[activePendingNonces.length - 1] : analysis.chainNonce - 1;
    const highestKnown = Math.max(highestPending, analysis.chainNonce - 1);
    const address = await this.getAddress();
    const gapFillResults = [];
    for (let n = analysis.chainNonce; n <= highestKnown; n += 1) {
      if (!this.pending.has(n)) {
        const gasOverrides = {};
        if (safeFees) {
          gasOverrides.maxFeePerGas = safeFees.maxFeePerGas;
          gasOverrides.maxPriorityFeePerGas = safeFees.maxPriorityFeePerGas;
        }
        try {
          const tx = await this.wallet.sendTransaction({
            to: address,
            value: 0n,
            gasLimit: BigInt(this.defaultGasLimit),
            nonce: n,
            ...gasOverrides,
          });
          this.pending.set(n, {
            nonce: n,
            hash: tx.hash,
            addedAt: now,
            operation: 'gap-fill',
            to: address,
            data: tx.data || null,
            value: 0n,
            maxFeePerGas: tx.maxFeePerGas != null ? tx.maxFeePerGas : null,
            maxPriorityFeePerGas: tx.maxPriorityFeePerGas != null ? tx.maxPriorityFeePerGas : null,
            gasPrice: tx.gasPrice != null ? tx.gasPrice : null,
            originalMaxFeePerGas: tx.maxFeePerGas != null ? tx.maxFeePerGas : null,
            originalMaxPriorityFeePerGas: tx.maxPriorityFeePerGas != null ? tx.maxPriorityFeePerGas : null,
            originalGasPrice: tx.gasPrice != null ? tx.gasPrice : null,
          });
          gapFillResults.push({ nonce: n, success: true, hash: tx.hash });
          this.log('info', 'Filled nonce gap', { nonce: n, hash: tx.hash });
        } catch (e) {
          const msg = e && e.message ? e.message : String(e);
          gapFillResults.push({ nonce: n, success: false, error: msg });
          this.log('error', 'Failed to fill nonce gap', {
            nonce: n,
            error: msg,
          });
        }
      }
    }
    const allNonces = Array.from(this.pending.keys());
    const onChainNonce = await this.getOnChainNonce();
    const highest = allNonces.length > 0 ? Math.max(...allNonces, onChainNonce - 1) : onChainNonce - 1;
    this.lastNonce = highest + 1;
    this.saveState();
    this.notifyPendingChange();
    return {
      resendResults,
      gapFillResults,
    };
  }
  async detectAndClearStuckTransactions() {
    await this.loadState();
    if (!this.autoClearStuck) {
      const onChain = await this.getOnChainNonce();
      const pendingNonces = Array.from(this.pending.keys());
      const allNonces = pendingNonces.length > 0 ? [...pendingNonces, onChain - 1] : [onChain - 1];
      this.lastNonce = Math.max(...allNonces) + 1;
      this.saveState();
      return;
    }
    const result = await this.clearStuckTransactions();
    const chainNonce = await this.getOnChainNonce();
    const pendingNonces = Array.from(this.pending.keys());
    const allNonces = pendingNonces.length > 0 ? [...pendingNonces, chainNonce - 1] : [chainNonce - 1];
    this.lastNonce = Math.max(...allNonces) + 1;
    this.saveState();
    return result;
  }
  async waitForConfirmation(txHash, timeoutMs) {
    const startedAt = Date.now();
    const pollMs = 5000;
    while (true) {
      const receipt = await this.provider.getTransactionReceipt(txHash);
      if (receipt) {
        if (receipt.status === 0) {
          this.log('warn', 'Transaction reverted', {
            hash: txHash,
            status: receipt.status,
          });
        }
        return receipt;
      }
      if (timeoutMs && timeoutMs > 0) {
        const elapsed = Date.now() - startedAt;
        if (elapsed >= timeoutMs) {
          this.log('warn', 'Transaction confirmation timeout', {
            hash: txHash,
            elapsed,
          });
          return null;
        }
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  }
  async cleanExpiredTransactions(maxAgeMs = this.maxPendingAgeMs) {
    const expired = [];
    const now = Date.now();
    for (const [nonce, entry] of this.pending.entries()) {
      if (entry && entry.addedAt && now - entry.addedAt > maxAgeMs) {
        expired.push(nonce);
      }
    }
    for (const nonce of expired) {
      this.pending.delete(nonce);
      this.log('info', 'Cleaned expired transaction', { nonce });
    }
    if (expired.length > 0) {
      this.saveState();
      this.notifyPendingChange();
    }
    return expired.length;
  }
}
