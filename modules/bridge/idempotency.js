const DEFAULT_KEY_TTL_MS = Number(process.env.IDEMPOTENCY_KEY_TTL_MS || 3600000);
const DEFAULT_MAX_KEYS = Number(process.env.IDEMPOTENCY_MAX_KEYS || 10000);
const DEFAULT_CLEANUP_INTERVAL_MS = Number(process.env.IDEMPOTENCY_CLEANUP_INTERVAL_MS || 60000);
export class IdempotencyTracker {
  constructor(options = {}) {
    this.ttlMs = options.ttlMs || DEFAULT_KEY_TTL_MS;
    this.maxKeys = options.maxKeys || DEFAULT_MAX_KEYS;
    this.cleanupIntervalMs = options.cleanupIntervalMs || DEFAULT_CLEANUP_INTERVAL_MS;
    this.keys = new Map();
    this.cleanupTimer = null;
    this._startCleanup();
  }
  _startCleanup() {
    if (this.cleanupTimer) return;
    this.cleanupTimer = setInterval(() => this._cleanup(), this.cleanupIntervalMs);
    if (this.cleanupTimer.unref) this.cleanupTimer.unref();
  }
  _cleanup() {
    const now = Date.now();
    const expired = [];
    for (const [key, entry] of this.keys) {
      if (now - entry.timestamp > this.ttlMs) expired.push(key);
    }
    for (const key of expired) this.keys.delete(key);
    if (this.keys.size > this.maxKeys) {
      const sorted = [...this.keys.entries()].sort((a, b) => a[1].timestamp - b[1].timestamp);
      const excess = this.keys.size - this.maxKeys;
      for (let i = 0; i < excess; i++) this.keys.delete(sorted[i][0]);
    }
  }
  generateKey(operation, ...params) {
    const parts = [operation, ...params.map(p => {
      if (p === null || p === undefined) return '';
      if (typeof p === 'object') return JSON.stringify(p);
      return String(p);
    })];
    return parts.join(':');
  }
  check(key) {
    const entry = this.keys.get(key);
    if (!entry) return null;
    if (Date.now() - entry.timestamp > this.ttlMs) {
      this.keys.delete(key);
      return null;
    }
    return entry;
  }
  tryAcquire(key, metadata = {}) {
    const existing = this.check(key);
    if (existing) return { acquired: false, existing };
    const entry = { key, timestamp: Date.now(), status: 'pending', metadata, result: null };
    this.keys.set(key, entry);
    return { acquired: true, entry };
  }
  complete(key, result, status = 'completed') {
    const entry = this.keys.get(key);
    if (!entry) return false;
    entry.status = status;
    entry.result = result;
    entry.completedAt = Date.now();
    return true;
  }
  fail(key, error) {
    const entry = this.keys.get(key);
    if (!entry) return false;
    entry.status = 'failed';
    entry.error = error instanceof Error ? error.message : String(error);
    entry.failedAt = Date.now();
    return true;
  }
  release(key) {
    return this.keys.delete(key);
  }
  clear() {
    this.keys.clear();
  }
  stats() {
    let pending = 0, completed = 0, failed = 0;
    for (const entry of this.keys.values()) {
      if (entry.status === 'pending') pending++;
      else if (entry.status === 'completed') completed++;
      else if (entry.status === 'failed') failed++;
    }
    return { total: this.keys.size, pending, completed, failed, maxKeys: this.maxKeys, ttlMs: this.ttlMs };
  }
  destroy() {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    this.keys.clear();
  }
}
export async function withIdempotency(tracker, key, operation, options = {}) {
  const { metadata = {}, throwOnDuplicate = false } = options;
  const acquisition = tracker.tryAcquire(key, metadata);
  if (!acquisition.acquired) {
    const existing = acquisition.existing;
    if (existing.status === 'completed' && existing.result !== null) return existing.result;
    if (existing.status === 'pending') {
      if (throwOnDuplicate) throw new Error(`Duplicate operation in progress: ${key}`);
      return null;
    }
  }
  try {
    const result = await operation();
    tracker.complete(key, result);
    return result;
  } catch (err) {
    tracker.fail(key, err);
    throw err;
  }
}
const defaultTracker = new IdempotencyTracker();
export function getDefaultTracker() {
  return defaultTracker;
}
export default { IdempotencyTracker, withIdempotency, getDefaultTracker };
