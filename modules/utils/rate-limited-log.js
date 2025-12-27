import metrics from '../core/metrics.js';
const RATE_LIMITED_LOG_INTERVAL_MS = Number(process.env.RATE_LIMITED_LOG_INTERVAL_MS || 60000);
const lastLogAt = new Map();
export function logSwallowedError(key, err, labels = {}) {
  const k = typeof key === 'string' && key ? key : 'unknown';
  metrics.incrementCounter('swallowed_errors_total', 1, { key: k, ...labels });
  const now = Date.now();
  const last = lastLogAt.get(k) || 0;
  if (now - last < RATE_LIMITED_LOG_INTERVAL_MS) return;
  lastLogAt.set(k, now);
  const msg = err && err.message ? err.message : String(err);
  console.log(`[WARN] ${k}: ${msg}`);
}
