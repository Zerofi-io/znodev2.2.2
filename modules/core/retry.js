const DEFAULT_RETRYABLE_PATTERNS = [
  /ECONNREFUSED/i,
  /ETIMEDOUT/i,
  /ECONNRESET/i,
  /EHOSTUNREACH/i,
  /ENETUNREACH/i,
  /timeout/i,
  /rate.?limit/i,
  /too many requests/i,
  /429/,
  /503/,
  /502/,
  /504/,
  /temporary/i,
  /retry/i,
];
const DEFAULT_NON_RETRYABLE_PATTERNS = [
  /unauthorized/i,
  /forbidden/i,
  /not.?found/i,
  /invalid.*argument/i,
  /401/,
  /403/,
  /404/,
];
function isRetryableError(error, retryablePatterns, nonRetryablePatterns) {
  const msg = error && error.message ? error.message : String(error);
  for (const pattern of nonRetryablePatterns) {
    if (pattern.test(msg)) return false;
  }
  for (const pattern of retryablePatterns) {
    if (pattern.test(msg)) return true;
  }
  return true;
}
function addJitter(baseMs, jitterFactor = 0.2) {
  const jitter = baseMs * jitterFactor * (Math.random() * 2 - 1);
  return Math.max(0, Math.floor(baseMs + jitter));
}
class RetryResult {
  constructor(success, value, error, attempts, totalTimeMs) {
    this.success = success;
    this.value = value;
    this.error = error;
    this.attempts = attempts;
    this.totalTimeMs = totalTimeMs;
  }
  static ok(value, attempts, totalTimeMs) {
    return new RetryResult(true, value, null, attempts, totalTimeMs);
  }
  static fail(error, attempts, totalTimeMs) {
    return new RetryResult(false, null, error, attempts, totalTimeMs);
  }
}
async function withRetry(fn, options = {}) {
  const maxAttempts = options.maxAttempts || 3;
  const baseDelayMs = options.baseDelayMs || 1000;
  const maxDelayMs = options.maxDelayMs || 60000;
  const maxTotalTimeMs = options.maxTotalTimeMs || 0;
  const jitterFactor = options.jitterFactor != null ? options.jitterFactor : 0.2;
  const label = options.label || 'operation';
  const cleanup = options.cleanup;
  const signal = options.signal;
  const logger = options.logger || null;
  const retryablePatterns = options.retryablePatterns || DEFAULT_RETRYABLE_PATTERNS;
  const nonRetryablePatterns = options.nonRetryablePatterns || DEFAULT_NON_RETRYABLE_PATTERNS;
  const shouldRetry = options.shouldRetry || null;
  const onRetry = options.onRetry || null;
  const log = (level, msg, meta) => {
    if (!logger) return;
    if (typeof logger === 'function') {
      logger(level, msg, meta);
    } else if (logger[level]) {
      logger[level](meta ? { msg, ...meta } : msg);
    }
  };
  const startTime = Date.now();
  let lastError = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    if (signal && signal.aborted) {
      const abortError = new Error(`${label} aborted`);
      return RetryResult.fail(abortError, attempt - 1, Date.now() - startTime);
    }
    if (maxTotalTimeMs > 0 && Date.now() - startTime >= maxTotalTimeMs) {
      const timeoutError = new Error(`${label} exceeded max total time of ${maxTotalTimeMs}ms`);
      return RetryResult.fail(timeoutError, attempt - 1, Date.now() - startTime);
    }
    log('info', `${label} attempt ${attempt}/${maxAttempts}`, { attempt, maxAttempts, label });
    try {
      const result = await fn(attempt);
      return RetryResult.ok(result, attempt, Date.now() - startTime);
    } catch (e) {
      lastError = e;
      const elapsed = Date.now() - startTime;
      log('warn', `${label} attempt ${attempt} failed: ${e.message || e}`, { attempt, error: e.message || String(e), elapsed });
      const canRetry = shouldRetry
        ? shouldRetry(e, attempt)
        : isRetryableError(e, retryablePatterns, nonRetryablePatterns);
      if (!canRetry) {
        log('error', `${label} non-retryable error`, { error: e.message || String(e) });
        return RetryResult.fail(e, attempt, elapsed);
      }
      if (attempt < maxAttempts) {
        const rawDelay = Math.min(baseDelayMs * Math.pow(2, attempt - 1), maxDelayMs);
        const delay = addJitter(rawDelay, jitterFactor);
        if (maxTotalTimeMs > 0 && elapsed + delay >= maxTotalTimeMs) {
          return RetryResult.fail(new Error(`${label} would exceed max total time`), attempt, elapsed);
        }
        log('info', `${label} waiting ${delay}ms before retry`, { delay, attempt });
        if (onRetry) {
          try {
            onRetry({ attempt, delay, error: e, elapsed });
          } catch {}
        }
        await new Promise((resolve, reject) => {
          let timer = null;
          const onAbort = () => {
            if (timer) clearTimeout(timer);
            reject(new Error(`${label} aborted during delay`));
          };
          timer = setTimeout(() => {
            if (signal) signal.removeEventListener('abort', onAbort);
            resolve();
          }, delay);
          if (signal) {
            signal.addEventListener('abort', onAbort, { once: true });
          }
        }).catch((abortErr) => {
          lastError = abortErr;
        });
        if (signal && signal.aborted) {
          return RetryResult.fail(lastError, attempt, Date.now() - startTime);
        }
        if (typeof cleanup === 'function') {
          try {
            await cleanup(e, attempt);
          } catch (cleanupErr) {
            log('warn', `${label} cleanup error: ${cleanupErr.message || cleanupErr}`, { cleanupError: cleanupErr.message || String(cleanupErr) });
            if (options.throwOnCleanupError) {
              return RetryResult.fail(cleanupErr, attempt, Date.now() - startTime);
            }
          }
        }
      }
    }
  }
  return RetryResult.fail(lastError, maxAttempts, Date.now() - startTime);
}
async function withRetrySimple(fn, options = {}) {
  const result = await withRetry(fn, options);
  if (result.success) return result.value;
  throw result.error;
}
export { withRetry, withRetrySimple, RetryResult, isRetryableError, addJitter, DEFAULT_RETRYABLE_PATTERNS, DEFAULT_NON_RETRYABLE_PATTERNS };
