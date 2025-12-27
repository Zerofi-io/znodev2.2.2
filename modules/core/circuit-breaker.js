const STATES = {
  CLOSED: 'CLOSED',
  OPEN: 'OPEN',
  HALF_OPEN: 'HALF_OPEN',
};
const ERROR_TYPES = {
  TRANSIENT: 'transient',
  PERMANENT: 'permanent',
  UNKNOWN: 'unknown',
};
const DEFAULT_TRANSIENT_PATTERNS = [
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
];
const DEFAULT_PERMANENT_PATTERNS = [
  /unauthorized/i,
  /forbidden/i,
  /not.?found/i,
  /invalid/i,
  /401/,
  /403/,
  /404/,
];
function classifyError(error, transientPatterns, permanentPatterns) {
  const msg = error && error.message ? error.message : String(error);
  for (const pattern of permanentPatterns) {
    if (pattern.test(msg)) return ERROR_TYPES.PERMANENT;
  }
  for (const pattern of transientPatterns) {
    if (pattern.test(msg)) return ERROR_TYPES.TRANSIENT;
  }
  return ERROR_TYPES.UNKNOWN;
}
function addJitter(baseMs, jitterFactor = 0.2) {
  const jitter = baseMs * jitterFactor * (Math.random() * 2 - 1);
  return Math.max(0, Math.floor(baseMs + jitter));
}
class CircuitBreaker {
  constructor(name, options = {}) {
    this.name = name;
    this.state = STATES.CLOSED;
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = null;
    this.nextAttemptTime = null;
    this.lastError = null;
    this.failureThreshold = options.failureThreshold || 5;
    this.successThreshold = options.successThreshold || 2;
    this.timeout = options.timeout || 60000;
    this.resetTimeout = options.resetTimeout || 30000;
    this.halfOpenMaxCalls = options.halfOpenMaxCalls || 3;
    this.halfOpenCalls = 0;
    this.jitterFactor = options.jitterFactor != null ? options.jitterFactor : 0.2;
    this.onlyTransientTrips = options.onlyTransientTrips != null ? options.onlyTransientTrips : true;
    this.transientPatterns = options.transientPatterns || DEFAULT_TRANSIENT_PATTERNS;
    this.permanentPatterns = options.permanentPatterns || DEFAULT_PERMANENT_PATTERNS;
    this._onStateChange = options.onStateChange || null;
    this._onFailure = options.onFailure || null;
    this._onSuccess = options.onSuccess || null;
  }
  isOpen() {
    return this.state === STATES.OPEN;
  }
  isHalfOpen() {
    return this.state === STATES.HALF_OPEN;
  }
  isClosed() {
    return this.state === STATES.CLOSED;
  }
  _setState(newState, reason) {
    if (this.state === newState) return;
    const oldState = this.state;
    this.state = newState;
    if (this._onStateChange) {
      try {
        this._onStateChange({ name: this.name, from: oldState, to: newState, reason });
      } catch {}
    }
  }
  async execute(fn, fallback = null) {
    if (this.state === STATES.OPEN) {
      const now = Date.now();
      if (now < this.nextAttemptTime) {
        if (fallback) return fallback();
        const remainingMs = this.nextAttemptTime - now;
        throw new Error(`Circuit breaker '${this.name}' is OPEN. Retry in ${Math.ceil(remainingMs / 1000)}s`);
      }
      this._setState(STATES.HALF_OPEN, 'timeout_expired');
      this.successCount = 0;
      this.halfOpenCalls = 0;
    }
    if (this.state === STATES.HALF_OPEN && this.halfOpenCalls >= this.halfOpenMaxCalls) {
      if (fallback) return fallback();
      throw new Error(`Circuit breaker '${this.name}' half-open limit reached`);
    }
    let timer = null;
    try {
      if (this.state === STATES.HALF_OPEN) this.halfOpenCalls++;
      const timeoutPromise = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Circuit breaker timeout: ${this.timeout}ms`)), this.timeout);
      });
      const result = await Promise.race([fn(), timeoutPromise]);
      clearTimeout(timer);
      this._handleSuccess();
      return result;
    } catch (error) {
      if (timer) clearTimeout(timer);
      this._handleFailure(error);
      throw error;
    }
  }
  async call(fn, fallback = null) {
    return this.execute(fn, fallback);
  }
  _handleSuccess() {
    this.failureCount = 0;
    if (this._onSuccess) {
      try {
        this._onSuccess({ name: this.name, state: this.state });
      } catch {}
    }
    if (this.state === STATES.HALF_OPEN) {
      this.successCount++;
      if (this.successCount >= this.successThreshold) {
        this._setState(STATES.CLOSED, 'recovery_success');
        this.successCount = 0;
        this.halfOpenCalls = 0;
      }
    }
  }
  _handleFailure(error) {
    this.lastFailureTime = Date.now();
    this.lastError = error;
    this.failureCount++;
    const errorType = classifyError(error, this.transientPatterns, this.permanentPatterns);
    if (this._onFailure) {
      try {
        this._onFailure({ name: this.name, error, errorType, failureCount: this.failureCount });
      } catch {}
    }
    if (this.onlyTransientTrips && errorType === ERROR_TYPES.PERMANENT) {
      return;
    }
    if (this.state === STATES.HALF_OPEN) {
      this._trip('half_open_failure');
    } else if (this.failureCount >= this.failureThreshold) {
      this._trip('threshold_exceeded');
    }
  }
  _trip(reason) {
    this._setState(STATES.OPEN, reason);
    const resetMs = addJitter(this.resetTimeout, this.jitterFactor);
    this.nextAttemptTime = Date.now() + resetMs;
    this.successCount = 0;
    this.halfOpenCalls = 0;
  }
  getState() {
    return {
      name: this.name,
      state: this.state,
      failureCount: this.failureCount,
      successCount: this.successCount,
      lastFailureTime: this.lastFailureTime,
      nextAttemptTime: this.nextAttemptTime,
      halfOpenCalls: this.halfOpenCalls,
    };
  }
  reset() {
    this._setState(STATES.CLOSED, 'manual_reset');
    this.failureCount = 0;
    this.successCount = 0;
    this.lastFailureTime = null;
    this.nextAttemptTime = null;
    this.halfOpenCalls = 0;
    this.lastError = null;
  }
  forceOpen(durationMs = null) {
    this._setState(STATES.OPEN, 'forced_open');
    const resetMs = durationMs != null ? durationMs : this.resetTimeout;
    this.nextAttemptTime = Date.now() + addJitter(resetMs, this.jitterFactor);
  }
  forceClose() {
    this._setState(STATES.CLOSED, 'forced_close');
    this.failureCount = 0;
    this.successCount = 0;
    this.halfOpenCalls = 0;
  }
}
function createCircuitBreaker(name, options) {
  return new CircuitBreaker(name, options);
}
export default CircuitBreaker;
export { CircuitBreaker, createCircuitBreaker, classifyError, addJitter, STATES, ERROR_TYPES, DEFAULT_TRANSIENT_PATTERNS, DEFAULT_PERMANENT_PATTERNS };
