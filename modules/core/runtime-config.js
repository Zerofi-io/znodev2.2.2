const DEFAULT_CIRCUIT_BREAKER_CONFIG = {
  failureThreshold: 5,
  successThreshold: 2,
  timeout: 60000,
  resetTimeout: 30000,
  halfOpenMaxCalls: 3,
  jitterFactor: 0.2,
};
const DEFAULT_RETRY_CONFIG = {
  maxAttempts: 3,
  baseDelayMs: 1000,
  maxDelayMs: 60000,
  maxTotalTimeMs: 180000,
  jitterFactor: 0.2,
};
const DEFAULT_RPC_CIRCUIT_BREAKER_CONFIG = {
  windowMs: 300000,
  maxFailures: 10,
  cooldownMs: 60000,
  waitOnCall: false,
};
class RuntimeConfig {
  constructor() {
    this._config = {
      TEST_MODE: false,
      DRY_RUN: false,
      EFFECTIVE_RPC_URL: null,
      usedDemoRpc: false,
      logLevel: 'info',
      circuitBreaker: { ...DEFAULT_CIRCUIT_BREAKER_CONFIG },
      retry: { ...DEFAULT_RETRY_CONFIG },
      rpcCircuitBreaker: { ...DEFAULT_RPC_CIRCUIT_BREAKER_CONFIG },
      featureFlags: {},
    };
    this._subscribers = new Set();
    this._initialized = false;
  }
  init(env) {
    if (this._initialized) return this;
    const TEST_MODE = env.TEST_MODE === '1';
    const DRY_RUN = TEST_MODE ? env.DRY_RUN !== '0' : env.DRY_RUN === '1';
    let EFFECTIVE_RPC_URL = null;
    let usedDemoRpc = false;
    if (env.RPC_URL || env.ETH_RPC_URL) {
      EFFECTIVE_RPC_URL = env.RPC_URL || env.ETH_RPC_URL;
    } else if (TEST_MODE) {
      EFFECTIVE_RPC_URL = 'https://eth-sepolia.g.alchemy.com/v2/demo';
      usedDemoRpc = true;
    }
    this._config.TEST_MODE = TEST_MODE;
    this._config.DRY_RUN = DRY_RUN;
    this._config.EFFECTIVE_RPC_URL = EFFECTIVE_RPC_URL;
    this._config.usedDemoRpc = usedDemoRpc;
    this._config.logLevel = env.LOG_LEVEL || 'info';
    this._config.circuitBreaker = {
      failureThreshold: this._parseInt(env.CIRCUIT_BREAKER_FAILURE_THRESHOLD, DEFAULT_CIRCUIT_BREAKER_CONFIG.failureThreshold),
      successThreshold: this._parseInt(env.CIRCUIT_BREAKER_SUCCESS_THRESHOLD, DEFAULT_CIRCUIT_BREAKER_CONFIG.successThreshold),
      timeout: this._parseInt(env.CIRCUIT_BREAKER_TIMEOUT_MS, DEFAULT_CIRCUIT_BREAKER_CONFIG.timeout),
      resetTimeout: this._parseInt(env.CIRCUIT_BREAKER_RESET_TIMEOUT_MS, DEFAULT_CIRCUIT_BREAKER_CONFIG.resetTimeout),
      halfOpenMaxCalls: this._parseInt(env.CIRCUIT_BREAKER_HALF_OPEN_MAX_CALLS, DEFAULT_CIRCUIT_BREAKER_CONFIG.halfOpenMaxCalls),
      jitterFactor: this._parseFloat(env.CIRCUIT_BREAKER_JITTER_FACTOR, DEFAULT_CIRCUIT_BREAKER_CONFIG.jitterFactor),
    };
    this._config.retry = {
      maxAttempts: this._parseInt(env.RETRY_MAX_ATTEMPTS, DEFAULT_RETRY_CONFIG.maxAttempts),
      baseDelayMs: this._parseInt(env.RETRY_BASE_DELAY_MS, DEFAULT_RETRY_CONFIG.baseDelayMs),
      maxDelayMs: this._parseInt(env.RETRY_MAX_DELAY_MS, DEFAULT_RETRY_CONFIG.maxDelayMs),
      maxTotalTimeMs: this._parseInt(env.RETRY_MAX_TOTAL_TIME_MS, DEFAULT_RETRY_CONFIG.maxTotalTimeMs),
      jitterFactor: this._parseFloat(env.RETRY_JITTER_FACTOR, DEFAULT_RETRY_CONFIG.jitterFactor),
    };
    this._config.rpcCircuitBreaker = {
      windowMs: this._parseInt(env.RPC_CIRCUIT_BREAKER_WINDOW_MS, DEFAULT_RPC_CIRCUIT_BREAKER_CONFIG.windowMs),
      maxFailures: this._parseInt(env.RPC_CIRCUIT_BREAKER_MAX_FAILURES, DEFAULT_RPC_CIRCUIT_BREAKER_CONFIG.maxFailures),
      cooldownMs: this._parseInt(env.RPC_CIRCUIT_BREAKER_COOLDOWN_MS, DEFAULT_RPC_CIRCUIT_BREAKER_CONFIG.cooldownMs),
      waitOnCall: env.RPC_CIRCUIT_BREAKER_WAIT_ON_CALL === '1',
    };
    if (env.FEATURE_FLAGS) {
      try {
        this._config.featureFlags = JSON.parse(env.FEATURE_FLAGS);
      } catch {}
    }
    this._initialized = true;
    return this;
  }
  _parseInt(value, defaultValue) {
    if (value == null || value === '') return defaultValue;
    const parsed = parseInt(value, 10);
    return isNaN(parsed) ? defaultValue : parsed;
  }
  _parseFloat(value, defaultValue) {
    if (value == null || value === '') return defaultValue;
    const parsed = parseFloat(value);
    return isNaN(parsed) ? defaultValue : parsed;
  }
  get(key) {
    if (key == null) return { ...this._config };
    return this._config[key];
  }
  set(key, value) {
    const oldValue = this._config[key];
    if (oldValue === value) return;
    this._config[key] = value;
    this._notify(key, value, oldValue);
  }
  update(updates) {
    for (const [key, value] of Object.entries(updates)) {
      this.set(key, value);
    }
  }
  subscribe(callback) {
    this._subscribers.add(callback);
    return () => this._subscribers.delete(callback);
  }
  _notify(key, newValue, oldValue) {
    for (const callback of this._subscribers) {
      try {
        callback({ key, newValue, oldValue });
      } catch {}
    }
  }
  isFeatureEnabled(flag) {
    return !!this._config.featureFlags[flag];
  }
  setFeatureFlag(flag, enabled) {
    const flags = { ...this._config.featureFlags };
    flags[flag] = enabled;
    this.set('featureFlags', flags);
  }
  getCircuitBreakerConfig(overrides = {}) {
    return { ...this._config.circuitBreaker, ...overrides };
  }
  getRetryConfig(overrides = {}) {
    return { ...this._config.retry, ...overrides };
  }
  getRpcCircuitBreakerConfig() {
    return { ...this._config.rpcCircuitBreaker };
  }
}
const runtimeConfig = new RuntimeConfig();
function resolveRuntimeConfig(env) {
  runtimeConfig.init(env);
  return {
    TEST_MODE: runtimeConfig.get('TEST_MODE'),
    DRY_RUN: runtimeConfig.get('DRY_RUN'),
    EFFECTIVE_RPC_URL: runtimeConfig.get('EFFECTIVE_RPC_URL'),
    usedDemoRpc: runtimeConfig.get('usedDemoRpc'),
  };
}
export { resolveRuntimeConfig, runtimeConfig, RuntimeConfig, DEFAULT_CIRCUIT_BREAKER_CONFIG, DEFAULT_RETRY_CONFIG, DEFAULT_RPC_CIRCUIT_BREAKER_CONFIG };
