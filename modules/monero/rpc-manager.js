import path from 'path';
import fs from 'fs';
import { execFile } from 'child_process';
import { runtimeConfig } from '../core/runtime-config.js';
import { withRetry } from '../core/retry.js';
import { CircuitBreaker } from '../core/circuit-breaker.js';
class AsyncMutex {
  constructor() {
    this._lock = Promise.resolve();
  }
  async acquire() {
    let release;
    const newLock = new Promise((resolve) => { release = resolve; });
    const prevLock = this._lock;
    this._lock = newLock;
    await prevLock;
    return release;
  }
  async withLock(fn) {
    const release = await this.acquire();
    try { return await fn(); }
    finally { release(); }
  }
}
class RPCManager {
  constructor({ url, scriptPath, logger, onHealthFailure } = {}) {
    this.url = url;
    this._timer = null;
    this._logger = logger || null;
    this._onHealthFailure = onHealthFailure || null;
    this._restartMutex = new AsyncMutex();
    this._healthHistory = [];
    this._healthHistoryMax = 100;
    const defaultScript = path.join(process.cwd(), 'scripts', 'start-monero-rpc.sh');
    this.scriptPath = scriptPath || process.env.MONERO_RPC_START_SCRIPT || defaultScript;
    this._circuitBreaker = new CircuitBreaker('MoneroRPCManager', runtimeConfig.getCircuitBreakerConfig({
      failureThreshold: 3,
      resetTimeout: 60000,
      onStateChange: (e) => this._log('warn', `Circuit breaker: ${e.from} -> ${e.to}`, { reason: e.reason }),
    }));
  }
  _log(level, msg, meta = {}) {
    if (!this._logger) return;
    if (typeof this._logger === 'function') {
      this._logger(level, msg, meta);
    } else if (this._logger[level]) {
      this._logger[level]({ msg, ...meta });
    }
  }
  _validateScript(scriptPath) {
    if (!scriptPath) return { valid: false, reason: 'no_script' };
    try {
      const resolved = path.resolve(scriptPath);
      const stat = fs.statSync(resolved);
      if (!stat.isFile()) return { valid: false, reason: 'not_file' };
      const mode = stat.mode;
      const isExecutable = (mode & fs.constants.S_IXUSR) || (mode & fs.constants.S_IXGRP) || (mode & fs.constants.S_IXOTH);
      if (!isExecutable) return { valid: false, reason: 'not_executable' };
      return { valid: true, path: resolved };
    } catch (e) {
      return { valid: false, reason: 'not_found', error: e.message };
    }
  }
  async restart(monero, lastWallet) {
    return this._restartMutex.withLock(async () => {
      return this._doRestart(monero, lastWallet);
    });
  }
  async _doRestart(monero, lastWallet) {
    const result = { success: false, scriptRan: false, rpcReady: false, walletReopened: false, errors: [] };
    const scriptValidation = this._validateScript(this.scriptPath);
    if (!scriptValidation.valid) {
      this._log('warn', `No valid Monero RPC start script: ${scriptValidation.reason}`, { scriptPath: this.scriptPath });
    } else {
      const scriptResult = await this._runScript(scriptValidation.path);
      result.scriptRan = scriptResult.success;
      result.scriptOutput = scriptResult.output;
      if (!scriptResult.success) {
        result.errors.push({ phase: 'script', error: scriptResult.error });
      }
    }
    if (monero && typeof monero.call === 'function') {
      this._log('info', 'Waiting for Monero RPC to become ready...');
      const config = runtimeConfig.get('moneroRpc') || {};
      const maxRetries = config.readyRetries || Number(process.env.RPC_READY_RETRIES || 60);
      const baseDelay = config.readyIntervalMs || Number(process.env.RPC_READY_INTERVAL_MS || 1000);
      const readyResult = await withRetry(
        async () => {
          await monero.call('get_version', {}, 5000);
          return true;
        },
        { maxAttempts: maxRetries, baseDelayMs: baseDelay, maxDelayMs: 10000, label: 'RPC readiness' }
      );
      result.rpcReady = readyResult.success;
      if (!readyResult.success) {
        result.errors.push({ phase: 'readiness', error: readyResult.error?.message || 'RPC not ready' });
      } else {
        this._log('info', 'Monero RPC is ready');
      }
    }
    if (!result.scriptRan && monero && monero.url && !result.rpcReady) {
      const err = new Error('Monero RPC restart failed');
      err.details = result;
      throw err;
    }
    if (lastWallet && result.rpcReady) {
      const filename = typeof lastWallet === 'string' ? lastWallet : lastWallet.filename;
      const password = typeof lastWallet === 'object' && lastWallet.password != null
        ? lastWallet.password : process.env.MONERO_WALLET_PASSWORD || '';
      if (filename && monero && typeof monero.openWallet === 'function') {
        try {
          await monero.openWallet(filename, password);
          result.walletReopened = true;
          this._log('info', 'Reopened wallet after RPC restart', { wallet: filename });
        } catch (e) {
          result.errors.push({ phase: 'wallet', error: e.message });
          this._log('error', 'Failed to reopen wallet', { error: e.message });
        }
      }
    }
    result.success = result.rpcReady || result.scriptRan;
    return result;
  }
  async _runScript(scriptPath) {
    return new Promise((resolve) => {
      const stdout = [];
      const stderr = [];
      const child = execFile('bash', [scriptPath], { timeout: 30000 });
      if (child.stdout) child.stdout.on('data', (d) => stdout.push(d));
      if (child.stderr) child.stderr.on('data', (d) => stderr.push(d));
      const timeout = setTimeout(() => {
        try { child.kill('SIGTERM'); } catch {}
        this._log('error', 'Monero RPC start script timed out');
        resolve({ success: false, error: 'timeout', output: { stdout: stdout.join(''), stderr: stderr.join('') } });
      }, 30000);
      child.on('exit', (code) => {
        clearTimeout(timeout);
        const output = { stdout: stdout.join(''), stderr: stderr.join('') };
        if (code === 0) {
          this._log('info', 'Monero wallet RPC restart script completed');
          resolve({ success: true, output });
        } else {
          this._log('error', `Monero wallet RPC restart script exited with code ${code}`);
          resolve({ success: false, error: `exit_code_${code}`, output });
        }
      });
      child.on('error', (err) => {
        clearTimeout(timeout);
        this._log('error', 'Failed to execute Monero RPC start script', { error: err.message });
        resolve({ success: false, error: err.message, output: { stdout: stdout.join(''), stderr: stderr.join('') } });
      });
    });
  }
  startHealthWatch(monero) {
    if (this._timer) return;
    if (!monero || typeof monero.call !== 'function') {
      this._log('warn', 'No Monero RPC client configured for health watch');
      return;
    }
    const config = runtimeConfig.get('moneroRpc') || {};
    const intervalMs = config.healthIntervalMs || Number(process.env.RPC_HEALTH_INTERVAL_MS || 60000);
    const timeoutMs = config.healthTimeoutMs || Number(process.env.RPC_HEALTH_TIMEOUT_MS || 30000);
    this._timer = setInterval(async () => {
      await this._performHealthCheck(monero, timeoutMs);
    }, intervalMs);
  }
  stopHealthWatch() {
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }
  async _performHealthCheck(monero, timeoutMs) {
    const startTime = Date.now();
    const entry = { timestamp: startTime, success: false, latencyMs: 0, error: null };
    try {
      await this._circuitBreaker.execute(async () => {
        await monero.call('get_version', {}, timeoutMs);
      });
      entry.success = true;
      entry.latencyMs = Date.now() - startTime;
    } catch (e) {
      entry.latencyMs = Date.now() - startTime;
      entry.error = e.message || String(e);
      this._log('error', 'Monero RPC health check failed', { error: entry.error, latencyMs: entry.latencyMs });
      if (this._onHealthFailure) {
        try {
          await this._onHealthFailure({ error: e, entry, circuitBreakerState: this._circuitBreaker.getState() });
        } catch {}
      }
    }
    this._healthHistory.push(entry);
    if (this._healthHistory.length > this._healthHistoryMax) {
      this._healthHistory.shift();
    }
  }
  getHealthHistory() {
    return [...this._healthHistory];
  }
  getHealthSummary() {
    if (this._healthHistory.length === 0) return { checks: 0, successRate: 0, avgLatencyMs: 0 };
    const successful = this._healthHistory.filter((e) => e.success);
    const avgLatency = successful.length > 0
      ? successful.reduce((sum, e) => sum + e.latencyMs, 0) / successful.length
      : 0;
    return {
      checks: this._healthHistory.length,
      successRate: successful.length / this._healthHistory.length,
      avgLatencyMs: Math.round(avgLatency),
      lastCheck: this._healthHistory[this._healthHistory.length - 1],
      circuitBreakerState: this._circuitBreaker.getState(),
    };
  }
  getCircuitBreaker() {
    return this._circuitBreaker;
  }
}
export default RPCManager;
