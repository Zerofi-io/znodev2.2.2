import pino from 'pino';
const pinoOptions = {
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level: (label) => {
      return { level: label.toUpperCase() };
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
};
const shouldUsePretty = process.env.LOG_PRETTY !== '0';
if (shouldUsePretty) {
  pinoOptions.transport = {
    target: 'pino-pretty',
    options: {
      colorize: false,
      hideObject: true,
      ignore: 'pid,hostname,context,time,level',
      messageFormat: '{msg}',
      
    },
  };
}
const rootLogger = pino(pinoOptions);
const sensitiveKeys = [
  'password',
  'privatekey',
  'private_key',
  'secret',
  'mnemonic',
  'seed',
  'passphrase',
  'backup_passphrase',
  'wallet_backup_passphrase',
  'rpc_login',
  'login',
  'wallet_password',
  'monero_wallet_password',
  'rpc_user',
  'rpc_password',
  'monero_wallet_rpc_user',
  'monero_wallet_rpc_password',
  'key',
  'api_key',
  'apikey',
  'bearer',
  'authorization',
  'cookie',
];
const sensitivePatterns = ['token', 'auth', 'credential', 'pass'];
function redactSensitive(obj) {
  if (!obj || typeof obj !== 'object') {
    return obj;
  }
  const redacted = Array.isArray(obj) ? [...obj] : { ...obj };
  for (const key of Object.keys(redacted)) {
    const lowerKey = key.toLowerCase();
    const isExactMatch = sensitiveKeys.includes(lowerKey);
    const isPatternMatch = sensitivePatterns.some(
      (pattern) =>
        lowerKey === pattern || lowerKey.endsWith(pattern) || lowerKey.startsWith(pattern),
    );
    if (isExactMatch || isPatternMatch) {
      redacted[key] = '[REDACTED]';
    } else if (typeof redacted[key] === 'object' && redacted[key] !== null) {
      redacted[key] = redactSensitive(redacted[key]);
    }
  }
  return redacted;
}
class Logger {
  constructor(context = 'ZNode') {
    this.context = context;
    this.logger = rootLogger.child({ context });
    this.metrics = {
      debug: 0,
      info: 0,
      warn: 0,
      error: 0,
    };
    this.startTime = Date.now();
  }
  debug(message, meta = {}) {
    this.metrics.debug++;
    const redactedMeta = redactSensitive(meta);
    this.logger.debug(redactedMeta, message);
  }
  info(message, meta = {}) {
    this.metrics.info++;
    const redactedMeta = redactSensitive(meta);
    this.logger.info(redactedMeta, message);
  }
  warn(message, meta = {}) {
    this.metrics.warn++;
    const redactedMeta = redactSensitive(meta);
    this.logger.warn(redactedMeta, message);
  }
  error(message, error = null, meta = {}) {
    this.metrics.error++;
    const errorMeta = error
      ? {
          ...meta,
          error: error.message,
          stack: error.stack,
        }
      : meta;
    const redactedMeta = redactSensitive(errorMeta);
    this.logger.error(redactedMeta, message);
  }
  getMetrics() {
    const uptime = Math.floor((Date.now() - this.startTime) / 1000);
    return {
      context: this.context,
      uptime_seconds: uptime,
      log_counts: { ...this.metrics },
      total_logs: Object.values(this.metrics).reduce((a, b) => a + b, 0),
    };
  }
  logMetrics() {
    const metrics = this.getMetrics();
    this.info('Logger metrics', metrics);
  }
  createChild(childContext) {
    return new Logger(`${this.context}:${childContext}`);
  }
}
export default Logger;
