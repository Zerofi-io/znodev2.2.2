const LOG_LEVELS = { error: 0, warn: 1, info: 2, debug: 3, trace: 4 };
const currentLevel = LOG_LEVELS[process.env.LOG_LEVEL?.toLowerCase()] ?? LOG_LEVELS.info;
const debugEnabled = process.env.DEBUG_CATCH === '1';
function formatMessage(level, component, message, meta) {
  const ts = new Date().toISOString();
  const metaStr = meta && Object.keys(meta).length > 0 ? ' ' + JSON.stringify(meta) : '';
  return `${ts} [${level.toUpperCase()}] [${component}] ${message}${metaStr}`;
}
const log = {
  error(component, message, meta) {
    if (currentLevel >= LOG_LEVELS.error) {
      console.error(formatMessage('error', component, message, meta));
    }
  },
  warn(component, message, meta) {
    if (currentLevel >= LOG_LEVELS.warn) {
      console.warn(formatMessage('warn', component, message, meta));
    }
  },
  info(component, message, meta) {
    if (currentLevel >= LOG_LEVELS.info) {
      console.log(formatMessage('info', component, message, meta));
    }
  },
  debug(component, message, meta) {
    if (currentLevel >= LOG_LEVELS.debug) {
      console.log(formatMessage('debug', component, message, meta));
    }
  },
  trace(component, message, meta) {
    if (currentLevel >= LOG_LEVELS.trace) {
      console.log(formatMessage('trace', component, message, meta));
    }
  },
  catchDebug(component, error, context) {
    if (debugEnabled && error) {
      const msg = error instanceof Error ? error.message : String(error);
      console.log(formatMessage('debug', component, `Caught: ${msg}`, { context }));
    }
  },
};
export default log;
export { log, LOG_LEVELS };
