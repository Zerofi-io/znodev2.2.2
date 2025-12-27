function parseEnvInt(key, defaultValue, min = null, max = null) {
  const raw = process.env[key];
  if (raw == null || raw === '') return defaultValue;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) {
    console.warn(`[Config] Invalid integer for ${key}="${raw}", using default ${defaultValue}`);
    return defaultValue;
  }
  let value = parsed;
  if (min !== null && value < min) {
    console.warn(`[Config] ${key}=${value} below minimum ${min}, clamping`);
    value = min;
  }
  if (max !== null && value > max) {
    console.warn(`[Config] ${key}=${value} above maximum ${max}, clamping`);
    value = max;
  }
  return value;
}
function parseEnvFloat(key, defaultValue, min = null, max = null) {
  const raw = process.env[key];
  if (raw == null || raw === '') return defaultValue;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    console.warn(`[Config] Invalid number for ${key}="${raw}", using default ${defaultValue}`);
    return defaultValue;
  }
  let value = parsed;
  if (min !== null && value < min) {
    console.warn(`[Config] ${key}=${value} below minimum ${min}, clamping`);
    value = min;
  }
  if (max !== null && value > max) {
    console.warn(`[Config] ${key}=${value} above maximum ${max}, clamping`);
    value = max;
  }
  return value;
}
function parseEnvBigInt(key, defaultValue) {
  const raw = process.env[key];
  if (raw == null || raw === '') return defaultValue;
  try {
    const trimmed = raw.trim();
    if (!/^-?\d+$/.test(trimmed)) {
      console.warn(`[Config] Invalid bigint for ${key}="${raw}", using default`);
      return defaultValue;
    }
    return BigInt(trimmed);
  } catch (e) {
    console.warn(`[Config] Failed to parse bigint for ${key}="${raw}": ${e.message}, using default`);
    return defaultValue;
  }
}
function parseEnvBoolean(key, defaultValue) {
  const raw = process.env[key];
  if (raw == null || raw === '') return defaultValue;
  const lower = raw.toLowerCase().trim();
  if (lower === '1' || lower === 'true' || lower === 'yes' || lower === 'on') {
    return true;
  }
  if (lower === '0' || lower === 'false' || lower === 'no' || lower === 'off') {
    return false;
  }
  console.warn(`[Config] Invalid boolean for ${key}="${raw}", using default ${defaultValue}`);
  return defaultValue;
}
function parseEnvString(key, defaultValue, allowedValues = null) {
  const raw = process.env[key];
  if (raw == null || raw === '') return defaultValue;
  const value = raw.trim();
  if (allowedValues && !allowedValues.includes(value)) {
    console.warn(`[Config] Invalid value for ${key}="${value}", allowed: ${allowedValues.join(', ')}, using default ${defaultValue}`);
    return defaultValue;
  }
  return value;
}
export {
  parseEnvInt,
  parseEnvFloat,
  parseEnvBigInt,
  parseEnvBoolean,
  parseEnvString,
};
