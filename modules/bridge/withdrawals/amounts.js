export function formatXmrAtomic(atomic) {
  let v;
  try {
    v = typeof atomic === 'bigint' ? atomic : BigInt(atomic);
  } catch {
    return String(atomic);
  }
  const neg = v < 0n;
  if (neg) v = -v;
  let s = v.toString();
  if (s.length <= 12) s = s.padStart(13, '0');
  const i = s.slice(0, -12) || '0';
  const f = s.slice(-12).replace(/0+$/, '');
  return (neg ? '-' : '') + i + (f ? '.' + f : '');
}
export function parseAtomicAmount(v) {
  if (v == null) return null;
  if (typeof v === 'bigint') return v;
  if (typeof v === 'number') {
    if (!Number.isFinite(v) || !Number.isSafeInteger(v) || v < 0) return null;
    return BigInt(v);
  }
  if (typeof v === 'string') {
    const s = v.trim();
    if (!s) return null;
    try { return BigInt(s); } catch { return null; }
  }
  try { return BigInt(v); } catch { return null; }
}
