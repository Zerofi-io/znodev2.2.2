function getState(node) {
  if (!node._moneroLock || typeof node._moneroLock !== 'object') {
    node._moneroLock = { owner: null, queue: [] };
  }
  return node._moneroLock;
}
function normStr(v, fallback) {
  if (typeof v !== 'string') return fallback;
  const s = v.trim();
  return s ? s : fallback;
}
function normTimeoutMs(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.floor(n);
}
function makeAnonKey() {
  return `anon-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}
function setBusy(node, busy) {
  node._moneroSigningInProgress = !!busy;
}
async function acquire(node, { key, op, timeoutMs } = {}) {
  if (!node) throw new Error('monero_lock_no_node');
  const state = getState(node);
  const k = normStr(key, makeAnonKey());
  const o = normStr(op, 'monero');
  const owner = state.owner;
  if (!owner) {
    state.owner = { key: k, op: o, depth: 1, startedAt: Date.now() };
    setBusy(node, true);
    return { key: k };
  }
  if (owner.key === k) {
    owner.depth += 1;
    return { key: k };
  }
  const waitMs = normTimeoutMs(timeoutMs);
  return await new Promise((resolve, reject) => {
    const entry = { key: k, op: o, resolve, reject, timer: null };
    if (waitMs > 0) {
      entry.timer = setTimeout(() => {
        const q = state.queue;
        const idx = q.indexOf(entry);
        if (idx >= 0) q.splice(idx, 1);
        reject(new Error('monero_lock_timeout'));
      }, waitMs);
    }
    state.queue.push(entry);
  });
}
function release(node, token) {
  if (!node || !token || !token.key) return;
  const state = getState(node);
  const owner = state.owner;
  if (!owner || owner.key !== token.key) return;
  if (owner.depth > 1) {
    owner.depth -= 1;
    return;
  }
  const q = state.queue;
  while (q.length > 0) {
    const next = q.shift();
    if (!next) continue;
    if (next.timer) clearTimeout(next.timer);
    state.owner = { key: next.key, op: next.op, depth: 1, startedAt: Date.now() };
    setBusy(node, true);
    next.resolve({ key: next.key });
    return;
  }
  state.owner = null;
  setBusy(node, false);
}
export async function withMoneroLock(node, { key, op, timeoutMs } = {}, fn) {
  const token = await acquire(node, { key, op, timeoutMs });
  try {
    return await fn();
  } finally {
    release(node, token);
  }
}
