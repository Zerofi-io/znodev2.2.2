import fs from 'fs';

export async function loadDowntimeSeen(node) {
  if (!node || !node._downtimeSeenPath) return;
  if (node._downtimeSeen) return;
  node._downtimeSeen = new Map();
  try {
    const exists = await fs.promises
      .access(node._downtimeSeenPath)
      .then(() => true)
      .catch(() => false);
    if (!exists) return;
    const raw = await fs.promises.readFile(node._downtimeSeenPath, 'utf8');
    const parsed = JSON.parse(raw);
    const seen = parsed && typeof parsed === 'object' ? parsed.seen : null;
    if (!seen || typeof seen !== 'object') return;
    for (const [addr, ts] of Object.entries(seen)) {
      const k = String(addr || '').toLowerCase();
      const v = Number(ts);
      if (!k || !Number.isFinite(v) || v <= 0) continue;
      node._downtimeSeen.set(k, v);
    }
  } catch {}
}

export function saveDowntimeSeen(node) {
  if (!node || !node._downtimeSeenPath || !node._downtimeSeen) return;
  if (node._downtimeSeenSavePending) {
    node._downtimeSeenSaveQueued = true;
    return;
  }
  node._downtimeSeenSavePending = true;
  node._downtimeSeenSaveQueued = false;
  setImmediate(() => {
    try {
      const obj = {};
      const MAX_RECORDS = 5000;
      let count = 0;
      for (const [k, v] of node._downtimeSeen.entries()) {
        if (count >= MAX_RECORDS) break;
        if (!k || !Number.isFinite(v) || v <= 0) continue;
        obj[k] = v;
        count++;
      }
      const data = {
        seen: obj,
        updated: new Date().toISOString(),
      };
      const tmpPath = node._downtimeSeenPath + '.tmp';
      fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2), { mode: 0o600 });
      fs.renameSync(tmpPath, node._downtimeSeenPath);
    } catch {} finally {
      node._downtimeSeenSavePending = false;
      if (node._downtimeSeenSaveQueued) {
        saveDowntimeSeen(node);
      }
    }
  });
}

export default { loadDowntimeSeen, saveDowntimeSeen };
