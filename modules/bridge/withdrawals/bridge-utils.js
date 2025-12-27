export function getBridgeAddressLower(node) {
  if (!node || !node.bridge) return null;
  const a = node.bridge.target || node.bridge.address;
  if (!a) return null;
  try { return String(a).toLowerCase(); } catch { return null; }
}
