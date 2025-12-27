export function normalizeAddress(address) {
  if (!address || typeof address !== 'string') return '';
  return address.toLowerCase();
}
export function normalizeAddressList(addresses) {
  if (!Array.isArray(addresses)) return [];
  return addresses.map(addr => normalizeAddress(addr)).filter(addr => addr.length > 0);
}
export function addressEquals(addr1, addr2) {
  return normalizeAddress(addr1) === normalizeAddress(addr2);
}
