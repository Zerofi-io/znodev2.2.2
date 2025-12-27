class LRUCache {
  constructor(maxSize) {
    this.maxSize = maxSize > 0 ? maxSize : 1000;
    this.cache = new Map();
  }
  get(key) {
    if (!this.cache.has(key)) return undefined;
    const value = this.cache.get(key);
    this.cache.delete(key);
    this.cache.set(key, value);
    return value;
  }
  set(key, value) {
    if (this.cache.has(key)) {
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      const oldest = this.cache.keys().next().value;
      this.cache.delete(oldest);
    }
    this.cache.set(key, value);
    return this;
  }
  has(key) {
    return this.cache.has(key);
  }
  delete(key) {
    return this.cache.delete(key);
  }
  clear() {
    this.cache.clear();
  }
  get size() {
    return this.cache.size;
  }
  entries() {
    return this.cache.entries();
  }
  keys() {
    return this.cache.keys();
  }
  values() {
    return this.cache.values();
  }
  [Symbol.iterator]() {
    return this.cache[Symbol.iterator]();
  }
}
export default LRUCache;
export { LRUCache };
