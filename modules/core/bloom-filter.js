function hash1(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) - h + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}
function hash2(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) + h + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}
function hash3(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = (h * 31 + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}
export class BloomFilter {
  constructor(options = {}) {
    this.size = options.size || 65536;
    this.bucketMs = options.bucketMs || 60000;
    this.maxBuckets = options.maxBuckets || 5;
    this.buckets = new Map();
  }
  _getBucketKey() {
    return Math.floor(Date.now() / this.bucketMs) * this.bucketMs;
  }
  _getOrCreateBucket(key) {
    if (!this.buckets.has(key)) {
      this.buckets.set(key, new Uint8Array(Math.ceil(this.size / 8)));
      this._pruneOldBuckets();
    }
    return this.buckets.get(key);
  }
  _pruneOldBuckets() {
    if (this.buckets.size <= this.maxBuckets) return;
    const keys = Array.from(this.buckets.keys()).sort((a, b) => a - b);
    while (keys.length > this.maxBuckets) {
      const oldest = keys.shift();
      this.buckets.delete(oldest);
    }
  }
  _getIndices(item) {
    const str = typeof item === 'string' ? item : JSON.stringify(item);
    return [hash1(str) % this.size, hash2(str) % this.size, hash3(str) % this.size];
  }
  _setBit(arr, index) {
    const byteIndex = Math.floor(index / 8);
    const bitIndex = index % 8;
    arr[byteIndex] |= (1 << bitIndex);
  }
  _getBit(arr, index) {
    const byteIndex = Math.floor(index / 8);
    const bitIndex = index % 8;
    return (arr[byteIndex] & (1 << bitIndex)) !== 0;
  }
  add(item) {
    const bucketKey = this._getBucketKey();
    const bucket = this._getOrCreateBucket(bucketKey);
    const indices = this._getIndices(item);
    for (const idx of indices) {
      this._setBit(bucket, idx);
    }
  }
  mightContain(item) {
    const indices = this._getIndices(item);
    for (const bucket of this.buckets.values()) {
      let found = true;
      for (const idx of indices) {
        if (!this._getBit(bucket, idx)) {
          found = false;
          break;
        }
      }
      if (found) return true;
    }
    return false;
  }
  addIfNew(item) {
    if (this.mightContain(item)) return false;
    this.add(item);
    return true;
  }
  clear() {
    this.buckets.clear();
  }
  stats() {
    let totalBits = 0;
    let setBits = 0;
    for (const bucket of this.buckets.values()) {
      for (let i = 0; i < bucket.length; i++) {
        totalBits += 8;
        let byte = bucket[i];
        while (byte) {
          setBits += byte & 1;
          byte >>= 1;
        }
      }
    }
    return { bucketCount: this.buckets.size, size: this.size, totalBits, setBits, fillRatio: totalBits > 0 ? setBits / totalBits : 0 };
  }
}
export function createMessageDeduplicator(options = {}) {
  const size = options.size || Number(process.env.BLOOM_FILTER_SIZE || 131072);
  const bucketMs = options.bucketMs || Number(process.env.BLOOM_BUCKET_MS || 30000);
  const maxBuckets = options.maxBuckets || Number(process.env.BLOOM_MAX_BUCKETS || 10);
  return new BloomFilter({ size, bucketMs, maxBuckets });
}
export default { BloomFilter, createMessageDeduplicator };
