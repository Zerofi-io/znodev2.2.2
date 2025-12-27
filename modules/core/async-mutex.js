export class AsyncMutex {
  constructor() {
    this._locked = false;
    this._queue = [];
  }
  async acquire() {
    if (!this._locked) {
      this._locked = true;
      return;
    }
    await new Promise((resolve) => this._queue.push(resolve));
  }
  release() {
    if (this._queue.length > 0) {
      const next = this._queue.shift();
      next();
    } else {
      this._locked = false;
    }
  }
  isLocked() {
    return this._locked;
  }
  async withLock(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}
export default AsyncMutex;
