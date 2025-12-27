class P2PMetrics {
  constructor() {
    this.messagesReceived = 0;
    this.messagesSent = 0;
    this.messagesDropped = 0;
    this.messagesFailed = 0;
    this.messagesRateLimited = 0;
    this.messagesReplay = 0;
    this.messagesDeduplicated = 0;
    this.messagesOversized = 0;
    this.jsonParseFailures = 0;
    this.signatureVerificationFailures = 0;
    this.authorizationFailures = 0;
    this.peerBindingChanges = 0;
    this.roundDataCleanups = 0;
    this.messageLatencySum = 0;
    this.messageLatencyCount = 0;
    this.perPeerMetrics = new Map();
    this.lastReset = Date.now();
  }
  incrementMessagesReceived(peer = null) {
    this.messagesReceived++;
    if (peer) this._incrementPeerMetric(peer, 'received');
  }
  incrementMessagesSent(peer = null) {
    this.messagesSent++;
    if (peer) this._incrementPeerMetric(peer, 'sent');
  }
  incrementMessagesDropped(reason = 'unknown') {
    this.messagesDropped++;
  }
  incrementMessagesFailed() {
    this.messagesFailed++;
  }
  incrementMessagesRateLimited(peer = null) {
    this.messagesRateLimited++;
    if (peer) this._incrementPeerMetric(peer, 'rateLimited');
  }
  incrementMessagesReplay() {
    this.messagesReplay++;
  }
  incrementMessagesDeduplicated() {
    this.messagesDeduplicated++;
  }
  incrementMessagesOversized() {
    this.messagesOversized++;
  }
  incrementJsonParseFailures() {
    this.jsonParseFailures++;
  }
  incrementSignatureVerificationFailures() {
    this.signatureVerificationFailures++;
  }
  incrementAuthorizationFailures() {
    this.authorizationFailures++;
  }
  incrementPeerBindingChanges() {
    this.peerBindingChanges++;
  }
  incrementRoundDataCleanups(count = 1) {
    this.roundDataCleanups += count;
  }
  recordMessageLatency(latencyMs) {
    if (typeof latencyMs === 'number' && latencyMs >= 0) {
      this.messageLatencySum += latencyMs;
      this.messageLatencyCount++;
    }
  }
  _incrementPeerMetric(peer, metric) {
    if (!peer) return;
    if (!this.perPeerMetrics.has(peer)) {
      this.perPeerMetrics.set(peer, { received: 0, sent: 0, rateLimited: 0 });
    }
    const metrics = this.perPeerMetrics.get(peer);
    if (metrics[metric] !== undefined) {
      metrics[metric]++;
    }
  }
  getStats() {
    const avgLatency = this.messageLatencyCount > 0
      ? this.messageLatencySum / this.messageLatencyCount
      : 0;
    return {
      messagesReceived: this.messagesReceived,
      messagesSent: this.messagesSent,
      messagesDropped: this.messagesDropped,
      messagesFailed: this.messagesFailed,
      messagesRateLimited: this.messagesRateLimited,
      messagesReplay: this.messagesReplay,
      messagesDeduplicated: this.messagesDeduplicated,
      messagesOversized: this.messagesOversized,
      jsonParseFailures: this.jsonParseFailures,
      signatureVerificationFailures: this.signatureVerificationFailures,
      authorizationFailures: this.authorizationFailures,
      peerBindingChanges: this.peerBindingChanges,
      roundDataCleanups: this.roundDataCleanups,
      avgMessageLatencyMs: Math.round(avgLatency),
      uptimeSeconds: Math.floor((Date.now() - this.lastReset) / 1000),
    };
  }
  getPeerStats(peer) {
    return this.perPeerMetrics.get(peer) || { received: 0, sent: 0, rateLimited: 0 };
  }
  reset() {
    this.messagesReceived = 0;
    this.messagesSent = 0;
    this.messagesDropped = 0;
    this.messagesFailed = 0;
    this.messagesRateLimited = 0;
    this.messagesReplay = 0;
    this.messagesDeduplicated = 0;
    this.messagesOversized = 0;
    this.jsonParseFailures = 0;
    this.signatureVerificationFailures = 0;
    this.authorizationFailures = 0;
    this.peerBindingChanges = 0;
    this.roundDataCleanups = 0;
    this.messageLatencySum = 0;
    this.messageLatencyCount = 0;
    this.perPeerMetrics.clear();
    this.lastReset = Date.now();
  }
}
const globalMetrics = new P2PMetrics();
export function getMetrics() {
  return globalMetrics;
}
export function getStats() {
  return globalMetrics.getStats();
}
export function getPeerStats(peer) {
  return globalMetrics.getPeerStats(peer);
}
export function resetMetrics() {
  globalMetrics.reset();
}
