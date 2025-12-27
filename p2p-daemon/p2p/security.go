package p2p

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

const (
	MaxMessageSize = 2 * 1024 * 1024

	MessageMaxAgeMs    int64 = 300000
	MessageMaxFutureMs int64 = 120000

	MaxReplayCacheSize = 10000
	RateLimitPerSecond = 100
	RateLimitBurst     = 200

	RoundDataTTLMs    int64 = 24 * 60 * 60 * 1000
	CleanupIntervalMs int64 = 60 * 60 * 1000
)

type MessageCache struct {
	mu      sync.RWMutex
	cache   map[string]int64
	queue   *list.List
	maxSize int
}

func NewMessageCache(maxSize int) *MessageCache {
	return &MessageCache{
		cache:   make(map[string]int64),
		queue:   list.New(),
		maxSize: maxSize,
	}
}

func (mc *MessageCache) Add(hash string, seenAtMs int64) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if _, exists := mc.cache[hash]; exists {
		return false
	}
	mc.cache[hash] = seenAtMs
	mc.queue.PushBack(hash)
	if mc.queue.Len() > mc.maxSize {
		oldest := mc.queue.Front()
		if oldest != nil {
			oldHash := oldest.Value.(string)
			delete(mc.cache, oldHash)
			mc.queue.Remove(oldest)
		}
	}
	return true
}

func (mc *MessageCache) Cleanup(nowMs int64, ttlMs int64) int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	cutoff := nowMs - ttlMs
	removed := 0
	for e := mc.queue.Front(); e != nil; {
		hash := e.Value.(string)
		ts := mc.cache[hash]
		if ts < cutoff {
			next := e.Next()
			delete(mc.cache, hash)
			mc.queue.Remove(e)
			removed++
			e = next
			continue
		}
		break
	}
	return removed
}

type RateLimiter struct {
	mu         sync.Mutex
	peers      map[string]*tokenBucket
	capacity   int
	refillRate float64
}

type tokenBucket struct {
	tokens     float64
	lastRefill time.Time
}

func NewRateLimiter(ratePerSecond, burst int) *RateLimiter {
	return &RateLimiter{
		peers:      make(map[string]*tokenBucket),
		capacity:   burst,
		refillRate: float64(ratePerSecond),
	}
}

func (rl *RateLimiter) Allow(peerID string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	bucket, exists := rl.peers[peerID]
	if !exists {
		bucket = &tokenBucket{tokens: float64(rl.capacity), lastRefill: time.Now()}
		rl.peers[peerID] = bucket
	}
	now := time.Now()
	elapsed := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsed * rl.refillRate
	if bucket.tokens > float64(rl.capacity) {
		bucket.tokens = float64(rl.capacity)
	}
	bucket.lastRefill = now
	if bucket.tokens >= 1 {
		bucket.tokens--
		return true
	}
	return false
}

func (rl *RateLimiter) Cleanup(idleTimeSeconds int) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-time.Duration(idleTimeSeconds) * time.Second)
	removed := 0
	for peer, bucket := range rl.peers {
		if bucket.lastRefill.Before(cutoff) {
			delete(rl.peers, peer)
			removed++
		}
	}
	return removed
}

func ComputeMessageHash(clusterID string, round int, address, payload string, timestamp int64) string {
	data := fmt.Sprintf("%s|%d|%s|%s|%d", clusterID, round, address, payload, timestamp)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func ValidateTimestampWindow(timestampMs int64, nowMs int64, maxAgeMs int64, maxFutureMs int64) bool {
	if maxAgeMs < 0 || maxFutureMs < 0 {
		return false
	}
	if timestampMs < nowMs-maxAgeMs {
		return false
	}
	if timestampMs > nowMs+maxFutureMs {
		return false
	}
	return true
}
