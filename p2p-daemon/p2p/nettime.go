package p2p

import (
	"sync"
	"time"
)

type NetTime struct {
	mu       sync.RWMutex
	baseMs   int64
	baseMono time.Time
	active   bool
}

func NewNetTime() *NetTime {
	return &NetTime{}
}

func (nt *NetTime) NowMs() int64 {
	nt.mu.RLock()
	baseMs := nt.baseMs
	baseMono := nt.baseMono
	active := nt.active
	nt.mu.RUnlock()
	if !active {
		return time.Now().UnixMilli()
	}
	return baseMs + time.Since(baseMono).Milliseconds()
}

func (nt *NetTime) Active() bool {
	nt.mu.RLock()
	active := nt.active
	nt.mu.RUnlock()
	return active
}

func (nt *NetTime) UpdateBase(chainTimeMs int64) int64 {
	if chainTimeMs <= 0 {
		return nt.NowMs()
	}
	nt.mu.Lock()
	defer nt.mu.Unlock()
	nowMono := time.Now()
	if !nt.active {
		nt.baseMs = chainTimeMs
		nt.baseMono = nowMono
		nt.active = true
		return chainTimeMs
	}
	cur := nt.baseMs + time.Since(nt.baseMono).Milliseconds()
	nt.baseMono = nowMono
	if chainTimeMs < cur {
		nt.baseMs = cur
		return cur
	}
	nt.baseMs = chainTimeMs
	return chainTimeMs
}
