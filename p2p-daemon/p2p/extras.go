package p2p

import (
	"strings"
)

func (h *Host) ClearClusterSignatures(clusterID string, rounds []int) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if len(rounds) == 0 {
		for round := range cs.Signatures {
			delete(cs.Signatures, round)
		}
		return
	}

	for _, round := range rounds {
		delete(cs.Signatures, round)
	}
}

func (h *Host) ClearClusterRounds(clusterID string, rounds []int) {
	cs := h.getCluster(clusterID)
	if cs == nil {
		return
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if len(rounds) == 0 {
		for round := range cs.RoundData {
			delete(cs.RoundData, round)
		}
		return
	}

	for _, round := range rounds {
		delete(cs.RoundData, round)
	}
}

func (h *Host) GetLastHeartbeat(address string) int64 {
	ts, _ := h.GetLastHeartbeatWithProof(address)
	return ts
}

func (h *Host) GetLastHeartbeatWithProof(address string) (int64, string) {
	addrLower := strings.ToLower(address)
	_, allowed, inCluster := h.activeIsolationMembers()
	if inCluster {
		if _, ok := allowed[addrLower]; !ok {
			return 0, ""
		}
	}

	hm := h.GetHeartbeatManager()
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	if presence, ok := hm.recentPeers[addrLower]; ok {
		return presence.LastSeen.UnixMilli(), presence.Signature
	}
	return 0, ""
}

func (h *Host) GetHeartbeats(ttlMs int) map[string]int64 {
	hm := h.GetHeartbeatManager()
	return hm.GetHeartbeats(ttlMs)
}

func (h *Host) GetHeartbeatsWithApiBase(ttlMs int) []map[string]interface{} {
	hm := h.GetHeartbeatManager()
	return hm.GetHeartbeatsWithApiBase(ttlMs)
}

func (h *Host) GetRecentPeers(ttlMs int) []string {
	hm := h.GetHeartbeatManager()
	return hm.GetRecentPeers(ttlMs)
}
