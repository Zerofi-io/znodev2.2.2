package p2p

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	ethcrypto "github.com/ethereum/go-ethereum/crypto"
)

func baseClusterID(clusterID string) string {
	id := strings.ToLower(strings.TrimSpace(clusterID))
	if idx := strings.IndexByte(id, ':'); idx >= 0 {
		return id[:idx]
	}
	return id
}

func isKeccakClusterID(id string) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	if len(id) != 66 || !strings.HasPrefix(id, "0x") {
		return false
	}
	for i := 2; i < len(id); i++ {
		c := id[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func computeClusterIDFromMembers(members []string) (string, error) {
	if len(members) == 0 {
		return "", fmt.Errorf("empty member list")
	}

	seen := make(map[string]struct{}, len(members))
	canon := make([]string, 0, len(members))
	for _, m := range members {
		addr := strings.ToLower(strings.TrimSpace(m))
		if !common.IsHexAddress(addr) {
			return "", fmt.Errorf("invalid member address")
		}
		if _, ok := seen[addr]; ok {
			return "", fmt.Errorf("duplicate member address")
		}
		seen[addr] = struct{}{}
		canon = append(canon, addr)
	}

	sort.Slice(canon, func(i, j int) bool {
		return canon[i] < canon[j]
	})

	var pad [12]byte
	buf := make([]byte, 0, 32*len(canon))
	for _, a := range canon {
		addr := common.HexToAddress(a).Bytes()
		buf = append(buf, pad[:]...)
		buf = append(buf, addr...)
	}

	h := ethcrypto.Keccak256Hash(buf)
	return strings.ToLower(h.Hex()), nil
}

func (h *Host) activeIsolationMembers() (string, map[string]struct{}, bool) {
	self := strings.ToLower(h.ethAddress)

	h.mu.RLock()
	defer h.mu.RUnlock()

	var bestKey string
	var bestBase string
	var bestMembers map[string]struct{}
	bestCount := 0
	bestIsBase := false

	for key, cs := range h.clusters {
		base := baseClusterID(key)
		if !isKeccakClusterID(base) {
			continue
		}

		cs.mu.RLock()
		set := make(map[string]struct{}, len(cs.Members))
		for _, m := range cs.Members {
			addr := strings.ToLower(strings.TrimSpace(m.Address))
			if addr != "" {
				set[addr] = struct{}{}
			}
		}
		_, hasSelf := set[self]
		count := len(set)
		cs.mu.RUnlock()

		if !hasSelf || count < 2 {
			continue
		}

		isBase := strings.ToLower(key) == base
		if bestMembers == nil {
			bestKey = key
			bestBase = base
			bestMembers = set
			bestCount = count
			bestIsBase = isBase
			continue
		}

		if isBase && !bestIsBase {
			bestKey = key
			bestBase = base
			bestMembers = set
			bestCount = count
			bestIsBase = true
			continue
		}
		if isBase == bestIsBase {
			if count > bestCount || (count == bestCount && strings.ToLower(key) < strings.ToLower(bestKey)) {
				bestKey = key
				bestBase = base
				bestMembers = set
				bestCount = count
				bestIsBase = isBase
			}
		}
	}

	if bestMembers == nil {
		return "", nil, false
	}
	return bestBase, bestMembers, true
}
