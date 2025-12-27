package p2p

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type DiscoveryExchangeRequest struct {
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
	Limit     int    `json:"limit,omitempty"`
}

type DiscoveryExchangePeer struct {
	PeerID string   `json:"peerId"`
	Addrs  []string `json:"addrs"`
}

type DiscoveryExchangeResponse struct {
	Peers []DiscoveryExchangePeer `json:"peers"`
}

func (h *Host) discoveryExchangeLimit(reqLimit int) int {
	limit := reqLimit
	if limit <= 0 {
		limit = 64
	}
	if limit > 200 {
		limit = 200
	}
	return limit
}

func (h *Host) discoveryExchangePayload(peerID, address string, timestamp int64, nonce string) string {
	return fmt.Sprintf("znode-discovery|%s|%s|%d|%s", peerID, strings.ToLower(address), timestamp, nonce)
}

func (h *Host) handleDiscoveryExchangeStream(stream network.Stream) {
	defer stream.Close()
	remotePeer := stream.Conn().RemotePeer()
	if h.rateLimiter != nil {
		if !h.rateLimiter.Allow(remotePeer.String()) {
			return
		}
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(stream)
	line, err := r.ReadBytes('\n')
	if err != nil {
		if err == io.EOF {
			return
		}
		return
	}
	var req DiscoveryExchangeRequest
	if err := json.Unmarshal(bytesTrimSpace(line), &req); err != nil {
		return
	}
	peerIDStr := strings.TrimSpace(req.PeerID)
	if peerIDStr == "" || peerIDStr != remotePeer.String() {
		return
	}
	addr := strings.ToLower(strings.TrimSpace(req.Address))
	if addr == "" {
		return
	}
	now := h.NetNowMs()
	if !ValidateTimestampWindow(req.Timestamp, now, MessageMaxAgeMs, MessageMaxFutureMs) {
		return
	}
	if req.Nonce == "" {
		return
	}
	payload := h.discoveryExchangePayload(peerIDStr, addr, req.Timestamp, req.Nonce)
	if !verifyPayloadSignature(payload, strings.TrimSpace(req.Signature), addr) {
		return
	}
	if h.messageCache != nil {
		hash := ComputeMessageHash("znode-discovery", 0, addr, req.Nonce, req.Timestamp)
		if !h.messageCache.Add(hash, now) {
			return
		}
	}
	limit := h.discoveryExchangeLimit(req.Limit)
	peers := h.samplePeersForExchange(remotePeer, limit)
	resp := DiscoveryExchangeResponse{Peers: peers}
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	b = append(b, '\n')
	stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	stream.Write(b)
}

func (h *Host) samplePeersForExchange(requester peer.ID, limit int) []DiscoveryExchangePeer {
	if limit <= 0 {
		return nil
	}
	all := h.host.Peerstore().Peers()
	out := make([]DiscoveryExchangePeer, 0, limit)
	self := h.host.ID()
	tmp := make([]peer.ID, 0, len(all))
	for _, pid := range all {
		if pid == self || pid == requester {
			continue
		}
		addrs := h.host.Peerstore().Addrs(pid)
		if len(addrs) == 0 {
			continue
		}
		tmp = append(tmp, pid)
	}
	shufflePeerIDs(tmp)
	for _, pid := range tmp {
		if len(out) >= limit {
			break
		}
		addrs := filterDialableMultiaddrs(h.host.Peerstore().Addrs(pid))
		if len(addrs) == 0 {
			continue
		}
		addrStrs := make([]string, 0, len(addrs))
		for _, a := range addrs {
			addrStrs = append(addrStrs, a.String())
		}
		out = append(out, DiscoveryExchangePeer{PeerID: pid.String(), Addrs: addrStrs})
	}
	return out
}

func (h *Host) RequestDiscoveryPeers(peerID peer.ID, limit int) ([]peer.AddrInfo, error) {
	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	stream, err := h.host.NewStream(ctx, peerID, ProtocolDiscoveryExchange)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	nonce := randomHex(8)
	ts := h.NetNowMs()
	payload := h.discoveryExchangePayload(h.host.ID().String(), h.ethAddress, ts, nonce)
	sig, err := h.signPayload(payload)
	if err != nil {
		return nil, err
	}
	req := DiscoveryExchangeRequest{Address: h.ethAddress, PeerID: h.host.ID().String(), Timestamp: ts, Nonce: nonce, Signature: sig, Limit: limit}
	b, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	b = append(b, '\n')
	stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Write(b); err != nil {
		return nil, err
	}
	stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(stream)
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var resp DiscoveryExchangeResponse
	if err := json.Unmarshal(bytesTrimSpace(line), &resp); err != nil {
		return nil, err
	}
	infos := make([]peer.AddrInfo, 0, len(resp.Peers))
	for _, p := range resp.Peers {
		pid, err := peer.Decode(strings.TrimSpace(p.PeerID))
		if err != nil {
			continue
		}
		if pid == h.host.ID() {
			continue
		}
		addrs := make([]ma.Multiaddr, 0, len(p.Addrs))
		for _, s := range p.Addrs {
			a, err := ma.NewMultiaddr(strings.TrimSpace(s))
			if err != nil {
				continue
			}
			addrs = append(addrs, a)
		}
		addrs = filterDialableMultiaddrs(addrs)
		if len(addrs) == 0 {
			continue
		}
		info := peer.AddrInfo{ID: pid, Addrs: addrs}
		h.host.Peerstore().AddAddrs(pid, addrs, 6*time.Hour)
		if h.peerStore != nil {
			h.peerStore.RememberPeer(info)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (h *Host) markDiscoveryExchangeAttempt(peerID peer.ID, minInterval time.Duration) bool {
	now := time.Now()
	h.discoveryExchangeMu.Lock()
	defer h.discoveryExchangeMu.Unlock()
	if h.lastDiscoveryExchange == nil {
		h.lastDiscoveryExchange = make(map[peer.ID]time.Time)
	}
	if last, ok := h.lastDiscoveryExchange[peerID]; ok && now.Sub(last) < minInterval {
		return false
	}
	h.lastDiscoveryExchange[peerID] = now
	return true
}

func (h *Host) BootstrapDiscoveryFromPeer(peerID peer.ID) {
	if !h.markDiscoveryExchangeAttempt(peerID, 30*time.Second) {
		return
	}
	infos, err := h.RequestDiscoveryPeers(peerID, 64)
	if err != nil || len(infos) == 0 {
		return
	}
	maxDial := 8
	if maxDial > len(infos) {
		maxDial = len(infos)
	}
	for i := 0; i < maxDial; i++ {
		info := infos[i]
		if h.host.Network().Connectedness(info.ID) == network.Connected {
			continue
		}
		if cm := h.host.ConnManager(); cm != nil {
			cm.TagPeer(info.ID, "discovery", 5)
		}
		go func(pi peer.AddrInfo) {
			ctx, cancel := context.WithTimeout(h.ctx, 15*time.Second)
			defer cancel()
			_ = h.host.Connect(ctx, pi)
		}(info)
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func bytesTrimSpace(b []byte) []byte {
	return bytes.TrimSpace(b)
}

func shufflePeerIDs(ids []peer.ID) {
	if len(ids) < 2 {
		return
	}
	seed := uint64(time.Now().UnixNano())
	for i := len(ids) - 1; i > 0; i-- {
		seed = seed*1664525 + 1013904223
		j := int(seed % uint64(i+1))
		ids[i], ids[j] = ids[j], ids[i]
	}
}
