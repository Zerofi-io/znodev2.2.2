package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/zerofi-io/znodev2/p2p-daemon/p2p"
)

type Handlers struct {
	host *p2p.Host
}

func NewHandlers(host *p2p.Host) *Handlers {
	return &Handlers{host: host}
}

type StartParams struct {
}

type StartResult struct {
	PeerID     string   `json:"peerId"`
	Multiaddrs []string `json:"multiaddrs"`
}

func (h *Handlers) Start(params json.RawMessage) (interface{}, *Error) {

	addrs := h.host.Addrs()
	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}
	return &StartResult{
		PeerID:     h.host.PeerID(),
		Multiaddrs: addrStrs,
	}, nil
}

type StopResult struct{}

func (h *Handlers) Stop(params json.RawMessage) (interface{}, *Error) {

	return &StopResult{}, nil
}

type StatusResult struct {
	PeerID         string   `json:"peerId"`
	ConnectedPeers int      `json:"connectedPeers"`
	ActiveClusters []string `json:"activeClusters"`
	ListenAddrs    []string `json:"listenAddrs"`
}

func (h *Handlers) Status(params json.RawMessage) (interface{}, *Error) {
	addrs := h.host.Addrs()
	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}
	return &StatusResult{
		PeerID:         h.host.PeerID(),
		ConnectedPeers: h.host.ConnectedPeerCount(),
		ActiveClusters: h.host.ActiveClusters(),
		ListenAddrs:    addrStrs,
	}, nil
}

type FeaturesResult struct {
	ProtocolVersion int    `json:"protocolVersion"`
	RequireAll      bool   `json:"requireAll"`
	PBFTQuorumEnv   string `json:"pbftQuorumEnv"`
	ClusterIDAlgo   string `json:"clusterIdAlgo"`
}

func (h *Handlers) Features(params json.RawMessage) (interface{}, *Error) {
	return &FeaturesResult{ProtocolVersion: 1, RequireAll: true, PBFTQuorumEnv: "PBFT_QUORUM", ClusterIDAlgo: "abi-packed-address-array-v1"}, nil
}

type SetTimeOracleParams struct {
	ChainTimeMs int64  `json:"chainTimeMs"`
	BlockNumber uint64 `json:"blockNumber,omitempty"`
	Source      string `json:"source,omitempty"`
}

type SetTimeOracleResult struct {
	NowMs       int64 `json:"nowMs"`
	UsingOracle bool  `json:"usingOracle"`
}

func (h *Handlers) SetTimeOracle(params json.RawMessage) (interface{}, *Error) {
	var p SetTimeOracleParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.ChainTimeMs <= 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "chainTimeMs is required"}
	}
	nowMs := h.host.UpdateTimeOracle(p.ChainTimeMs)
	log.Printf("[TimeOracle] Baseline set: chainTimeMs=%d nowMs=%d source=%s block=%d", p.ChainTimeMs, nowMs, p.Source, p.BlockNumber)
	return &SetTimeOracleResult{NowMs: nowMs, UsingOracle: h.host.TimeOracleActive()}, nil
}

type JoinClusterParams struct {
	ClusterID     string   `json:"clusterId"`
	Members       []Member `json:"members"`
	IsCoordinator bool     `json:"isCoordinator"`
}

type Member struct {
	Address string `json:"address"`
	PeerID  string `json:"peerId,omitempty"`
}

type JoinClusterResult struct {
	Joined bool `json:"joined"`
}

func (h *Handlers) JoinCluster(params json.RawMessage) (interface{}, *Error) {
	var p JoinClusterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	peers := make([]p2p.PeerInfo, len(p.Members))
	for i, m := range p.Members {
		peers[i] = p2p.PeerInfo{Address: m.Address, PeerID: m.PeerID}
	}

	if err := h.host.JoinCluster(p.ClusterID, peers, p.IsCoordinator); err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &JoinClusterResult{Joined: true}, nil
}

type LeaveClusterParams struct {
	ClusterID string `json:"clusterId"`
}

type LeaveClusterResult struct{}

func (h *Handlers) LeaveCluster(params json.RawMessage) (interface{}, *Error) {
	var p LeaveClusterParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	if err := h.host.LeaveCluster(p.ClusterID); err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &LeaveClusterResult{}, nil
}

type BroadcastRoundDataParams struct {
	ClusterID string          `json:"clusterId"`
	Round     int             `json:"round"`
	Payload   json.RawMessage `json:"payload"`
}

type BroadcastRoundDataResult struct {
	Delivered int `json:"delivered"`
}

func normalizeBroadcastPayload(raw json.RawMessage) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}

	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return "", fmt.Errorf("invalid payload string: %w", err)
		}
		return s, nil
	}

	buf := &bytes.Buffer{}
	if err := json.Compact(buf, trimmed); err != nil {
		return "", fmt.Errorf("invalid payload json: %w", err)
	}
	return buf.String(), nil
}

func (h *Handlers) BroadcastRoundData(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastRoundDataParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	payloadStr, err := normalizeBroadcastPayload(p.Payload)
	if err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	delivered, err := h.host.BroadcastRoundData(p.ClusterID, p.Round, payloadStr)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastRoundDataResult{Delivered: delivered}, nil
}

type WaitForRoundCompletionParams struct {
	ClusterID  string   `json:"clusterId"`
	Round      int      `json:"round"`
	Members    []string `json:"members"`
	TimeoutMs  int      `json:"timeoutMs"`
	RequireAll bool     `json:"requireAll"`
}

type WaitForRoundCompletionResult struct {
	Complete bool     `json:"complete"`
	Received []string `json:"received"`
	Missing  []string `json:"missing"`
}

func (h *Handlers) WaitForRoundCompletion(params json.RawMessage) (interface{}, *Error) {
	var p WaitForRoundCompletionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForRoundCompletion(p.ClusterID, p.Round, p.Members, p.TimeoutMs, p.RequireAll)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

type GetPeerPayloadsParams struct {
	ClusterID  string   `json:"clusterId"`
	Round      int      `json:"round"`
	Members    []string `json:"members"`
	RequireAll bool     `json:"requireAll,omitempty"`
}

type PeerPayload struct {
	Address string `json:"address"`
	Payload string `json:"payload"`
}

type GetPeerPayloadsResult struct {
	Payloads []PeerPayload `json:"payloads"`
}

func (h *Handlers) GetPeerPayloads(params json.RawMessage) (interface{}, *Error) {
	var p GetPeerPayloadsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	payloads := h.host.GetPeerPayloads(p.ClusterID, p.Round, p.Members)
	result := make([]PeerPayload, 0, len(payloads))
	for addr, payload := range payloads {
		result = append(result, PeerPayload{Address: addr, Payload: payload})
	}

	return &GetPeerPayloadsResult{Payloads: result}, nil
}

type BroadcastSignatureParams struct {
	ClusterID string `json:"clusterId"`
	Round     int    `json:"round"`
	DataHash  string `json:"dataHash"`
	Signature string `json:"signature"`
}

type BroadcastSignatureResult struct {
	Delivered int `json:"delivered"`
}

func (h *Handlers) BroadcastSignature(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastSignatureParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	delivered, err := h.host.BroadcastSignature(p.ClusterID, p.Round, p.DataHash, p.Signature)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastSignatureResult{Delivered: delivered}, nil
}

type WaitForSignatureBarrierParams struct {
	ClusterID  string   `json:"clusterId"`
	Round      int      `json:"round"`
	Members    []string `json:"members"`
	RequireAll bool     `json:"requireAll,omitempty"`
	TimeoutMs  int      `json:"timeoutMs"`
}

type SignatureInfo struct {
	Address   string `json:"address"`
	DataHash  string `json:"dataHash"`
	Signature string `json:"signature"`
}

type WaitForSignatureBarrierResult struct {
	Complete      bool            `json:"complete"`
	Signatures    []SignatureInfo `json:"signatures"`
	Missing       []string        `json:"missing"`
	ConsensusHash string          `json:"consensusHash"`
	Mismatched    []string        `json:"mismatched,omitempty"`
}

func (h *Handlers) WaitForSignatureBarrier(params json.RawMessage) (interface{}, *Error) {
	var p WaitForSignatureBarrierParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForSignatureBarrier(p.ClusterID, p.Round, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

type BroadcastIdentityParams struct {
	ClusterID string `json:"clusterId"`
	PublicKey string `json:"publicKey"`
}

type BroadcastIdentityResult struct {
	Delivered int `json:"delivered"`
}

func (h *Handlers) BroadcastIdentity(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastIdentityParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	delivered, err := h.host.BroadcastIdentity(p.ClusterID, p.PublicKey)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastIdentityResult{Delivered: delivered}, nil
}

type WaitForIdentitiesParams struct {
	ClusterID  string   `json:"clusterId"`
	Members    []string `json:"members"`
	RequireAll bool     `json:"requireAll,omitempty"`
	TimeoutMs  int      `json:"timeoutMs"`
}

type IdentityInfo struct {
	Address   string `json:"address"`
	PeerID    string `json:"peerId"`
	PublicKey string `json:"publicKey"`
}

type WaitForIdentitiesResult struct {
	Identities []IdentityInfo `json:"identities"`
}

func (h *Handlers) WaitForIdentities(params json.RawMessage) (interface{}, *Error) {
	var p WaitForIdentitiesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForIdentities(p.ClusterID, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

type GetPeerBindingsParams struct {
	ClusterID string `json:"clusterId"`
}

type PeerBinding struct {
	Address string `json:"address"`
	PeerID  string `json:"peerId"`
}

type GetPeerBindingsResult struct {
	Bindings []PeerBinding `json:"bindings"`
}

func (h *Handlers) GetPeerBindings(params json.RawMessage) (interface{}, *Error) {
	var p GetPeerBindingsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	bindings := h.host.GetPeerBindings(p.ClusterID)
	result := make([]PeerBinding, 0, len(bindings))
	for addr, peerID := range bindings {
		result = append(result, PeerBinding{Address: addr, PeerID: peerID})
	}

	return &GetPeerBindingsResult{Bindings: result}, nil
}

type ClearPeerBindingsParams struct {
	ClusterID string `json:"clusterId"`
}

type ClearPeerBindingsResult struct{}

func (h *Handlers) ClearPeerBindings(params json.RawMessage) (interface{}, *Error) {
	var p ClearPeerBindingsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ClearPeerBindings(p.ClusterID)
	return &ClearPeerBindingsResult{}, nil
}

type GetPeerScoresParams struct {
	ClusterID string `json:"clusterId"`
}

type PeerScore struct {
	Address string  `json:"address"`
	Score   float64 `json:"score"`
}

type GetPeerScoresResult struct {
	Scores []PeerScore `json:"scores"`
}

func (h *Handlers) GetPeerScores(params json.RawMessage) (interface{}, *Error) {
	var p GetPeerScoresParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	scores := h.host.GetPeerScores(p.ClusterID)
	result := make([]PeerScore, 0, len(scores))
	for addr, score := range scores {
		result = append(result, PeerScore{Address: addr, Score: score})
	}

	return &GetPeerScoresResult{Scores: result}, nil
}

type ReportPeerFailureParams struct {
	ClusterID string `json:"clusterId"`
	Address   string `json:"address"`
	Reason    string `json:"reason"`
}

type ReportPeerFailureResult struct{}

func (h *Handlers) ReportPeerFailure(params json.RawMessage) (interface{}, *Error) {
	var p ReportPeerFailureParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ReportPeerFailure(p.ClusterID, p.Address, p.Reason)
	return &ReportPeerFailureResult{}, nil
}

func (h *Handlers) SetHeartbeatDomain(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		ChainID        int64  `json:"chainId"`
		StakingAddress string `json:"stakingAddress"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}

	hm := h.host.GetHeartbeatManager()
	hm.SetDomain(req.ChainID, req.StakingAddress)

	return map[string]interface{}{
		"success": true,
	}, nil
}

func (h *Handlers) StartHeartbeat(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		IntervalMs int `json:"intervalMs"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}

	if req.IntervalMs <= 0 {
		req.IntervalMs = 30000
	}

	hm := h.host.GetHeartbeatManager()
	if err := hm.Start(req.IntervalMs); err != nil {
		return nil, &Error{Code: -32000, Message: "Failed to start heartbeat: " + err.Error()}
	}

	return map[string]interface{}{
		"success": true,
	}, nil
}

func (h *Handlers) StopHeartbeat(params json.RawMessage) (interface{}, *Error) {
	hm := h.host.GetHeartbeatManager()
	hm.Stop()

	return map[string]interface{}{
		"success": true,
	}, nil
}

func (h *Handlers) BroadcastHeartbeat(params json.RawMessage) (interface{}, *Error) {
	hm := h.host.GetHeartbeatManager()
	if err := hm.Broadcast(); err != nil {
		return nil, &Error{Code: -32000, Message: "Failed to broadcast: " + err.Error()}
	}

	return map[string]interface{}{
		"success": true,
	}, nil
}

func (h *Handlers) GetRecentPeers(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		TTLMs int `json:"ttlMs"`
	}
	if params != nil {
		json.Unmarshal(params, &req)
	}

	peers := h.host.GetRecentPeers(req.TTLMs)

	return map[string]interface{}{
		"peers": peers,
	}, nil
}

func (h *Handlers) StartQueueDiscovery(params json.RawMessage) (interface{}, *Error) {
	var req struct {
		StakingAddress string `json:"stakingAddress"`
		ChainID        int64  `json:"chainId"`
		IntervalMs     int    `json:"intervalMs"`
		ApiBase        string `json:"apiBase"`
	}
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}
	if req.IntervalMs <= 0 {
		req.IntervalMs = 30000
	}
	hm := h.host.GetHeartbeatManager()
	hm.SetDomain(req.ChainID, req.StakingAddress)
	if req.ApiBase != "" {
		hm.SetApiBase(req.ApiBase)
	}
	if err := hm.Start(req.IntervalMs); err != nil {
		return nil, &Error{Code: -32000, Message: "Failed to start: " + err.Error()}
	}
	return map[string]interface{}{"success": true}, nil
}

type ClearClusterSignaturesParams struct {
	ClusterID string `json:"clusterId"`
	Rounds    []int  `json:"rounds"`
}

func (h *Handlers) ClearClusterSignatures(params json.RawMessage) (interface{}, *Error) {
	var p ClearClusterSignaturesParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ClearClusterSignatures(p.ClusterID, p.Rounds)
	return map[string]interface{}{"success": true}, nil
}

type ClearClusterRoundsParams struct {
	ClusterID string `json:"clusterId"`
	Rounds    []int  `json:"rounds"`
}

func (h *Handlers) ClearClusterRounds(params json.RawMessage) (interface{}, *Error) {
	var p ClearClusterRoundsParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	h.host.ClearClusterRounds(p.ClusterID, p.Rounds)
	return map[string]interface{}{"success": true}, nil
}

type GetLastHeartbeatParams struct {
	Address string `json:"address"`
}

func (h *Handlers) GetLastHeartbeat(params json.RawMessage) (interface{}, *Error) {
	var p GetLastHeartbeatParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	timestamp := h.host.GetLastHeartbeat(p.Address)
	return map[string]interface{}{
		"address":   p.Address,
		"timestamp": timestamp,
	}, nil
}

type GetLastHeartbeatWithProofParams struct {
	Address string `json:"address"`
}

func (h *Handlers) GetLastHeartbeatWithProof(params json.RawMessage) (interface{}, *Error) {
	var p GetLastHeartbeatWithProofParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	timestampMs, signature := h.host.GetLastHeartbeatWithProof(p.Address)
	return map[string]interface{}{
		"address":     p.Address,
		"timestampMs": timestampMs,
		"signature":   signature,
	}, nil
}

type GetHeartbeatsParams struct {
	TTLMs int `json:"ttlMs"`
}

func (h *Handlers) GetHeartbeats(params json.RawMessage) (interface{}, *Error) {
	var p GetHeartbeatsParams
	if params != nil {
		json.Unmarshal(params, &p)
	}
	heartbeats := h.host.GetHeartbeatsWithApiBase(p.TTLMs)
	return map[string]interface{}{"heartbeats": heartbeats}, nil
}

type WaitForIdentityBarrierParams struct {
	ClusterID  string   `json:"clusterId"`
	Members    []string `json:"members"`
	RequireAll bool     `json:"requireAll,omitempty"`
	TimeoutMs  int      `json:"timeoutMs"`
}

func (h *Handlers) WaitForIdentityBarrier(params json.RawMessage) (interface{}, *Error) {
	var p WaitForIdentityBarrierParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	result, err := h.host.WaitForIdentities(p.ClusterID, p.Members, p.TimeoutMs)
	if err != nil {
		return nil, &Error{Code: ErrCodeTimeout, Message: err.Error(), Data: result}
	}

	return result, nil
}

type ComputeConsensusHashParams struct {
	ClusterID  string   `json:"clusterId"`
	Round      int      `json:"round"`
	Members    []string `json:"members"`
	RequireAll bool     `json:"requireAll,omitempty"`
}

type ComputeConsensusHashResult struct {
	Hash string `json:"hash"`
}

func (h *Handlers) ComputeConsensusHash(params json.RawMessage) (interface{}, *Error) {
	var p ComputeConsensusHashParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	hash := h.host.ComputePayloadConsensusHash(p.ClusterID, p.Round, p.Members)
	if hash == "" {
		return nil, &Error{Code: ErrCodeInternal, Message: "cannot compute hash - missing round data"}
	}

	return &ComputeConsensusHashResult{Hash: hash}, nil
}

type PBFTDebugParams struct {
	ClusterID string `json:"clusterId"`
	Phase     string `json:"phase"`
}

type PBFTDebugResult struct {
	Exists bool                         `json:"exists"`
	State  *p2p.PBFTConsensusDebugState `json:"state,omitempty"`
}

func (h *Handlers) PBFTDebug(params json.RawMessage) (interface{}, *Error) {
	var p PBFTDebugParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	if p.ClusterID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "clusterId is required"}
	}
	if p.Phase == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "phase is required"}
	}

	mgr := h.host.GetPBFTManager()
	if mgr == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "PBFT manager not initialized"}
	}

	state := mgr.GetConsensusDebugState(p.ClusterID, p.Phase)
	if state == nil {
		return &PBFTDebugResult{Exists: false}, nil
	}

	return &PBFTDebugResult{
		Exists: true,
		State:  state,
	}, nil
}

type RunConsensusParams struct {
	ClusterID  string   `json:"clusterId"`
	Phase      string   `json:"phase"`
	Data       string   `json:"data"`
	Members    []string `json:"members"`
	RequireAll bool     `json:"requireAll,omitempty"`
}

type RunConsensusResult struct {
	IsCoordinator bool     `json:"isCoordinator"`
	Coordinator   string   `json:"coordinator"`
	Success       bool     `json:"success"`
	Phase         string   `json:"phase"`
	ViewNumber    int      `json:"viewNumber"`
	Digest        string   `json:"digest"`
	Participants  []string `json:"participants,omitempty"`
	Missing       []string `json:"missing,omitempty"`
	Aborted       bool     `json:"aborted"`
	AbortReason   string   `json:"abortReason,omitempty"`
}

func (h *Handlers) RunConsensus(params json.RawMessage) (interface{}, *Error) {
	var p RunConsensusParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	if p.ClusterID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "clusterId is required"}
	}
	if p.Phase == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "phase is required"}
	}
	if len(p.Members) == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "members is required"}
	}

	result, err := h.host.RunConsensus(p.ClusterID, p.Phase, p.Data, p.Members, p.RequireAll)
	if err != nil {

		if result != nil {
			return &RunConsensusResult{
				IsCoordinator: result.IsCoordinator,
				Coordinator:   result.Coordinator,
				Success:       result.Success,
				Phase:         result.Phase,
				ViewNumber:    result.ViewNumber,
				Digest:        result.Digest,
				Missing:       result.Missing,
				Aborted:       result.Aborted,
				AbortReason:   result.AbortReason,
			}, &Error{Code: ErrCodeTimeout, Message: err.Error()}
		}
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &RunConsensusResult{
		IsCoordinator: result.IsCoordinator,
		Coordinator:   result.Coordinator,
		Success:       result.Success,
		Phase:         result.Phase,
		ViewNumber:    result.ViewNumber,
		Digest:        result.Digest,
		Participants:  result.Participants,
		Aborted:       result.Aborted,
	}, nil
}

type BroadcastPreSelectionParams struct {
	BlockNumber uint64   `json:"blockNumber"`
	EpochSeed   string   `json:"epochSeed"`
	Candidates  []string `json:"candidates"`
}

type BroadcastPreSelectionResult struct {
	ProposalID string `json:"proposalId"`
	Success    bool   `json:"success"`
}

func (h *Handlers) BroadcastPreSelection(params json.RawMessage) (interface{}, *Error) {
	var p BroadcastPreSelectionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.BlockNumber == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "blockNumber is required"}
	}
	if len(p.Candidates) == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "candidates is required"}
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	proposal, err := psm.BroadcastProposal(p.BlockNumber, p.EpochSeed, p.Candidates)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &BroadcastPreSelectionResult{
		ProposalID: proposal.ProposalID,
		Success:    true,
	}, nil
}

type VotePreSelectionParams struct {
	ProposalID      string   `json:"proposalId"`
	Approved        bool     `json:"approved"`
	LocalCandidates []string `json:"localCandidates,omitempty"`
}

type VotePreSelectionResult struct {
	Success bool `json:"success"`
}

func (h *Handlers) VotePreSelection(params json.RawMessage) (interface{}, *Error) {
	var p VotePreSelectionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.ProposalID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "proposalId is required"}
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	err := psm.VoteOnProposal(p.ProposalID, p.Approved, p.LocalCandidates)
	if err != nil {
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &VotePreSelectionResult{Success: true}, nil
}

type WaitPreSelectionParams struct {
	ProposalID     string   `json:"proposalId"`
	ExpectedVoters []string `json:"expectedVoters"`
	TimeoutMs      int      `json:"timeoutMs,omitempty"`
}

type WaitPreSelectionResult struct {
	ProposalID string   `json:"proposalId"`
	Success    bool     `json:"success"`
	Approved   int      `json:"approved"`
	Total      int      `json:"total"`
	Missing    []string `json:"missing,omitempty"`
}

func (h *Handlers) WaitPreSelection(params json.RawMessage) (interface{}, *Error) {
	var p WaitPreSelectionParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}
	if p.ProposalID == "" {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "proposalId is required"}
	}
	if len(p.ExpectedVoters) == 0 {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: "expectedVoters is required"}
	}

	timeoutMs := p.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 60000
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	result, err := psm.WaitForConsensus(p.ProposalID, p.ExpectedVoters, timeoutMs)
	if err != nil {
		if result != nil {
			return &WaitPreSelectionResult{
				ProposalID: result.ProposalID,
				Success:    result.Success,
				Approved:   result.Approved,
				Total:      result.Total,
				Missing:    result.Missing,
			}, &Error{Code: ErrCodeTimeout, Message: err.Error()}
		}
		return nil, &Error{Code: ErrCodeInternal, Message: err.Error()}
	}

	return &WaitPreSelectionResult{
		ProposalID: result.ProposalID,
		Success:    result.Success,
		Approved:   result.Approved,
		Total:      result.Total,
	}, nil
}

type GetPreSelectionProposalParams struct {
	ProposalID string `json:"proposalId,omitempty"`
}

func (h *Handlers) GetPreSelectionProposal(params json.RawMessage) (interface{}, *Error) {
	var p GetPreSelectionProposalParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: ErrCodeInvalidArgs, Message: err.Error()}
	}

	psm := h.host.GetPreSelectionManager()
	if psm == nil {
		return nil, &Error{Code: ErrCodeInternal, Message: "pre-selection manager not initialized"}
	}

	var proposal *p2p.PreSelectionProposal
	if p.ProposalID != "" {
		proposal = psm.GetPendingProposal(p.ProposalID)
	} else {
		proposal = psm.GetLatestProposal()
	}

	if proposal == nil {
		return map[string]interface{}{"found": false}, nil
	}

	return map[string]interface{}{
		"found":       true,
		"proposalId":  proposal.ProposalID,
		"blockNumber": proposal.BlockNumber,
		"epochSeed":   proposal.EpochSeed,
		"candidates":  proposal.Candidates,
		"coordinator": proposal.Coordinator,
		"timestamp":   proposal.Timestamp,
	}, nil
}

type GetRoundDataParams struct {
	ClusterID string `json:"clusterId"`
	Round     int    `json:"round"`
}

type GetRoundDataResult struct {
	Data map[string]string `json:"data"`
}

func (h *Handlers) GetRoundData(params json.RawMessage) (interface{}, *Error) {
	var p GetRoundDataParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &Error{Code: -32602, Message: "Invalid params: " + err.Error()}
	}
	data := h.host.GetRoundData(p.ClusterID, p.Round)
	return GetRoundDataResult{Data: data}, nil
}

func (h *Handlers) ResetDiscovery(params json.RawMessage) (interface{}, *Error) {
	h.host.ResetDiscovery()
	return map[string]interface{}{
		"success": true,
		"message": "Discovery dial state reset",
	}, nil
}

func (h *Handlers) ForceBootstrap(params json.RawMessage) (interface{}, *Error) {
	h.host.ForceBootstrap()
	return map[string]interface{}{
		"success": true,
		"message": "Force bootstrap triggered",
	}, nil
}

func (h *Handlers) DebugDialState(params json.RawMessage) (interface{}, *Error) {
	debug := h.host.GetDialStateDebug()
	return debug, nil
}
