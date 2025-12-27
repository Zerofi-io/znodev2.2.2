package rpc

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/zerofi-io/znodev2/p2p-daemon/p2p"
)

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id"`
}

type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *Error      `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type Error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

const (
	ErrCodeParse       = -32700
	ErrCodeInvalidReq  = -32600
	ErrCodeMethodNF    = -32601
	ErrCodeInvalidArgs = -32602
	ErrCodeInternal    = -32603
	ErrCodeTimeout     = -32000
	ErrCodePeerFailed  = -32001
	ErrCodeBusy        = -32002
)

const (
	maxRequestLineBytes  = 4 << 20
	maxInFlightRequests  = 64
	defaultReadDeadline  = 30 * time.Second
	defaultWriteDeadline = 30 * time.Second
)

type Server struct {
	host     *p2p.Host
	handlers *Handlers
}

func NewServer(host *p2p.Host) *Server {
	return &Server{
		host:     host,
		handlers: NewHandlers(host),
	}
}

func (s *Server) HandleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, maxRequestLineBytes+1)
	encoder := json.NewEncoder(conn)
	var writeMu sync.Mutex
	sem := make(chan struct{}, maxInFlightRequests)

	write := func(resp Response) bool {
		writeMu.Lock()
		_ = conn.SetWriteDeadline(time.Now().Add(defaultWriteDeadline))
		err := encoder.Encode(resp)
		writeMu.Unlock()
		if err != nil {
			log.Printf("Write error: %v", err)
			return false
		}
		return true
	}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(defaultReadDeadline))
		line, err := reader.ReadSlice('\n')
		if err != nil {
			if err == bufio.ErrBufferFull {
				log.Printf("Read error: request too large")
				return
			}
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() && len(line) == 0 {
				continue
			}
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}
		if len(line) > maxRequestLineBytes {
			log.Printf("Read error: request too large")
			return
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			resp := Response{
				JSONRPC: "2.0",
				Error: &Error{
					Code:    ErrCodeParse,
					Message: "Parse error",
				},
				ID: nil,
			}
			if !write(resp) {
				return
			}
			continue
		}

		if req.JSONRPC != "2.0" {
			resp := Response{
				JSONRPC: "2.0",
				Error: &Error{
					Code:    ErrCodeInvalidReq,
					Message: "Invalid request: must use JSON-RPC 2.0",
				},
				ID: req.ID,
			}
			if !write(resp) {
				return
			}
			continue
		}

		reqCopy := req
		select {
		case sem <- struct{}{}:
			go func() {
				defer func() { <-sem }()
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Panic in %s: %v", reqCopy.Method, r)
						resp := Response{
							JSONRPC: "2.0",
							Error: &Error{
								Code:    ErrCodeInternal,
								Message: "Internal error",
							},
							ID: reqCopy.ID,
						}
						if !write(resp) {
							conn.Close()
						}
					}
				}()
				result, rpcErr := s.dispatch(reqCopy.Method, reqCopy.Params)
				resp := Response{
					JSONRPC: "2.0",
					ID:      reqCopy.ID,
				}
				if rpcErr != nil {
					resp.Error = rpcErr
				} else {
					resp.Result = result
				}
				if !write(resp) {
					conn.Close()
				}
			}()
		default:
			resp := Response{
				JSONRPC: "2.0",
				Error: &Error{
					Code:    ErrCodeBusy,
					Message: "Server busy",
				},
				ID: reqCopy.ID,
			}
			if !write(resp) {
				return
			}
		}
	}
}

func (s *Server) dispatch(method string, params json.RawMessage) (interface{}, *Error) {
	switch method {
	case "P2P.Start":
		return s.handlers.Start(params)
	case "P2P.Stop":
		return s.handlers.Stop(params)
	case "P2P.Status":
		return s.handlers.Status(params)
	case "P2P.Features":
		return s.handlers.Features(params)
	case "P2P.SetTimeOracle":
		return s.handlers.SetTimeOracle(params)
	case "P2P.JoinCluster":
		return s.handlers.JoinCluster(params)
	case "P2P.LeaveCluster":
		return s.handlers.LeaveCluster(params)
	case "P2P.BroadcastRoundData":
		return s.handlers.BroadcastRoundData(params)
	case "P2P.WaitForRoundCompletion":
		return s.handlers.WaitForRoundCompletion(params)
	case "P2P.GetRoundData":
		return s.handlers.GetRoundData(params)
	case "P2P.GetPeerPayloads":
		return s.handlers.GetPeerPayloads(params)
	case "P2P.BroadcastSignature":
		return s.handlers.BroadcastSignature(params)
	case "P2P.WaitForSignatureBarrier":
		return s.handlers.WaitForSignatureBarrier(params)
	case "P2P.ComputeConsensusHash":
		return s.handlers.ComputeConsensusHash(params)
	case "P2P.RunConsensus":
		return s.handlers.RunConsensus(params)
	case "P2P.PBFTDebug":
		return s.handlers.PBFTDebug(params)
	case "P2P.BroadcastIdentity":
		return s.handlers.BroadcastIdentity(params)
	case "P2P.WaitForIdentities":
		return s.handlers.WaitForIdentities(params)
	case "P2P.GetPeerBindings":
		return s.handlers.GetPeerBindings(params)
	case "P2P.ClearPeerBindings":
		return s.handlers.ClearPeerBindings(params)
	case "P2P.GetPeerScores":
		return s.handlers.GetPeerScores(params)
	case "P2P.ReportPeerFailure":
		return s.handlers.ReportPeerFailure(params)
	case "P2P.SetHeartbeatDomain":
		return s.handlers.SetHeartbeatDomain(params)
	case "P2P.StartHeartbeat":
		return s.handlers.StartHeartbeat(params)
	case "P2P.StopHeartbeat":
		return s.handlers.StopHeartbeat(params)
	case "P2P.BroadcastHeartbeat":
		return s.handlers.BroadcastHeartbeat(params)
	case "P2P.GetRecentPeers":
		return s.handlers.GetRecentPeers(params)
	case "P2P.StartQueueDiscovery":
		return s.handlers.StartQueueDiscovery(params)
	case "P2P.GetLastHeartbeat":
		return s.handlers.GetLastHeartbeat(params)
	case "P2P.GetLastHeartbeatWithProof":
		return s.handlers.GetLastHeartbeatWithProof(params)
	case "P2P.ClearClusterSignatures":
		return s.handlers.ClearClusterSignatures(params)
	case "P2P.ClearClusterRounds":
		return s.handlers.ClearClusterRounds(params)
	case "P2P.GetHeartbeats":
		return s.handlers.GetHeartbeats(params)
	case "P2P.WaitForIdentityBarrier":
		return s.handlers.WaitForIdentityBarrier(params)
	case "P2P.BroadcastPreSelection":
		return s.handlers.BroadcastPreSelection(params)
	case "P2P.VotePreSelection":
		return s.handlers.VotePreSelection(params)
	case "P2P.WaitPreSelection":
		return s.handlers.WaitPreSelection(params)
	case "P2P.GetPreSelectionProposal":
		return s.handlers.GetPreSelectionProposal(params)
	case "P2P.ResetDiscovery":
		return s.handlers.ResetDiscovery(params)
	case "P2P.ForceBootstrap":
		return s.handlers.ForceBootstrap(params)
	case "P2P.DebugDialState":
		return s.handlers.DebugDialState(params)
	default:
		return nil, &Error{
			Code:    ErrCodeMethodNF,
			Message: "Method not found: " + method,
		}
	}
}
