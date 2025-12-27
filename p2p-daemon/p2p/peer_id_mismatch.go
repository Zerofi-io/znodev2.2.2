package p2p

import (
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
)

func parsePeerIDMismatchError(err error) (peer.ID, peer.ID, bool) {
	if err == nil {
		return "", "", false
	}
	s := err.Error()
	const prefix = "peer id mismatch: expected "
	i := strings.Index(s, prefix)
	if i < 0 {
		return "", "", false
	}
	s = s[i+len(prefix):]
	parts := strings.SplitN(s, ", but remote key matches ", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	expectedStr := strings.TrimSpace(parts[0])
	remotePart := strings.TrimSpace(parts[1])
	remoteFields := strings.Fields(remotePart)
	if len(remoteFields) == 0 {
		return "", "", false
	}
	remoteStr := remoteFields[0]
	expected, err1 := peer.Decode(expectedStr)
	remote, err2 := peer.Decode(remoteStr)
	if err1 != nil || err2 != nil {
		return "", "", false
	}
	return expected, remote, true
}
