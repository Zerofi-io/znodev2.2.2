package p2p

import "time"

func (h *Host) NetNowMs() int64 {
	if h == nil || h.netTime == nil {
		return time.Now().UnixMilli()
	}
	return h.netTime.NowMs()
}

func (h *Host) TimeOracleActive() bool {
	if h == nil || h.netTime == nil {
		return false
	}
	return h.netTime.Active()
}

func (h *Host) UpdateTimeOracle(chainTimeMs int64) int64 {
	if h == nil || h.netTime == nil {
		return 0
	}
	return h.netTime.UpdateBase(chainTimeMs)
}
