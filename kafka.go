package kafka

// RequestLog is a struct that contains metadata about an HTTP request
type RequestLog struct {
	// Location and timestamp
	TimestampUnixNanos uint64 `json:"ts"`
	ZoneID             uint64 `json:"zone_id"`
	ColoID             uint64 `json:"colo_id"`

	// Request stats
	Status        uint64 `json:"status"`
	RequestBytes  uint64 `json:"request_bytes"`
	ResponseBytes uint64 `json:"response_bytes"`
	CacheTTFB     uint64 `json:"cache_ttfb"`

	// Argo status
	ArgoUsed    bool `json:"argo_used"`
	SmartRouted bool `json:"smart_routed"`
}

// GetRequestLogFeed returns a channel on which RequestLog objects are written.
// rateLimit controls whether to generate logs at the maximum possible rate or
// cap the rate at 100 logs per second.
func GetRequestLogFeed(rateLimit bool) (<-chan RequestLog, error) {
	c := make(chan RequestLog)
	go generateRequestLogs(c, rateLimit)

	return c, nil
}
