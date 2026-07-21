package kafka

import (
	"math/rand"
	"time"
)

var (
	// Set of zone IDs that are used for this run
	zoneIDs []uint64

	// Time between request-logs if rate-limiting is enabled
	logPeriod = 10 * time.Millisecond
)

func init() {
	// Initialize a random set of zone IDs to use for this run of the program
	numZones := 7500 + rand.Intn(5000)
	for i := 0; i < numZones; i++ {
		zoneIDs = append(zoneIDs, uint64(rand.Int63n(899999)+1000))
	}
}

func generateRequestLogs(c chan<- RequestLog, rateLimit bool) {
	ticker := time.NewTicker(logPeriod)
	for {
		c <- generateRequestLog()
		if rateLimit {
			<-ticker.C
		}
	}
}

// GenerateRequestLog returns a RequestLog that is randomly generated.
func generateRequestLog() (r RequestLog) {
	argoRandom := rand.Intn(100)

	return RequestLog{
		TimestampUnixNanos: uint64(time.Now().UnixNano()),
		ZoneID:             zoneIDs[rand.Intn(len(zoneIDs))],
		ColoID:             uint64(rand.Int63n(150) + 1),
		Status:             generateStatusCode(),
		RequestBytes:       uint64(200 + rand.Int63n(1*1000*1000)),
		ResponseBytes:      uint64(200 + rand.Int63n(100*1000*1000)),
		CacheTTFB:          uint64(3 + rand.Int63n(950)),
		ArgoUsed:           argoRandom >= 10,
		SmartRouted:        argoRandom >= 33,
	}
}

// GenerateStatusCode returns a random HTTP status code. Frequency of status
// codes is determined by weightings.
func generateStatusCode() uint64 {
	non200s := []struct {
		status  uint64
		percent int
	}{
		{301, 2},
		{400, 3},
		{401, 2},
		{404, 5},
		{500, 1},
		{502, 2},
		{504, 2},
	}

	random := rand.Intn(100)
	for _, non200 := range non200s {
		random -= non200.percent
		if random <= 0 {
			return non200.status
		}
	}
	return 200
}
