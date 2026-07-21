package main

import (
	"encoding/json"
	"log_consumer/lib/kafka"
	"net/http"
	"strconv"
	"sync"

	"log"
)

const maxLogsPerZone = 1000

type Store struct {
	mu    sync.RWMutex
	zones map[uint64][]kafka.RequestLog
}

func NewStore() *Store {
	return &Store{zones: make(map[uint64][]kafka.RequestLog)}
}

func (s *Store) Add(l kafka.RequestLog) {
	// lock, append, trim to last 1000

	s.mu.Lock()
	defer s.mu.Unlock()

	logs := append(s.zones[l.ZoneID], l)

	if len(logs) > maxLogsPerZone {

		// logs[x:y]
		// logs[x:]
		logs = logs[len(logs)-maxLogsPerZone:]
	}

	s.zones[l.ZoneID] = logs
}

func (s *Store) Logs(zoneID uint64) ([]kafka.RequestLog, bool) {
	// rlock, return a copy
	// return nil, false

	s.mu.RLock()
	defer s.mu.RUnlock()

	logs, ok := s.zones[zoneID]

	if !ok {
		return nil, false
	}

	out := make([]kafka.RequestLog, len(logs))
	copy(out, logs)

	return out, true
}

func (s *Store) ZoneIDs() []uint64 {
	// rlock, collect keys

	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]uint64, 0, len(s.zones))
	for id := range s.zones {
		ids = append(ids, id)
	}

	return ids
}

//

// a) getting a list of zone IDs; and b) getting all stored logs for a given zone ID.

var (
	// This will limit request log generation to 100 logs per second. Remove
	// this rate limit once basic functionality is complete.
	rateLimit = true
)

func main() {
	// TODO: Call kafka.GetRequestLogFeed(rateLimit) to get channel of RequestLogs

	// get the channel of request logs from the kafka feed
	feed, err := kafka.GetRequestLogFeed(rateLimit)

	if err != nil {
		// log
	}

	store := NewStore()

	// consumer: goroutine

	go func() {
		for l := range feed {
			store.Add(l)
		}
	}()

	mux := http.NewServeMux()

	// GET /zones
	mux.HandleFunc("GET /zones", func(w http.ResponseWriter, r *http.Request) {
		log.Println("api invoked")
		writeJson(w, store.ZoneIDs())
	})

	// GET /zones/{id}
	mux.HandleFunc("GET /zones/{id}", func(w http.ResponseWriter, r *http.Request) {

		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)

		if err != nil {
			// error
			http.Error(w, "invalid zone id", http.StatusBadRequest)
			return
		}

		logs, ok := store.Logs(id)

		if !ok {
			http.Error(w, "zone not found", http.StatusNotFound)
			return
		}
		writeJson(w, logs)

	})

	log.Println("listening on: 8080")
	log.Fatal(http.ListenAndServe(":8080", mux))

}

func writeJson(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}
