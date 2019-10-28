package stats

import (
	"sync"
	"sync/atomic"
)

const (
	Packets     = "packets"
	Queries     = "queries"
	Streams     = "streams"
	Connections = "connections"

	FailedQueries = "err.queries"
)

var (
	nPackets int64
	nQueries int64
	nStreams int64
	nConns   int64

	nErrQueries int64

	others = make(map[string]int64)
	lock   sync.RWMutex
)

func Add(name string, delta int64) int64 {
	switch name {
	case Packets:
		return atomic.AddInt64(&nPackets, delta)
	case Queries:
		return atomic.AddInt64(&nQueries, delta)
	case Streams:
		return atomic.AddInt64(&nStreams, delta)
	case Connections:
		return atomic.AddInt64(&nConns, delta)
	case FailedQueries:
		return atomic.AddInt64(&nErrQueries, delta)
	default:
		lock.Lock()
		defer lock.Unlock()
		others[name] += delta
		return others[name]
	}
}

func Get(name string) int64 {
	switch name {
	case Packets:
		return atomic.LoadInt64(&nPackets)
	case Queries:
		return atomic.LoadInt64(&nQueries)
	case Streams:
		return atomic.LoadInt64(&nStreams)
	case Connections:
		return atomic.LoadInt64(&nConns)
	case FailedQueries:
		return atomic.LoadInt64(&nErrQueries)
	default:
		lock.RLock()
		defer lock.RUnlock()
		return others[name]
	}
}

func Dump() map[string]int64 {
	lock.RLock()
	defer lock.RUnlock()

	out := make(map[string]int64, len(others)+5)
	for k, v := range others {
		out[k] = v
	}
	out[Packets] = nPackets
	out[Queries] = nQueries
	out[Streams] = nStreams
	out[Connections] = nConns
	out[FailedQueries] = nErrQueries

	return out
}
