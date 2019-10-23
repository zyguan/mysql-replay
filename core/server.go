package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"go.uber.org/zap"
)

func NewHandler(opts HandlerOptions) http.Handler {
	return &handler{HandlerOptions: opts, ws: make(map[string]struct{})}
}

type HandlerOptions struct {
	Store   ReplayReqStore
	Fetcher ReplayDataFetcher
	Filter  PacketFilter
	Emit    func(gopacket.Packet)
	Speed   float64
}

type handler struct {
	HandlerOptions
	ws   map[string]struct{}
	lock sync.Mutex
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req ReplayReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	zap.L().Info("recv replay request", zap.Any("req", req))

	if err := h.Store.Push(req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	go func() {
		p, err := h.Fetcher.Fetch(req)
		if err != nil {
			zap.L().Warn("fetch file", zap.String("url", req.URL), zap.Error(err))
			h.Store.Call(req.CH, func(req ReplayReq) (s string, e error) { return p, err })
		} else {
			h.withLock(func() {
				_, ok := h.ws[req.CH]
				if ok {
					return
				}
				h.ws[req.CH] = struct{}{}
				go h.startWorker(req.CH)
				zap.L().Info("start a new worker to process request channel", zap.String("ch", req.CH))
			})
		}
	}()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Code int `json:"code"`
	}{Code: 0})
}

func (h *handler) startWorker(ch string) {
	d := 200
	for {
		err := h.Store.Call(ch, func(req ReplayReq) (string, error) {
			p, err := h.Fetcher.Fetch(req)
			if err != nil {
				zap.L().Error("fetch error", zap.Error(err))
				return p, err
			}
			OfflineReplayTask{File: p, Filter: h.Filter, SpeedRatio: h.Speed}.Run(h.Emit)
			return p, err
		})
		if err == NOOP {
			time.Sleep(time.Duration(d) * time.Millisecond)
			if d < 60000 {
				d += 200
			}
		} else {
			d = 200
		}
	}
}

func (h *handler) withLock(f func()) {
	h.lock.Lock()
	defer h.lock.Unlock()
	f()
}

type OfflineReplayTask struct {
	File       string
	Filter     PacketFilter
	SpeedRatio float64
}

func (t OfflineReplayTask) Run(consume func(pkt gopacket.Packet)) {
	h, err := pcap.OpenOffline(t.File)
	if err != nil {
		zap.L().Error("cannot open pcap file", zap.Error(err))
		return
	}
	src := gopacket.NewPacketSource(h, h.LinkType())
	t0 := time.Now()
	var meta0 *gopacket.PacketMetadata
	for pkt := range src.Packets() {
		l := pkt.Layer(layers.LayerTypeTCP)
		if l == nil {
			zap.L().Info("filter out non-tcp packet")
			continue
		}
		if meta0 == nil {
			meta0 = pkt.Metadata()
		}
		tcp := l.(*layers.TCP)
		if t.Filter != nil {
			if reason, ok := t.Filter(tcp); !ok {
				zap.L().Warn("filter out packet", zap.String("reason", reason))
				continue
			}
		}
		d := pkt.Metadata().Timestamp.Sub(meta0.Timestamp)
		for t.SpeedRatio > 0 && float64(time.Now().Sub(t0))*t.SpeedRatio < float64(d) {
			time.Sleep(time.Millisecond)
		}
		if consume != nil {
			consume(pkt)
		} else {
			zap.L().Warn("ignore packet due to consume is nil")
		}
	}
}

type PacketFilter func(*layers.TCP) (string, bool)

func FilterByPort(ports []int) PacketFilter {
	return func(tcp *layers.TCP) (string, bool) {
		for _, port := range ports {
			if port == int(tcp.SrcPort) || port == int(tcp.DstPort) {
				return "", true
			}
		}
		return fmt.Sprintf("nether src port (%d) nor dst port (%d) is in whitelist %v",
			tcp.SrcPort, tcp.DstPort, ports), false
	}
}
