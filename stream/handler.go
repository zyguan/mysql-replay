package stream

import (
	"database/sql"
	"encoding/hex"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"go.uber.org/zap"
)

type MySQLStreamHandler interface {
	Accept(tcp *layers.TCP, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence) bool
	OnPayload(p MySQLPayload)
	OnClose()
}

var _ MySQLStreamHandler = &defaultHandler{}

type defaultHandler struct{ Log *zap.Logger }

func defaultStreamHandlerFactory(key ConnKey) MySQLStreamHandler {
	log := zap.L().With(zap.String("conn", key.String()))
	log.Info("open")
	return &defaultHandler{Log: log}
}

func (h *defaultHandler) Accept(tcp *layers.TCP, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence) bool {
	return true
}

func (h *defaultHandler) OnPayload(p MySQLPayload) {
	pkts := make([]string, len(p.Packets))
	for i, pkt := range p.Packets {
		pkts[i] = hex.EncodeToString(pkt)
	}
	h.Log.Info("send", zap.Time("t", p.T), zap.String("dir", p.Dir.String()), zap.Strings("pkts", pkts))
}

func (h *defaultHandler) OnClose() {
	h.Log.Info("close")
}

func RejectConn(key ConnKey) MySQLStreamHandler {
	return &rejectHandler{}
}

var _ MySQLStreamHandler = &rejectHandler{}

type rejectHandler struct{}

func (r *rejectHandler) Accept(tcp *layers.TCP, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence) bool {
	return false
}

func (r *rejectHandler) OnPayload(p MySQLPayload) {}

func (r *rejectHandler) OnClose() {}

type ReplayOptions struct {
	DryRun    bool
	TargetDSN string
}

func (o ReplayOptions) NewStreamHandler(key ConnKey) MySQLStreamHandler {
	log := zap.L().Named("mysql-stream")
	rh := &replayHandler{opts: o, key: key, log: log}
	if o.DryRun {
		log.Info("connect to target db", zap.String("dsn", o.TargetDSN))
		return rh
	}
	var err error
	rh.db, err = sql.Open("mysql", o.TargetDSN)
	if err != nil {
		log.Error("reject connection due to error",
			zap.String("dsn", o.TargetDSN), zap.Error(err))
		return RejectConn(key)
	}
	rh.log.Debug("open connection to " + rh.opts.TargetDSN)
	return rh
}

var _ MySQLStreamHandler = &replayHandler{}

type replayHandler struct {
	opts ReplayOptions
	key  ConnKey
	log  *zap.Logger
	db   *sql.DB
}

func (rh *replayHandler) Accept(tcp *layers.TCP, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence) bool {
	return true
}

func (rh *replayHandler) OnPayload(p MySQLPayload) {
	if p.Dir == reassembly.TCPDirClientToServer {
		// TODO: session may be created on other database (rather than target dsn), we'd better check handshake packets and reconstruct r.db if needed.

		if len(p.Packets) == 0 || len(p.Packets[0]) == 0 {
			rh.l(p.Dir).Warn("drop empty payload", zap.ByteStrings("packets", p.Packets))
			return
		}
		raw := p.Packets[0]
		cmd := raw[0]
		if p.StartSeq == 0 && cmd == comQuery {
			query := string(raw[1:])
			if rh.db == nil {
				rh.l(p.Dir).Info("execute query", zap.String("sql", query))
				return
			}
			if _, err := rh.db.Exec(query); err != nil {
				rh.l(p.Dir).Warn("execute query", zap.String("sql", query), zap.Error(err))
			}
		} else {
			switch cmd {
			case comFieldList:
			default:
				rh.l(p.Dir).Info("ignore non-query request", zap.String("raw", hex.EncodeToString(raw)))
			}
		}
	} else {
		// TODO: we can handle prepared stmts here.
	}
}

func (rh *replayHandler) OnClose() {
	rh.log.Debug("close connection to " + rh.opts.TargetDSN)
	if rh.db != nil {
		rh.db.Close()
	}
}

func (rh *replayHandler) l(dir reassembly.TCPFlowDirection) *zap.Logger {
	if dir == reassembly.TCPDirClientToServer {
		return rh.log.With(zap.String("conn", rh.key.String()))
	} else {
		return rh.log.With(zap.String("conn", rh.key.Reverse().String()))
	}
}
