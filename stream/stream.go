package stream

import (
	"bytes"
	"encoding/hex"
	"io"
	"strconv"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"github.com/juju/errors"
	"github.com/zyguan/mysql-replay/stats"
	"go.uber.org/zap"
)

type ConnKey [2]gopacket.Flow

func (k ConnKey) SrcAddr() string {
	return k[0].Src().String() + ":" + k[1].Src().String()
}

func (k ConnKey) DstAddr() string {
	return k[0].Dst().String() + ":" + k[1].Dst().String()
}

func (k ConnKey) String() string {
	return k.SrcAddr() + "->" + k.DstAddr()
}

func (k ConnKey) Reverse() ConnKey {
	return ConnKey{k[0].Reverse(), k[1].Reverse()}
}

type FactoryOptions struct {
	ConnCacheSize uint
}

func NewMySQLStreamFactory(factory func(key ConnKey) MySQLStreamHandler, opts FactoryOptions) reassembly.StreamFactory {
	if factory == nil {
		factory = defaultStreamHandlerFactory
	}
	return &mysqlStreamFactory{new: factory, opts: opts}
}

var _ reassembly.StreamFactory = &mysqlStreamFactory{}

type mysqlStreamFactory struct {
	new  func(key ConnKey) MySQLStreamHandler
	opts FactoryOptions
}

func (f *mysqlStreamFactory) New(netFlow, tcpFlow gopacket.Flow, tcp *layers.TCP, ac reassembly.AssemblerContext) reassembly.Stream {
	key := ConnKey{netFlow, tcpFlow}
	log := zap.L().Named("mysql-stream").With(zap.String("conn", key.String()))
	handler, ch, done := f.new(key), make(chan MySQLPayload, f.opts.ConnCacheSize), make(chan struct{})
	go func() {
		defer close(done)
		for p := range ch {
			handler.OnPayload(p)
		}
	}()
	stats.Add(stats.Streams, 1)
	return &mysqlStream{key, log, nil, 0, ch, done, handler, f.opts}
}

var _ reassembly.Stream = &mysqlStream{}

type mysqlStream struct {
	key ConnKey
	log *zap.Logger

	buf *bytes.Buffer
	seq int

	ch   chan MySQLPayload
	done chan struct{}

	h    MySQLStreamHandler
	opts FactoryOptions
}

func (s *mysqlStream) Accept(tcp *layers.TCP, ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence, start *bool, ac reassembly.AssemblerContext) bool {
	// TODO: do basic validation, ref: https://github.com/google/gopacket/blob/ec90f6c2c025516eabdf6bf374b615ff0bf32c21/examples/reassemblydump/main.go#L336
	if !s.h.Accept(tcp, dir, nextSeq) {
		return false
	}
	*start = true
	return true
}

func (s *mysqlStream) ReassembledSG(sg reassembly.ScatterGather, ac reassembly.AssemblerContext) {
	length, _ := sg.Lengths()
	if length == 0 {
		return
	}

	data := sg.Fetch(length)
	dir, _, _, _ := sg.Info()

	p := MySQLPayload{T: ac.GetCaptureInfo().Timestamp, Key: s.key, Dir: dir}
	buf := s.buf
	if buf == nil {
		buf = bytes.NewBuffer(data)
		s.seq = lookupPacketSeq(buf)
	} else {
		buf.Write(data)
	}
	for buf.Len() > 0 {
		restData, size := buf.Bytes(), lookupPacketSize(buf)
		if size+4 > buf.Len() {
			s.buf = buf
			return
		}
		pkt, err := readPacket(buf)
		if err != nil {
			s.log.Error("read mysql packet",
				zap.Int("size", size),
				zap.Int("len", len(data)),
				zap.String("raw", hex.EncodeToString(restData)),
				zap.Error(err))
		}
		p.Packets = append(p.Packets, pkt)
	}
	p.StartSeq = s.seq
	s.buf, s.seq = nil, 0

	s.ch <- p
	stats.Add(stats.Packets, 1)
}

func (s *mysqlStream) ReassemblyComplete(ac reassembly.AssemblerContext) bool {
	close(s.ch)
	<-s.done
	s.h.OnClose()
	stats.Add(stats.Streams, -1)
	return false
}

func lookupPacketSize(buf *bytes.Buffer) int {
	if buf.Len() < 3 {
		return -1
	}
	bs := buf.Bytes()[:3]
	return int(uint32(bs[0]) | uint32(bs[1])<<8 | uint32(bs[2])<<16)
}

func lookupPacketSeq(buf *bytes.Buffer) int {
	if buf.Len() < 4 {
		return -1
	}
	return int(buf.Bytes()[3])
}

func readPacket(r io.Reader) ([]byte, error) {
	data, err := readOnePacket(r, nil)
	if err != nil {
		return nil, err
	}
	if len(data) < maxPacketSize {
		return data, nil
	}

	// handle multi-packet
	var seq uint8
	for {
		seq += 1
		buf, err := readOnePacket(r, &seq)
		if err != nil {
			return nil, err
		}
		data = append(data, buf...)
		if len(buf) < maxPacketSize {
			break
		}
	}

	return data, nil
}

func readOnePacket(r io.Reader, seq *uint8) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, errors.Annotate(err, "read header")
	}

	if seq != nil && header[3] != *seq {
		return nil, errors.New("invalid sequence: " + strconv.Itoa(int(header[3])) + " != " + strconv.Itoa(int(*seq)))
	}
	size := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, errors.Annotate(err, "read payload")
	}

	return payload, nil
}

type MySQLPayload struct {
	T        time.Time
	Key      ConnKey
	Dir      reassembly.TCPFlowDirection
	StartSeq int
	Packets  [][]byte
}
