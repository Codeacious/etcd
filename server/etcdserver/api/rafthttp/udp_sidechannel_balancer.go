package rafthttp

import (
	"context"
	"math/rand/v2"
	"net"
	"strconv"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"
)

// Used to enable the load balancer in gRPC
const UdpSidechannelBalancerName = "etcd_read_gate"

func RegisterUdpSidechannelBalancer(magic uint16, udpPort int) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		return err
	}
	balancer.Register(base.NewBalancerBuilder(
		UdpSidechannelBalancerName,
		&udpSidechannelBalancerBuilder{
			conn:    conn,
			magic:   magic,
			udpPort: udpPort,
			fromID:  rand.Uint64(),
		},
		base.Config{},
	))
	return nil
}

type udpSidechannelBalancerBuilder struct {
	conn    *net.UDPConn
	magic   uint16
	udpPort int
	fromID  uint64
}

type readGatePick struct {
	sc      balancer.SubConn
	udpAddr *net.UDPAddr
}

func (b *udpSidechannelBalancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var picks []readGatePick
	for sc, sci := range info.ReadySCs {
		host, _, err := net.SplitHostPort(sci.Address.Addr)
		if err != nil {
			continue
		}
		picks = append(picks, readGatePick{
			sc:      sc,
			udpAddr: &net.UDPAddr{IP: net.ParseIP(host), Port: b.udpPort},
		})
	}
	if len(picks) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	return &udpSidechannelBalancer{b: b, picks: picks}
}

type udpSidechannelBalancer struct {
	b     *udpSidechannelBalancerBuilder
	picks []readGatePick
	cur   uint32 // atomic round-robin counter
}

func (p *udpSidechannelBalancer) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	n := atomic.AddUint32(&p.cur, 1)
	pick := p.picks[int(n-1)%len(p.picks)]

	if md, ok := metadata.FromOutgoingContext(info.Ctx); ok {
		if marker := ExtractReadGateMarker(md); marker != 0 {
			msg := EncodeReadGateMsg(p.b.magic, p.b.fromID, 0, marker)
			p.b.conn.WriteToUDP(msg[:], pick.udpAddr)
		}
	}

	return balancer.PickResult{SubConn: pick.sc}, nil
}

// Attaches the read-gate marker to the outgoing gRPC ctx metadata.
// Should be called for any outgoing linearizable read.
func AttachReadGateMarker(ctx context.Context, marker uint64) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		ReadGateMarkerMetaKey, strconv.FormatUint(marker, 10))
}

// Parses the read-gate marker from gRPC metadata.
// Use the appropriate extraction function before calling this.
// (metadata.FromIncomingContext on the server, metadata.FromOutgoingContext on the client)
func ExtractReadGateMarker(md metadata.MD) uint64 {
	vals := md.Get(ReadGateMarkerMetaKey)
	if len(vals) == 0 {
		return 0
	}
	v, _ := strconv.ParseUint(vals[0], 10, 64)
	return v
}
