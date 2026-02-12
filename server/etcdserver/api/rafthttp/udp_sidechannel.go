package rafthttp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	// DefaultUdpSidechannelIP is the default IP address the UDP sidechannel listens on.
	DefaultUdpSidechannelIP = "0.0.0.0"

	// DefaultUdpSidechannelPort is the default base port for the UDP sidechannel.
	DefaultUdpSidechannelPort = 7700

	// DefaultUdpSidechannelMagic is the default 16-bit magic number used in the
	// UDP sidechannel protocol header.
	DefaultUdpSidechannelMagic uint16 = 0xFEED
)

type UdpSidechannel struct {
	id        types.ID
	conn      *net.UDPConn
	listen    *net.UDPAddr
	out       chan udpSidechannelMsg
	ctx       context.Context
	cancel    context.CancelFunc
	stopc     chan struct{}
	lg        *zap.Logger
	magic     uint16
	marker    uint64
	peers     map[types.ID]*udpPeerInfo
	peersLock sync.RWMutex
}

type udpPeerInfo struct {
	id             types.ID
	remote         *net.UDPAddr
	magic          uint16
	refcount       int
	waiting        chan raftpb.Message
	ready          chan raftpb.Message
	seenMarker     uint64
	switchAckIndex uint64
}

type udpSidechannelMsg struct {
	msg  [35]byte
	peer *udpPeerInfo
}

func NewUdpSidechannel(id types.ID, ip string, port int, magic uint16, lg *zap.Logger) *UdpSidechannel {
	if lg == nil {
		fmt.Fprintln(os.Stderr, "UDP sidechannel logger is nil, not creating sidechannel")
		return nil
	}
	listen := &net.UDPAddr{
		IP:   net.ParseIP(ip),
		Port: port,
	}
	conn, err := net.ListenUDP("udp", listen)
	if err != nil {
		lg.Warn("failed to create UDP sidechannel", zap.Error(err))
		return nil
	}
	if magic == 0x0000 {
		magic = DefaultUdpSidechannelMagic
	}

	sc := &UdpSidechannel{
		id:     id,
		conn:   conn,
		listen: listen,
		out:    make(chan udpSidechannelMsg, 128), // TODO: Check if this is safe
		stopc:  make(chan struct{}),
		lg:     lg,
		magic:  magic,
		peers:  make(map[types.ID]*udpPeerInfo),
	}
	sc.ctx, sc.cancel = context.WithCancel(context.Background())

	go sc.readLoop()
	return sc
}

func (sc *UdpSidechannel) Stop() {
	sc.cancel()
	sc.conn.Close()
	<-sc.stopc
}

func (sc *UdpSidechannel) readLoop() {
	buf := make([]byte, 9000) // Maybe jumbos, idk
	for {
		select {
		case <-sc.ctx.Done():
			// sc.conn.Close()
			close(sc.out)
			close(sc.stopc)
			return
		default:
		}

		// sc.conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		n, _, err := sc.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok &&
				(netErr.Timeout() || errors.Is(err, net.ErrClosed)) {
				continue
			}
			sc.lg.Warn("error reading from UDP sidechannel", zap.Error(err))
			continue
		}

		if n < 35 {
			sc.lg.Warn("received UDP sidechannel message with invalid length",
				zap.Int("length", n))
			continue
		}

		magic := binary.BigEndian.Uint16(buf[0:2])
		msgType := raftpb.MessageType(buf[2])
		to := binary.BigEndian.Uint64(buf[3:11])
		from := binary.BigEndian.Uint64(buf[11:19])
		marker := binary.BigEndian.Uint64(buf[19:27])
		value := binary.BigEndian.Uint64(buf[27:35])

		sc.peersLock.RLock()
		peer, ok := sc.peers[types.ID(from)]
		if !ok {
			sc.lg.Warn("received UDP sidechannel message with unknown peer ID",
				zap.Uint64("peerID", from))
			sc.peersLock.RUnlock()
			continue
		}

		if magic != sc.magic &&
			!(msgType == raftpb.MsgAskAckIndexResp && magic == peer.magic) {
			sc.lg.Warn("received UDP sidechannel message with invalid magic",
				zap.Uint16("magic", magic))
			sc.peersLock.RUnlock()
			continue
		}

		switch msgType {
		case raftpb.MsgApp:
			// We don't care about this one; these are purely for the switch.
			// TODO: Consider logging (should switch drop these?)
			sc.peersLock.RUnlock()
			continue
		case raftpb.MsgAskAckIndex:
			// Same as for MsgApp; this is for the switch.
			// TODO: Consider logging (should switch drop these?)
			sc.peersLock.RUnlock()
			continue
		case raftpb.MsgReadIndex:
			// This is the big one- read the updated index from the switch,
			// if it exists. Release a matching MsgReadIndex if available.
			if value == ^uint64(0) {
				sc.lg.Warn("received UDP sidechannel MsgReadIndex with unmarked value. Is the switch active?")
				sc.peersLock.RUnlock()
				continue
			}
			if len(peer.waiting) == 0 {
				sc.lg.Warn("received UDP sidechannel MsgReadIndex response but no matching request is waiting",
					zap.String("peerID", peer.id.String()))
				sc.peersLock.RUnlock()
				continue
			}
			// TODO: peer.waiting needs to be a prio queue on m.Index,
			// else this becomes a very ugly O(n^2) operation during bursts
			for range len(peer.waiting) {
				m := <-peer.waiting
				if m.Index <= marker {
					m.Commit = value
					select {
					case peer.ready <- m:
					default:
						sc.lg.Warn("UDP sidechannel peer.ready full, dropping MsgReadIndex",
							zap.String("peerID", peer.id.String()))
					}
				} else {
					// TODO: Zero the retry timer if this happens
					peer.waiting <- m
				}
			}
		case raftpb.MsgAskAckIndexResp:
			// This needs to generate a new raftpb.Message to go into Raft.
			// These only come from the switch in response to MsgApp or MsgAskAckIndex.
			genMsg := raftpb.Message{
				Type:  raftpb.MsgAskAckIndexResp,
				To:    to,
				From:  from,
				Index: value,
			}
			select {
			case peer.ready <- genMsg:
			default:
				sc.lg.Warn("UDP sidechannel peer.ready full, dropping MsgAskAckIndexResp",
					zap.String("peerID", peer.id.String()))
			}
		default:
			sc.lg.Warn("received UDP sidechannel message with unknown type",
				zap.Uint8("type", uint8(msgType)))
		}
		sc.peersLock.RUnlock()
	}
}

func (sc *UdpSidechannel) Flush() {
	for {
		select {
		case msg := <-sc.out:
			_, err := sc.conn.WriteToUDP(msg.msg[:], msg.peer.remote)
			if err != nil {
				sc.lg.Warn("failed to send MsgApp UDP sidechannel message",
					zap.String("peerID", msg.peer.id.String()),
					zap.Error(err))
			}
		default:
			return
		}
	}
	// TODO: Maybe send retry for messages waiting in <-ready here?
}

func (sc *UdpSidechannel) ProcessOutgoingMessage(m *raftpb.Message) {
	if !(m.Type == raftpb.MsgApp || m.Type == raftpb.MsgReadIndex ||
		m.Type == raftpb.MsgAskAckIndex) { // ||
		// m.Type == raftpb.MsgHeartbeat) {
		return
	}

	sc.peersLock.RLock()
	defer sc.peersLock.RUnlock()
	peer, ok := sc.peers[types.ID(m.To)]
	if !ok {
		return
	}

	if len(sc.out) >= cap(sc.out)-1 {
		sc.lg.Warn("UDP sidechannel out channel full, flushing before adding new message",
			zap.String("peerID", peer.id.String()))
		sc.Flush() // Flush now
	}

	sideChannelMsg := udpSidechannelMsg{peer: peer}
	switch m.Type {
	// For MsgApp, we update the switch's index for the peerID, Index,
	// and the switch should generate a MsgAskAckIndexResp to throw back to this host.
	case raftpb.MsgApp:
		proposedIndex := m.Index + uint64(len(m.Entries))
		binary.BigEndian.PutUint16(sideChannelMsg.msg[0:2], peer.magic)
		sideChannelMsg.msg[2] = byte(raftpb.MsgApp)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[3:11], uint64(peer.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[11:19], uint64(sc.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[19:27], 0)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[27:35], proposedIndex)
		sc.out <- sideChannelMsg
	// For MsgReadIndex, we write a -1 value that the switch should swap to its saved index
	// value for the peer that it saved from an earlier MsgApp.
	case raftpb.MsgReadIndex:
		cMarker := atomic.AddUint64(&sc.marker, 1)
		binary.BigEndian.PutUint16(sideChannelMsg.msg[0:2], peer.magic)
		sideChannelMsg.msg[2] = byte(raftpb.MsgReadIndex)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[3:11], uint64(peer.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[11:19], uint64(sc.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[19:27], cMarker)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[27:35], ^uint64(0))
		m.Index = cMarker
		sc.out <- sideChannelMsg
	// For MsgAskAckIndex, we should generate a UDP version the switch can respond to.
	case raftpb.MsgAskAckIndex:
		binary.BigEndian.PutUint16(sideChannelMsg.msg[0:2], peer.magic)
		sideChannelMsg.msg[2] = byte(raftpb.MsgAskAckIndex)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[3:11], uint64(peer.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[11:19], uint64(sc.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[19:27], 0)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[27:35], 0)
		sc.out <- sideChannelMsg
	default:
		sc.lg.Error("Somehow got to default case of ProcessOutgoingMessage(), should never happen")
	}
}

func (sc *UdpSidechannel) ProcessIncomingMessage(ctx context.Context, m raftpb.Message) bool {
	if !(m.Type == raftpb.MsgReadIndex) {
		return false
	}

	sc.peersLock.RLock()
	defer sc.peersLock.RUnlock()
	peer, ok := sc.peers[types.ID(m.From)]
	if !ok {
		return false
	}
	select {
	case peer.waiting <- m:
	default:
		sc.lg.Warn("UDP sidechannel peer.waiting full, dropping MsgReadIndex",
			zap.String("peerID", peer.id.String()))
	}
	return true
}

func (sc *UdpSidechannel) AttachPeer(id types.ID, ipStr string, portStr string, magicStr string) {
	magic, err := MagicFromStr(magicStr)
	if err != nil {
		sc.lg.Warn("failed to parse UDP sidechannel magic from string",
			zap.String("magicStr", magicStr), zap.Error(err))
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		sc.lg.Warn("failed to parse UDP sidechannel port from string",
			zap.String("portStr", portStr), zap.Error(err))
		return
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		sc.lg.Warn("failed to parse UDP sidechannel IP from string",
			zap.String("ipStr", ipStr))
		return
	}

	sc.peersLock.Lock()
	defer sc.peersLock.Unlock()
	peer, ok := sc.peers[id]
	if ok {
		peer.refcount++
		if !peer.remote.IP.Equal(ip) || peer.remote.Port != port || peer.magic != magic {
			sc.lg.Warn("UDP sidechannel peer info changed for peer, updating",
				zap.String("peerID", id.String()),
				zap.String("oldIP", peer.remote.IP.String()),
				zap.Int("oldPort", peer.remote.Port),
				zap.String("oldMagic", fmt.Sprintf("%04x", peer.magic)),
				zap.String("newIP", ipStr),
				zap.String("newPort", portStr),
				zap.String("newMagic", magicStr))
		} else {
			return
		}
	}
	remote := &net.UDPAddr{
		IP:   ip,
		Port: port,
	}

	if peer == nil {
		peer = &udpPeerInfo{
			id:       id,
			remote:   remote,
			magic:    magic,
			refcount: 1,
			waiting:  make(chan raftpb.Message, streamBufSize),
			ready:    make(chan raftpb.Message, streamBufSize),
		}
		sc.peers[id] = peer
	} else {
		peer.remote = remote
		peer.magic = magic
	}
}

func (sc *UdpSidechannel) DetachPeer(id types.ID) {
	sc.peersLock.Lock()
	defer sc.peersLock.Unlock()
	peer, ok := sc.peers[id]
	if !ok {
		return
	}
	peer.refcount--
	if peer.refcount <= 0 {
		delete(sc.peers, id)
		close(peer.ready)
		close(peer.waiting)
	}
}

func (sc *UdpSidechannel) GetReadyChan(id types.ID) <-chan raftpb.Message {
	sc.peersLock.RLock()
	defer sc.peersLock.RUnlock()
	peer, ok := sc.peers[id]
	if !ok {
		return nil
	}
	return peer.ready
}

func (sc *UdpSidechannel) Port() string {
	return strconv.Itoa(sc.listen.Port)
}

func (sc *UdpSidechannel) Ip() string {
	return sc.listen.IP.String()
}

func (sc *UdpSidechannel) MagicAsStr() string {
	return fmt.Sprintf("%04x", sc.magic)
}

func MagicFromStr(s string) (uint16, error) {
	val, err := strconv.ParseUint(s, 16, 16)
	if err != nil {
		return 0, err
	}
	return uint16(val), nil
}
