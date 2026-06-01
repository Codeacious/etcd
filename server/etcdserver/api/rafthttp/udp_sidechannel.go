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
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	// DefaultUdpSidechannelIP is the default IP address the UDP sidechannel listens on.
	DefaultUdpSidechannelIP = "0.0.0.0"

	// DefaultUdpSidechannelPort is the default port for the UDP sidechannel.
	DefaultUdpSidechannelPort = 7700

	// DefaultUdpSidechannelMagic is the default 16-bit magic number used in the
	// UDP sidechannel protocol header.
	DefaultUdpSidechannelMagic uint16 = 0xFEED

	ReadGateAskTimeoutMillis = 3
)

type UdpSidechannel struct {
	id         types.ID
	conn       *net.UDPConn
	listen     *net.UDPAddr
	out        chan udpSidechannelMsg
	outDropped uint64 // atomic: count of messages dropped because out was full
	ctx        context.Context
	cancel     context.CancelFunc
	stopc      chan struct{}
	lg         *zap.Logger
	magic      uint16
	peers      sync.Map   // types.ID -> *udpPeerInfo; Load is lock-free on the streamWriter hot path
	attachLock sync.Mutex // serializes AttachPeer/DetachPeer refcount updates only (off the hot path)
	readGate   *SwitchReadGate
	raft       Raft
}

type udpPeerInfo struct {
	id            types.ID
	remote        *net.UDPAddr
	magic         uint16
	refcount      int
	lastSentIndex uint64 // atomic: highest MsgApp proposed index emitted to the switch
}

type udpSidechannelMsg struct {
	msg    [35]byte
	remote *net.UDPAddr
	peerID types.ID
}

type SwitchReadGate struct {
	latestIndex        uint64   // atomic: latest switchIndex received
	pending            sync.Map // map of read gate markers that need to be released
	resolved           sync.Map // map of read gate markers that resolved before being added to pending
	markerSeq          uint64   // atomic: marker counter for server-initiated queries
	pendingMarkers     chan uint64
	pendingBatches     []pendingMarkerBatch // batches of markers that have had a local switch query initiated
	pendingBatchesLock sync.Mutex
}

type pendingMarkerBatch struct {
	localMarker uint64
	markers     []uint64
}

func NewUdpSidechannel(id types.ID, ip string, port int, magic uint16, r Raft, lg *zap.Logger) *UdpSidechannel {
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
		id:       id,
		conn:     conn,
		listen:   listen,
		out:      make(chan udpSidechannelMsg, 128), // TODO: Check if this is safe
		stopc:    make(chan struct{}),
		lg:       lg,
		magic:    magic,
		readGate: &SwitchReadGate{markerSeq: 1, pendingMarkers: make(chan uint64, 256)},
		raft:     r,
	}
	sc.ctx, sc.cancel = context.WithCancel(context.Background())

	go sc.readLoop()
	go sc.sendLoop()
	go sc.querySwitchIndexLoop()
	return sc
}

func (sc *UdpSidechannel) Stop() {
	sc.cancel()
	sc.conn.Close()
	<-sc.stopc
}

func (sc *UdpSidechannel) readLoop() {
	buf := make([]byte, 9000)
	for {
		select {
		case <-sc.ctx.Done():
			close(sc.stopc)
			return
		default:
		}

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

		sc.handleUdpSidechannelMsg(buf[:n])
	}
}

func (sc *UdpSidechannel) handleUdpSidechannelMsg(buf []byte) {
	magic := binary.BigEndian.Uint16(buf[0:2])
	msgType := raftpb.MessageType(buf[2])
	to := binary.BigEndian.Uint64(buf[3:11])
	from := binary.BigEndian.Uint64(buf[11:19])
	marker := binary.BigEndian.Uint64(buf[19:27])
	value := binary.BigEndian.Uint64(buf[27:35])

	// MsgReadIndex is sent by the proxy (not a cluster peer) and tagged by the switch.
	// Handle it before the peer lookup since the proxy's ID is not in sc.peers.
	if msgType == raftpb.MsgReadIndex {
		if magic != sc.magic {
			sc.lg.Warn("received MsgReadIndex with invalid magic",
				zap.Uint16("magic", magic))
			return
		}
		if value == ^uint64(0) {
			sc.lg.Warn("received UDP sidechannel MsgReadIndex with unmarked value. Is the switch active?")
			return
		}
		sc.updateSwitchIndex(marker, value)
		return
	}

	_, ok := sc.peers.Load(types.ID(from))
	if !ok && types.ID(from) != sc.id {
		sc.lg.Warn("received UDP sidechannel message with unknown peer ID",
			zap.Uint64("peerID", from))
		return
	}

	if magic != sc.magic { // &&
		// !(msgType == raftpb.MsgAskAckIndexResp && peer != nil && magic == peer.magic) {
		sc.lg.Warn("received UDP sidechannel message with invalid magic",
			zap.Uint16("magic", magic))
		return
	}

	switch msgType {
	case raftpb.MsgApp:
		// We don't care about this one; these are purely for the switch.
		sc.lg.Info("Got a MsgApp via UDP sidechannel")
		return
	case raftpb.MsgAskAckIndex:
		// Same as for MsgApp; this is for the switch.
		sc.lg.Info("Got a MsgAskAckIndex via UDP sidechannel")
		return
	case raftpb.MsgAskAckIndexResp:
		// This is a switch response query; the marker could be for a batch of requests.
		sc.updateSwitchIndex(0, value)
		sc.releasePendingBatches(marker)
		if sc.raft != nil {
			genMsg := raftpb.Message{
				Type:  raftpb.MsgAskAckIndexResp,
				To:    to,
				From:  from,
				Index: value,
			}
			if err := sc.raft.Process(sc.ctx, genMsg); err != nil {
				sc.lg.Warn("failed to process MsgAskAckIndexResp via Raft",
					zap.Error(err))
			}
		}
	default:
		sc.lg.Warn("received UDP sidechannel message with unknown type",
			zap.Uint8("type", uint8(msgType)))
	}
}

func (sc *UdpSidechannel) sendLoop() {
	for {
		select {
		case msg := <-sc.out:
			_, err := sc.conn.WriteToUDP(msg.msg[:], msg.remote)
			if err != nil {
				sc.lg.Warn("failed to send UDP sidechannel message",
					zap.String("peerID", msg.peerID.String()),
					zap.Error(err))
			}
		case <-sc.ctx.Done():
			return
		}
	}
}

// enqueue hands a message to the sender goroutine without ever blocking the
// caller (the raft streamWriter). The switch hint is best-effort, so if the
// channel is full we drop and count rather than stall replication.
func (sc *UdpSidechannel) enqueue(msg udpSidechannelMsg) {
	select {
	case sc.out <- msg:
	default:
		if n := atomic.AddUint64(&sc.outDropped, 1); n%128 == 1 {
			sc.lg.Warn("UDP sidechannel out channel full, dropping switch hint",
				zap.String("peerID", msg.peerID.String()),
				zap.Uint64("totalDropped", n))
		}
	}
}

func (sc *UdpSidechannel) ProcessOutgoingMessage(m *raftpb.Message) {
	if !(m.Type == raftpb.MsgApp || m.Type == raftpb.MsgAskAckIndex) {
		return
	}

	v, ok := sc.peers.Load(types.ID(m.To))
	if !ok {
		return
	}
	peer := v.(*udpPeerInfo)

	sideChannelMsg := udpSidechannelMsg{remote: peer.remote, peerID: peer.id}
	switch m.Type {
	case raftpb.MsgApp:
		proposedIndex := m.Index + uint64(len(m.Entries))
		// The switch only needs the latest proposed index for a peer.
		// Skip MsgApps that don't advance it.
		for {
			last := atomic.LoadUint64(&peer.lastSentIndex)
			if proposedIndex <= last {
				return
			}
			if atomic.CompareAndSwapUint64(&peer.lastSentIndex, last, proposedIndex) {
				break
			}
		}
		binary.BigEndian.PutUint16(sideChannelMsg.msg[0:2], peer.magic)
		sideChannelMsg.msg[2] = byte(raftpb.MsgApp)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[3:11], uint64(peer.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[11:19], uint64(sc.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[19:27], 0)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[27:35], proposedIndex)
		sc.enqueue(sideChannelMsg)
	case raftpb.MsgAskAckIndex:
		binary.BigEndian.PutUint16(sideChannelMsg.msg[0:2], peer.magic)
		sideChannelMsg.msg[2] = byte(raftpb.MsgAskAckIndex)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[3:11], uint64(peer.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[11:19], uint64(sc.id))
		binary.BigEndian.PutUint64(sideChannelMsg.msg[19:27], 0)
		binary.BigEndian.PutUint64(sideChannelMsg.msg[27:35], 0)
		sc.enqueue(sideChannelMsg)
	default:
		sc.lg.Error("Somehow got to default case of ProcessOutgoingMessage(), should never happen")
	}
}

func (sc *UdpSidechannel) querySwitchIndexLoop() {
	var currentBatch []uint64
	askTimer := time.NewTimer(1 * time.Second)

	defer askTimer.Stop()
	for {
		select {
		case marker := <-sc.readGate.pendingMarkers:
			if len(currentBatch) == 0 {
				askTimer.Reset(ReadGateAskTimeoutMillis * time.Millisecond)
			}
			currentBatch = append(currentBatch, marker)
		case <-askTimer.C:
			var askingBatch []uint64
			if len(currentBatch) == 0 {
				askTimer.Reset(1 * time.Second)
				continue
			}

			for _, marker := range currentBatch {
				if _, ok := sc.readGate.pending.Load(marker); ok {
					askingBatch = append(askingBatch, marker)
				}
			}
			if len(askingBatch) > 0 {
				batchMarker := atomic.AddUint64(&sc.readGate.markerSeq, 1)
				sc.readGate.pendingBatchesLock.Lock()
				sc.readGate.pendingBatches = append(sc.readGate.pendingBatches, pendingMarkerBatch{
					localMarker: batchMarker,
					markers:     askingBatch,
				})
				sc.readGate.pendingBatchesLock.Unlock()
				sc.QuerySwitchIndex(batchMarker)
			}
			askTimer.Reset(1 * time.Second)
		case <-sc.ctx.Done():
			return
		}
	}
}

// Sends a MsgAskAckIndex with the given marker through any
// connected peer's UDP path (since we need some destination).
// The node's ToR should reflect this marker back in a MsgAskAckIndexResp
// with the switch's ack index.
func (sc *UdpSidechannel) QuerySwitchIndex(marker uint64) error {
	// Pick any connected peer
	var peer *udpPeerInfo
	sc.peers.Range(func(_, v any) bool {
		peer = v.(*udpPeerInfo)
		return false
	})
	if peer == nil {
		sc.lg.Warn("no connected peers for UDP sidechannel switch index query")
		return fmt.Errorf("no connected peers for switch index query")
	}

	var msg [35]byte
	binary.BigEndian.PutUint16(msg[0:2], sc.magic) // Use our own magic, we want our switch
	msg[2] = byte(raftpb.MsgAskAckIndex)
	binary.BigEndian.PutUint64(msg[3:11], uint64(sc.id)) // Use our own ID
	binary.BigEndian.PutUint64(msg[11:19], uint64(sc.id))
	binary.BigEndian.PutUint64(msg[19:27], marker)
	binary.BigEndian.PutUint64(msg[27:35], 0)

	_, err := sc.conn.WriteToUDP(msg[:], peer.remote)
	if err != nil {
		sc.lg.Warn("failed to send QuerySwitchIndex UDP message",
			zap.String("peerID", peer.id.String()),
			zap.Error(err))
		return err
	} else {
		sc.lg.Info("sent QuerySwitchIndex UDP message",
			zap.String("peerID", peer.id.String()),
			zap.Uint64("marker", marker))
	}
	return nil
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

	remote := &net.UDPAddr{
		IP:   ip,
		Port: port,
	}

	sc.attachLock.Lock()
	defer sc.attachLock.Unlock()
	if v, ok := sc.peers.Load(id); ok {
		peer := v.(*udpPeerInfo)
		if peer.remote.IP.Equal(ip) && peer.remote.Port == port && peer.magic == magic {
			peer.refcount++
			return
		}
		sc.lg.Warn("UDP sidechannel peer info changed for peer, updating",
			zap.String("peerID", id.String()),
			zap.String("oldIP", peer.remote.IP.String()),
			zap.Int("oldPort", peer.remote.Port),
			zap.String("oldMagic", fmt.Sprintf("%04x", peer.magic)),
			zap.String("newIP", ipStr),
			zap.String("newPort", portStr),
			zap.String("newMagic", magicStr))
		// Hot-path readers access peer fields without a lock, so publish a fresh
		// immutable struct rather than mutating the existing one in place.
		sc.peers.Store(id, &udpPeerInfo{
			id:            id,
			remote:        remote,
			magic:         magic,
			refcount:      peer.refcount + 1,
			lastSentIndex: atomic.LoadUint64(&peer.lastSentIndex),
		})
		return
	}

	sc.peers.Store(id, &udpPeerInfo{
		id:       id,
		remote:   remote,
		magic:    magic,
		refcount: 1,
	})
}

func (sc *UdpSidechannel) DetachPeer(id types.ID) {
	sc.attachLock.Lock()
	defer sc.attachLock.Unlock()
	v, ok := sc.peers.Load(id)
	if !ok {
		return
	}
	peer := v.(*udpPeerInfo)
	peer.refcount--
	if peer.refcount <= 0 {
		sc.peers.Delete(id)
	}
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

// Blocks until a tagged UDP response for the given marker
// arrives from the switch, or a local switch index query releases it.
func (sc *UdpSidechannel) WaitForReadGate(ctx context.Context, marker uint64) (uint64, error) {
	ch := make(chan uint64, 1)
	sc.readGate.pending.Store(marker, ch)

	if val, ok := sc.readGate.resolved.LoadAndDelete(marker); ok {
		sc.readGate.pending.Delete(marker)
		return val.(uint64), nil
	}

	sc.readGate.pendingMarkers <- marker

	for {
		select {
		case switchIndex := <-ch:
			return switchIndex, nil
		case <-ctx.Done():
			sc.readGate.pending.Delete(marker)
			return ^uint64(0), ctx.Err()
		}
	}
}

func (sc *UdpSidechannel) LatestSwitchIndex() uint64 {
	return atomic.LoadUint64(&sc.readGate.latestIndex)
}

// Resolves a pending marker with the given switchIndex.
func (sc *UdpSidechannel) updateSwitchIndex(marker, switchIndex uint64) {
	// Update the latest index atomically
	for {
		current := atomic.LoadUint64(&sc.readGate.latestIndex)
		if switchIndex <= current {
			break
		}
		if atomic.CompareAndSwapUint64(&sc.readGate.latestIndex, current, switchIndex) {
			break
		}
	}

	// Resolve the per-marker channel if present
	if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
		ch := val.(chan uint64)
		select {
		case ch <- switchIndex:
		default:
		}
	} else { // Add the marker to resolved, this probably arrived first
		sc.readGate.resolved.Store(marker, switchIndex)
		// This is ugly, but one of the two sides has to check one of the maps twice
		if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
			ch := val.(chan uint64)
			select {
			case ch <- switchIndex:
			default:
			}
			sc.readGate.resolved.Delete(marker)
		}
	}
}

func (sc *UdpSidechannel) releasePendingBatches(batchMarker uint64) {
	if batchMarker == 0 {
		return
	}

	sc.readGate.pendingBatchesLock.Lock()
	defer sc.readGate.pendingBatchesLock.Unlock()
	consumed := 0
	for consumed < len(sc.readGate.pendingBatches) {
		batch := sc.readGate.pendingBatches[consumed]
		if batch.localMarker > batchMarker {
			break
		}
		for _, marker := range batch.markers {
			if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
				ch := val.(chan uint64)
				select {
				case ch <- sc.LatestSwitchIndex():
				default:
				}
			}
		}
		consumed++
	}
	if consumed > 0 {
		n := copy(sc.readGate.pendingBatches, sc.readGate.pendingBatches[consumed:])
		sc.readGate.pendingBatches = sc.readGate.pendingBatches[:n]
	}
}
