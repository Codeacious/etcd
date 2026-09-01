package rafthttp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
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

	// ReadGateDeadlineMillis bounds how long a follower waits for a marked
	// switch index it can use for a read. On expiry the read is forwarded to the leader.
	// Try to keep this above what the expected p99 latency of the switch's response time is
	// (or p95/p90 depending on the latency distrubution).
	ReadGateDeadlineMillis = 50

	// resolvedMarkerTTL bounds how long a switch answer that arrived before its
	// read is kept waiting for that read to claim it. Rotation is by generation,
	// so an entry actually lives between one and two of these.
	// Keep this long- it's only to stop memory leaking by clearing the resolved marker map.
	resolvedMarkerTTL = time.Second
)

// readGateAsk is a marker for a read waiting on the switch, with the time it becomes
// eligible to be included in a switch query. Per-read rather than batched because the
// ask timer is shared.
type readGateAsk struct {
	marker    uint64
	nextAskAt time.Time
}

// resolvedMarkers are stamped switch indexes for reads that arrive before the actual client read.
// We maintain a 'current' and 'previous' map that gets shifted back every resolvedMarkerTTL.
type resolvedMarkers struct {
	mu         sync.RWMutex
	curr       map[uint64]uint64 // on resolvedMarkerTTL -> move curr to prev
	prev       map[uint64]uint64 // on resolvedMarkerTTL -> drop prev (before moving curr)
	lastRotate time.Time
}

func newResolvedMarkers() *resolvedMarkers {
	return &resolvedMarkers{
		curr:       make(map[uint64]uint64),
		prev:       make(map[uint64]uint64),
		lastRotate: time.Now(),
	}
}

func (r *resolvedMarkers) store(marker, switchIndex uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now := time.Now(); now.Sub(r.lastRotate) >= resolvedMarkerTTL {
		r.prev = r.curr
		r.curr = make(map[uint64]uint64)
		r.lastRotate = now
	}
	r.curr[marker] = switchIndex
}

// take returns and removes a marker's answer from either generation.
func (r *resolvedMarkers) take(marker uint64) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.curr[marker]; ok {
		delete(r.curr, marker)
		return v, true
	}
	if v, ok := r.prev[marker]; ok {
		delete(r.prev, marker)
		return v, true
	}
	return 0, false
}

func (r *resolvedMarkers) remove(marker uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.curr, marker)
	delete(r.prev, marker)
}

func (r *resolvedMarkers) load(marker uint64) (uint64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if v, ok := r.curr[marker]; ok {
		return v, true
	}
	v, ok := r.prev[marker]
	return v, ok
}

func (r *resolvedMarkers) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.curr) + len(r.prev)
}

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
	attachLock sync.Mutex // serializes AttachPeer/DetachPeer refcount updates only
	readGate   *SwitchReadGate
	raft       Raft
	// Allows setting a predicate on whether the gate should be used.
	// Used to allow leaders to bypass the gate. nil means the gate is always used.
	gateActive func() bool
}

type udpPeerInfo struct {
	id            types.ID
	remote        *net.UDPAddr
	magic         uint16
	conn          *net.UDPConn
	refcount      int
	lastSentIndex uint64 // atomic: highest MsgApp proposed index emitted to the switch
	// Atomic bool tracking if we know the switch's saved proposed index on this path
	// has been updated within the last RefreshIdleSwitches interval
	switchUpdated uint32
}

type udpSidechannelMsg struct {
	msg    [35]byte
	conn   *net.UDPConn
	remote *net.UDPAddr
	peerID types.ID
}

type SwitchReadGate struct {
	latestIndex        uint64   // atomic: the highest switch index this node has ever observed
	zeroAnswers        uint64   // atomic: switch answers discarded as unusable
	gateTimeouts       uint64   // atomic: reads that gave up waiting and forwarded
	pending            sync.Map // map of read gate markers that need to be released
	resolved           *resolvedMarkers
	markerSeq          uint64 // atomic: marker counter for server-initiated queries
	pendingMarkers     chan readGateAsk
	pendingBatches     []pendingMarkerBatch // batches of markers that have had a local switch query initiated
	pendingBatchesLock sync.Mutex
}

type pendingMarkerBatch struct {
	localMarker uint64
	markers     []uint64
}

func (sc *UdpSidechannel) SetGateActiveChecker(f func() bool) { sc.gateActive = f }

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
		id:     id,
		conn:   conn,
		listen: listen,
		out:    make(chan udpSidechannelMsg, 128),
		stopc:  make(chan struct{}),
		lg:     lg,
		magic:  magic,
		readGate: &SwitchReadGate{
			markerSeq:      1,
			pendingMarkers: make(chan readGateAsk, 256),
			resolved:       newResolvedMarkers(),
		},
		raft: r,
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
	// Close every connected per-peer socket so their peerReadLoops exit
	sc.peers.Range(func(_, v any) bool {
		if peer := v.(*udpPeerInfo); peer.conn != nil {
			peer.conn.Close()
		}
		return true
	})
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

		// The listener socket receives proxy MsgReadIndex and QuerySwitchIndex
		// replies, neither of which is attributable to a peer's path.
		sc.handleUdpSidechannelMsg(buf[:n], nil)
	}
}

// handleUdpSidechannelMsg decodes one sidechannel datagram. src is the peer
// whose connected socket delivered it, or nil for the shared listener.
func (sc *UdpSidechannel) handleUdpSidechannelMsg(buf []byte, src *udpPeerInfo) {
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
		// Skip the gate if our getActive() predicate is true.
		// Used by the leader to skip enqueuing into `resolved` needlessly.
		// Races against a leadership change but is benign in both directions: drop what
		// a read would have claimed and that read waits for a manual switch query instead;
		// enqueue something that's never claimed, and generation rotation wipes it.
		if sc.gateActive != nil && !sc.gateActive() {
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

	if magic != sc.magic {
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
		// Mark the switch as updated (i.e. not freshly rebooted/wiped) on a nonzero value.
		if src != nil && value != 0 {
			atomic.StoreUint32(&src.switchUpdated, 1)
		}
		sc.updateSwitchIndex(0, value)
		// Passing 0 here is fine; releasePendingBatches doesn't do anything if either arg is 0.
		sc.releasePendingBatches(marker, value)
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
			var err error
			if msg.conn != nil {
				_, err = msg.conn.Write(msg.msg[:])
			} else {
				_, err = sc.conn.WriteToUDP(msg.msg[:], msg.remote)
			}
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
	if !(m.Type == raftpb.MsgApp || m.Type == raftpb.MsgAskAckIndex ||
		m.Type == raftpb.MsgAskReadLeaseResp) {
		return
	}

	v, ok := sc.peers.Load(types.ID(m.To))
	if !ok {
		return
	}
	peer := v.(*udpPeerInfo)

	// A granted lease means this peer is about to start serving reads locally,
	// gated on its switch. Re-arm that path now instead of waiting for the idle
	// ticker to notice the new lease.
	if m.Type == raftpb.MsgAskReadLeaseResp {
		if !m.Reject {
			sc.refreshPeerSwitch(peer)
		}
		return
	}

	sideChannelMsg := udpSidechannelMsg{conn: peer.conn, remote: peer.remote, peerID: peer.id}
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

// RefreshIdleSwitches re-sends, to every peer whose switch has said nothing
// since the previous call, the highest proposed index we have already sent to that
// peer. Only the leader can do this; it is the only node allowed to update switch indexes.
//
// This is necessary because the switch register is only ever updated by a proposed write,
// so a rebooted switch (register reset to 0) has no way to recover if the
// cluster is only serving reads.
//
// Per-peer rather than global because each peer's path can cross a different
// switch, and each switch holds its own register.
// holders is the set of peers this leader currently has an unexpired read lease
// with (raft's Status().ActiveReadLeases). Peers outside it are skipped: a
// peer with no lease cannot serve a read locally, so whatever its switch holds
// is moot until it gets one, and a lease grant re-arms the path anyway
// via the MsgAskReadLeaseResp hook in ProcessOutgoingMessage.
func (sc *UdpSidechannel) RefreshIdleSwitches(holders []uint64) {
	for _, id := range holders {
		v, ok := sc.peers.Load(types.ID(id))
		if !ok {
			continue
		}
		peer := v.(*udpPeerInfo)

		// If this peer's switch reported holding nonzero register state since the
		// last check, clear the flag and skip.
		// Note that a successful refresh's own reflection is nonzero and will set the flag,
		// so a healthy idle peer is refreshed every OTHER tick.
		if atomic.SwapUint32(&peer.switchUpdated, 0) == 1 {
			continue
		}
		sc.refreshPeerSwitch(peer)
	}
}

// refreshPeerSwitch re-sends the highest proposed index already sent to this
// peer, restoring the register on that peer's path.
func (sc *UdpSidechannel) refreshPeerSwitch(peer *udpPeerInfo) {
	lastSent := atomic.LoadUint64(&peer.lastSentIndex)
	if lastSent == 0 {
		// Never tapped anything for this peer; nothing to restore.
		return
	}

	// We don't use ProcessOutgoingMessage on purpose, this is a retransmission:
	// ProcessOutgoingMessage CASes lastSentIndex (we don't want to modify it here) and it only sends
	// on a strictly higher index, so it would refuse to re-send a value it has already sent.
	msg := udpSidechannelMsg{conn: peer.conn, remote: peer.remote, peerID: peer.id}
	binary.BigEndian.PutUint16(msg.msg[0:2], peer.magic)
	msg.msg[2] = byte(raftpb.MsgApp)
	binary.BigEndian.PutUint64(msg.msg[3:11], uint64(peer.id))
	binary.BigEndian.PutUint64(msg.msg[11:19], uint64(sc.id))
	binary.BigEndian.PutUint64(msg.msg[19:27], 0)
	binary.BigEndian.PutUint64(msg.msg[27:35], lastSent)
	sc.enqueue(msg)
}

// querySwitchIndexLoop asks the switch for its register to ungate reads that have been waiting on
// a switch marker, either due to the read not being marked, or the switch's response
// being dropped/unusuable.
//
// One interval, and the timer is armed only while markers are actually waiting:
// a lease-holding follower with no pending reads has nothing to learn from the
// switch, so the loop blocks on pendingMarkers. Each marker carries its own
// nextAskAt, so a marker is never queried before it has had a full
// ReadGateAskTimeoutMillis to be answered by its own reflection; the shared
// timer decides only when to next look, not who is eligible. While any read is
// unanswered, it will runs every interval.
// Batches unanswered reads together so that a single switch response can unblock all of them.
func (sc *UdpSidechannel) querySwitchIndexLoop() {
	askInterval := ReadGateAskTimeoutMillis * time.Millisecond
	var waiting []readGateAsk

	askTimer := time.NewTimer(time.Hour)
	if !askTimer.Stop() {
		<-askTimer.C
	}
	armed := false
	armedFor := time.Time{}
	armAt := func(deadline time.Time) {
		if armed {
			if armedFor.Before(deadline) || armedFor.Equal(deadline) {
				return
			}
			if !askTimer.Stop() {
				select {
				case <-askTimer.C:
				default:
				}
			}
		}
		d := time.Until(deadline)
		if d < 0 {
			d = 0
		}
		askTimer.Reset(d)
		armed, armedFor = true, deadline
	}

	for {
		select {
		case ask := <-sc.readGate.pendingMarkers:
			waiting = append(waiting, ask)
			armAt(ask.nextAskAt)
		case <-askTimer.C:
			armed = false

			// Rebuild from what is still waiting. Markers leave `pending` when
			// they are answered or when their gate deadline expires.
			now := time.Now()
			var askingBatch []uint64
			var nextWake time.Time
			live := waiting[:0]
			for _, a := range waiting {
				if _, ok := sc.readGate.pending.Load(a.marker); !ok {
					continue
				}
				if !a.nextAskAt.After(now) {
					askingBatch = append(askingBatch, a.marker)
					a.nextAskAt = now.Add(askInterval)
				}
				live = append(live, a)
				if nextWake.IsZero() || a.nextAskAt.Before(nextWake) {
					nextWake = a.nextAskAt
				}
			}
			waiting = live

			if len(askingBatch) > 0 {
				batchMarker := atomic.AddUint64(&sc.readGate.markerSeq, 1)

				sc.readGate.pendingBatchesLock.Lock()
				// Drop batches with nothing left waiting on them.
				kept := sc.readGate.pendingBatches[:0]
				for _, b := range sc.readGate.pendingBatches {
					for _, m := range b.markers {
						if _, ok := sc.readGate.pending.Load(m); ok {
							kept = append(kept, b)
							break
						}
					}
				}
				sc.readGate.pendingBatches = append(kept, pendingMarkerBatch{
					localMarker: batchMarker,
					markers:     askingBatch,
				})
				sc.readGate.pendingBatchesLock.Unlock()

				sc.QuerySwitchIndex(batchMarker)
			}

			if !nextWake.IsZero() {
				armAt(nextWake)
			}
		case <-sc.ctx.Done():
			return
		}
	}
}

// Sends a MsgAskAckIndex with the given marker from the listener socket to our switch
// by using any attached peer's address as a routing target; the
// P4 switch intercepts and reflects, so the per-peer connected sockets
// are not needed here and the response arrives back on the main UDP listener.
// The switch reflects the marker in a MsgAskAckIndexResp carrying its saved index.
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
		sc.lg.Debug("sent QuerySwitchIndex UDP message",
			zap.String("peerID", peer.id.String()),
			zap.Uint64("marker", marker))
	}
	return nil
}

// AttachPeer refcount-attaches a peer's sidechannel endpoint for the life of a
// raft stream. It reports whether the peer was counted; the caller must only
// pair a DetachPeer with a true return. An arg-parse failure counts
// no reference, so detaching anyway would decrement a refcount another live stream
// owns and close its socket out from under it.
func (sc *UdpSidechannel) AttachPeer(id types.ID, ipStr string, portStr string, magicStr string) bool {
	magic, err := MagicFromStr(magicStr)
	if err != nil {
		sc.lg.Warn("failed to parse UDP sidechannel magic from string",
			zap.String("magicStr", magicStr), zap.Error(err))
		return false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		sc.lg.Warn("failed to parse UDP sidechannel port from string",
			zap.String("portStr", portStr), zap.Error(err))
		return false
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		sc.lg.Warn("failed to parse UDP sidechannel IP from string",
			zap.String("ipStr", ipStr))
		return false
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
			return true
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
		newPeer := &udpPeerInfo{
			id:            id,
			remote:        remote,
			magic:         magic,
			conn:          sc.dialPeer(id, remote),
			refcount:      peer.refcount + 1,
			lastSentIndex: atomic.LoadUint64(&peer.lastSentIndex),
		}
		sc.peers.Store(id, newPeer)
		// Closing the old socket unblocks its peerReadLoop, which then exits.
		if peer.conn != nil {
			peer.conn.Close()
		}
		if newPeer.conn != nil {
			go sc.peerReadLoop(newPeer)
		}
		return true
	}

	newPeer := &udpPeerInfo{
		id:       id,
		remote:   remote,
		magic:    magic,
		conn:     sc.dialPeer(id, remote),
		refcount: 1,
	}
	sc.peers.Store(id, newPeer)
	if newPeer.conn != nil {
		go sc.peerReadLoop(newPeer)
	}
	return true
}

// dialPeer opens a connected UDP socket to a remote so sends skip the per-call
// route lookup. Returns nil on failure; the caller then falls back to the
// shared listener socket (conn-less WriteToUDP) for that peer.
func (sc *UdpSidechannel) dialPeer(id types.ID, remote *net.UDPAddr) *net.UDPConn {
	conn, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		sc.lg.Warn("failed to dial connected UDP sidechannel socket; falling back to listener",
			zap.String("peerID", id.String()),
			zap.String("remote", remote.String()),
			zap.Error(err))
		return nil
	}
	return conn
}

// peerReadLoop consumes the switch's reflected responses (MsgAskAckIndexResp)
// that return to a connected per-peer socket's ephemeral source port. The
// main UDP listener never sees these because the switch reflects to the source
// port the packet was sent from. Exits when the socket is closed.
func (sc *UdpSidechannel) peerReadLoop(peer *udpPeerInfo) {
	buf := make([]byte, 9000)
	for {
		n, err := peer.conn.Read(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// Connected UDP sockets surface ICMP errors (e.g. port unreachable)
			// on Read; these are transient, so keep going rather than tearing
			// the loop down.
			sc.lg.Warn("error reading from connected UDP sidechannel socket",
				zap.String("peerID", peer.id.String()), zap.Error(err))
			continue
		}
		if n < 35 {
			sc.lg.Warn("received UDP sidechannel message with invalid length",
				zap.Int("length", n))
			continue
		}
		sc.handleUdpSidechannelMsg(buf[:n], peer)
	}
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
	if peer.refcount < 0 {
		// Entries are deleted at zero, so a live one always holds refcount >= 1;
		// a negative count means an unbalanced detach reached a peer another
		// stream still owns, and its socket is about to be closed.
		sc.lg.Warn("UDP sidechannel peer refcount went negative; unbalanced detach",
			zap.String("peerID", id.String()),
			zap.Int("refcount", peer.refcount))
	}
	if peer.refcount <= 0 {
		sc.peers.Delete(id)
		// Closing unblocks peerReadLoop, which then exits.
		if peer.conn != nil {
			peer.conn.Close()
		}
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

// WaitForReadGate blocks until the switch answers nonzero for the given marker, either via
// a tagged UDP reflection of the client's own read-gate packet, or a batched
// local query that releases it. It returns the index the switch reported.
//
// After ReadGateDeadlineMillis it gives up and returns MaxUint64,
// meaning "no known switch marker for this read"; this causes a leaseholding raft follower
// to forward the read to the leader after it times out on waiting for the impossible-to-reach
// commit index of MaxUint64.
//
// Only the caller's own deadline produces an error, which is a genuinely failed read.
func (sc *UdpSidechannel) WaitForReadGate(ctx context.Context, marker uint64) (uint64, error) {
	if marker == 0 {
		// Marker-less read (client not speaking the assist protocol): mint a
		// server-side marker from markerSeq. These have an upper 32 bits of 0,
		// a namespace clients never use (see sidechannel.NewMarkerSeed), so they
		// can't collide with a reflected client marker.
		// querySwitchIndexLoop's batched query will resolve this new marker.
		marker = atomic.AddUint64(&sc.readGate.markerSeq, 1)
	}

	ch := make(chan uint64, 1)
	sc.readGate.pending.Store(marker, ch)

	// UDP normally beats TCP, so the switch marker usually arrives first.
	// This MUST be after pending.Store(),
	// else there's a gap where a marker can arrive and isn't consumed.
	if val, ok := sc.readGate.resolved.take(marker); ok {
		sc.readGate.pending.Delete(marker)
		return val, nil
	}

	gateTimer := time.NewTimer(ReadGateDeadlineMillis * time.Millisecond)
	defer gateTimer.Stop()

	ask := readGateAsk{
		marker:    marker,
		nextAskAt: time.Now().Add(ReadGateAskTimeoutMillis * time.Millisecond),
	}
	select {
	case sc.readGate.pendingMarkers <- ask:
	case <-gateTimer.C:
		// querySwitchIndexLoop only drains this queue (cap 256) between its own
		// select arms, so a burst can completely fill it. Give up on a local serve rather
		// than parking here (same outcome as the wait below).
		sc.readGate.pending.Delete(marker)
		atomic.AddUint64(&sc.readGate.gateTimeouts, 1)
		return math.MaxUint64, nil
	case <-ctx.Done():
		sc.readGate.pending.Delete(marker)
		return 0, ctx.Err()
	}

	select {
	case switchIndex := <-ch:
		return switchIndex, nil
	case <-gateTimer.C:
		sc.readGate.pending.Delete(marker)
		atomic.AddUint64(&sc.readGate.gateTimeouts, 1)
		return math.MaxUint64, nil
	case <-ctx.Done():
		sc.readGate.pending.Delete(marker)
		return 0, ctx.Err()
	}
}

func (sc *UdpSidechannel) LatestSwitchIndex() uint64 {
	return atomic.LoadUint64(&sc.readGate.latestIndex)
}

// SwitchGateStats reports counters for the read gate: switch answers discarded
// as unusable, and reads that gave up waiting and were forwarded to the leader.
// Both climbing means assist has degraded to leader-routed reads, which shows up
// as a latency shift with no other symptom.
func (sc *UdpSidechannel) SwitchGateStats() (zeroAnswers, gateTimeouts uint64) {
	return atomic.LoadUint64(&sc.readGate.zeroAnswers),
		atomic.LoadUint64(&sc.readGate.gateTimeouts)
}

// updateSwitchIndex records a switch observation and resolves the marker it
// answers, if any.
//
// A switchIndex of 0 is discarded outright, as it means we have gained no information.
//
// Because markers are minted per read, a matching tagged answer proves the switch's register
// held the returned index at some time AFTER the read began.
func (sc *UdpSidechannel) updateSwitchIndex(marker, switchIndex uint64) {
	if switchIndex == 0 {
		atomic.AddUint64(&sc.readGate.zeroAnswers, 1)
		return
	}

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

	// Marker 0 is never minted (markerSeq starts at 1 and only ever increments),
	// so a MsgAskAckIndexResp carrying no marker has nothing it can resolve.
	// Storing it would leak an entry in resolved that nothing ever consumes.
	if marker == 0 {
		return
	}

	// Resolve the per-marker channel if present
	if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
		ch := val.(chan uint64)
		select {
		case ch <- switchIndex: // Channel can never be full
		default:
		}
	} else { // Add the marker to resolved, this probably arrived first
		sc.readGate.resolved.store(marker, switchIndex)
		// This is ugly, but one of the two sides has to check one of the maps twice
		if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
			ch := val.(chan uint64)
			select {
			case ch <- switchIndex:
			default:
			}
			sc.readGate.resolved.remove(marker)
		}
	}
}

// releasePendingBatches releases every batch at or below batchMarker, handing
// each waiting read answeredIndex, which is the value the switch actually returned for
// this query, NOT the node-global maximum.
//
// That value is a valid stamp for every marker released here: the query was
// sent after each of those reads entered the gate, so its answer describes the
// register at a time after each read began.
// Handing out the global max instead would gate reads on an index a later, unrelated read happened
// to observe, holding them at the follower for writes that may have began after the read.
// In a write-heavy workload this can mean reads are unduly delayed.
func (sc *UdpSidechannel) releasePendingBatches(batchMarker, answeredIndex uint64) {
	if batchMarker == 0 || answeredIndex == 0 {
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
				case ch <- answeredIndex:
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
