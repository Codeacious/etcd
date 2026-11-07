package rafthttp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
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
	// read is kept waiting for that read to claim it. Rotation is by generation
	// and happens only on store, so an entry lives between one and two of these
	// while answers keep arriving, and longer when they stop. Safety does not
	// rely on it: markers do not recur (see isClientMarker).
	// Keep this long- it's only to stop memory leaking by clearing the resolved marker map.
	resolvedMarkerTTL = time.Second

	// udpSidechannelMsgLen is the fixed sidechannel payload length:
	// magic(2) type(1) to(8) from(8) marker(8) value(8) bootID(4) term(4).
	udpSidechannelMsgLen = 43

	// msgSwitchNack is the sidechannel-only type a switch reflects instead of
	// applying a write whose boot ID is not its own (SAFE-1). It sits outside
	// the raftpb range so it can never be mistaken for a raft message.
	msgSwitchNack = 0x80
)

// switchStamp is what a switch reported for a read: its register, and the
// highest term among the writes it has accepted since it booted. Raft uses the
// index only in that term (SAFE-1). A zero index or term carries no information.
type switchStamp struct {
	index uint64
	term  uint32
}

// sidechannelMsg is one sidechannel datagram, decoded or about to be encoded.
type sidechannelMsg struct {
	magic   uint16
	msgType byte
	to      uint64
	from    uint64
	marker  uint64
	value   uint64
	bootID  uint32
	term    uint32
}

func (m sidechannelMsg) encode() (buf [udpSidechannelMsgLen]byte) {
	binary.BigEndian.PutUint16(buf[0:2], m.magic)
	buf[2] = m.msgType
	binary.BigEndian.PutUint64(buf[3:11], m.to)
	binary.BigEndian.PutUint64(buf[11:19], m.from)
	binary.BigEndian.PutUint64(buf[19:27], m.marker)
	binary.BigEndian.PutUint64(buf[27:35], m.value)
	binary.BigEndian.PutUint32(buf[35:39], m.bootID)
	binary.BigEndian.PutUint32(buf[39:43], m.term)
	return buf
}

// decodeSidechannelMsg decodes a datagram of at least udpSidechannelMsgLen bytes.
func decodeSidechannelMsg(buf []byte) sidechannelMsg {
	return sidechannelMsg{
		magic:   binary.BigEndian.Uint16(buf[0:2]),
		msgType: buf[2],
		to:      binary.BigEndian.Uint64(buf[3:11]),
		from:    binary.BigEndian.Uint64(buf[11:19]),
		marker:  binary.BigEndian.Uint64(buf[19:27]),
		value:   binary.BigEndian.Uint64(buf[27:35]),
		bootID:  binary.BigEndian.Uint32(buf[35:39]),
		term:    binary.BigEndian.Uint32(buf[39:43]),
	}
}

// readGateAsk is a marker for a read waiting on the switch, with the time it becomes
// eligible to be included in a switch query. Per-read rather than batched because the
// ask timer is shared.
type readGateAsk struct {
	marker    uint64
	nextAskAt time.Time
}

// resolvedMarkers are stamped switch indexes for reads that arrive before the actual client read.
// We maintain a 'current' and 'previous' map that a store shifts back once resolvedMarkerTTL has passed since the last shift.
type resolvedMarkers struct {
	mu         sync.RWMutex
	curr       map[uint64]switchStamp // on resolvedMarkerTTL -> move curr to prev
	prev       map[uint64]switchStamp // on resolvedMarkerTTL -> drop prev (before moving curr)
	lastRotate time.Time
}

func newResolvedMarkers() *resolvedMarkers {
	return &resolvedMarkers{
		curr:       make(map[uint64]switchStamp),
		prev:       make(map[uint64]switchStamp),
		lastRotate: time.Now(),
	}
}

func (r *resolvedMarkers) store(marker uint64, stamp switchStamp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now := time.Now(); now.Sub(r.lastRotate) >= resolvedMarkerTTL {
		r.prev = r.curr
		r.curr = make(map[uint64]switchStamp)
		r.lastRotate = now
	}
	r.curr[marker] = stamp
}

// take returns and removes a marker's answer from either generation.
func (r *resolvedMarkers) take(marker uint64) (switchStamp, bool) {
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
	return switchStamp{}, false
}

func (r *resolvedMarkers) remove(marker uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.curr, marker)
	delete(r.prev, marker)
}

func (r *resolvedMarkers) load(marker uint64) (switchStamp, bool) {
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

	// writeMu guards highWater and leaderTerm, which change together.
	writeMu sync.Mutex
	// highWater is the largest index this node has proposed to any switch in
	// leaderTerm: the end of every MsgApp it has copied in that term, and
	// nothing else. Every write carries it, and every credit is capped at it,
	// so whichever write reaches a freshly booted switch first covers every
	// credit this node has given in the term (SAFE-1). It starts over when
	// leaderTerm rises: the switch keeps one (term, index) pair, and an
	// earlier term's mark would hold this term's reads back until commit
	// passed it (LIVE-3 Part B).
	highWater uint64
	// leaderTerm is the highest term of any MsgApp this node has copied to a
	// switch, i.e. a term in which it was leader. Refreshes and re-seeds,
	// which no raft message carries a term for, write with it.
	leaderTerm uint64

	// switches maps a switch's magic (unique per switch by configuration) to
	// what this node knows about it. Every path crossing a switch shares it.
	switches sync.Map // uint16 -> *switchState

	// leaseHolders is the peers holding a read lease from this node, as of the
	// leader's last switch-monitor tick (nil when not leader). A switch reply
	// is credited to every one of them behind that switch.
	leaseHolders atomic.Pointer[[]uint64]
}

// SetLeaseHolders records the peers currently holding a read lease from this
// node (raft's Status().ActiveReadLeases), or nil when this node isn't leader.
func (sc *UdpSidechannel) SetLeaseHolders(holders []uint64) {
	if holders == nil {
		sc.leaseHolders.Store(nil)
		return
	}
	hs := append([]uint64(nil), holders...)
	sc.leaseHolders.Store(&hs)
}

// switchState is what this node knows about one switch.
type switchState struct {
	// bootID is the switch's boot ID, learned from its NACKs; 0 until then.
	// Writes on every path crossing the switch carry it, and only replies
	// carrying it are credited. It is per switch, not per path: a path that
	// has not yet seen a reboot must not keep crediting the dead boot's replies.
	bootID uint32 // atomic
}

// switchFor returns the shared state of the switch with the given magic.
func (sc *UdpSidechannel) switchFor(magic uint16) *switchState {
	v, _ := sc.switches.LoadOrStore(magic, &switchState{})
	return v.(*switchState)
}

type udpPeerInfo struct {
	id       types.ID
	remote   *net.UDPAddr
	magic    uint16
	conn     *net.UDPConn
	refcount int
	// Atomic bool tracking if we know the switch's saved proposed index on this path
	// has been updated within the last RefreshIdleSwitches interval
	switchUpdated uint32
	// sw is the switch this path crosses, shared with every other path
	// advertising the same magic.
	sw *switchState

	// sentMu guards the (term, high-water mark) last written on this path,
	// which dedupes MsgApp copies. Keyed on the term too: a re-elected leader
	// must write in its new term even if its high-water mark has not moved.
	sentMu            sync.Mutex
	lastSentTerm      uint64
	lastSentHighWater uint64
}

// raiseOnCopy records a MsgApp copy ending at end in term, and returns the
// high-water mark to write. A copy from a newer term starts the mark over at
// end; one from an older term (a copy delayed past this node's next term as
// leader) is not written at all (ok false), and leaves the mark alone.
func (sc *UdpSidechannel) raiseOnCopy(end, term uint64) (highWater uint64, ok bool) {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	switch {
	case term < sc.leaderTerm:
		return 0, false
	case term > sc.leaderTerm:
		sc.highWater, sc.leaderTerm = end, term
	default:
		sc.highWater = max(sc.highWater, end)
	}
	return sc.highWater, true
}

// writeState returns the high-water mark and the term it belongs to.
func (sc *UdpSidechannel) writeState() (highWater, leaderTerm uint64) {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	return sc.highWater, sc.leaderTerm
}

// recordLastSent records a MsgApp copy of (term, highWater) on this path and
// reports whether it is newer than the last one written.
func (p *udpPeerInfo) recordLastSent(term, highWater uint64) bool {
	p.sentMu.Lock()
	defer p.sentMu.Unlock()
	if term < p.lastSentTerm || (term == p.lastSentTerm && highWater <= p.lastSentHighWater) {
		return false
	}
	p.lastSentTerm, p.lastSentHighWater = term, highWater
	return true
}

// resetLastSent forgets the last write on this path, so the next MsgApp copy goes out.
func (p *udpPeerInfo) resetLastSent() {
	p.sentMu.Lock()
	defer p.sentMu.Unlock()
	p.lastSentTerm, p.lastSentHighWater = 0, 0
}

type udpSidechannelMsg struct {
	msg    [udpSidechannelMsgLen]byte
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
	markerSeq          uint64 // atomic: counter for server-minted read markers (marker-less reads); see nextReadMarker
	batchIncarnation   uint32 // random, nonzero upper half of this instance's batch markers; see nextBatchMarker
	batchSeq           uint32 // atomic: lower half of the last batch marker minted
	pendingMarkers     chan readGateAsk
	pendingBatches     []pendingMarkerBatch // batches of markers that have had a local switch query initiated
	pendingBatchesLock sync.Mutex
}

type pendingMarkerBatch struct {
	localMarker uint64
	markers     []uint64
}

// newBatchIncarnation returns a random, nonzero batch-marker upper half.
func newBatchIncarnation() uint32 {
	if inc := rand.Uint32(); inc != 0 {
		return inc
	}
	return 1
}

// nextBatchMarker mints the marker for a new QuerySwitchIndex batch:
// batchIncarnation in the upper 32 bits, the batch sequence in the lower 32.
//
// SAFE-3: a counter restarting at 1 on every boot let a query reply delayed
// across a restart release this instance's batches (release is cumulative,
// see releasePendingBatches) with a register value read before their reads
// began. A random incarnation per instance makes such replies identifiable,
// and the reply path drops them. Mirrors the incarnation in raft's lease ids.
//
// Batch markers never enter pending, and travel only in this node's own
// queries and their replies (to == from == self), so they cannot meet a
// client marker despite the nonzero upper half. Server-minted read markers
// stay in the zero-upper-32 namespace (markerSeq).
//
// The sequence wraps within the lower 32 bits, keeping the incarnation;
// releasePendingBatches compares it wrap-safely.
func (g *SwitchReadGate) nextBatchMarker() uint64 {
	return uint64(g.batchIncarnation)<<32 | uint64(atomic.AddUint32(&g.batchSeq, 1))
}

// isClientMarker reports whether a marker lies in the client namespace: a
// nonzero upper 32 bits (see sidechannel.NewMarkerSeed). The zero-upper-32
// namespace belongs to this node's server-minted read markers.
func isClientMarker(marker uint64) bool { return marker>>32 != 0 }

// nextReadMarker mints a server-side marker for a marker-less read. It stays in
// the zero-upper-32 namespace, wrapping within the lower 32 bits and skipping
// 0, so it can never alias a client marker (SAFE-7).
//
// Read markers restart at 1 on every boot. That is safe because they never go
// on the wire: only a batch release (incarnation-checked, SAFE-3) or a reflected
// client MsgReadIndex can resolve one, and the latter is dropped unless its
// marker is in the client namespace.
func (g *SwitchReadGate) nextReadMarker() uint64 {
	for {
		if m := atomic.AddUint64(&g.markerSeq, 1) & math.MaxUint32; m != 0 {
			return m
		}
	}
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
			markerSeq:        1,
			batchIncarnation: newBatchIncarnation(),
			pendingMarkers:   make(chan readGateAsk, 256),
			resolved:         newResolvedMarkers(),
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

		if n < udpSidechannelMsgLen {
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
	m := decodeSidechannelMsg(buf)
	msgType := raftpb.MessageType(m.msgType)

	// MsgReadIndex is sent by the proxy (not a cluster peer) and tagged by the switch.
	// Handle it before the peer lookup since the proxy's ID is not in sc.peers.
	if msgType == raftpb.MsgReadIndex {
		if m.magic != sc.magic {
			sc.lg.Warn("received MsgReadIndex with invalid magic",
				zap.Uint16("magic", m.magic))
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
		// SAFE-7: a zero-upper-32 marker aliases this node's server-minted read
		// markers. Resolving one could hand a marker-less read a stamp taken
		// before that read began, so the packet is dropped whole.
		if !isClientMarker(m.marker) {
			sc.lg.Debug("dropped MsgReadIndex with a marker outside the client namespace",
				zap.Uint64("marker", m.marker))
			return
		}
		sc.updateSwitchIndex(m.marker, switchStamp{index: m.value, term: m.term})
		return
	}

	// A reply to this node's own QuerySwitchIndex: to == from == self.
	if types.ID(m.from) == sc.id {
		if m.magic != sc.magic {
			sc.lg.Warn("received UDP sidechannel query reply with invalid magic",
				zap.Uint16("magic", m.magic))
			return
		}
		if msgType == raftpb.MsgAskAckIndexResp {
			// SAFE-3: an answer to a query an earlier instance of this node
			// sent. Its value may predate every read now waiting, so it must
			// release none of them.
			if uint32(m.marker>>32) != sc.readGate.batchIncarnation {
				sc.lg.Debug("dropped UDP sidechannel query reply from another incarnation",
					zap.Uint64("marker", m.marker))
				return
			}
			// The marker could be for a batch of requests.
			stamp := switchStamp{index: m.value, term: m.term}
			sc.updateSwitchIndex(0, stamp)
			// Passing 0 here is fine; releasePendingBatches doesn't do anything if either arg is 0.
			sc.releasePendingBatches(m.marker, stamp)
		}
		return
	}

	// Everything else answers a write or ask this node sent on a peer's path.
	// The connected per-peer socket names the path; without one (dial failure)
	// the reply arrives on the listener and the peer is found by id.
	peer := src
	if peer == nil {
		v, ok := sc.peers.Load(types.ID(m.from))
		if !ok {
			sc.lg.Warn("received UDP sidechannel message with unknown peer ID",
				zap.Uint64("peerID", m.from))
			return
		}
		peer = v.(*udpPeerInfo)
	}
	// The switch on this path answers in the peer's magic, not necessarily ours.
	if m.magic != peer.magic {
		sc.lg.Warn("received UDP sidechannel message with invalid magic",
			zap.Uint16("magic", m.magic), zap.Uint16("want", peer.magic))
		return
	}

	switch msgType {
	case raftpb.MsgApp:
		// We don't care about this one; these are purely for the switch.
		sc.lg.Info("Got a MsgApp via UDP sidechannel")
	case raftpb.MsgAskAckIndex:
		// Same as for MsgApp; this is for the switch.
		sc.lg.Info("Got a MsgAskAckIndex via UDP sidechannel")
	case msgSwitchNack:
		sc.handleSwitchNack(peer, m)
	case raftpb.MsgAskAckIndexResp:
		sc.creditSwitchReply(peer, m)
	default:
		sc.lg.Warn("received UDP sidechannel message with unknown type",
			zap.Uint8("type", m.msgType))
	}
}

// creditSwitchReply hands raft the credit in a switch's reply to a write or
// ask on peer's path: a promise that every later stamp that switch gives is
// at least m.value.
//
// The promise only holds within one switch boot, so a reply is credited only
// if it carries the switch's current boot ID; one from any other boot (sent
// before a reboot and delivered late) is dropped. Replies never change the
// boot ID; only a NACK does (handleSwitchNack).
//
// The credit is capped at this node's high-water mark. A register value above
// it can only come from another (deposed) leader's write; crediting it would
// put the credit above what a re-seed after the next reboot writes. A smaller
// credit is always safe: it only lets the leader commit less without the
// holder's own ack, and the leader cannot commit beyond its own proposals.
func (sc *UdpSidechannel) creditSwitchReply(peer *udpPeerInfo, m sidechannelMsg) {
	// Read the high-water mark before the boot check. If the check passes, any
	// adoption of a newer boot comes after this read, and its re-seed then
	// writes at least this much, covering the credit below.
	highWater, _ := sc.writeState()
	if m.bootID != atomic.LoadUint32(&peer.sw.bootID) {
		return
	}
	sc.updateSwitchIndex(0, switchStamp{index: m.value, term: m.term})
	if m.value == 0 {
		// Nothing written on this boot yet: no information, and no credit.
		return
	}
	// Mark the switch as updated (i.e. not freshly rebooted/wiped).
	atomic.StoreUint32(&peer.switchUpdated, 1)
	credit := min(m.value, highWater)
	if credit == 0 {
		return // nothing proposed yet
	}
	sc.injectCredit(m.to, m.from, credit, m.term)

	// Credit every other lease holder behind this switch too, not just the
	// follower whose path carried the write: the switch's one register stamps
	// all of their reads, so its promise covers each of them. The commit clamp
	// then lifts on the first reply through the switch rather than the slowest
	// path's. Others are credited only if they held a lease at the last
	// monitor tick; raft ignores, and logs, a credit to a non-holder.
	if hs := sc.leaseHolders.Load(); hs != nil {
		for _, id := range *hs {
			if id == m.from {
				continue
			}
			if v, ok := sc.peers.Load(types.ID(id)); ok && v.(*udpPeerInfo).sw == peer.sw {
				sc.injectCredit(m.to, id, credit, m.term)
			}
		}
	}
}

// injectCredit hands raft a switch credit for follower from: a Term-0
// MsgAskAckIndexResp raising that holder's grant to index. LogTerm carries the
// term the switch reported with it; raft credits it only in that term, since
// the switch's index rises only within a term (LIVE-3 Part B).
func (sc *UdpSidechannel) injectCredit(to, from, index uint64, term uint32) {
	if sc.raft == nil {
		return
	}
	genMsg := raftpb.Message{
		Type:    raftpb.MsgAskAckIndexResp,
		To:      to,
		From:    from,
		Index:   index,
		LogTerm: uint64(term),
	}
	if err := sc.raft.Process(sc.ctx, genMsg); err != nil {
		sc.lg.Warn("failed to process MsgAskAckIndexResp via Raft",
			zap.Error(err))
	}
}

// handleSwitchNack handles a switch refusing a write on peer's path because
// it carried the wrong boot ID. The NACK names the switch's boot ID, and its
// marker echoes the boot ID the write was sent under.
//
// The switch's boot ID is adopted only if the NACK answers a write sent under
// the boot ID this node currently believes, i.e. it refutes the current
// belief. A NACK delayed from an earlier boot answers an older belief and is
// ignored; adopting it would roll every path back to a dead boot. On adoption
// the dedupe state of every path crossing the switch is cleared, so their next
// MsgApp copies go out under the new boot ID.
//
// Then, if the NACK's boot ID is current, the path is re-seeded with the
// high-water mark: a switch accepts no write, and so stamps nothing usable,
// until one carrying its boot ID lands. A re-seed that is lost is retried by
// the next MsgApp copy or by RefreshIdleSwitches.
func (sc *UdpSidechannel) handleSwitchNack(peer *udpPeerInfo, m sidechannelMsg) {
	if m.bootID == 0 {
		return // never reported by a switch
	}
	sw := peer.sw
	cur := atomic.LoadUint32(&sw.bootID)
	if m.marker == uint64(cur) && m.bootID != cur &&
		atomic.CompareAndSwapUint32(&sw.bootID, cur, m.bootID) {
		sc.lg.Info("UDP sidechannel switch boot ID changed; re-seeding",
			zap.String("peerID", peer.id.String()),
			zap.Uint16("magic", peer.magic),
			zap.Uint32("bootID", m.bootID))
		sc.peers.Range(func(_, v any) bool {
			if p := v.(*udpPeerInfo); p.sw == sw {
				p.resetLastSent()
			}
			return true
		})
	}
	if m.bootID == atomic.LoadUint32(&sw.bootID) {
		sc.refreshPeerSwitch(peer)
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

	switch m.Type {
	case raftpb.MsgApp:
		// Every write carries the node-wide high-water mark, not this MsgApp's
		// own end index: a lagging peer's MsgApps end below indexes the switch
		// has already reflected, and after a reboot whichever write lands first
		// must cover all of them (SAFE-1). The term is the MsgApp's own, a term
		// in which this node was leader; never the node's current term, which a
		// deposed leader may already have advanced.
		bootID := atomic.LoadUint32(&peer.sw.bootID) // before the high-water mark; see switchWrite
		testHookBetweenBootAndHighWater()
		highWater, ok := sc.raiseOnCopy(m.Index+uint64(len(m.Entries)), m.Term)
		if !ok || !peer.recordLastSent(m.Term, highWater) {
			return
		}
		sc.enqueue(sc.switchWrite(peer, bootID, highWater, m.Term))
	case raftpb.MsgAskAckIndex:
		sc.enqueue(udpSidechannelMsg{conn: peer.conn, remote: peer.remote, peerID: peer.id,
			msg: sidechannelMsg{
				magic:   peer.magic,
				msgType: byte(raftpb.MsgAskAckIndex),
				to:      uint64(peer.id),
				from:    uint64(sc.id),
			}.encode()})
	default:
		sc.lg.Error("Somehow got to default case of ProcessOutgoingMessage(), should never happen")
	}
}

// RefreshIdleSwitches re-sends, to every peer whose switch has said nothing
// since the previous call, this node's high-water mark. Only the leader can do
// this; it is the only node allowed to update switch indexes.
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

// refreshPeerSwitch writes this node's high-water mark to the switch on
// peer's path, under the path's known boot ID and the last term this node led.
// It restores a register that a reboot emptied, and is the re-seed after a NACK.
// It bypasses the MsgApp dedupe on purpose: it is a retransmission.
func (sc *UdpSidechannel) refreshPeerSwitch(peer *udpPeerInfo) {
	bootID := atomic.LoadUint32(&peer.sw.bootID) // before the high-water mark; see switchWrite
	testHookBetweenBootAndHighWater()
	highWater, leaderTerm := sc.writeState()
	if highWater == 0 {
		// Never tapped anything; nothing to restore.
		return
	}
	sc.enqueue(sc.switchWrite(peer, bootID, highWater, leaderTerm))
}

// testHookBetweenBootAndHighWater runs between a write's boot ID load and its
// high-water mark read. Tests only.
var testHookBetweenBootAndHighWater = func() {}

// switchWrite builds a write (MsgApp) of highWater at term to the switch on
// peer's path, under bootID. The switch applies it only if that boot ID is its
// own and NACKs it otherwise. The marker carries the same boot ID, which the
// switch echoes, so a NACK names the belief it refutes (see handleSwitchNack).
// The switch carries the term in 32 bits.
//
// Callers must load bootID BEFORE reading highWater. A write under a newly
// adopted boot ID must carry at least every credit given under the old one,
// and those credits were capped at a high-water mark read before the adoption.
// Read in the other order, a write could take a low mark, stall across a
// credit and an adoption, then go out under the new boot ID and land below the
// credit before the adoption's re-seed does.
func (sc *UdpSidechannel) switchWrite(peer *udpPeerInfo, bootID uint32, highWater, term uint64) udpSidechannelMsg {
	return udpSidechannelMsg{conn: peer.conn, remote: peer.remote, peerID: peer.id,
		msg: sidechannelMsg{
			magic:   peer.magic,
			msgType: byte(raftpb.MsgApp),
			to:      uint64(peer.id),
			from:    uint64(sc.id),
			marker:  uint64(bootID),
			value:   highWater,
			bootID:  bootID,
			term:    uint32(term),
		}.encode()}
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
				batchMarker := sc.readGate.nextBatchMarker()

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

	msg := sidechannelMsg{
		magic:   sc.magic, // Use our own magic, we want our switch
		msgType: byte(raftpb.MsgAskAckIndex),
		to:      uint64(sc.id), // Use our own ID
		from:    uint64(sc.id),
		marker:  marker,
	}.encode()

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
		// A changed path may cross a different switch (its magic says which),
		// and its dedupe state starts over.
		newPeer := &udpPeerInfo{
			id:       id,
			remote:   remote,
			magic:    magic,
			sw:       sc.switchFor(magic),
			conn:     sc.dialPeer(id, remote),
			refcount: peer.refcount + 1,
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
		sw:       sc.switchFor(magic),
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
		if n < udpSidechannelMsgLen {
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
// local query that releases it. It returns the index the switch reported and the
// term it stamped with it; raft uses the index only in that term (SAFE-1).
//
// After ReadGateDeadlineMillis it gives up and returns MaxUint64,
// meaning "no known switch marker for this read"; this causes a leaseholding raft follower
// to forward the read to the leader after it times out on waiting for the impossible-to-reach
// commit index of MaxUint64.
//
// Only the caller's own deadline produces an error, which is a genuinely failed read.
func (sc *UdpSidechannel) WaitForReadGate(ctx context.Context, marker uint64) (index uint64, term uint64, err error) {
	if !isClientMarker(marker) {
		// Marker-less read (client not speaking the assist protocol), or a
		// client marker in the server namespace (SAFE-7: treated as none, since
		// its reflection is dropped). Mint a server-side marker; the zero-upper-32
		// namespace is never accepted from a reflected client marker, so the two
		// can't collide. querySwitchIndexLoop's batched query will resolve it.
		marker = sc.readGate.nextReadMarker()
	}

	ch := make(chan switchStamp, 1)
	sc.readGate.pending.Store(marker, ch)

	// UDP normally beats TCP, so the switch marker usually arrives first.
	// This MUST be after pending.Store(),
	// else there's a gap where a marker can arrive and isn't consumed.
	if stamp, ok := sc.readGate.resolved.take(marker); ok {
		sc.readGate.pending.Delete(marker)
		return stamp.index, uint64(stamp.term), nil
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
		return math.MaxUint64, 0, nil
	case <-ctx.Done():
		sc.readGate.pending.Delete(marker)
		return 0, 0, ctx.Err()
	}

	select {
	case stamp := <-ch:
		return stamp.index, uint64(stamp.term), nil
	case <-gateTimer.C:
		sc.readGate.pending.Delete(marker)
		atomic.AddUint64(&sc.readGate.gateTimeouts, 1)
		return math.MaxUint64, 0, nil
	case <-ctx.Done():
		sc.readGate.pending.Delete(marker)
		return 0, 0, ctx.Err()
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
// A stamp with a zero index or term is discarded outright, as it means we have
// gained no information: the switch has accepted no write since it booted.
//
// Because markers are minted per read, a matching tagged answer proves the switch's register
// held the returned index at some time AFTER the read began.
func (sc *UdpSidechannel) updateSwitchIndex(marker uint64, stamp switchStamp) {
	if stamp.index == 0 || stamp.term == 0 {
		atomic.AddUint64(&sc.readGate.zeroAnswers, 1)
		return
	}
	switchIndex := stamp.index

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

	// Marker 0 is never minted (nextReadMarker skips it, and client markers have
	// a nonzero upper half), so a MsgAskAckIndexResp carrying no marker has nothing it can resolve.
	// Storing it would leak an entry in resolved that nothing ever consumes.
	if marker == 0 {
		return
	}

	// Resolve the per-marker channel if present
	if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
		ch := val.(chan switchStamp)
		select {
		case ch <- stamp: // Channel can never be full
		default:
		}
	} else { // Add the marker to resolved, this probably arrived first
		sc.readGate.resolved.store(marker, stamp)
		// This is ugly, but one of the two sides has to check one of the maps twice
		if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
			ch := val.(chan switchStamp)
			select {
			case ch <- stamp:
			default:
			}
			sc.readGate.resolved.remove(marker)
		}
	}
}

// releasePendingBatches releases every batch at or below batchMarker, handing
// each waiting read the stamp the switch actually returned for this query,
// NOT the node-global maximum.
//
// That value is a valid stamp for every marker released here: the query was
// sent after each of those reads entered the gate, so its answer describes the
// register at a time after each read began.
// Handing out the global max instead would gate reads on an index a later, unrelated read happened
// to observe, holding them at the follower for writes that may have began after the read.
// In a write-heavy workload this can mean reads are unduly delayed.
//
// The caller has checked that batchMarker is this instance's (SAFE-3), so only
// the lower halves are compared, as RFC 1982 serial numbers: pendingBatches is
// in minting order and spans far less than 2^31 batches, so "at or below"
// stays correct across the sequence's wrap.
func (sc *UdpSidechannel) releasePendingBatches(batchMarker uint64, answered switchStamp) {
	if batchMarker == 0 || answered.index == 0 || answered.term == 0 {
		return
	}

	sc.readGate.pendingBatchesLock.Lock()
	defer sc.readGate.pendingBatchesLock.Unlock()
	consumed := 0
	for consumed < len(sc.readGate.pendingBatches) {
		batch := sc.readGate.pendingBatches[consumed]
		if int32(uint32(batch.localMarker)-uint32(batchMarker)) > 0 {
			break
		}
		for _, marker := range batch.markers {
			if val, ok := sc.readGate.pending.LoadAndDelete(marker); ok {
				ch := val.(chan switchStamp)
				select {
				case ch <- answered:
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
