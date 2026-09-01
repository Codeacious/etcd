// Copyright 2024 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafthttp

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3/raftpb"
)

// newRefreshTestSidechannel builds a UdpSidechannel with no sockets. Only the
// register-refresh bookkeeping is exercised; sends land in sc.out.
func newRefreshTestSidechannel(t *testing.T) *UdpSidechannel {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// A real (unread) listener socket: QuerySwitchIndex writes through it, and
	// net.UDPConn methods panic rather than error on a nil receiver.
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return &UdpSidechannel{
		id:     types.ID(1),
		ctx:    ctx,
		cancel: cancel,
		conn:   conn,
		out:    make(chan udpSidechannelMsg, 8),
		lg:     zaptest.NewLogger(t),
		magic:  DefaultUdpSidechannelMagic,
		readGate: &SwitchReadGate{
			markerSeq:      1,
			pendingMarkers: make(chan readGateAsk, 8),
			resolved:       newResolvedMarkers(),
		},
	}
}

func addRefreshTestPeer(sc *UdpSidechannel, id uint64, lastSentIndex uint64) *udpPeerInfo {
	peer := &udpPeerInfo{
		id:            types.ID(id),
		magic:         sc.magic,
		lastSentIndex: lastSentIndex,
		// A routing target for QuerySwitchIndex; nothing listens on it.
		remote: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
	}
	sc.peers.Store(types.ID(id), peer)
	return peer
}

func encodeAskAckIndexResp(sc *UdpSidechannel, from uint64, value uint64) []byte {
	return encodeAskAckIndexRespWithMarker(sc, from, 0, value)
}

func encodeAskAckIndexRespWithMarker(sc *UdpSidechannel, from, marker, value uint64) []byte {
	buf := make([]byte, 35)
	binary.BigEndian.PutUint16(buf[0:2], sc.magic)
	buf[2] = byte(raftpb.MsgAskAckIndexResp)
	binary.BigEndian.PutUint64(buf[3:11], uint64(sc.id))
	binary.BigEndian.PutUint64(buf[11:19], from)
	binary.BigEndian.PutUint64(buf[19:27], marker)
	binary.BigEndian.PutUint64(buf[27:35], value)
	return buf
}

func drain(sc *UdpSidechannel) []udpSidechannelMsg {
	var out []udpSidechannelMsg
	for {
		select {
		case m := <-sc.out:
			out = append(out, m)
		default:
			return out
		}
	}
}

// TestSwitchUpdatedFlagOnlySetByNonzero is the livelock guard. A leader taps a
// MsgAskAckIndex to every lease holder every 5 ms, and the switch reflects each
// one. Against a REBOOTED switch those reflections carry 0; if they marked the
// path as healthy, the refresh that repairs the empty register would never fire
// while every gated read forwarded.
func TestSwitchUpdatedFlagOnlySetByNonzero(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	peer := addRefreshTestPeer(sc, 2, 500)

	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 0), peer)
	if got := atomic.LoadUint32(&peer.switchUpdated); got != 0 {
		t.Fatalf("a zero answer marked the path as holding state (flag=%d); it is the condition the refresh repairs", got)
	}

	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 500), peer)
	if got := atomic.LoadUint32(&peer.switchUpdated); got != 1 {
		t.Fatalf("a nonzero answer did not mark the path as holding state (flag=%d)", got)
	}
}

// TestRefreshIdleSwitchesRepeatsWhileRegisterEmpty walks the recovery loop: a
// switch answering 0 must be retried on every tick, not every other one.
func TestRefreshIdleSwitchesRepeatsWhileRegisterEmpty(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	peer := addRefreshTestPeer(sc, 2, 500)
	holders := []uint64{2}

	for tick := 0; tick < 3; tick++ {
		sc.RefreshIdleSwitches(holders)
		msgs := drain(sc)
		if len(msgs) != 1 {
			t.Fatalf("tick %d: got %d refreshes, want 1", tick, len(msgs))
		}
		if got := binary.BigEndian.Uint64(msgs[0].msg[27:35]); got != 500 {
			t.Fatalf("tick %d: refreshed with index %d, want lastSentIndex 500", tick, got)
		}
		if got := msgs[0].msg[2]; got != byte(raftpb.MsgApp) {
			t.Fatalf("tick %d: refresh type %d, want MsgApp", tick, got)
		}
		// The switch is still empty, so its answer is 0 and the flag stays clear.
		sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 0), peer)
	}

	// Once the register takes the value, the answer is nonzero and the next
	// tick backs off.
	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 500), peer)
	sc.RefreshIdleSwitches(holders)
	if msgs := drain(sc); len(msgs) != 0 {
		t.Fatalf("refreshed a path that reported holding state: %d msgs", len(msgs))
	}
}

func TestRefreshIdleSwitchesSkipsPeersWithoutLease(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 500)
	addRefreshTestPeer(sc, 3, 700)

	// Only peer 2 holds a lease; peer 3 cannot serve locally, so its switch
	// state is moot until it is granted one.
	sc.RefreshIdleSwitches([]uint64{2})
	msgs := drain(sc)
	if len(msgs) != 1 {
		t.Fatalf("got %d refreshes, want 1", len(msgs))
	}
	if msgs[0].peerID != types.ID(2) {
		t.Fatalf("refreshed peer %s, want 2", msgs[0].peerID)
	}

	sc.RefreshIdleSwitches(nil)
	if msgs := drain(sc); len(msgs) != 0 {
		t.Fatalf("refreshed with no lease holders: %d msgs", len(msgs))
	}
}

func TestRefreshIdleSwitchesSkipsUntappedPeer(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 0) // never tapped anything

	sc.RefreshIdleSwitches([]uint64{2})
	if msgs := drain(sc); len(msgs) != 0 {
		t.Fatalf("refreshed a peer with no proposed index to restore: %d msgs", len(msgs))
	}
}

// TestGrantedLeaseRefreshesSwitch covers the on-grant hook: a peer that just
// got a lease must not wait a tick for its path to be re-armed.
func TestGrantedLeaseRefreshesSwitch(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 500)

	granted := raftpb.Message{Type: raftpb.MsgAskReadLeaseResp, To: 2, From: 1}
	sc.ProcessOutgoingMessage(&granted)
	msgs := drain(sc)
	if len(msgs) != 1 {
		t.Fatalf("granting a lease sent %d refreshes, want 1", len(msgs))
	}
	if got := binary.BigEndian.Uint64(msgs[0].msg[27:35]); got != 500 {
		t.Fatalf("refreshed with index %d, want lastSentIndex 500", got)
	}

	rejected := raftpb.Message{Type: raftpb.MsgAskReadLeaseResp, To: 2, From: 1, Reject: true}
	sc.ProcessOutgoingMessage(&rejected)
	if msgs := drain(sc); len(msgs) != 0 {
		t.Fatalf("a rejected lease ask refreshed the switch: %d msgs", len(msgs))
	}
}

// TestGrantHookLeavesTapCASAlone guards the interaction that would break the
// MsgApp tap: the refresh must not advance lastSentIndex, or the next real tap
// at that index would be deduped away.
func TestGrantHookLeavesTapCASAlone(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	peer := addRefreshTestPeer(sc, 2, 500)

	granted := raftpb.Message{Type: raftpb.MsgAskReadLeaseResp, To: 2, From: 1}
	sc.ProcessOutgoingMessage(&granted)
	drain(sc)

	if got := atomic.LoadUint64(&peer.lastSentIndex); got != 500 {
		t.Fatalf("refresh moved lastSentIndex to %d, want it untouched at 500", got)
	}

	// A real tap at a higher index must still go out.
	app := raftpb.Message{Type: raftpb.MsgApp, To: 2, Index: 500, Entries: []raftpb.Entry{{Index: 501}}}
	sc.ProcessOutgoingMessage(&app)
	msgs := drain(sc)
	if len(msgs) != 1 {
		t.Fatalf("tap after refresh sent %d msgs, want 1", len(msgs))
	}
	if got := binary.BigEndian.Uint64(msgs[0].msg[27:35]); got != 501 {
		t.Fatalf("tap carried index %d, want 501", got)
	}
}

func encodeReadIndexReflection(sc *UdpSidechannel, marker, value uint64) []byte {
	buf := make([]byte, 35)
	binary.BigEndian.PutUint16(buf[0:2], sc.magic)
	buf[2] = byte(raftpb.MsgReadIndex)
	binary.BigEndian.PutUint64(buf[3:11], uint64(sc.id))
	binary.BigEndian.PutUint64(buf[11:19], uint64(sc.id))
	binary.BigEndian.PutUint64(buf[19:27], marker)
	binary.BigEndian.PutUint64(buf[27:35], value)
	return buf
}

// TestUnstampedReadIndexIsDiscarded inverts the 2026-08-20 behaviour. That
// change made a zero-valued read-gate reflection RESOLVE its marker, on the
// reasoning that the read would then be forwarded by raft's serve gate. It is
// not: the hint does not come from this read's answer but from the node's
// cached index, and a warm cache sails through the gate. A zero is evidence of
// nothing — the register is empty — so it must resolve nobody and leave no
// trace. The read keeps waiting, the query loop keeps re-asking, and the gate
// deadline forwards it if the register never comes back.
func TestUnstampedReadIndexIsDiscarded(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	const marker = uint64(0xABCD00000001)

	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, marker, 0), nil)

	if _, ok := sc.readGate.resolved.load(marker); ok {
		t.Fatal("a zero-valued read-gate reflection resolved its marker; it carries no usable index")
	}
	if zeros, _ := sc.SwitchGateStats(); zeros != 1 {
		t.Fatalf("zeroAnswers = %d, want 1", zeros)
	}
}

// TestUnstampedReadIndexLeavesCacheAlone: a 0 answer must not lower the node's
// monotone switch-index cache. Monotonicity is load-bearing twice over — it
// keeps the gate conservative against a rebooted register, and it is what
// defends against a delayed in-flight reflection reporting an older register
// state than one already observed.
func TestUnstampedReadIndexLeavesCacheAlone(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	sc.updateSwitchIndex(0, 500)

	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, 1, 0), nil)

	if got := sc.LatestSwitchIndex(); got != 500 {
		t.Fatalf("cached switch index = %d after a zero answer, want it held at 500", got)
	}
}

// TestZeroAskAckIndexRespKeepsBatchQueued covers the other release path. The
// existing guard in releasePendingBatches is on the batch MARKER, not on the
// answer, so a real batch marker carrying value 0 used to release that whole
// batch at the stale cached index. The batch must stay queued instead, and be
// picked up by the next usable answer.
func TestZeroAskAckIndexRespKeepsBatchQueued(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)
	sc.updateSwitchIndex(0, 500) // warm cache, as after a switch reboot

	ch := make(chan uint64, 1)
	const readMarker = uint64(0xBEEF01)
	sc.readGate.pending.Store(readMarker, ch)
	sc.readGate.pendingBatches = append(sc.readGate.pendingBatches, pendingMarkerBatch{
		localMarker: 7,
		markers:     []uint64{readMarker},
	})

	// Marker 7 is a REAL batch marker, so releasePendingBatches' own
	// batchMarker == 0 guard does not fire. Only a guard on the value stops it.
	sc.handleUdpSidechannelMsg(encodeAskAckIndexRespWithMarker(sc, 2, 7, 0), nil)
	if len(ch) != 0 {
		t.Fatal("a zero-valued MsgAskAckIndexResp released a gated read at the stale cache")
	}
	if n := len(sc.readGate.pendingBatches); n != 1 {
		t.Fatalf("pendingBatches = %d, want the batch left queued for a later answer", n)
	}

	// A usable answer arrives: now the batch releases.
	sc.handleUdpSidechannelMsg(encodeAskAckIndexRespWithMarker(sc, 2, 7, 900), nil)
	select {
	case got := <-ch:
		if got != 900 {
			t.Fatalf("released at %d, want the refreshed 900", got)
		}
	default:
		t.Fatal("a usable answer did not release the queued batch")
	}
}

// TestAskAckIndexRespLeavesNoZeroMarker: the MsgAskAckIndexResp path calls
// updateSwitchIndex with marker 0, which is never minted, so storing it in
// resolved leaks an entry nothing ever consumes.
func TestAskAckIndexRespLeavesNoZeroMarker(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)

	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 900), nil)

	if _, ok := sc.readGate.resolved.load(uint64(0)); ok {
		t.Fatal("marker 0 was stored in resolved; nothing ever consumes it")
	}
}

// TestBatchReleasedAtAnsweredIndex pins that a read released by the server's own
// batched query is stamped with the value that query returned, not the node's
// global maximum. Both are safe indices, but the global max gates the read on
// something a later unrelated read observed, which under write load holds it at
// the follower for entries it never needed to see.
func TestBatchReleasedAtAnsweredIndex(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)

	// Some other read has already pushed the global maximum well ahead.
	sc.updateSwitchIndex(0, 5000)

	ch := make(chan uint64, 1)
	const readMarker = uint64(0xBEEF02)
	sc.readGate.pending.Store(readMarker, ch)
	sc.readGate.pendingBatches = append(sc.readGate.pendingBatches, pendingMarkerBatch{
		localMarker: 7,
		markers:     []uint64{readMarker},
	})

	sc.handleUdpSidechannelMsg(encodeAskAckIndexRespWithMarker(sc, 2, 7, 1200), nil)

	select {
	case got := <-ch:
		if got != 1200 {
			t.Fatalf("released at %d, want this query's own answer 1200", got)
		}
	default:
		t.Fatal("the batch was not released")
	}
}

// TestDroppedReflectionRecoveredByQuery is the case a client-minted marker has
// to survive: the read request arrives but its sidechannel packet is lost, so no
// tagged reflection will ever come back. Every marker — client-minted or not —
// is enrolled in querySwitchIndexLoop, so the server's own query picks it up
// within ReadGateAskTimeoutMillis and stamps it, instead of the read waiting out
// the gate deadline and forwarding.
func TestDroppedReflectionRecoveredByQuery(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)
	go sc.querySwitchIndexLoop()

	const clientMarker = uint64(0xABCD00000042) // upper 32 bits set: client-minted
	done := make(chan uint64, 1)
	go func() {
		got, err := sc.WaitForReadGate(context.Background(), clientMarker)
		if err != nil {
			t.Errorf("gate errored: %v", err)
		}
		done <- got
	}()

	// Answer the server's query, never the client's marker.
	deadline := time.Now().Add(2 * ReadGateDeadlineMillis * time.Millisecond)
	for time.Now().Before(deadline) {
		sc.readGate.pendingBatchesLock.Lock()
		var newest uint64
		for _, b := range sc.readGate.pendingBatches {
			if b.localMarker > newest {
				newest = b.localMarker
			}
		}
		sc.readGate.pendingBatchesLock.Unlock()
		if newest != 0 {
			sc.handleUdpSidechannelMsg(encodeAskAckIndexRespWithMarker(sc, 2, newest, 1200), nil)
			break
		}
		time.Sleep(time.Millisecond)
	}

	select {
	case got := <-done:
		if got != 1200 {
			t.Fatalf("recovered read stamped %d, want 1200 (MaxUint64 means it waited out the gate)", got)
		}
	case <-time.After(2 * ReadGateDeadlineMillis * time.Millisecond):
		t.Fatal("read never released")
	}
}

// TestWaitForReadGateForwardsOnDeadline: an expired gate is not a failed read.
// It returns MaxUint64 with no error, so the read proceeds and raft forwards it
// to the leader. Only the caller's own deadline is an error.
func TestWaitForReadGateForwardsOnDeadline(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)

	start := time.Now()
	got, err := sc.WaitForReadGate(context.Background(), 0)
	if err != nil {
		t.Fatalf("an expired gate must not error, got %v", err)
	}
	if got != math.MaxUint64 {
		t.Fatalf("expired gate returned %d, want MaxUint64", got)
	}
	if elapsed := time.Since(start); elapsed < ReadGateDeadlineMillis*time.Millisecond {
		t.Fatalf("gate gave up after %v, want at least %d ms", elapsed, ReadGateDeadlineMillis)
	}
	if _, timeouts := sc.SwitchGateStats(); timeouts != 1 {
		t.Fatalf("gateTimeouts = %d, want 1", timeouts)
	}
}

// TestWaitForReadGateCallerDeadlineErrors: the caller's own deadline is the one
// outcome that fails the read rather than forwarding it.
func TestWaitForReadGateCallerDeadlineErrors(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := sc.WaitForReadGate(ctx, 0); err == nil {
		t.Fatal("expected the caller's deadline to fail the read")
	}
}

// TestQuerySwitchIndexLoopReasksWhileWaiting pins the re-ask cadence that makes
// a discarded zero recoverable. The old loop never truncated its marker slice,
// so its fast-path arm was reachable only before the first marker ever arrived
// and every later query waited a full second — useless as a retry path. Each
// ask queues a batch, so counting batches counts asks.
func TestQuerySwitchIndexLoopReasksWhileWaiting(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)
	go sc.querySwitchIndexLoop()

	// A marker that nothing will ever answer.
	const marker = uint64(0xFEED01)
	sc.readGate.pending.Store(marker, make(chan uint64, 1))
	sc.readGate.pendingMarkers <- readGateAsk{marker: marker, nextAskAt: time.Now()}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		sc.readGate.pendingBatchesLock.Lock()
		n := len(sc.readGate.pendingBatches)
		sc.readGate.pendingBatchesLock.Unlock()
		if n >= 2 {
			return // asked at least twice well inside a second
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("loop did not re-ask while a marker waited")
}

// TestQuerySwitchIndexLoopDropsDeadBatches: against a switch that never answers,
// each interval would otherwise queue a batch forever. Batches with nothing
// still waiting on them are dropped, so the list cannot outlive the gate
// deadline that clears their markers.
func TestQuerySwitchIndexLoopDropsDeadBatches(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)

	// Two batches whose markers are long gone, plus one live marker. Push
	// markerSeq past them so the loop's own batch markers cannot collide.
	sc.readGate.pendingBatches = []pendingMarkerBatch{
		{localMarker: 1, markers: []uint64{0xDEAD01}},
		{localMarker: 2, markers: []uint64{0xDEAD02}},
	}
	sc.readGate.markerSeq = 1000
	const live = uint64(0xFEED02)
	sc.readGate.pending.Store(live, make(chan uint64, 1))

	go sc.querySwitchIndexLoop()
	sc.readGate.pendingMarkers <- readGateAsk{marker: live, nextAskAt: time.Now()}

	time.Sleep(50 * time.Millisecond)
	sc.readGate.pendingBatchesLock.Lock()
	defer sc.readGate.pendingBatchesLock.Unlock()
	for _, b := range sc.readGate.pendingBatches {
		if b.localMarker == 1 || b.localMarker == 2 {
			t.Fatalf("dead batch %d was never dropped", b.localMarker)
		}
	}
}

// TestGateInactiveDropsReflection: the receive path runs independently of any
// read, so on a leader — which skips the gate entirely — every client reflection
// would take updateSwitchIndex's "arrived before its request" branch and park in
// resolved with nothing to ever claim it. Under the harness's round-robin read
// target that is ~1/3 of all reads.
func TestGateInactiveDropsReflection(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	sc.SetGateActiveChecker(func() bool { return false })

	const marker = uint64(0xABCD00000009)
	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, marker, 1200), nil)

	if _, ok := sc.readGate.resolved.load(marker); ok {
		t.Fatal("a reflection was parked while this node was not gating reads")
	}

	// Gating again, the same reflection is parked for the read to claim.
	sc.SetGateActiveChecker(func() bool { return true })
	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, marker, 1200), nil)
	if _, ok := sc.readGate.resolved.load(marker); !ok {
		t.Fatal("a reflection was dropped while this node was gating reads")
	}
}

// TestResolvedRotationBoundsUnclaimed: unclaimed answers expire by generation,
// not by sweep or per-entry timer. Two rotations must reclaim an entry no read
// ever came for.
func TestResolvedRotationBoundsUnclaimed(t *testing.T) {
	r := newResolvedMarkers()
	r.store(1, 100)
	if n := r.len(); n != 1 {
		t.Fatalf("len = %d, want 1", n)
	}

	// Force a rotation: the entry moves to prev but is still claimable.
	r.lastRotate = time.Now().Add(-2 * resolvedMarkerTTL)
	r.store(2, 200)
	if _, ok := r.load(1); !ok {
		t.Fatal("an entry was dropped after only one rotation")
	}

	// A second rotation drops the generation holding it.
	r.lastRotate = time.Now().Add(-2 * resolvedMarkerTTL)
	r.store(3, 300)
	if _, ok := r.load(1); ok {
		t.Fatal("an unclaimed entry survived two rotations")
	}
	if _, ok := r.load(2); !ok {
		t.Fatal("the second entry was dropped too early")
	}
}

// TestResolvedTakeSpansGenerations: a read claiming its answer must find it
// whichever generation holds it, and claiming must remove it.
func TestResolvedTakeSpansGenerations(t *testing.T) {
	r := newResolvedMarkers()
	r.store(1, 100)
	r.lastRotate = time.Now().Add(-2 * resolvedMarkerTTL)
	r.store(2, 200) // rotates; 1 is now in prev, 2 in curr

	for _, tc := range []struct{ marker, want uint64 }{{1, 100}, {2, 200}} {
		got, ok := r.take(tc.marker)
		if !ok || got != tc.want {
			t.Fatalf("take(%d) = %d, %v; want %d, true", tc.marker, got, ok, tc.want)
		}
		if _, ok := r.load(tc.marker); ok {
			t.Fatalf("take(%d) left the entry behind", tc.marker)
		}
	}
	if n := r.len(); n != 0 {
		t.Fatalf("len = %d after claiming both, want 0", n)
	}
}

// TestQueryLoopWaitsForMarkerMaturity pins the per-marker window. The ask timer
// is shared, so a marker arriving just before a fire used to be queried almost
// immediately — long before its own reflection had the interval it is supposed
// to get. Eligibility is per-marker; the timer only decides when to look.
func TestQueryLoopWaitsForMarkerMaturity(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2, 100)
	go sc.querySwitchIndexLoop()

	const marker = uint64(0xFEED03)
	sc.readGate.pending.Store(marker, make(chan uint64, 1))
	// Deliberately far in the future: no query may name this marker yet.
	sc.readGate.pendingMarkers <- readGateAsk{
		marker:    marker,
		nextAskAt: time.Now().Add(time.Hour),
	}

	time.Sleep(10 * ReadGateAskTimeoutMillis * time.Millisecond)

	sc.readGate.pendingBatchesLock.Lock()
	defer sc.readGate.pendingBatchesLock.Unlock()
	for _, b := range sc.readGate.pendingBatches {
		for _, m := range b.markers {
			if m == marker {
				t.Fatal("an immature marker was queried before its window elapsed")
			}
		}
	}
}
