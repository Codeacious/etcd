// Copyright 2026 The etcd Authors
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
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// SAFE-1: a switch's reply carrying V promises that every later stamp it gives
// is at least V. A reboot empties the register, so the switch must not stamp
// anything usable until a write that covers every earlier credit lands, and
// only a write carrying its new boot ID may land.

// fakeSwitch implements the sidechannel contract of tack-switch-agen.p4 over
// UDP (ETCD.md, "Wire format"). Boot IDs are deterministic so tests can name
// them.
type fakeSwitch struct {
	conn  *net.UDPConn
	magic uint16

	mu    sync.Mutex
	boots uint32 // reboots so far; the boot ID is derived from it
	boot  uint32 // 0 until the first packet after a (re)boot
	reg   uint64
	term  uint32
	fwd   *net.UDPAddr
}

func newFakeSwitch(t *testing.T) *fakeSwitch {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeSwitch{conn: c, magic: DefaultUdpSidechannelMagic}
	t.Cleanup(func() { c.Close() })
	go s.run()
	return s
}

func (s *fakeSwitch) port() string { return strconv.Itoa(s.conn.LocalAddr().(*net.UDPAddr).Port) }

func (s *fakeSwitch) reboot() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.boots++
	s.boot, s.reg, s.term = 0, 0, 0
}

// stamp is what the switch would put on a read right now.
func (s *fakeSwitch) stamp() switchStamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return switchStamp{index: s.reg, term: s.term}
}

func (s *fakeSwitch) bootID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.boot
}

func (s *fakeSwitch) run() {
	buf := make([]byte, 9000)
	for {
		n, src, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < udpSidechannelMsgLen {
			continue
		}
		m := decodeSidechannelMsg(buf[:n])
		if m.magic != s.magic {
			continue
		}
		s.mu.Lock()
		if s.boot == 0 {
			s.boot = 0xB0070000 | (s.boots + 1)
		}
		reflect := true
		switch m.msgType {
		case byte(raftpb.MsgApp):
			if m.bootID == s.boot {
				s.reg = max(s.reg, m.value)
				s.term = max(s.term, m.term)
				m.msgType, m.value = byte(raftpb.MsgAskAckIndexResp), s.reg
			} else {
				m.msgType, m.value = msgSwitchNack, 0
			}
		case byte(raftpb.MsgAskAckIndex):
			m.msgType, m.value = byte(raftpb.MsgAskAckIndexResp), s.reg
		case byte(raftpb.MsgReadIndex):
			m.value, reflect = s.reg, false
		default:
			s.mu.Unlock()
			continue
		}
		m.bootID, m.term = s.boot, s.term
		fwd := s.fwd
		s.mu.Unlock()

		out := m
		if reflect {
			out.to, out.from = m.from, m.to
		}
		pkt := out.encode()
		if reflect {
			s.conn.WriteToUDP(pkt[:], src)
		} else if fwd != nil {
			s.conn.WriteToUDP(pkt[:], fwd)
		}
	}
}

// recRaft records the credits the sidechannel injects into raft, along with
// the node's high-water mark at the moment each one arrived.
type recRaft struct {
	sc *UdpSidechannel
	ch chan recCredit
}

type recCredit struct {
	m         raftpb.Message
	highWater uint64
}

func newRecRaft() *recRaft { return &recRaft{ch: make(chan recCredit, 64)} }

func (r *recRaft) Process(_ context.Context, m raftpb.Message) error {
	var hw uint64
	if r.sc != nil {
		hw = atomic.LoadUint64(&r.sc.highWater)
	}
	r.ch <- recCredit{m: m, highWater: hw}
	return nil
}
func (r *recRaft) IsIDRemoved(uint64) bool                    { return false }
func (r *recRaft) ReportUnreachable(uint64)                   {}
func (r *recRaft) ReportSnapshot(uint64, raft.SnapshotStatus) {}

func (r *recRaft) next(t *testing.T) recCredit {
	t.Helper()
	select {
	case c := <-r.ch:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("no credit injected")
	}
	return recCredit{}
}

func (r *recRaft) none(t *testing.T, wait time.Duration) {
	t.Helper()
	select {
	case c := <-r.ch:
		t.Fatalf("unexpected credit %+v", c.m)
	case <-time.After(wait):
	}
}

func tapMsgApp(sc *UdpSidechannel, to, term, prevIndex uint64, nEntries int) {
	m := raftpb.Message{Type: raftpb.MsgApp, To: to, Term: term, Index: prevIndex, Entries: make([]raftpb.Entry, nEntries)}
	sc.ProcessOutgoingMessage(&m)
}

// newSafe1Leader starts a leader sidechannel (id 1) whose peers 2 and 3 both
// sit behind sw.
func newSafe1Leader(t *testing.T, sw *fakeSwitch) (*UdpSidechannel, *recRaft) {
	t.Helper()
	lr := newRecRaft()
	leader := NewUdpSidechannel(types.ID(1), "127.0.0.1", 0, 0, lr, zaptest.NewLogger(t))
	if leader == nil {
		t.Fatal("sidechannel not created")
	}
	lr.sc = leader
	t.Cleanup(leader.Stop)
	if !leader.AttachPeer(types.ID(2), "127.0.0.1", sw.port(), "feed") ||
		!leader.AttachPeer(types.ID(3), "127.0.0.1", sw.port(), "feed") {
		t.Fatal("attach failed")
	}
	return leader, lr
}

// TestSwitchRebootReseedCoversCredit is the SAFE-1 sequence. Up-to-date A (2)
// takes the register to 100; lagging B (3) is credited with it. The switch
// reboots. Whatever write reaches it next (B's own catch-up MsgApp, the idle
// refresh, or the grant hook), nothing may be stamped below 100: the stale
// write is NACKed, and the re-seed carries the high-water mark under the new
// boot ID.
func TestSwitchRebootReseedCoversCredit(t *testing.T) {
	for _, variant := range []string{"lagging-tap", "idle-refresh", "grant-hook"} {
		t.Run(variant, func(t *testing.T) {
			sw := newFakeSwitch(t)
			leader, lr := newSafe1Leader(t, sw)

			// A's first write draws a NACK (no boot ID known yet); the re-seed is
			// then credited.
			tapMsgApp(leader, 2, 5, 99, 1)
			if c := lr.next(t); c.m.From != 2 || c.m.Index != 100 || c.m.Term != 0 {
				t.Fatalf("A credit: %+v", c.m)
			}
			// B lags: its MsgApp ends at 50, but the write carries the high-water
			// mark, and B is credited with the register, 100.
			tapMsgApp(leader, 3, 5, 49, 1)
			if c := lr.next(t); c.m.From != 3 || c.m.Index != 100 {
				t.Fatalf("B credit: %+v", c.m)
			}

			sw.reboot()
			if st := sw.stamp(); st.index != 0 {
				t.Fatalf("rebooted switch stamps %+v", st)
			}

			switch variant {
			case "lagging-tap":
				// B's next catch-up MsgApp does not raise the high-water mark, so
				// it is deduped; a new entry for A does, and its write is NACKed
				// and re-seeds.
				tapMsgApp(leader, 3, 5, 59, 1)
				tapMsgApp(leader, 2, 5, 100, 1)
			case "idle-refresh":
				leader.RefreshIdleSwitches([]uint64{3}) // clears the flag the B credit set
				leader.RefreshIdleSwitches([]uint64{3})
			case "grant-hook":
				gm := raftpb.Message{Type: raftpb.MsgAskReadLeaseResp, To: 3}
				leader.ProcessOutgoingMessage(&gm)
			}
			c := lr.next(t) // the re-seed's own credit
			if c.m.Index < 100 {
				t.Fatalf("post-reboot credit %d is below the pre-reboot credit 100", c.m.Index)
			}
			if st := sw.stamp(); st.index < 100 || st.term != 5 {
				t.Fatalf("post-reboot stamp %+v, want >= 100 at term 5", st)
			}
		})
	}
}

// TestDelayedPreRebootWriteRejected: a write sent before a reboot and delivered
// after it carries the old boot ID. The new switch must not apply it, however
// it arrives, and stamps nothing until a write with its own boot ID lands.
func TestDelayedPreRebootWriteRejected(t *testing.T) {
	sw := newFakeSwitch(t)
	leader, lr := newSafe1Leader(t, sw)
	tapMsgApp(leader, 2, 5, 99, 1)
	lr.next(t)
	oldBoot := sw.bootID()

	sw.reboot()

	// The delayed write: an older, lower high-water mark under the old boot.
	c, err := net.DialUDP("udp", nil, sw.conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stale := sidechannelMsg{magic: DefaultUdpSidechannelMagic, msgType: byte(raftpb.MsgApp),
		to: 2, from: 1, value: 50, bootID: oldBoot, term: 5}.encode()
	if _, err := c.Write(stale[:]); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	reply := decodeSidechannelMsg(buf[:n])
	if reply.msgType != msgSwitchNack || reply.bootID == oldBoot || reply.bootID == 0 {
		t.Fatalf("stale write answered %+v, want a NACK carrying the new boot ID", reply)
	}
	if st := sw.stamp(); st.index != 0 || st.term != 0 {
		t.Fatalf("the stale write landed: stamp %+v", st)
	}
}

// TestReplyFromOtherBootNotCredited: a reply that does not carry the path's
// boot ID (a pre-reboot reply delivered late) is dropped. It credits nothing,
// does not raise the high-water mark or mark the path healthy, and does not
// adopt its boot ID, which would roll the path back to a dead boot.
func TestReplyFromOtherBootNotCredited(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	lr := newRecRaft()
	sc.raft = lr
	peer := addRefreshTestPeer(sc, 2)
	sc.highWater = 100

	late := sidechannelMsg{magic: sc.magic, msgType: byte(raftpb.MsgAskAckIndexResp),
		to: uint64(sc.id), from: 2, value: 900, bootID: testBootID + 1, term: testTerm}.encode()
	sc.handleUdpSidechannelMsg(late[:], peer)

	lr.none(t, 20*time.Millisecond)
	if sc.highWater != 100 {
		t.Fatalf("high-water mark moved to %d", sc.highWater)
	}
	if atomic.LoadUint32(&peer.switchUpdated) != 0 {
		t.Fatal("a reply from another boot marked the path healthy")
	}
	if got := atomic.LoadUint32(&peer.sw.bootID); got != testBootID {
		t.Fatalf("switch boot ID changed to %#x", got)
	}
	if msgs := drain(sc); len(msgs) != 0 {
		t.Fatalf("a reply from another boot triggered %d writes", len(msgs))
	}
}

// TestCreditCappedAtHighWater: a credit never exceeds this node's own
// high-water mark. A register value above it can only be another (deposed)
// leader's write, and a re-seed after the next reboot writes only the
// high-water mark. Replies never raise the high-water mark.
func TestCreditCappedAtHighWater(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	lr := newRecRaft()
	sc.raft = lr
	peer := addRefreshTestPeer(sc, 2)
	sc.highWater = 100

	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 900), peer)
	if c := lr.next(t); c.m.Index != 100 {
		t.Fatalf("credit %d, want it capped at the high-water mark 100", c.m.Index)
	}
	if sc.highWater != 100 {
		t.Fatalf("a reply moved the high-water mark to %d", sc.highWater)
	}

	// Below the mark the switch's own promise is the credit.
	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 60), peer)
	if c := lr.next(t); c.m.Index != 60 {
		t.Fatalf("credit %d, want the reply's 60", c.m.Index)
	}

	// Nothing proposed yet: nothing to credit.
	sc.highWater = 0
	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 60), peer)
	lr.none(t, 20*time.Millisecond)
}

// TestLateReplyOnUnrebootedPathDropped is the per-path residual: after one
// path learns of a reboot from a NACK, a pre-reboot reply arriving late on
// another path through the same switch must not be credited. Its value may sit
// above the re-seed the new boot received.
func TestLateReplyOnUnrebootedPathDropped(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	lr := newRecRaft()
	sc.raft = lr
	a := addRefreshTestPeer(sc, 2)
	b := addRefreshTestPeer(sc, 3)
	sc.highWater, sc.leaderTerm = 105, 6
	const newBoot = uint32(0xB0072)

	// A's write under the old boot is NACKed by the rebooted switch.
	nack := sidechannelMsg{magic: sc.magic, msgType: msgSwitchNack, to: uint64(sc.id), from: 2,
		marker: uint64(testBootID), bootID: newBoot}.encode()
	sc.handleUdpSidechannelMsg(nack[:], a)
	drain(sc)

	// B's pre-reboot reply, delayed, arrives on B's path.
	sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 3, 105), b) // carries the dead testBootID
	lr.none(t, 20*time.Millisecond)
	if atomic.LoadUint32(&b.switchUpdated) != 0 {
		t.Fatal("a dead boot's reply marked B's path healthy")
	}
}

// TestDelayedNackDoesNotRollBack: a NACK carries the boot ID the switch had
// when it sent it. One delayed from an earlier boot answers a write sent under
// an older belief (its marker), so it must not move the switch's boot ID back.
func TestDelayedNackDoesNotRollBack(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	peer := addRefreshTestPeer(sc, 2) // believes testBootID
	sc.highWater = 100

	// From the boot before testBootID, answering a write sent under ID 0.
	stale := sidechannelMsg{magic: sc.magic, msgType: msgSwitchNack, to: uint64(sc.id), from: 2,
		marker: 0, bootID: 0xDEAD1}.encode()
	sc.handleUdpSidechannelMsg(stale[:], peer)
	if got := atomic.LoadUint32(&peer.sw.bootID); got != testBootID {
		t.Fatalf("a delayed NACK rolled the switch back to %#x", got)
	}
	if msgs := drain(sc); len(msgs) != 0 {
		t.Fatalf("a delayed NACK triggered %d writes", len(msgs))
	}
}

// TestAdoptionResetsDedupeOnSwitchPaths: once a switch's new boot ID is
// adopted, every path through it must be able to write again, even if its
// (term, high-water mark) was already written under the old boot.
func TestAdoptionResetsDedupeOnSwitchPaths(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	a := addRefreshTestPeer(sc, 2)
	addRefreshTestPeer(sc, 3)
	tapMsgApp(sc, 2, 5, 899, 1)
	tapMsgApp(sc, 3, 5, 899, 1)
	drain(sc)

	nack := sidechannelMsg{magic: sc.magic, msgType: msgSwitchNack, to: uint64(sc.id), from: 2,
		marker: uint64(testBootID), bootID: 0xB0072}.encode()
	sc.handleUdpSidechannelMsg(nack[:], a)
	drain(sc) // A's re-seed

	tapMsgApp(sc, 3, 5, 899, 1) // same (term, high-water mark) as before the reboot
	msgs := drain(sc)
	if len(msgs) != 1 {
		t.Fatalf("B's copy after adoption sent %d writes, want 1", len(msgs))
	}
	if w := decodeSidechannelMsg(msgs[0].msg[:]); w.bootID != 0xB0072 || w.marker != 0xB0072 || w.to != 3 {
		t.Fatalf("B's write %+v, want it under the new boot ID", w)
	}
}

// TestWritesReadHighWaterAfterBootID: a write under a newly adopted boot ID
// must cover every credit given before the adoption. The hook runs between a
// write's two loads and plays out the race: a credit of 200 (so the high-water
// mark reaches 200) and an adoption of a new boot ID. A write that went out
// under the new boot ID must carry at least 200.
func TestWritesReadHighWaterAfterBootID(t *testing.T) {
	const newBoot = uint32(0xB0072)
	for _, path := range []string{"tap", "refresh"} {
		t.Run(path, func(t *testing.T) {
			sc := newRefreshTestSidechannel(t)
			peer := addRefreshTestPeer(sc, 2)
			sc.highWater, sc.leaderTerm = 100, 5
			fired := false
			testHookBetweenBootAndHighWater = func() {
				if !fired {
					fired = true
					atomicMax(&sc.highWater, 200)
					atomic.StoreUint32(&peer.sw.bootID, newBoot)
				}
			}
			t.Cleanup(func() { testHookBetweenBootAndHighWater = func() {} })

			if path == "tap" {
				tapMsgApp(sc, 2, 5, 99, 1)
			} else {
				sc.refreshPeerSwitch(peer)
			}
			msgs := drain(sc)
			if len(msgs) != 1 {
				t.Fatalf("got %d writes, want 1", len(msgs))
			}
			if w := decodeSidechannelMsg(msgs[0].msg[:]); w.bootID == newBoot && w.value < 200 {
				t.Fatalf("write %d under the new boot ID is below the credit 200", w.value)
			}
		})
	}
}

// TestNackAdoptsBootAndReseeds: a NACK answering a write sent under the
// current boot ID names the switch's new one; every path through the switch
// adopts it, and this path is re-seeded with the high-water mark under it.
func TestNackAdoptsBootAndReseeds(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	peer := addRefreshTestPeer(sc, 2)
	sc.highWater, sc.leaderTerm = 700, 6
	const newBoot = uint32(0xB0072)

	nack := sidechannelMsg{magic: sc.magic, msgType: msgSwitchNack,
		to: uint64(sc.id), from: 2, marker: uint64(testBootID), bootID: newBoot}.encode()
	sc.handleUdpSidechannelMsg(nack[:], peer)
	if got := atomic.LoadUint32(&peer.sw.bootID); got != newBoot {
		t.Fatalf("switch boot ID %#x, want %#x", got, newBoot)
	}
	msgs := drain(sc)
	if len(msgs) != 1 {
		t.Fatalf("NACK sent %d writes, want one re-seed", len(msgs))
	}
	w := decodeSidechannelMsg(msgs[0].msg[:])
	if w.msgType != byte(raftpb.MsgApp) || w.value != 700 || w.bootID != newBoot || w.marker != uint64(newBoot) || w.term != 6 {
		t.Fatalf("re-seed %+v, want MsgApp of 700 under boot %#x at term 6", w, newBoot)
	}

	// The same NACK again (another write that was in flight under the old
	// boot): the boot ID stays, and the path is re-seeded under it once more.
	sc.handleUdpSidechannelMsg(nack[:], peer)
	if got := atomic.LoadUint32(&peer.sw.bootID); got != newBoot {
		t.Fatalf("a repeated NACK changed the boot ID to %#x", got)
	}
	if msgs := drain(sc); len(msgs) != 1 || decodeSidechannelMsg(msgs[0].msg[:]).bootID != newBoot {
		t.Fatalf("a repeated NACK sent %d writes, want one re-seed under the current boot", len(msgs))
	}

	// A boot ID of 0 is never a switch's; ignore it.
	zero := sidechannelMsg{magic: sc.magic, msgType: msgSwitchNack, to: uint64(sc.id), from: 2,
		marker: uint64(newBoot)}.encode()
	sc.handleUdpSidechannelMsg(zero[:], peer)
	if got := atomic.LoadUint32(&peer.sw.bootID); got != newBoot {
		t.Fatalf("a zero boot ID was adopted")
	}
}

// TestTapCarriesHighWaterMark: a lagging peer's MsgApp ends below what the
// switch has already reflected for an up-to-date peer. Its write must carry
// the node-wide high-water mark, not its own end index.
func TestTapCarriesHighWaterMark(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2)
	addRefreshTestPeer(sc, 3)

	tapMsgApp(sc, 2, 5, 899, 1) // up to date: ends at 900
	tapMsgApp(sc, 3, 5, 499, 1) // lagging: ends at 500
	msgs := drain(sc)
	if len(msgs) != 2 {
		t.Fatalf("got %d writes, want 2", len(msgs))
	}
	for _, m := range msgs {
		w := decodeSidechannelMsg(m.msg[:])
		if w.value != 900 || w.term != 5 || w.bootID != testBootID {
			t.Fatalf("write to %d = %+v, want 900 at term 5 under the path's boot ID", w.to, w)
		}
	}
}

// TestTapDedupesOnTermAndHighWater: a write goes out when the term or the
// high-water mark advances. A re-elected leader whose high-water mark is left
// over from an earlier term must still write in its new term, or the switch
// keeps the old term and followers reject every stamp for the whole term.
func TestTapDedupesOnTermAndHighWater(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	addRefreshTestPeer(sc, 2)

	steps := []struct {
		term, prev uint64
		want       bool
	}{
		{5, 899, true},  // first write
		{5, 899, false}, // nothing new
		{5, 499, false}, // a catch-up MsgApp below the high-water mark
		{7, 499, true},  // same high-water mark, new term
		{6, 999, false}, // an older term never writes, even with a higher index
		{7, 999, true},  // higher index in the current term
	}
	for i, st := range steps {
		tapMsgApp(sc, 2, st.term, st.prev, 1)
		msgs := drain(sc)
		if got := len(msgs) == 1; got != st.want {
			t.Fatalf("step %d (term %d, end %d): wrote=%v, want %v", i, st.term, st.prev+1, got, st.want)
		}
		if st.want {
			if w := decodeSidechannelMsg(msgs[0].msg[:]); w.term != uint32(st.term) {
				t.Fatalf("step %d: write term %d, want the MsgApp's %d", i, w.term, st.term)
			}
		}
	}
}

// TestReplyUsesPeerMagic: each switch acts only on its own magic, so a reply on
// a peer's path carries that peer's magic, which need not be this node's.
func TestReplyUsesPeerMagic(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	lr := newRecRaft()
	sc.raft = lr
	peer := addRefreshTestPeer(sc, 2)
	peer.magic, peer.sw = 0xBEEF, sc.switchFor(0xBEEF)
	atomic.StoreUint32(&peer.sw.bootID, testBootID)
	sc.highWater = 1000

	ours := encodeAskAckIndexResp(sc, 2, 300) // carries sc.magic, 0xFEED
	sc.handleUdpSidechannelMsg(ours, peer)
	lr.none(t, 20*time.Millisecond)

	theirs := sidechannelMsg{magic: 0xBEEF, msgType: byte(raftpb.MsgAskAckIndexResp),
		to: uint64(sc.id), from: 2, value: 300, bootID: testBootID, term: testTerm}.encode()
	sc.handleUdpSidechannelMsg(theirs[:], peer)
	if c := lr.next(t); c.m.Index != 300 {
		t.Fatalf("credit %+v", c.m)
	}
}

// TestStampCarriesTermToTheGate: a read's stamp reaches the caller with the
// term the switch put on it; a stamp with no term carries no information.
func TestStampCarriesTermToTheGate(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	const marker = uint64(0xABCD00000077)

	done := make(chan [2]uint64, 1)
	go func() {
		idx, term, err := sc.WaitForReadGate(context.Background(), marker)
		if err != nil {
			t.Errorf("gate errored: %v", err)
		}
		done <- [2]uint64{idx, term}
	}()
	// No term: discarded, the read keeps waiting.
	noTerm := sidechannelMsg{magic: sc.magic, msgType: byte(raftpb.MsgReadIndex),
		to: uint64(sc.id), from: 9, marker: marker, value: 400, bootID: testBootID}.encode()
	sc.handleUdpSidechannelMsg(noTerm[:], nil)
	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, marker, 500), nil)

	select {
	case got := <-done:
		if got[0] != 500 || got[1] != uint64(testTerm) {
			t.Fatalf("read released at %d, term %d; want 500 at %d", got[0], got[1], testTerm)
		}
	case <-time.After(2 * ReadGateDeadlineMillis * time.Millisecond):
		t.Fatal("read never released")
	}
}
