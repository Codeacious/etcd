package rafthttp

import (
	"context"
	"math"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/client/pkg/v3/types"
)

// testIncarnation is the batch-marker upper half the SAFE-3 tests run under.
const testIncarnation = uint32(0x5AFE3)

func batchMarker(inc, seq uint32) uint64 { return uint64(inc)<<32 | uint64(seq) }

// newestBatchMarker waits for querySwitchIndexLoop to queue a batch and
// returns the newest batch marker.
func newestBatchMarker(t *testing.T, sc *UdpSidechannel) uint64 {
	t.Helper()
	deadline := time.Now().Add(2 * ReadGateDeadlineMillis * time.Millisecond)
	for time.Now().Before(deadline) {
		sc.readGate.pendingBatchesLock.Lock()
		var newest uint64
		for _, b := range sc.readGate.pendingBatches {
			newest = max(newest, b.localMarker)
		}
		sc.readGate.pendingBatchesLock.Unlock()
		if newest != 0 {
			return newest
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no batch created")
	return 0
}

// TestBatchMarkersCarryIncarnation: every instance draws its own nonzero
// incarnation, and every batch marker it mints carries it.
func TestBatchMarkersCarryIncarnation(t *testing.T) {
	lg := zaptest.NewLogger(t)
	a := NewUdpSidechannel(types.ID(1), "127.0.0.1", 0, 0, nil, lg)
	defer a.Stop()
	b := NewUdpSidechannel(types.ID(1), "127.0.0.1", 0, 0, nil, lg)
	defer b.Stop()

	if a.readGate.batchIncarnation == 0 || b.readGate.batchIncarnation == 0 {
		t.Fatal("zero incarnation")
	}
	// 2^-32 flake odds.
	if a.readGate.batchIncarnation == b.readGate.batchIncarnation {
		t.Fatalf("two instances share incarnation %#x", a.readGate.batchIncarnation)
	}
	for i := 0; i < 3; i++ {
		if m := a.readGate.nextBatchMarker(); uint32(m>>32) != a.readGate.batchIncarnation {
			t.Fatalf("batch marker %#x lacks incarnation %#x", m, a.readGate.batchIncarnation)
		}
	}
}

// TestStaleIncarnationQueryReplyDropped is the SAFE-3 repro: a QuerySwitchIndex
// reply addressed to a previous instance of this node (same id, same listener)
// arrives after a restart. It must release none of the new instance's reads,
// whatever its sequence, since its value predates them. The reply to this
// instance's own query still releases them.
func TestStaleIncarnationQueryReplyDropped(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	sc.readGate.batchIncarnation = testIncarnation
	addRefreshTestPeer(sc, 2)
	go sc.querySwitchIndexLoop()

	done := make(chan uint64, 1)
	go func() {
		v, _, err := sc.WaitForReadGate(context.Background(), 0) // marker-less read
		if err != nil {
			t.Errorf("gate errored: %v", err)
		}
		done <- v
	}()
	newest := newestBatchMarker(t, sc)

	for _, stale := range []uint64{
		5000,                                 // the pre-fix shape: incarnation 0, sequence ahead
		batchMarker(testIncarnation+1, 5000), // another incarnation, sequence ahead
		batchMarker(testIncarnation+1, uint32(newest)), // another incarnation, same sequence
	} {
		sc.handleUdpSidechannelMsg(encodeQueryReply(sc, stale, 7), nil)
	}
	select {
	case v := <-done:
		t.Fatalf("read released at %d by a previous incarnation's reply", v)
	case <-time.After(5 * time.Millisecond):
	}

	sc.handleUdpSidechannelMsg(encodeQueryReply(sc, newest, 1200), nil)
	select {
	case v := <-done:
		if v != 1200 {
			t.Fatalf("released at %d, want this instance's answer 1200 (MaxUint64 means it waited out the gate)", v)
		}
	case <-time.After(2 * ReadGateDeadlineMillis * time.Millisecond):
		t.Fatal("this instance's own reply did not release the read")
	}
}

// TestBatchReleaseAcrossSeqWrap: release is cumulative, so it must order
// batches correctly across the lower half's wrap. A reply to the last batch
// before the wrap releases nothing minted after it; a reply after the wrap
// releases everything before it.
func TestBatchReleaseAcrossSeqWrap(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	sc.readGate.batchIncarnation = testIncarnation
	addRefreshTestPeer(sc, 2)

	seqs := []uint32{math.MaxUint32 - 1, math.MaxUint32, 0, 1}
	chs := make([]chan switchStamp, len(seqs))
	for i, seq := range seqs {
		chs[i] = make(chan switchStamp, 1)
		read := uint64(0xBEEF00 + i)
		sc.readGate.pending.Store(read, chs[i])
		sc.readGate.pendingBatches = append(sc.readGate.pendingBatches, pendingMarkerBatch{
			localMarker: batchMarker(testIncarnation, seq),
			markers:     []uint64{read},
		})
	}
	released := func() (n int) {
		for _, ch := range chs {
			if len(ch) != 0 {
				n++
			}
		}
		return n
	}

	sc.handleUdpSidechannelMsg(encodeQueryReply(sc, batchMarker(testIncarnation, math.MaxUint32), 900), nil)
	if n := released(); n != 2 {
		t.Fatalf("reply before the wrap released %d batches, want the 2 minted up to it", n)
	}
	sc.handleUdpSidechannelMsg(encodeQueryReply(sc, batchMarker(testIncarnation, 1), 1200), nil)
	if n := released(); n != 4 {
		t.Fatalf("reply after the wrap released %d batches in all, want 4", n)
	}
	if n := len(sc.readGate.pendingBatches); n != 0 {
		t.Fatalf("pendingBatches = %d, want all consumed", n)
	}
}
