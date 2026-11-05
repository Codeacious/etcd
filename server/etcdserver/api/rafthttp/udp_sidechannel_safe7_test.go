package rafthttp

import (
	"context"
	"math"
	"testing"
	"time"
)

// waitPendingMarker waits for exactly one read to be waiting in the gate and
// returns its marker.
func waitPendingMarker(t *testing.T, sc *UdpSidechannel) uint64 {
	t.Helper()
	deadline := time.Now().Add(ReadGateDeadlineMillis * time.Millisecond / 2)
	for time.Now().Before(deadline) {
		var markers []uint64
		sc.readGate.pending.Range(func(k, _ any) bool {
			markers = append(markers, k.(uint64))
			return true
		})
		if len(markers) == 1 {
			return markers[0]
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no read waiting in the gate")
	return 0
}

// TestServerNamespaceReflectionCannotResolveMintedRead is the SAFE-7 aliasing
// repro: a reflected client MsgReadIndex whose marker has a zero upper half
// equals a marker this node mints for a marker-less read. Its stamp was taken
// before that read began (here, before it was even invoked, or while it waits),
// so it must resolve nothing; with no query loop running, the read gives up
// and forwards.
func TestServerNamespaceReflectionCannotResolveMintedRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before bool // reflection lands before the read is invoked (in resolved), or while it waits (in pending)
	}{
		{"before", true},
		{"during", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc := newRefreshTestSidechannel(t)
			minted := sc.readGate.markerSeq + 1 // the next marker-less read's marker

			if tc.before {
				sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, minted, 100), nil)
				if n := sc.readGate.resolved.len(); n != 0 {
					t.Fatalf("resolved holds %d entries from a server-namespace marker, want 0", n)
				}
			}
			done := make(chan uint64, 1)
			go func() {
				v, _, err := sc.WaitForReadGate(context.Background(), 0)
				if err != nil {
					t.Errorf("gate errored: %v", err)
				}
				done <- v
			}()
			if !tc.before {
				if got := waitPendingMarker(t, sc); got != minted {
					t.Fatalf("read minted %#x, want %#x", got, minted)
				}
				sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, minted, 100), nil)
			}

			select {
			case v := <-done:
				if v != math.MaxUint64 {
					t.Fatalf("marker-less read released at %d by a server-namespace reflection, want MaxUint64 (forward)", v)
				}
			case <-time.After(4 * ReadGateDeadlineMillis * time.Millisecond):
				t.Fatal("read neither released nor gave up")
			}
		})
	}
}

// TestServerNamespaceClientMarkerIsMintedOver: a client marker with a zero
// upper half is treated as no marker. The read waits under a freshly minted
// server marker, not the client's, so the client's own reflection (dropped
// anyway) could not name it either.
func TestServerNamespaceClientMarkerIsMintedOver(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	const clientMarker = uint64(0x42)

	go sc.WaitForReadGate(context.Background(), clientMarker) // gives up at the deadline
	got := waitPendingMarker(t, sc)
	if got == clientMarker || got == 0 || isClientMarker(got) {
		t.Fatalf("read waits under %#x, want a fresh server-namespace marker", got)
	}
}

// TestClientNamespaceReflectionStillResolves: the namespace check passes real
// client markers through.
func TestClientNamespaceReflectionStillResolves(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	const clientMarker = uint64(0xABCD00000007)

	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, clientMarker, 700), nil)
	v, _, err := sc.WaitForReadGate(context.Background(), clientMarker)
	if err != nil || v != 700 {
		t.Fatalf("got %d, %v; want 700", v, err)
	}
}

// TestReadMarkerWrapsWithinServerNamespace: server read markers wrap within the
// lower 32 bits and skip 0, never reaching the client namespace.
func TestReadMarkerWrapsWithinServerNamespace(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	sc.readGate.markerSeq = math.MaxUint32 - 1

	want := []uint64{math.MaxUint32, 1, 2}
	for i, w := range want {
		if got := sc.readGate.nextReadMarker(); got != w {
			t.Fatalf("mint %d = %#x, want %#x", i, got, w)
		}
	}
}

// TestSameMarkerConcurrentReadsFailClosed ports the review's
// TestAuditMarkerCollisionClobbersPending. Two concurrent reads with one client
// marker: pending.Store keeps only the second waiter, so one stamp releases one
// read and the other forwards. Collisions are not detected (SAFE-7 assumes
// random 32-bit client namespaces are unique); this pins down that the
// concurrent case at least fails closed.
func TestSameMarkerConcurrentReadsFailClosed(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	const marker = uint64(0xAB00000001)

	results := make(chan uint64, 2)
	read := func() {
		v, _, err := sc.WaitForReadGate(context.Background(), marker)
		if err != nil {
			t.Errorf("gate errored: %v", err)
		}
		results <- v
	}
	go read()
	waitPendingMarker(t, sc)
	go read()
	time.Sleep(2 * time.Millisecond)
	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, marker, 500), nil)

	got := map[uint64]int{}
	for i := 0; i < 2; i++ {
		select {
		case v := <-results:
			got[v]++
		case <-time.After(4 * ReadGateDeadlineMillis * time.Millisecond):
			t.Fatal("read neither released nor gave up")
		}
	}
	if got[500] != 1 || got[math.MaxUint64] != 1 {
		t.Fatalf("results %v, want one release at 500 and one forward", got)
	}
}
