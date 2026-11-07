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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"go.etcd.io/raft/v3/raftpb"
)

// counterDelta returns how much c rose while f ran. The counters are
// process-wide, so tests compare deltas.
func counterDelta(c prometheus.Counter, f func()) float64 {
	before := testutil.ToFloat64(c)
	f()
	return testutil.ToFloat64(c) - before
}

func wantDelta(t *testing.T, what string, c prometheus.Counter, want float64, f func()) {
	t.Helper()
	if got := counterDelta(c, f); got != want {
		t.Errorf("%s rose by %v, want %v", what, got, want)
	}
}

// TestSidechannelReplyAndCreditCounters (EXP-6): each reply outcome is counted
// once, and each credit by whom it went to.
func TestSidechannelReplyAndCreditCounters(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	lr := newRecRaft()
	sc.raft = lr
	a := addRefreshTestPeer(sc, 2)
	addRefreshTestPeer(sc, 3)
	sc.highWater = 1000
	sc.SetLeaseHolders([]uint64{2, 3})

	wantDelta(t, "credited replies", repliesCredited, 1, func() {
		wantDelta(t, "path credits", creditsPath, 1, func() {
			wantDelta(t, "other-holder credits", creditsOtherHolder, 1, func() {
				sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 300), a)
				lr.next(t)
				lr.next(t)
			})
		})
	})

	late := sidechannelMsg{magic: sc.magic, msgType: byte(raftpb.MsgAskAckIndexResp),
		to: uint64(sc.id), from: 2, value: 900, bootID: testBootID + 1, term: testTerm}.encode()
	wantDelta(t, "stale-boot replies", repliesStaleBoot, 1, func() {
		sc.handleUdpSidechannelMsg(late[:], a)
	})
	wantDelta(t, "no-value replies", repliesNoValue, 1, func() {
		sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 0), a)
	})
	sc.highWater = 0
	wantDelta(t, "no-mark replies", repliesNoMark, 1, func() {
		sc.handleUdpSidechannelMsg(encodeAskAckIndexResp(sc, 2, 300), a)
	})
	lr.none(t, 20*time.Millisecond)
}

// TestSidechannelNackCounters (EXP-6): adoption, a re-seed under the current
// boot, a stale NACK and a zero boot ID are told apart.
func TestSidechannelNackCounters(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	peer := addRefreshTestPeer(sc, 2)
	sc.highWater, sc.leaderTerm = 700, 6
	const newBoot = uint32(0xB0072)
	nack := func(marker uint64, boot uint32) []byte {
		b := sidechannelMsg{magic: sc.magic, msgType: msgSwitchNack, to: uint64(sc.id), from: 2,
			marker: marker, bootID: boot}.encode()
		return b[:]
	}
	wantDelta(t, "adopted NACKs", nacksAdopted, 1, func() {
		sc.handleUdpSidechannelMsg(nack(uint64(testBootID), newBoot), peer)
	})
	wantDelta(t, "current NACKs", nacksCurrent, 1, func() {
		sc.handleUdpSidechannelMsg(nack(uint64(testBootID), newBoot), peer)
	})
	wantDelta(t, "stale NACKs", nacksStale, 1, func() {
		sc.handleUdpSidechannelMsg(nack(0, 0xDEAD1), peer)
	})
	wantDelta(t, "invalid NACKs", nacksInvalid, 1, func() {
		sc.handleUdpSidechannelMsg(nack(uint64(newBoot), 0), peer)
	})
}

// TestSidechannelGateWaitCounters (EXP-6): a client stamp already there is
// "ready"; an unmarked read that hears nothing gives up at the deadline.
func TestSidechannelGateWaitCounters(t *testing.T) {
	sc := newRefreshTestSidechannel(t)
	const clientMarker = uint64(0xABCD00000007)
	sc.handleUdpSidechannelMsg(encodeReadIndexReflection(sc, clientMarker, 700), nil)
	wantDelta(t, "client ready waits", gateWaitsClient.ready, 1, func() {
		if _, _, err := sc.WaitForReadGate(context.Background(), clientMarker); err != nil {
			t.Fatal(err)
		}
	})

	addRefreshTestPeer(sc, 2)
	wantDelta(t, "server deadline waits", gateWaitsServer.deadline, 1, func() {
		if _, _, err := sc.WaitForReadGate(context.Background(), 0); err != nil {
			t.Fatal(err)
		}
	})
}
