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

package etcdserver

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3"
)

type decodedCommitTrace struct {
	selfID, capacity, total uint64
	resetMono, resetWall    int64
	dumpMono, dumpWall      int64
	peers                   []uint64
	recs                    []commitTraceRec
}

// decodeCommitTrace parses the dump format documented on commitTrace.
func decodeCommitTrace(t *testing.T, b []byte) decodedCommitTrace {
	t.Helper()
	le := binary.LittleEndian
	require.GreaterOrEqual(t, len(b), commitTraceHeaderSize)
	require.Equal(t, commitTraceMagic, string(b[:8]))
	require.Equal(t, uint32(commitTraceVersion), le.Uint32(b[8:]))
	require.Equal(t, uint32(commitTraceRecSize), le.Uint32(b[12:]))
	d := decodedCommitTrace{
		selfID:    le.Uint64(b[16:]),
		capacity:  le.Uint64(b[24:]),
		total:     le.Uint64(b[32:]),
		resetMono: int64(le.Uint64(b[48:])),
		resetWall: int64(le.Uint64(b[56:])),
		dumpMono:  int64(le.Uint64(b[64:])),
		dumpWall:  int64(le.Uint64(b[72:])),
	}
	count := le.Uint64(b[40:])
	npeers := int(le.Uint32(b[80:]))
	off := commitTraceHeaderSize
	for i := 0; i < npeers; i++ {
		d.peers = append(d.peers, le.Uint64(b[off:]))
		off += 8
	}
	require.Len(t, b, off+int(count)*commitTraceRecSize)
	for i := uint64(0); i < count; i++ {
		d.recs = append(d.recs, commitTraceRec{ts: int64(le.Uint64(b[off:])), word: le.Uint64(b[off+8:])})
		off += commitTraceRecSize
	}
	return d
}

func dumpCommitTrace(t *testing.T, tr *commitTrace) decodedCommitTrace {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, tr.dump(&buf))
	return decodeCommitTrace(t, buf.Bytes())
}

func TestCommitTraceRoundTrip(t *testing.T) {
	tr := newCommitTrace(0xaa, 8)
	evs := []raft.CommitTraceEvent{
		{Kind: raft.CommitTraceAppend, Peer: 0xaa, Index: 7},
		{Kind: raft.CommitTraceMatch, Peer: 0xaa, Index: 7},
		{Kind: raft.CommitTraceMatch, Peer: 0xbb, Index: 7},
		{Kind: raft.CommitTraceAcked, Source: raft.CommitTraceSourceSwitchCredit, Peer: 0xcc, Index: 1<<48 - 1},
		{Kind: raft.CommitTraceCommit, Peer: 0xaa, Index: 7},
	}
	for _, ev := range evs {
		tr.record(ev)
	}
	d := dumpCommitTrace(t, tr)
	require.Equal(t, uint64(0xaa), d.selfID)
	require.Equal(t, uint64(8), d.capacity)
	require.Equal(t, uint64(len(evs)), d.total)
	require.Equal(t, []uint64{0xaa, 0xbb, 0xcc}, d.peers)
	require.Len(t, d.recs, len(evs))
	for i, r := range d.recs {
		ev := evs[i]
		require.Equal(t, ev.Index, r.word>>16, "record %d index", i)
		require.Equal(t, ev.Peer, d.peers[(r.word>>8)&0xff], "record %d peer", i)
		require.Equal(t, uint64(ev.Kind), (r.word>>4)&0xf, "record %d kind", i)
		require.Equal(t, uint64(ev.Source), r.word&0xf, "record %d source", i)
		require.GreaterOrEqual(t, r.ts, d.resetMono)
		require.LessOrEqual(t, r.ts, d.dumpMono)
		if i > 0 {
			require.GreaterOrEqual(t, r.ts, d.recs[i-1].ts)
		}
	}
	// Both anchors are one reading each, on the same monotonic origin.
	require.GreaterOrEqual(t, d.dumpWall-d.resetWall, d.dumpMono-d.resetMono-int64(1e9))
}

// TestCommitTraceOverflow: a full ring keeps the newest events, oldest first,
// and the dump's total says how many were overwritten.
func TestCommitTraceOverflow(t *testing.T) {
	tr := newCommitTrace(1, 4)
	for i := uint64(1); i <= 6; i++ {
		tr.record(raft.CommitTraceEvent{Kind: raft.CommitTraceAppend, Peer: 1, Index: i})
	}
	d := dumpCommitTrace(t, tr)
	require.Equal(t, uint64(6), d.total)
	var idx []uint64
	for _, r := range d.recs {
		idx = append(idx, r.word>>16)
	}
	require.Equal(t, []uint64{3, 4, 5, 6}, idx)

	tr.reset()
	d2 := dumpCommitTrace(t, tr)
	require.Zero(t, d2.total)
	require.Empty(t, d2.recs)
	require.Greater(t, d2.resetMono, d.resetMono)
	require.Equal(t, []uint64{1}, d2.peers, "slots survive a reset")
	tr.record(raft.CommitTraceEvent{Kind: raft.CommitTraceCommit, Peer: 1, Index: 9})
	d3 := dumpCommitTrace(t, tr)
	require.Len(t, d3.recs, 1)
	require.Equal(t, uint64(9), d3.recs[0].word>>16)
}

func TestCommitTraceSlotOverflow(t *testing.T) {
	tr := newCommitTrace(1, 512)
	for id := uint64(1); id <= commitTraceSlotOverflow+2; id++ {
		tr.record(raft.CommitTraceEvent{Kind: raft.CommitTraceMatch, Peer: id, Index: id})
	}
	d := dumpCommitTrace(t, tr)
	require.Len(t, d.peers, commitTraceSlotOverflow)
	last := d.recs[len(d.recs)-1]
	require.Equal(t, uint64(commitTraceSlotOverflow), (last.word>>8)&0xff)
}

func TestCommitTraceServerOff(t *testing.T) {
	s := &EtcdServer{}
	require.False(t, s.CommitTraceReset())
	var buf bytes.Buffer
	ok, err := s.CommitTraceDump(&buf)
	require.False(t, ok)
	require.NoError(t, err)
	require.Zero(t, buf.Len())
}
