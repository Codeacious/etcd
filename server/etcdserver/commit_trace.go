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
	"io"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
)

// commitTrace is the --experimental-commit-trace-events recorder: a fixed
// ring of the leader's raft replication events (raft Config.CommitTrace), for
// offline commit-latency analysis. It is allocated and touched once at
// startup, so recording never allocates or faults a page in. When full, it
// overwrites its oldest events; the dump says how many were lost.
//
// Only raft's goroutine records. Reset and dump take the same lock and are
// meant for outside a measurement window: a dump holds it while it copies the
// ring.
//
// Dump format, little-endian:
//
//	off  size  field
//	0    8     magic "TCKTRC01"
//	8    4     version (1)
//	12   4     record size (16)
//	16   8     this member's raft ID
//	24   8     capacity, in events
//	32   8     events recorded since the last reset (minus count = overwritten)
//	40   8     count: records that follow
//	48   16    reset anchor: monotonic ns, wall-clock Unix ns (one reading)
//	64   16    dump anchor: the same, taken while the dump holds the lock
//	80   4     npeers
//	84   4     reserved (0)
//	88   8*n   peer raft IDs, by slot
//	...  16*c  records, oldest first: int64 monotonic ns, uint64 word
//
// Every timestamp is ns since one monotonic origin, so (anchor wall - anchor
// mono) maps them onto this VM's wall clock. word = index<<16 | slot<<8 |
// kind<<4 | source, with kind and source the raft.CommitTrace* values. The
// index keeps its low 48 bits; slot 255 means more than 255 peers.
type commitTrace struct {
	selfID uint64
	origin time.Time

	mu      sync.Mutex
	recs    []commitTraceRec
	next    int    // ring position of the next record
	total   uint64 // events recorded since the last reset
	peers   [commitTraceSlotOverflow]uint64
	npeers  int
	resetAt commitTraceAnchor
}

type commitTraceRec struct {
	ts   int64
	word uint64
}

type commitTraceAnchor struct{ mono, wall int64 }

const (
	commitTraceMagic        = "TCKTRC01"
	commitTraceVersion      = 1
	commitTraceRecSize      = 16
	commitTraceHeaderSize   = 88
	commitTraceSlotOverflow = 255
)

func newCommitTrace(selfID uint64, events int) *commitTrace {
	t := &commitTrace{selfID: selfID, origin: time.Now(), recs: make([]commitTraceRec, events)}
	// Fault every page in now rather than on the raft goroutine.
	for i := 0; i < len(t.recs); i += 4096 / commitTraceRecSize {
		t.recs[i] = commitTraceRec{}
	}
	t.resetAt = t.anchor()
	return t
}

func (t *commitTrace) anchor() commitTraceAnchor {
	now := time.Now()
	return commitTraceAnchor{mono: int64(now.Sub(t.origin)), wall: now.UnixNano()}
}

// record is the raft Config.CommitTrace hook.
func (t *commitTrace) record(ev raft.CommitTraceEvent) {
	ts := int64(time.Since(t.origin))
	t.mu.Lock()
	t.recs[t.next] = commitTraceRec{
		ts:   ts,
		word: ev.Index<<16 | uint64(t.slotLocked(ev.Peer))<<8 | uint64(ev.Kind&0xf)<<4 | uint64(ev.Source&0xf),
	}
	if t.next++; t.next == len(t.recs) {
		t.next = 0
	}
	t.total++
	t.mu.Unlock()
}

func (t *commitTrace) slotLocked(id uint64) int {
	for i := 0; i < t.npeers; i++ {
		if t.peers[i] == id {
			return i
		}
	}
	if t.npeers == commitTraceSlotOverflow {
		return commitTraceSlotOverflow
	}
	t.peers[t.npeers] = id
	t.npeers++
	return t.npeers - 1
}

// reset empties the ring. Peer slots are kept.
func (t *commitTrace) reset() {
	t.mu.Lock()
	t.next, t.total = 0, 0
	t.resetAt = t.anchor()
	t.mu.Unlock()
}

// dump writes the ring to w in the format above.
func (t *commitTrace) dump(w io.Writer) error {
	t.mu.Lock()
	count, start := t.total, 0
	if count > uint64(len(t.recs)) {
		count, start = uint64(len(t.recs)), t.next
	}
	buf := bytes.NewBuffer(make([]byte, 0, commitTraceHeaderSize+8*t.npeers+commitTraceRecSize*int(count)))
	le := binary.LittleEndian
	at := t.anchor()
	buf.WriteString(commitTraceMagic)
	buf.Write(le.AppendUint32(nil, commitTraceVersion))
	buf.Write(le.AppendUint32(nil, commitTraceRecSize))
	for _, v := range []uint64{t.selfID, uint64(len(t.recs)), t.total, count,
		uint64(t.resetAt.mono), uint64(t.resetAt.wall), uint64(at.mono), uint64(at.wall)} {
		buf.Write(le.AppendUint64(nil, v))
	}
	buf.Write(le.AppendUint32(nil, uint32(t.npeers)))
	buf.Write(le.AppendUint32(nil, 0))
	for _, id := range t.peers[:t.npeers] {
		buf.Write(le.AppendUint64(nil, id))
	}
	var rec [commitTraceRecSize]byte
	for i := uint64(0); i < count; i++ {
		r := t.recs[(start+int(i))%len(t.recs)]
		le.PutUint64(rec[:8], uint64(r.ts))
		le.PutUint64(rec[8:], r.word)
		buf.Write(rec[:])
	}
	t.mu.Unlock()
	_, err := w.Write(buf.Bytes())
	return err
}

// CommitTraceReset empties the --experimental-commit-trace-events ring. It
// reports false when tracing is off.
func (s *EtcdServer) CommitTraceReset() bool {
	if s.commitTrace == nil {
		return false
	}
	s.commitTrace.reset()
	return true
}

// CommitTraceDump writes the --experimental-commit-trace-events ring to w (the
// format is in commit_trace.go). It reports false, writing nothing, when
// tracing is off.
func (s *EtcdServer) CommitTraceDump(w io.Writer) (bool, error) {
	if s.commitTrace == nil {
		return false, nil
	}
	return true, s.commitTrace.dump(w)
}
