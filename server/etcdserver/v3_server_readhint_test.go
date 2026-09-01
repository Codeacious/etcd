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

package etcdserver

import (
	"sync"
	"testing"
)

// TestReadHintHandoffNeverLosesAReader is the safety property of the per-read
// switch hint. Readers run per-request while sendReadIndex runs once per round
// and answers a whole batch, so a reader's stamp reaches raft only if the round
// that notifies its notifier also drained its contribution.
//
// The invariant: for every reader, the round that swapped away the notifier it
// captured observed a hint >= that reader's own. A reader answered by a round
// that saw a lower hint would be served locally against an index below its own
// stamp — exactly the stale read the gate exists to prevent.
func TestReadHintHandoffNeverLosesAReader(t *testing.T) {
	const readers = 400

	s := &EtcdServer{readNotifier: newNotifier()}

	var mu sync.Mutex
	captured := make(map[*notifier][]uint64) // notifier -> hints of its readers
	observed := make(map[*notifier]uint64)   // notifier -> hint its round drained

	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func(i int) {
			defer wg.Done()
			hint := uint64(i + 1)
			nc := s.captureReadNotifier(hint)
			mu.Lock()
			captured[nc] = append(captured[nc], hint)
			mu.Unlock()
		}(i)
	}

	// One round loop, racing the readers exactly as linearizableReadLoop does.
	done := make(chan struct{})
	var loopWG sync.WaitGroup
	loopWG.Add(1)
	go func() {
		defer loopWG.Done()
		for {
			nr, hint := s.swapReadNotifier()
			mu.Lock()
			observed[nr] = hint
			mu.Unlock()
			select {
			case <-done:
				// One final swap so the last readers' notifier is drained too.
				nr, hint := s.swapReadNotifier()
				mu.Lock()
				observed[nr] = hint
				mu.Unlock()
				return
			default:
			}
		}
	}()

	wg.Wait()
	close(done)
	loopWG.Wait()

	mu.Lock()
	defer mu.Unlock()
	for nc, hints := range captured {
		got, ok := observed[nc]
		if !ok {
			t.Fatalf("a captured notifier was never drained by any round (%d readers on it)", len(hints))
		}
		for _, h := range hints {
			if got < h {
				t.Fatalf("round drained hint %d but must cover a reader that required %d", got, h)
			}
		}
	}
}

// TestReadHintZeroContributesNothing: modes without a switch (everything but
// assist, plus leaders) pass 0, which must leave the accumulator alone so the
// round hints 0 — raft's "no extra requirement".
func TestReadHintZeroContributesNothing(t *testing.T) {
	s := &EtcdServer{readNotifier: newNotifier()}

	s.captureReadNotifier(0)
	s.captureReadNotifier(0)
	if _, hint := s.swapReadNotifier(); hint != 0 {
		t.Fatalf("hint = %d, want 0 when no reader needs a switch", hint)
	}

	// A mixed round takes the max: one gated reader forces its requirement on
	// the round even if others contribute nothing.
	s.captureReadNotifier(0)
	s.captureReadNotifier(900)
	s.captureReadNotifier(0)
	if _, hint := s.swapReadNotifier(); hint != 900 {
		t.Fatalf("hint = %d, want 900", hint)
	}
}

// TestReadHintDrainsPerRound: a round's hint must not leak into the next one.
// The accumulator describes the readers a single round answers.
func TestReadHintDrainsPerRound(t *testing.T) {
	s := &EtcdServer{readNotifier: newNotifier()}

	s.captureReadNotifier(1200)
	if _, hint := s.swapReadNotifier(); hint != 1200 {
		t.Fatalf("first round hint = %d, want 1200", hint)
	}
	if _, hint := s.swapReadNotifier(); hint != 0 {
		t.Fatalf("second round hint = %d, want it drained to 0", hint)
	}
}
