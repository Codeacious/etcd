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

package sidechannel

import (
	"encoding/binary"
	"testing"
)

// TestEncodeReadGateMsgLayout pins all 35 bytes. This package is one of only
// two independent encodings of the read-gate layout — the other is
// tack/tack-switch-agen.p4 — so a silent drift here desynchronizes the switch.
func TestEncodeReadGateMsgLayout(t *testing.T) {
	const (
		magic  = uint16(0xFEED)
		fromID = uint64(0x1122334455667788)
		toID   = uint64(0x99AABBCCDDEEFF00)
		marker = uint64(0x0102030405060708)
	)

	msg := EncodeReadGateMsg(magic, fromID, toID, marker)

	if got := binary.BigEndian.Uint16(msg[0:2]); got != magic {
		t.Errorf("magic = %#x, want %#x", got, magic)
	}
	if got := msg[2]; got != MsgTypeReadIndex {
		t.Errorf("type = %d, want MsgTypeReadIndex (%d)", got, MsgTypeReadIndex)
	}
	if got := binary.BigEndian.Uint64(msg[3:11]); got != toID {
		t.Errorf("to = %#x, want %#x", got, toID)
	}
	if got := binary.BigEndian.Uint64(msg[11:19]); got != fromID {
		t.Errorf("from = %#x, want %#x", got, fromID)
	}
	if got := binary.BigEndian.Uint64(msg[19:27]); got != marker {
		t.Errorf("marker = %#x, want %#x", got, marker)
	}
}

// TestEncodeReadGateMsgValueIsZero is the point of the 2026-08-20 sentinel
// removal. The client mints the value field as 0 and the switch overwrites it
// in flight; on the receiving side 0 means "no usable saved index" whether it
// got there because no switch stamped the packet or because the switch holds
// nothing. Emitting ^uint64(0) here again would put a value on the wire that
// the server now treats as a real, impossibly high log index — poisoning the
// receiver's monotone switch-index cache for the life of the process.
func TestEncodeReadGateMsgValueIsZero(t *testing.T) {
	msg := EncodeReadGateMsg(0xFEED, 1, 2, 3)
	if got := binary.BigEndian.Uint64(msg[27:35]); got != 0 {
		t.Fatalf("value = %#x, want 0", got)
	}
}

func TestNewMarkerSeedUpper32Nonzero(t *testing.T) {
	// Client markers must keep the nonzero-upper-32 invariant: server-minted
	// markers use zero-upper-32, and the two namespaces must never collide.
	for i := 0; i < 1000; i++ {
		if seed := NewMarkerSeed(); seed>>32 == 0 {
			t.Fatalf("NewMarkerSeed returned %#x with a zero upper 32 bits", seed)
		}
	}
}
