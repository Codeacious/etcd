// Package sidechannel holds the wire format and gRPC-metadata helpers for the
// P4 switch UDP read-gate protocol. It is intentionally small and dependency-
// light so client-side code (including out-of-tree experiment drivers) can
// import it without pulling server-side packages.
//
// The 35-byte wire format has exactly one other independent encoding: the P4
// program (tack/tack-switch-agen.p4). Layout changes must land in both. The
// rafthttp listener/balancer and the gcp client-worker consume this package
// rather than re-implementing the layout.
package sidechannel

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"strconv"

	"google.golang.org/grpc/metadata"
)

const (
	// ReadGateMarkerMetaKey is the gRPC metadata key carrying the per-request
	// read-gate marker for P4 switch-gated linearizable reads.
	ReadGateMarkerMetaKey = "x-etcd-read-gate-marker"

	// MsgTypeReadIndex is the raw byte the P4 program matches at offset 2 of
	// the sidechannel payload to identify a read-gate request. It must equal
	// raftpb.MessageType_MsgReadIndex; hard-coded here so this package stays
	// free of a go.etcd.io/raft/v3 dependency.
	MsgTypeReadIndex byte = 15

	// DefaultMagic is the canonical 2-byte wire magic at offset [0:2] of every
	// read-gate payload. The etcd UDP listener, the client drivers, and the P4
	// program must all agree on it; keep the three in sync.
	DefaultMagic uint16 = 0xFEED

	// DefaultPort is the canonical UDP port the switch listens on for read-gate
	// packets. Same cross-component sync requirement as DefaultMagic.
	DefaultPort = 7700
)

// EncodeReadGateMsg packs a 35-byte read-gate UDP payload.
//
// Layout (big-endian):
//
//	[0:2]   magic
//	[2]     message type (MsgTypeReadIndex)
//	[3:11]  to   (peer ID)
//	[11:19] from (peer ID)
//	[19:27] marker
//	[27:35] value (0 — the switch overwrites it in flight with its saved index)
func EncodeReadGateMsg(magic uint16, fromID, toID, marker uint64) [35]byte {
	var msg [35]byte
	binary.BigEndian.PutUint16(msg[0:2], magic)
	msg[2] = MsgTypeReadIndex
	binary.BigEndian.PutUint64(msg[3:11], toID)
	binary.BigEndian.PutUint64(msg[11:19], fromID)
	binary.BigEndian.PutUint64(msg[19:27], marker)
	binary.BigEndian.PutUint64(msg[27:35], 0)
	return msg
}

// NewMarkerSeed returns a mask for the upper 32 bits needed for client read-gate markers.
// The seed randomizes the upper 32 bits but forces them nonzero.
// Upper zero markers are reserved for leader-minted markers during fallback switch queries.
func NewMarkerSeed() uint64 {
	upper := rand.Uint32()
	if upper == 0 {
		upper = 1
	}
	return uint64(upper)<<32 | 1
}

// AttachReadGateMarker attaches the read-gate marker to the outgoing gRPC
// context metadata. Call for any outgoing linearizable read that the switch
// should gate.
func AttachReadGateMarker(ctx context.Context, marker uint64) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		ReadGateMarkerMetaKey, strconv.FormatUint(marker, 10))
}

// ExtractReadGateMarker parses the read-gate marker from gRPC metadata.
// Use metadata.FromIncomingContext on the server, metadata.FromOutgoingContext
// on the client before calling. Returns 0 if no marker is present.
func ExtractReadGateMarker(md metadata.MD) uint64 {
	vals := md.Get(ReadGateMarkerMetaKey)
	if len(vals) == 0 {
		return 0
	}
	v, _ := strconv.ParseUint(vals[0], 10, 64)
	return v
}
