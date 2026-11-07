// Copyright 2015 The etcd Authors
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

import "github.com/prometheus/client_golang/prometheus"

var (
	activePeers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "active_peers",
			Help:      "The current number of active peer connections.",
		},
		[]string{"Local", "Remote"},
	)

	disconnectedPeers = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "disconnected_peers_total",
			Help:      "The total number of disconnected peers.",
		},
		[]string{"Local", "Remote"},
	)

	sentBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "peer_sent_bytes_total",
			Help:      "The total number of bytes sent to peers.",
		},
		[]string{"To"},
	)

	receivedBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "peer_received_bytes_total",
			Help:      "The total number of bytes received from peers.",
		},
		[]string{"From"},
	)

	sentFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "peer_sent_failures_total",
			Help:      "The total number of send failures from peers.",
		},
		[]string{"To"},
	)

	recvFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "peer_received_failures_total",
			Help:      "The total number of receive failures from peers.",
		},
		[]string{"From"},
	)

	snapshotSend = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_send_success",
			Help:      "Total number of successful snapshot sends",
		},
		[]string{"To"},
	)

	snapshotSendInflights = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_send_inflights_total",
			Help:      "Total number of inflight snapshot sends",
		},
		[]string{"To"},
	)

	snapshotSendFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_send_failures",
			Help:      "Total number of snapshot send failures",
		},
		[]string{"To"},
	)

	snapshotSendSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_send_total_duration_seconds",
			Help:      "Total latency distributions of v3 snapshot sends",

			// lowest bucket start of upper bound 0.1 sec (100 ms) with factor 2
			// highest bucket start of 0.1 sec * 2^9 == 51.2 sec
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		},
		[]string{"To"},
	)

	snapshotReceive = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_receive_success",
			Help:      "Total number of successful snapshot receives",
		},
		[]string{"From"},
	)

	snapshotReceiveInflights = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_receive_inflights_total",
			Help:      "Total number of inflight snapshot receives",
		},
		[]string{"From"},
	)

	snapshotReceiveFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_receive_failures",
			Help:      "Total number of snapshot receive failures",
		},
		[]string{"From"},
	)

	snapshotReceiveSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "snapshot_receive_total_duration_seconds",
			Help:      "Total latency distributions of v3 snapshot receives",

			// lowest bucket start of upper bound 0.1 sec (100 ms) with factor 2
			// highest bucket start of 0.1 sec * 2^9 == 51.2 sec
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		},
		[]string{"From"},
	)

	rttSec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "etcd",
			Subsystem: "network",
			Name:      "peer_round_trip_time_seconds",
			Help:      "Round-Trip-Time histogram between peers",

			// lowest bucket start of upper bound 0.0001 sec (0.1 ms) with factor 2
			// highest bucket start of 0.0001 sec * 2^15 == 3.2768 sec
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
		},
		[]string{"To"},
	)
)

// UDP sidechannel counters (assist mode; EXP-6). A mode that has fallen back
// to a slower path still succeeds, so these are what tell it apart: a write
// committed without a switch credit, a read that waited for a query or gave up
// on the gate. They are read only at cell boundaries.
var (
	sidechannelReplies = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "network",
		Name:      "sidechannel_replies_total",
		Help: "Switch replies to this leader's writes, by outcome: credited (raft was fed a credit), " +
			"stale_boot (from another switch boot; dropped), no_value (the switch holds nothing this boot), " +
			"no_mark (nothing proposed yet, so nothing to credit).",
	}, []string{"result"})

	sidechannelCredits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "network",
		Name:      "sidechannel_credits_total",
		Help: "Switch credits fed to raft, by holder: path (the follower whose path carried the write) " +
			"or other_holder (another lease holder behind the same switch, credit-all).",
	}, []string{"holder"})

	sidechannelNacks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "network",
		Name:      "sidechannel_nacks_total",
		Help: "Switch NACKs, by outcome: adopted (a new boot ID), current (re-seed under the current boot), " +
			"stale (answers an older belief; ignored), invalid (boot ID 0).",
	}, []string{"result"})

	sidechannelSendDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "network",
		Name:      "sidechannel_send_dropped_total",
		Help:      "Sidechannel messages dropped because the send queue was full.",
	})

	sidechannelZeroAnswers = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "network",
		Name:      "sidechannel_zero_answers_total",
		Help:      "Switch stamps or query answers with index or term 0, discarded as unusable.",
	})

	sidechannelGateWaits = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "network",
		Name:      "sidechannel_read_gate_waits_total",
		Help: "Follower read-gate waits, by marker (client: the client's gate packet; server: minted, " +
			"answered by a batched switch query) and outcome (ready: stamp already there; waited: stamp " +
			"arrived during the wait; deadline: gave up, so the read is forwarded to the leader).",
	}, []string{"marker", "outcome"})
)

// Label children resolved once, so the hot paths don't hash labels.
var (
	repliesCredited  = sidechannelReplies.WithLabelValues("credited")
	repliesStaleBoot = sidechannelReplies.WithLabelValues("stale_boot")
	repliesNoValue   = sidechannelReplies.WithLabelValues("no_value")
	repliesNoMark    = sidechannelReplies.WithLabelValues("no_mark")

	creditsPath        = sidechannelCredits.WithLabelValues("path")
	creditsOtherHolder = sidechannelCredits.WithLabelValues("other_holder")

	nacksAdopted = sidechannelNacks.WithLabelValues("adopted")
	nacksCurrent = sidechannelNacks.WithLabelValues("current")
	nacksStale   = sidechannelNacks.WithLabelValues("stale")
	nacksInvalid = sidechannelNacks.WithLabelValues("invalid")

	gateWaitsClient = newGateWaits("client")
	gateWaitsServer = newGateWaits("server")
)

type gateWaits struct{ ready, waited, deadline prometheus.Counter }

func newGateWaits(marker string) gateWaits {
	return gateWaits{
		ready:    sidechannelGateWaits.WithLabelValues(marker, "ready"),
		waited:   sidechannelGateWaits.WithLabelValues(marker, "waited"),
		deadline: sidechannelGateWaits.WithLabelValues(marker, "deadline"),
	}
}

func init() {
	prometheus.MustRegister(sidechannelReplies)
	prometheus.MustRegister(sidechannelCredits)
	prometheus.MustRegister(sidechannelNacks)
	prometheus.MustRegister(sidechannelSendDropped)
	prometheus.MustRegister(sidechannelZeroAnswers)
	prometheus.MustRegister(sidechannelGateWaits)
}

func init() {
	prometheus.MustRegister(activePeers)
	prometheus.MustRegister(disconnectedPeers)
	prometheus.MustRegister(sentBytes)
	prometheus.MustRegister(receivedBytes)
	prometheus.MustRegister(sentFailures)
	prometheus.MustRegister(recvFailures)

	prometheus.MustRegister(snapshotSend)
	prometheus.MustRegister(snapshotSendInflights)
	prometheus.MustRegister(snapshotSendFailures)
	prometheus.MustRegister(snapshotSendSeconds)
	prometheus.MustRegister(snapshotReceive)
	prometheus.MustRegister(snapshotReceiveInflights)
	prometheus.MustRegister(snapshotReceiveFailures)
	prometheus.MustRegister(snapshotReceiveSeconds)

	prometheus.MustRegister(rttSec)
}
