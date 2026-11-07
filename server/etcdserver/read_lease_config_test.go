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
	"testing"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/raft/v3"
)

// TestRaftConfigMaxReadLeases checks --experimental-max-read-leases reaches
// raft: 0 keeps the default cap, a positive value replaces it, and modes that
// grant no follower leases set no cap at all.
func TestRaftConfigMaxReadLeases(t *testing.T) {
	tests := []struct {
		mode string
		flag int
		want int
	}{
		{"grant-leases", 0, DefaultMaxReadLeases},
		{"grant-leases", 30, 30},
		{"grant-leases", 1, 1},
		{"lease-based", 30, 0},
		{"safe", 30, 0},
	}
	for _, tt := range tests {
		cfg := config.ServerConfig{
			Logger:                    zaptest.NewLogger(t),
			ReadOnlyMode:              tt.mode,
			ElectionTicks:             10,
			ExperimentalMaxReadLeases: tt.flag,
		}
		c := raftConfig(cfg, 1, raft.NewMemoryStorage())
		if c.MaxNumReadLeases != tt.want {
			t.Errorf("mode %s, flag %d: MaxNumReadLeases = %d, want %d", tt.mode, tt.flag, c.MaxNumReadLeases, tt.want)
		}
	}
}
