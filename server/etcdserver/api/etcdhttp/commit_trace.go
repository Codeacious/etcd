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

package etcdhttp

import (
	"io"
	"net"
	"net/http"
)

// CommitTracePath serves the --experimental-commit-trace-events ring: GET
// dumps it, POST empties it. Loopback clients only.
const CommitTracePath = "/debug/tack/commit-trace"

// CommitTracer is the server side of CommitTracePath (*etcdserver.EtcdServer).
type CommitTracer interface {
	CommitTraceReset() bool
	CommitTraceDump(w io.Writer) (bool, error)
}

func HandleCommitTrace(mux *http.ServeMux, t CommitTracer) {
	mux.HandleFunc(CommitTracePath, func(w http.ResponseWriter, r *http.Request) {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err != nil || !isLoopback(host) {
			http.Error(w, "commit trace: loopback clients only", http.StatusForbidden)
			return
		}
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/octet-stream")
			if ok, _ := t.CommitTraceDump(w); !ok {
				http.Error(w, "commit trace is off", http.StatusNotFound)
			}
		case http.MethodPost:
			if !t.CommitTraceReset() {
				http.Error(w, "commit trace is off", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})
}

func isLoopback(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
