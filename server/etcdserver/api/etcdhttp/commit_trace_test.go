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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeCommitTracer struct {
	on     bool
	resets int
}

func (f *fakeCommitTracer) CommitTraceReset() bool {
	if f.on {
		f.resets++
	}
	return f.on
}

func (f *fakeCommitTracer) CommitTraceDump(w io.Writer) (bool, error) {
	if !f.on {
		return false, nil
	}
	_, err := w.Write([]byte("TRACE"))
	return true, err
}

func TestHandleCommitTrace(t *testing.T) {
	tests := []struct {
		name       string
		on         bool
		method     string
		remote     string
		wantStatus int
		wantBody   string
		wantResets int
	}{
		{"dump", true, http.MethodGet, "127.0.0.1:4000", http.StatusOK, "TRACE", 0},
		{"dump over IPv6 loopback", true, http.MethodGet, "[::1]:4000", http.StatusOK, "TRACE", 0},
		{"reset", true, http.MethodPost, "127.0.0.1:4000", http.StatusNoContent, "", 1},
		{"remote dump refused", true, http.MethodGet, "10.10.0.2:4000", http.StatusForbidden, "", 0},
		{"remote reset refused", true, http.MethodPost, "10.10.0.2:4000", http.StatusForbidden, "", 0},
		{"other method", true, http.MethodPut, "127.0.0.1:4000", http.StatusMethodNotAllowed, "", 0},
		{"dump when off", false, http.MethodGet, "127.0.0.1:4000", http.StatusNotFound, "", 0},
		{"reset when off", false, http.MethodPost, "127.0.0.1:4000", http.StatusNotFound, "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeCommitTracer{on: tt.on}
			mux := http.NewServeMux()
			HandleCommitTrace(mux, f)
			req := httptest.NewRequest(tt.method, CommitTracePath, nil)
			req.RemoteAddr = tt.remote
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			require.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				require.Equal(t, tt.wantBody, rec.Body.String())
			}
			require.Equal(t, tt.wantResets, f.resets)
		})
	}
}
