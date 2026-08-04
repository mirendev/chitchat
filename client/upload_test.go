package chitchat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// uploadGateway is a fake gateway that speaks the request_upload protocol: it
// mints an upload subject, signals ready, collects the streamed body, and then
// answers with a chunked reply and a terminal.
type uploadGateway struct {
	mu        sync.Mutex
	body      bytes.Buffer // what the client streamed to us
	reply     []byte       // what we answer with
	failWith  string       // non-empty: terminate with stream_error instead
	skipReady bool         // never send upload_ready, to test the wait
}

func (g *uploadGateway) received() []byte {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]byte(nil), g.body.Bytes()...)
}

func (g *uploadGateway) handler() http.HandlerFunc {
	up := websocket.Upgrader{}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		var streamID, uploadSubject string
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct {
				Action   string `json:"action"`
				Subject  string `json:"subject"`
				Data     string `json:"data"`
				StreamID string `json:"stream_id"`
				Error    string `json:"error"`
			}
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			switch m.Action {
			case "request_upload":
				streamID = m.StreamID
				uploadSubject = "_UPLOAD." + streamID
				_ = conn.WriteJSON(map[string]any{
					"type": "upload_started", "stream_id": streamID, "upload_subject": uploadSubject,
				})
				if !g.skipReady {
					_ = conn.WriteJSON(map[string]any{"type": "upload_ready", "stream_id": streamID})
				}
			case "stream_send":
				raw, _ := base64.StdEncoding.DecodeString(m.Data)
				g.mu.Lock()
				g.body.Write(raw)
				g.mu.Unlock()
			case "stream_end":
				if m.Subject != uploadSubject {
					continue
				}
				if g.failWith != "" {
					_ = conn.WriteJSON(map[string]any{
						"type": "stream_error", "stream_id": streamID, "error": g.failWith,
					})
					continue
				}
				// Answer in two chunks, to prove reassembly.
				half := len(g.reply) / 2
				for _, part := range [][]byte{g.reply[:half], g.reply[half:]} {
					_ = conn.WriteJSON(map[string]any{
						"type": "stream_chunk", "stream_id": streamID,
						"data": base64.StdEncoding.EncodeToString(part),
					})
				}
				_ = conn.WriteJSON(map[string]any{"type": "stream_end", "stream_id": streamID})
			}
		}
	}
}

func TestUploadRoundTrip(t *testing.T) {
	gw := &uploadGateway{reply: []byte(`{"ok":true,"sha":"abc123"}`)}
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	c, err := New(srv.URL, StaticToken("tok"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	// Larger than one chunk, so the pipelining path is exercised.
	body := bytes.Repeat([]byte("pack-data-"), streamChunkSize/5)

	reqCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got, err := c.Upload(reqCtx, "vcs.pack.receive", []byte(`{"repo":"x"}`), bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if string(got) != `{"ok":true,"sha":"abc123"}` {
		t.Errorf("reply = %s", got)
	}
	if recv := gw.received(); !bytes.Equal(recv, body) {
		t.Errorf("gateway received %d bytes, sent %d", len(recv), len(body))
	}

	// The in-flight registry must not leak once Upload returns.
	c.mu.Lock()
	n := len(c.uploads)
	c.mu.Unlock()
	if n != 0 {
		t.Errorf("upload registry still holds %d entries", n)
	}
}

func TestUploadSurfacesResponderError(t *testing.T) {
	gw := &uploadGateway{failWith: "pack rejected: bad object"}
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.Upload(reqCtx, "vcs.pack.receive", nil, strings.NewReader("body"))
	if err == nil {
		t.Fatal("expected the responder's error")
	}
	if !strings.Contains(err.Error(), "pack rejected") {
		t.Errorf("err = %v, want the responder's message", err)
	}
}

// TestUploadStopsWaitingWhenReadyNeverComes pins that a responder which never
// subscribes can't hang the caller past its deadline.
func TestUploadStopsWaitingWhenReadyNeverComes(t *testing.T) {
	gw := &uploadGateway{skipReady: true, reply: []byte("x")}
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	reqCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Upload(reqCtx, "vcs.pack.receive", nil, strings.NewReader("body"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %v; should give up on the caller's deadline", elapsed)
	}
}

// TestFailAllUploadsUnblocksInFlight covers the disconnect path. The
// gateway-minted upload subject dies with the connection, so a blocked Upload
// has to be told rather than left waiting out its context for a reply that can
// never arrive.
func TestFailAllUploadsUnblocksInFlight(t *testing.T) {
	c := offlineClient(t)
	u := &uploadState{
		started: make(chan string, 1),
		ready:   make(chan struct{}, 1),
		chunks:  make(chan []byte, 8),
		done:    make(chan error, 1),
	}
	c.mu.Lock()
	c.uploads["cs_test"] = u
	c.mu.Unlock()

	c.failAllUploads(ErrStreamDisconnected)

	select {
	case err := <-u.done:
		if !errors.Is(err, ErrStreamDisconnected) {
			t.Errorf("terminal = %v, want ErrStreamDisconnected", err)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight upload was not unblocked by the disconnect")
	}

	c.failAllUploads(ErrStreamDisconnected) // must not panic on a second pass
}

func TestOrStreamErr(t *testing.T) {
	real := errors.New("boom")
	if got := orStreamErr(real, "fallback"); got != real {
		t.Errorf("got %v, want the real error", got)
	}
	if got := orStreamErr(nil, "aborted early"); !strings.Contains(got.Error(), "aborted early") {
		t.Errorf("got %v, want the fallback", got)
	}
}
