package server

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

// delayedNATSConn speaks just enough of the NATS protocol for core subscribe
// tests. Each PING gets its own delayed PONG, which models network RTT: several
// concurrent flushes all finish after one delay, while serial flushes pay it
// once per subscription.
func delayedNATSConn(t *testing.T, delay time.Duration) (*nats.Conn, *atomic.Int64) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	serverDone := make(chan struct{})
	var outstanding atomic.Int64
	var maxOutstanding atomic.Int64
	go func() {
		defer close(serverDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(conn, "INFO {\"server_id\":\"test\",\"server_name\":\"test\",\"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":1048576}\r\n")

		var writeMu sync.Mutex
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			if scanner.Text() != "PING" {
				continue
			}
			current := outstanding.Add(1)
			for previous := maxOutstanding.Load(); current > previous && !maxOutstanding.CompareAndSwap(previous, current); previous = maxOutstanding.Load() {
			}
			go func() {
				time.Sleep(delay)
				writeMu.Lock()
				_, _ = io.WriteString(conn, "PONG\r\n")
				writeMu.Unlock()
				outstanding.Add(-1)
			}()
		}
	}()

	nc, err := nats.Connect("nats://"+ln.Addr().String(), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		nc.Close()
		<-serverDone
	})
	// Connect itself performs one flush. Start the test's high-water mark after
	// that handshake so it only measures subscription flushes.
	maxOutstanding.Store(0)
	return nc, &maxOutstanding
}

func allowAll(next http.Handler) http.Handler { return next }

// TestConcurrentCoreSubscribeFlushes is the MIR-1712 regression. The client
// sends its whole replay before waiting for acknowledgements, so the gateway
// should pipeline those independent NATS round trips. A later action remains
// ordered behind the complete subscription batch.
func TestConcurrentCoreSubscribeFlushes(t *testing.T) {
	const count = 12
	delay := 75 * time.Millisecond
	nc, maxOutstanding := delayedNATSConn(t, delay)
	s := New(nc, nil, allowAll, slog.New(slog.NewTextHandler(io.Discard, nil)))
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := range count {
		if err := conn.WriteJSON(map[string]any{
			"action":  "subscribe",
			"subject": fmt.Sprintf("mir1712.%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.WriteJSON(map[string]any{"action": "whoami"}); err != nil {
		t.Fatal(err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	acknowledged := 0
	for {
		var frame wsOutbound
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatal(err)
		}
		switch frame.Type {
		case "subscribed":
			acknowledged++
		case "error":
			t.Fatalf("subscribe failed: subject=%q error=%q", frame.Subject, frame.Error)
		case "whoami":
			if acknowledged != count {
				t.Fatalf("whoami overtook subscriptions: got %d/%d acknowledgements", acknowledged, count)
			}
			if got := maxOutstanding.Load(); got < count/2 {
				t.Fatalf("only %d/%d subscription flushes overlapped; want the replay pipelined", got, count)
			}
			return
		}
	}
}

func TestConcurrentDuplicateSubscribePreservesResponseOrder(t *testing.T) {
	nc, _ := delayedNATSConn(t, 25*time.Millisecond)
	s := New(nc, nil, allowAll, slog.New(slog.NewTextHandler(io.Discard, nil)))
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for range 2 {
		if err := conn.WriteJSON(map[string]any{"action": "subscribe", "subject": "mir1712.same"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := conn.WriteJSON(map[string]any{"action": "whoami"}); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	want := []string{"subscribed", "error", "whoami"}
	for i, wantType := range want {
		var frame wsOutbound
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatal(err)
		}
		if frame.Type != wantType {
			t.Fatalf("frame %d type = %q, want %q", i, frame.Type, wantType)
		}
	}
}

func TestSubscribeFailureIncludesSubjectAndLogs(t *testing.T) {
	nc, _ := delayedNATSConn(t, time.Millisecond)
	nc.Close()

	var logs bytes.Buffer
	s := New(nc, nil, allowAll, slog.New(slog.NewTextHandler(&logs, nil)))
	httpServer := httptest.NewServer(s.Handler())
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"action": "subscribe", "subject": "mir1712.failed"}); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var frame wsOutbound
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "error" || frame.Subject != "mir1712.failed" || !strings.Contains(frame.Error, "subscribe failed") {
		t.Fatalf("subscribe failure frame = %+v", frame)
	}
	if got := logs.String(); !strings.Contains(got, "subscribe failed") || !strings.Contains(got, "subject=mir1712.failed") {
		t.Fatalf("subscribe failure was not logged with its subject: %q", got)
	}
}

// TestStreamFrameFor covers the gateway's terminal-vs-chunk decision for inbox
// messages relayed to a streaming requester. The cc-stream-term header is set
// only by the gateway (stripped from client input), so it is the trusted marker.
func TestStreamFrameFor(t *testing.T) {
	chunk := &nats.Msg{Data: []byte("hello")}
	out, terminal := streamFrameFor("st_1", chunk)
	if terminal {
		t.Fatal("plain message should not be terminal")
	}
	if out.Type != "stream_chunk" || out.StreamID != "st_1" {
		t.Fatalf("unexpected chunk frame: %+v", out)
	}
	if got, _ := base64.StdEncoding.DecodeString(out.Data); string(got) != "hello" {
		t.Fatalf("chunk data = %q", got)
	}

	end := &nats.Msg{Header: nats.Header{headerStreamTerm: []string{"ok"}}}
	out, terminal = streamFrameFor("st_1", end)
	if !terminal || out.Type != "stream_end" || out.StreamID != "st_1" {
		t.Fatalf("ok terminal => %+v terminal=%v", out, terminal)
	}

	errMsg := &nats.Msg{Header: nats.Header{
		headerStreamTerm: []string{"error"},
		headerStreamErr:  []string{"upstream 429"},
	}}
	out, terminal = streamFrameFor("st_1", errMsg)
	if !terminal || out.Type != "stream_error" || out.Error != "upstream 429" {
		t.Fatalf("error terminal => %+v terminal=%v", out, terminal)
	}

	// A cc-stream-ready frame (upload responder has subscribed) becomes a
	// non-terminal upload_ready, not a chunk.
	ready := &nats.Msg{Header: nats.Header{headerStreamReady: []string{"1"}}}
	out, terminal = streamFrameFor("st_1", ready)
	if terminal || out.Type != "upload_ready" || out.StreamID != "st_1" {
		t.Fatalf("ready frame => %+v terminal=%v", out, terminal)
	}
}
