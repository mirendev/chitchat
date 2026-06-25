package server

import (
	"encoding/base64"
	"testing"

	"github.com/nats-io/nats.go"
)

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
