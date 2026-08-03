package chitchat

import (
	"errors"
	"testing"
	"time"
)

// offlineClient builds a Client that has never connected, so sendAction returns
// ErrNotConnected. Enough to exercise the lifecycle logic without a gateway.
func offlineClient(t *testing.T) *Client {
	t.Helper()
	c, err := New("http://127.0.0.1:1", StaticToken("tok"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestRequestStreamCleansUpWhenSendFails covers the crash CodeRabbit caught: the
// error path used to close st.ch unconditionally, but a dropped connection is
// exactly what makes sendAction fail, and that same drop runs failAllStreams,
// which may already have closed the channel. Closing twice panics the process.
// Only the goroutine that removed the stream from the map may close it.
func TestRequestStreamCleansUpWhenSendFails(t *testing.T) {
	c := offlineClient(t)

	ch, err := c.RequestStream(t.Context(), "code.stream", map[string]any{})
	if !errors.Is(err, ErrNotConnected) {
		t.Fatalf("RequestStream = %v, want ErrNotConnected", err)
	}
	if ch != nil {
		t.Error("no channel should be returned when the request never went out")
	}

	c.mu.Lock()
	n := len(c.streams)
	c.mu.Unlock()
	if n != 0 {
		t.Errorf("stream registry should be empty after a failed send, has %d", n)
	}

	// The stream is gone, so this has nothing to close. If ownership were wrong
	// this is where the double close would panic.
	c.failAllStreams(ErrStreamDisconnected)
}

// TestRequestStreamRaceWithDisconnect hammers the same ownership rule with the
// two paths actually running concurrently. Under -race a double close shows up
// as a panic rather than a quiet pass.
func TestRequestStreamRaceWithDisconnect(t *testing.T) {
	for i := 0; i < 50; i++ {
		c := offlineClient(t)
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.failAllStreams(ErrStreamDisconnected)
		}()
		_, _ = c.RequestStream(t.Context(), "code.stream", map[string]any{})
		<-done
	}
}

// TestFailAllStreamsDeliversTerminalOnFullBuffer pins that a disconnect can't be
// silently downgraded to a clean stream end. The old non-blocking send dropped
// the error frame whenever a slow consumer had filled the buffer, and the
// consumer then read a truncated stream as complete.
func TestFailAllStreamsDeliversTerminalOnFullBuffer(t *testing.T) {
	c := offlineClient(t)
	st := &streamSub{
		ch:   make(chan StreamFrame, 4),
		done: make(chan struct{}),
		fin:  make(chan struct{}),
	}
	for i := 0; i < 4; i++ { // fill it completely
		st.ch <- StreamFrame{Data: []byte("chunk")}
	}
	c.mu.Lock()
	c.streams["cs_test"] = st
	c.mu.Unlock()

	c.failAllStreams(ErrStreamDisconnected)

	var last StreamFrame
	got := 0
	for f := range st.ch {
		last = f
		got++
	}
	if got == 0 {
		t.Fatal("channel drained empty")
	}
	if !errors.Is(last.Err, ErrStreamDisconnected) {
		t.Errorf("final frame err = %v, want ErrStreamDisconnected", last.Err)
	}
}

// TestDeliverStreamClosesFin is the structural half of the goroutine-leak fix:
// a terminal has to close fin, since that's what lets RequestStream's cancel
// watcher exit instead of parking on the caller's context forever.
func TestDeliverStreamClosesFin(t *testing.T) {
	c := offlineClient(t)
	st := &streamSub{
		ch:   make(chan StreamFrame, 4),
		done: make(chan struct{}),
		fin:  make(chan struct{}),
	}
	c.mu.Lock()
	c.streams["cs_test"] = st
	c.mu.Unlock()

	c.deliverStream("cs_test", StreamFrame{Data: []byte("chunk")}, false)
	select {
	case <-st.fin:
		t.Fatal("fin closed on a non-terminal frame")
	default:
	}

	c.deliverStream("cs_test", StreamFrame{}, true)
	select {
	case <-st.fin:
	case <-time.After(time.Second):
		t.Error("fin not closed on the terminal frame, so the cancel watcher would leak")
	}
}

// --- responder side ---

func testResponder(t *testing.T, c *Client) (Responder, string) {
	t.Helper()
	cancelSubject := "code.stream.cancel.abc"
	r, ok := c.StreamResponderFor(Message{
		ReplySubject: "code.inbox.xyz",
		Headers:      map[string]string{headerStreamCancel: cancelSubject},
	})
	if !ok {
		t.Fatal("expected a streaming responder")
	}
	return r, cancelSubject
}

func TestStreamResponderForRequiresStreamingMarkers(t *testing.T) {
	c := offlineClient(t)
	if _, ok := c.StreamResponderFor(Message{ReplySubject: "code.inbox.x"}); ok {
		t.Error("a request without the cancel header is not a streaming request")
	}
	if _, ok := c.StreamResponderFor(Message{Headers: map[string]string{headerStreamCancel: "x"}}); ok {
		t.Error("a request without a reply subject is not a streaming request")
	}
}

// TestResponderCloseReleasesSubscription covers the leak CodeRabbit flagged: the
// cancel subscription lives in the persistent subs map, so a handler that
// panics or returns early without End or Fail leaves it there forever. It would
// then be replayed on every reconnect, and each stale subject adds an ack the
// replay has to wait out before its timeout.
func TestResponderCloseReleasesSubscription(t *testing.T) {
	c := offlineClient(t)
	r, cancelSubject := testResponder(t, c)

	c.mu.Lock()
	_, subbed := c.subs[cancelSubject]
	_, tracked := c.responders[cancelSubject]
	c.mu.Unlock()
	if !subbed || !tracked {
		t.Fatalf("responder should be subscribed and tracked (subbed=%v tracked=%v)", subbed, tracked)
	}

	r.Close()

	c.mu.Lock()
	_, subbed = c.subs[cancelSubject]
	_, tracked = c.responders[cancelSubject]
	c.mu.Unlock()
	if subbed || tracked {
		t.Errorf("Close should release the cancel subscription (subbed=%v tracked=%v)", subbed, tracked)
	}

	r.Close() // idempotent
}

// TestResponderFinishIsIdempotent pins that a second terminal is a no-op. Without
// the guard, End followed by Fail put two stream_end frames on the same reply
// subject.
func TestResponderFinishIsIdempotent(t *testing.T) {
	c := offlineClient(t)
	r, cancelSubject := testResponder(t, c)

	// Offline, so the first terminal's send fails and surfaces that error.
	if err := r.End(); !errors.Is(err, ErrNotConnected) {
		t.Fatalf("first End = %v, want ErrNotConnected", err)
	}
	// The second must not try to send at all.
	if err := r.Fail(errors.New("boom")); err != nil {
		t.Errorf("second terminal should be a no-op, got %v", err)
	}
	if err := r.End(); err != nil {
		t.Errorf("third terminal should be a no-op, got %v", err)
	}

	c.mu.Lock()
	_, subbed := c.subs[cancelSubject]
	c.mu.Unlock()
	if subbed {
		t.Error("the terminal should have released the cancel subscription")
	}
}

// TestDisconnectCancelsResponders makes Canceled's "or disconnects" promise real.
// Without this a handler keeps generating chunks and calling Send against a dead
// connection until it happens to finish on its own.
func TestDisconnectCancelsResponders(t *testing.T) {
	c := offlineClient(t)
	r, _ := testResponder(t, c)

	select {
	case <-r.Canceled():
		t.Fatal("responder cancelled before anything happened")
	default:
	}

	c.cancelAllResponders()

	select {
	case <-r.Canceled():
	case <-time.After(time.Second):
		t.Error("a dropped connection should cancel live responders")
	}

	c.cancelAllResponders() // must not double-close
}

func TestResponderSendFailsWhileDisconnected(t *testing.T) {
	c := offlineClient(t)
	r, _ := testResponder(t, c)
	if err := r.Send([]byte("chunk")); !errors.Is(err, ErrNotConnected) {
		t.Errorf("Send while disconnected = %v, want ErrNotConnected", err)
	}
}

func TestNewStreamIDIsUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id, err := newStreamID()
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("duplicate stream id %q", id)
		}
		seen[id] = true
	}
}

// TestStreamSubFinishIsIdempotent covers the hardening CodeRabbit asked for.
// Its specific scenario can't happen today (a failed sendAction means the
// request frame never reached the gateway, so no frames can come back for that
// stream id), but "nothing closes this twice" was an ordering invariant living
// three functions away in client.go. Sending on a closed channel panics the
// process, so the guarantee is local now.
func TestStreamSubFinishIsIdempotent(t *testing.T) {
	st := newStreamSub()

	if !st.finish(&StreamFrame{Err: ErrStreamDisconnected}) {
		t.Fatal("first finish should report that it closed the stream")
	}
	if st.finish(&StreamFrame{Err: ErrStreamingUnsupported}) {
		t.Error("second finish should be a no-op")
	}
	if st.finish(nil) {
		t.Error("third finish should be a no-op")
	}

	// Delivery after close is dropped rather than panicking.
	st.deliver(StreamFrame{Data: []byte("late")})

	var frames []StreamFrame
	for f := range st.ch {
		frames = append(frames, f)
	}
	if len(frames) != 1 {
		t.Fatalf("expected exactly the first terminal, got %d frames", len(frames))
	}
	if !errors.Is(frames[0].Err, ErrStreamDisconnected) {
		t.Errorf("terminal err = %v, want the first one", frames[0].Err)
	}
}

// TestStreamSubConcurrentDeliverAndFinish runs delivery against closure from
// separate goroutines. Under -race, an unguarded implementation shows up as a
// send-on-closed-channel panic.
func TestStreamSubConcurrentDeliverAndFinish(t *testing.T) {
	for i := 0; i < 100; i++ {
		st := newStreamSub()
		go st.deliver(StreamFrame{Data: []byte("chunk")})
		st.finish(&StreamFrame{Err: ErrStreamDisconnected})
		for range st.ch { //nolint:revive // draining
		}
	}
}
