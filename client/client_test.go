package chitchat

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func indexOf(ss []string, want string) int {
	for i, s := range ss {
		if s == want {
			return i
		}
	}
	return -1
}

// waitConnected blocks until the client reports a live connection.
func waitConnected(t *testing.T, c *Client) {
	t.Helper()
	waitFor(t, 3*time.Second, "client never connected", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.connected
	})
}

// waitFor polls cond until it's true or the deadline passes.
func waitFor(t *testing.T, d time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// fakeGateway is a minimal chitchat WebSocket gateway for tests. It records the
// order of inbound actions, delays the "subscribed" ack so a premature publish
// would be observable, and echoes a canned reply to a request's inbox.
type fakeGateway struct {
	mu       sync.Mutex
	order    []string
	ackDelay time.Duration
	reply    []byte            // raw bytes delivered to the request's reply subject
	replyHdr map[string]string // headers on that reply
}

func (g *fakeGateway) record(s string) {
	g.mu.Lock()
	g.order = append(g.order, s)
	g.mu.Unlock()
}

func (g *fakeGateway) sequence() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.order...)
}

func (g *fakeGateway) handler() http.HandlerFunc {
	up := websocket.Upgrader{}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct {
				Action       string `json:"action"`
				Subject      string `json:"subject"`
				ReplySubject string `json:"reply_subject"`
			}
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			switch m.Action {
			case "subscribe":
				g.record("subscribe")
				time.Sleep(g.ackDelay)
				g.record("ack-sent")
				_ = conn.WriteJSON(map[string]any{"type": "subscribed", "subject": m.Subject})
			case "publish":
				g.record("publish")
				if m.ReplySubject != "" {
					frame := map[string]any{
						"type":    "message",
						"subject": m.ReplySubject,
						"data":    base64.StdEncoding.EncodeToString(g.reply),
					}
					if g.replyHdr != nil {
						frame["headers"] = g.replyHdr
					}
					_ = conn.WriteJSON(frame)
				}
			case "unsubscribe":
				g.record("unsubscribe")
			}
		}
	}
}

// --- MIR-1513 regressions -------------------------------------------------

// replayGateway tracks which subjects were subscribed on each connection
// generation, and can hang up on the first one to force a reconnect.
type replayGateway struct {
	mu    sync.Mutex
	gen   int
	byGen map[int][]string
	drop  chan struct{} // closed to hang up on generation 1
}

func newReplayGateway() *replayGateway {
	return &replayGateway{byGen: map[int][]string{}, drop: make(chan struct{})}
}

func (g *replayGateway) subjects(gen int) []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := append([]string(nil), g.byGen[gen]...)
	sort.Strings(out)
	return out
}

func (g *replayGateway) generations() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.gen
}

func (g *replayGateway) handler() http.HandlerFunc {
	up := websocket.Upgrader{}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		g.mu.Lock()
		g.gen++
		gen := g.gen
		g.mu.Unlock()

		if gen == 1 {
			// Hang up when the test says so, which unblocks the client's read
			// loop and sends it into the reconnect path.
			go func() {
				<-g.drop
				conn.Close()
			}()
		}

		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct {
				Action  string `json:"action"`
				Subject string `json:"subject"`
			}
			if json.Unmarshal(data, &m) != nil || m.Action != "subscribe" {
				continue
			}
			g.mu.Lock()
			g.byGen[gen] = append(g.byGen[gen], m.Subject)
			g.mu.Unlock()
			_ = conn.WriteJSON(map[string]any{"type": "subscribed", "subject": m.Subject})
		}
	}
}

// TestReconnectReplaysEverySubscriptionUnderWritePressure is the MIR-1513
// regression.
//
// The old client kept one long-lived writeCh across connections. During an
// outage nothing drained it, background publishers filled all 64 slots, and the
// reconnect replayed subscriptions with a non-blocking send whose error was
// discarded — so every subscribe frame was dropped and the process came back
// connected, publishing fine, and subscribed to nothing.
//
// Here the queue is deliberately tiny and hammered with publishes throughout the
// outage. Every subscription must still be re-registered on the new connection.
func TestReconnectReplaysEverySubscriptionUnderWritePressure(t *testing.T) {
	gw := newReplayGateway()
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	want := []string{"code.event.>", "code.prs", "code.tools.register"}

	c, err := New(srv.URL, StaticToken("token"), quietLogger(), WithWriteQueue(4))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range want {
		c.Subscribe(s, func(context.Context, Message) {})
	}

	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()

	waitFor(t, 3*time.Second, "first generation never subscribed", func() bool {
		return len(gw.subjects(1)) == len(want)
	})

	// Drop the connection, then keep publishing hard while it's down. These are
	// the heartbeats and re-registrations that used to poison the queue.
	close(gw.drop)
	stopSpam := make(chan struct{})
	spamDone := make(chan struct{})
	go func() {
		defer close(spamDone)
		for {
			select {
			case <-stopSpam:
				return
			case <-time.After(time.Millisecond):
				// A tiny pause still saturates a 4-slot queue, without
				// starving the reconnect and replay goroutines this test is
				// actually measuring on a shared CI runner.
				_ = c.Publish("code.heartbeat", []byte("ping"), nil, "")
			}
		}
	}()

	waitFor(t, 15*time.Second, "client never reconnected", func() bool {
		return gw.generations() >= 2
	})
	waitFor(t, 15*time.Second, "second generation did not replay every subscription", func() bool {
		return len(gw.subjects(2)) == len(want)
	})
	close(stopSpam)
	<-spamDone

	got := gw.subjects(2)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("after reconnect gateway saw %v, want %v", got, want)
	}
}

// TestPublishWhileDisconnectedFails pins the other half of the fix: a publish
// issued while the socket is down reports ErrNotConnected instead of quietly
// queueing a frame that nobody is draining and that will be stale by the time a
// connection returns.
func TestPublishWhileDisconnectedFails(t *testing.T) {
	c, err := New("http://127.0.0.1:1", StaticToken("token"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Publish("code.heartbeat", []byte("ping"), nil, ""); !errors.Is(err, ErrNotConnected) {
		t.Errorf("Publish while disconnected = %v, want ErrNotConnected", err)
	}
	if err := c.PublishJSON("code.heartbeat", map[string]any{"x": 1}, ""); !errors.Is(err, ErrNotConnected) {
		t.Errorf("PublishJSON while disconnected = %v, want ErrNotConnected", err)
	}
}

// TestReplayFailureDropsConnection covers the third change: if the gateway
// accepts a subscribe but never confirms it, the client must tear the
// connection down and retry rather than sit there looking healthy.
func TestReplayFailureDropsConnection(t *testing.T) {
	var conns int
	var mu sync.Mutex
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		mu.Lock()
		conns++
		mu.Unlock()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			// Swallow every subscribe without ever acking it.
		}
	}))
	defer srv.Close()

	c, err := New(srv.URL, StaticToken("token"), quietLogger(), WithReplayTimeout(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	c.Subscribe("code.prs", func(context.Context, Message) {})

	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()

	// An unacked replay must not leave the connection standing: the client
	// should give up and come back around for another attempt.
	waitFor(t, 10*time.Second, "client did not retry after an unconfirmed replay", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return conns >= 2
	})
}

// TestRequestReturnsErrNoResponders covers the loose end from the incident.
// NATS answers a request with no listeners by synthesizing an empty message
// carrying "Status: 503", and the gateway relays it verbatim. The old client
// handed that empty slice back as a successful reply, so callers failed later
// with "unexpected end of JSON input" instead of learning that nothing was
// listening.
func TestRequestReturnsErrNoResponders(t *testing.T) {
	gw := &fakeGateway{
		reply:    nil,
		replyHdr: map[string]string{"Status": "503"},
	}
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	c, err := New(srv.URL, StaticToken("token"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer reqCancel()
	_, err = c.Request(reqCtx, "code.prs", map[string]any{})
	if !errors.Is(err, ErrNoResponders) {
		t.Fatalf("Request = %v, want ErrNoResponders", err)
	}
	if !strings.Contains(err.Error(), "code.prs") {
		t.Errorf("error should name the subject, got %q", err)
	}
}

// TestNoRespondersHeaderIsCaseInsensitive guards the header lookup, since NATS
// header casing isn't guaranteed to round-trip.
func TestNoRespondersHeaderIsCaseInsensitive(t *testing.T) {
	for _, key := range []string{"Status", "status", "STATUS"} {
		m := Message{Headers: map[string]string{key: "503"}}
		if !m.noResponders() {
			t.Errorf("header %q not recognized as no-responders", key)
		}
	}
	// A real reply with a body is never a no-responders marker, whatever the
	// status header says.
	m := Message{Data: []byte(`{"ok":true}`), Headers: map[string]string{"Status": "503"}}
	if m.noResponders() {
		t.Error("a reply with a body must not be treated as no-responders")
	}
}

// --- ported coverage ------------------------------------------------------

func TestRequestWaitsForSubscribed(t *testing.T) {
	gw := &fakeGateway{ackDelay: 80 * time.Millisecond, reply: []byte(`{"ok":true}`)}
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	c, err := New(srv.URL, StaticToken("token"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer reqCancel()
	data, err := c.Request(reqCtx, "rpc.test", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if string(data) != `{"ok":true}` {
		t.Errorf("reply = %s", data)
	}

	// The publish must come after the subscription was acked.
	seq := gw.sequence()
	subIdx, ackIdx, pubIdx := indexOf(seq, "subscribe"), indexOf(seq, "ack-sent"), indexOf(seq, "publish")
	if subIdx < 0 || ackIdx < 0 || pubIdx < 0 {
		t.Fatalf("missing frames in sequence %v", seq)
	}
	if !(subIdx < ackIdx && ackIdx < pubIdx) {
		t.Errorf("expected subscribe < ack-sent < publish, got %v", seq)
	}
}

func TestRequestTimesOutWithoutAck(t *testing.T) {
	// A gateway that never acks the subscription: Request must not publish and
	// must fail on the context deadline rather than hang.
	up := websocket.Upgrader{}
	var published bool
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct {
				Action string `json:"action"`
			}
			_ = json.Unmarshal(data, &m)
			if m.Action == "publish" {
				mu.Lock()
				published = true
				mu.Unlock()
			}
			// Never send a "subscribed" ack.
		}
	}))
	defer srv.Close()

	c, err := New(srv.URL, StaticToken("token"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	reqCtx, reqCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer reqCancel()
	if _, err := c.Request(reqCtx, "rpc.test", map[string]any{}); err == nil {
		t.Fatal("expected Request to fail without a subscription ack")
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if published {
		t.Error("client published before the subscription was confirmed")
	}
}

func TestReconnectDelay(t *testing.T) {
	short := time.Second // a connection that dropped quickly

	// Rapid flapping: backoff doubles each time, capped at maxBackoff.
	sleep, next := reconnectDelay(initialBackoff, short)
	if sleep != time.Second || next != 2*time.Second {
		t.Errorf("first flap: sleep=%v next=%v", sleep, next)
	}
	sleep, next = reconnectDelay(8*time.Second, short)
	if sleep != 8*time.Second || next != 16*time.Second {
		t.Errorf("mid flap: sleep=%v next=%v", sleep, next)
	}
	if _, next = reconnectDelay(20*time.Second, short); next != maxBackoff {
		t.Errorf("cap: next=%v, want %v", next, maxBackoff)
	}

	// Healthy connection (>= stablePeriod) resets the wait regardless of prior
	// backoff.
	sleep, next = reconnectDelay(maxBackoff, stablePeriod)
	if sleep != initialBackoff || next != 2*time.Second {
		t.Errorf("healthy reset: sleep=%v next=%v, want %v/%v", sleep, next, initialBackoff, 2*time.Second)
	}
}

func TestSubscribeAction(t *testing.T) {
	if a := subscribeAction("x", ""); a["session_id"] != nil {
		t.Errorf("core subscribe must omit session_id: %v", a)
	}
	a := subscribeAction("x", "sess")
	if a["action"] != "subscribe" || a["subject"] != "x" || a["session_id"] != "sess" {
		t.Errorf("durable subscribe frame = %v", a)
	}
}

func TestSubscribeWithSession(t *testing.T) {
	var mu sync.Mutex
	var gotSession string
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct {
				Action    string `json:"action"`
				Subject   string `json:"subject"`
				SessionID string `json:"session_id"`
			}
			if json.Unmarshal(data, &m) != nil || m.Action != "subscribe" {
				continue
			}
			mu.Lock()
			gotSession = m.SessionID
			mu.Unlock()
			_ = conn.WriteJSON(map[string]any{"type": "subscribed", "subject": m.Subject, "session_id": m.SessionID})
			// A durable consumer would replay a buffered message on connect.
			_ = conn.WriteJSON(map[string]any{
				"type":    "message",
				"subject": "code.event.pr.opened",
				"data":    base64.StdEncoding.EncodeToString([]byte(`{"type":"pr.opened"}`)),
			})
		}
	}))
	defer srv.Close()

	c, err := New(srv.URL, StaticToken("token"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan Message, 1)
	c.SubscribeWithSession("code.event.>", "billing-svc", func(_ context.Context, m Message) {
		select {
		case got <- m:
		default:
		}
	})
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()

	select {
	case m := <-got:
		if m.Subject != "code.event.pr.opened" {
			t.Errorf("subject = %q", m.Subject)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("did not receive durable message")
	}
	mu.Lock()
	defer mu.Unlock()
	if gotSession != "billing-svc" {
		t.Errorf("gateway saw session_id %q, want billing-svc", gotSession)
	}
}

func TestEnsureStream(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"CODE_EVENTS"}`))
	}))
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	err := c.EnsureStream(context.Background(), StreamSpec{
		Name: "CODE_EVENTS", Subjects: []string{"code.event.>"},
		Retention: "limits", MaxMsgsPerSubject: 1000,
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if gotPath != "/v1/streams" || gotAuth != "Bearer tok" {
		t.Errorf("path=%q auth=%q", gotPath, gotAuth)
	}
	var spec map[string]any
	if err := json.Unmarshal(gotBody, &spec); err != nil {
		t.Fatal(err)
	}
	if spec["name"] != "CODE_EVENTS" || spec["max_msgs_per_subject"].(float64) != 1000 {
		t.Errorf("posted spec = %v", spec)
	}
}

func TestEnsureStreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad subjects"}`))
	}))
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	err := c.EnsureStream(context.Background(), StreamSpec{Name: "X"})
	if err == nil {
		t.Fatal("expected an error from a 400")
	}
	if !strings.Contains(err.Error(), "bad subjects") {
		t.Errorf("error should carry the body, got %q", err)
	}
}

func TestMessageCallerIdentity(t *testing.T) {
	m := Message{Headers: map[string]string{
		"cc-id": "alice@example.com", "cc-auth": "jwt",
		"cc-email": "alice@example.com", "cc-sub": "user-123",
	}}
	if m.CallerID() != "alice@example.com" || m.CallerAuth() != "jwt" ||
		m.CallerEmail() != "alice@example.com" || m.CallerSubject() != "user-123" {
		t.Errorf("identity accessors wrong: %+v", m)
	}
	// Case-insensitive and missing-header handling.
	if (Message{Headers: map[string]string{"CC-ID": "apikey"}}).CallerID() != "apikey" {
		t.Error("expected case-insensitive header match")
	}
	if (Message{}).CallerID() != "" {
		t.Error("missing headers should yield an empty string")
	}
}

// TestIdentityStyles pins all three spellings the copied clients used, since
// every one of them has to keep compiling after the cutover.
func TestIdentityStyles(t *testing.T) {
	hdr := map[string]string{
		"cc-id": "svc@miren.dev", "cc-auth": "jwt",
		"cc-email": "svc@miren.dev", "cc-sub": "app:codeagent",
	}
	want := Identity{ID: "svc@miren.dev", Auth: "jwt", Email: "svc@miren.dev", Subject: "app:codeagent"}

	// Style 1: the parsed field, populated on delivery.
	if got := identityFromHeaders(hdr); got != want {
		t.Errorf("identityFromHeaders = %+v, want %+v", got, want)
	}
	// Style 2: the Caller() accessor.
	if got := (Message{Headers: hdr, Identity: want}).Caller(); got != want {
		t.Errorf("Caller() = %+v, want %+v", got, want)
	}
	// Style 3: the individual CallerX() accessors, covered above.

	if got := want.String(); got != "svc@miren.dev (jwt)" {
		t.Errorf("Identity.String() = %q", got)
	}
	if got := (Identity{}).String(); got != "(unverified)" {
		t.Errorf("empty Identity.String() = %q", got)
	}
}

func TestWhoami(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/whoami" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"identity":"svc@miren.dev","auth":"jwt","email":"svc@miren.dev","subject":"app:codeagent"}`))
	}))
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	id, err := c.Whoami(context.Background())
	if err != nil {
		t.Fatalf("Whoami: %v", err)
	}
	if id.ID != "svc@miren.dev" || id.Auth != "jwt" || id.Subject != "app:codeagent" {
		t.Errorf("identity = %+v", id)
	}
	// WhoAmI is the other spelling some callers use.
	if id2, err := c.WhoAmI(context.Background()); err != nil || id2 != id {
		t.Errorf("WhoAmI = %+v, %v", id2, err)
	}
}

// --- options --------------------------------------------------------------

func TestWithInboxPrefix(t *testing.T) {
	c, _ := New("http://x", StaticToken("t"), quietLogger(), WithInboxPrefix("code.inbox."))
	got, err := c.newInboxSubject()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "code.inbox.") {
		t.Errorf("inbox = %q, want the configured prefix", got)
	}

	// Default when unset.
	d, _ := New("http://x", StaticToken("t"), quietLogger())
	got, err = d.newInboxSubject()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, defaultInboxPrefix) {
		t.Errorf("default inbox = %q, want prefix %q", got, defaultInboxPrefix)
	}

	// Two inboxes never collide.
	a, _ := c.newInboxSubject()
	b, _ := c.newInboxSubject()
	if a == b {
		t.Error("inbox subjects must be unique")
	}
}

func TestOptionsRejectNonsense(t *testing.T) {
	c, _ := New("http://x", StaticToken("t"), quietLogger(),
		WithWriteQueue(0), WithWriteQueue(-5), WithReplayTimeout(0))
	if c.writeQueue != defaultWriteQueue {
		t.Errorf("writeQueue = %d, want the default %d", c.writeQueue, defaultWriteQueue)
	}
	if c.replayTimeout != defaultReplayTimeout {
		t.Errorf("replayTimeout = %v, want the default %v", c.replayTimeout, defaultReplayTimeout)
	}
}

func TestNewValidatesArgs(t *testing.T) {
	if _, err := New("", StaticToken("t"), quietLogger()); err == nil {
		t.Error("expected an error for an empty baseURL")
	}
	if _, err := New("http://x", nil, quietLogger()); err == nil {
		t.Error("expected an error for a nil TokenSource")
	}
	if _, err := New("http://x", StaticToken("t"), nil); err != nil {
		t.Errorf("a nil logger should default, got %v", err)
	}
}

func TestBuildWSURL(t *testing.T) {
	cases := []struct{ in, want string }{
		{"https://chitchat.miren.garden", "wss://chitchat.miren.garden/v1/ws"},
		{"http://127.0.0.1:8080", "ws://127.0.0.1:8080/v1/ws"},
		{"https://host/base/", "wss://host/base/v1/ws"},
	}
	for _, tc := range cases {
		got, err := buildWSURL(tc.in)
		if err != nil {
			t.Fatalf("buildWSURL(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("buildWSURL(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestHandlerForWildcard(t *testing.T) {
	c, _ := New("http://x", StaticToken("t"), quietLogger())
	c.Subscribe("code.event.>", func(context.Context, Message) {})
	c.Subscribe("code.prs", func(context.Context, Message) {})
	c.Subscribe("code.*.opened", func(context.Context, Message) {})

	if c.handlerFor("code.prs") == nil {
		t.Error("exact match should resolve")
	}
	if c.handlerFor("code.event.pr.opened") == nil {
		t.Error("prefix match should resolve for a `.>` pattern")
	}
	if c.handlerFor("code.issue.opened") == nil {
		t.Error("single-token `*` should resolve")
	}
	if c.handlerFor("other.subject") != nil {
		t.Error("unrelated subject should not resolve")
	}
}

// TestSubjectMatches pins NATS token semantics. handlerFor has to agree with
// what the gateway will actually deliver: the gateway registers the real NATS
// subscription, so frames for a `*` pattern arrive whether or not we can route
// them, and a matcher that only understood `.>` dropped them silently.
func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"code.prs", "code.prs", true},
		{"code.prs", "code.issues", false},
		{"code.*", "code.prs", true},
		{"code.*", "code.pr.opened", false}, // * is exactly one token
		{"code.*.opened", "code.issue.opened", true},
		{"code.*.opened", "code.issue.closed", false},
		{"code.*.opened", "code.opened", false},
		{"code.event.>", "code.event.pr.opened", true},
		{"code.event.>", "code.event.x", true},
		{"code.event.>", "code.event", false}, // > needs at least one token
		{"code.event.>", "code.other.x", false},
		{">", "anything.at.all", true},
		{">", "single", true},
		{"code.>.opened", "code.a.opened", false}, // > is only terminal
		{"*", "single", true},
		{"*", "two.tokens", false},
	}
	for _, tc := range cases {
		if got := subjectMatches(tc.pattern, tc.subject); got != tc.want {
			t.Errorf("subjectMatches(%q, %q) = %v, want %v", tc.pattern, tc.subject, got, tc.want)
		}
	}
}

// TestReplayGivesUpOnExpiredContext pins the bound CodeRabbit asked for. Each
// blocking send can wait out writeWait on a saturated queue, so a loop that
// never checked the context could spend len(replay) * writeWait before the ack
// loop noticed, and replayTimeout would stop bounding the replay at all.
func TestReplayGivesUpOnExpiredContext(t *testing.T) {
	c, err := New("http://127.0.0.1:1", StaticToken("tok"), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	replay := []replaySub{
		{subject: "code.event.>"},
		{subject: "code.prs"},
		{subject: "code.tools.register"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already expired before we start

	start := time.Now()
	err = c.replaySubscriptions(ctx, replay)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("replay should fail on an expired context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	// Nowhere near even a single writeWait, let alone one per subscription.
	if elapsed > time.Second {
		t.Errorf("replay took %v; it should give up immediately", elapsed)
	}
}

// TestCallerFromHandBuiltMessage covers a regression found by cutting
// clusteragent over: it constructs a Message from headers and calls Caller() to
// log who made an RPC. Returning the Identity field alone reported an empty
// caller for any Message the read loop didn't build, which is a quiet way to
// lose the identity audit logging depends on.
func TestCallerFromHandBuiltMessage(t *testing.T) {
	m := Message{
		Subject: "clusteragent.ping",
		Headers: map[string]string{"cc-id": "evan@miren.dev", "cc-auth": "jwt"},
	}
	got := m.Caller()
	if got.ID != "evan@miren.dev" || got.Auth != "jwt" {
		t.Errorf("Caller() = %+v, want the identity from the headers", got)
	}

	// A delivered message has both; they must agree.
	delivered := Message{Headers: m.Headers, Identity: identityFromHeaders(m.Headers)}
	if delivered.Caller() != got {
		t.Errorf("delivered Caller() = %+v, want %+v", delivered.Caller(), got)
	}

	// No headers at all falls back to whatever the field holds.
	only := Message{Identity: Identity{ID: "svc", Auth: "apikey"}}
	if only.Caller().ID != "svc" {
		t.Errorf("fallback to the Identity field failed: %+v", only.Caller())
	}

	if (Message{}).Caller() != (Identity{}) {
		t.Error("an empty Message should have an empty caller")
	}
}

// TestRequestWithHeaders pins that caller metadata rides along on the outbound
// message. inu needs this for cc-user-* end-user attribution.
func TestRequestWithHeaders(t *testing.T) {
	var mu sync.Mutex
	var gotHeaders map[string]string
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct {
				Action       string            `json:"action"`
				Subject      string            `json:"subject"`
				ReplySubject string            `json:"reply_subject"`
				Headers      map[string]string `json:"headers"`
			}
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			switch m.Action {
			case "subscribe":
				_ = conn.WriteJSON(map[string]any{"type": "subscribed", "subject": m.Subject})
			case "publish":
				mu.Lock()
				gotHeaders = m.Headers
				mu.Unlock()
				_ = conn.WriteJSON(map[string]any{
					"type": "message", "subject": m.ReplySubject,
					"data": base64.StdEncoding.EncodeToString([]byte(`{"ok":true}`)),
				})
			}
		}
	}))
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	reqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hdr := map[string]string{"cc-user-id": "alice@example.com", "cc-user-name": "Alice"}
	if _, err := c.RequestWithHeaders(reqCtx, "svc.do", map[string]any{}, hdr); err != nil {
		t.Fatalf("RequestWithHeaders: %v", err)
	}
	mu.Lock()
	seen := gotHeaders
	mu.Unlock()
	if seen["cc-user-id"] != "alice@example.com" || seen["cc-user-name"] != "Alice" {
		t.Errorf("gateway saw headers %v", seen)
	}

	// Plain Request must still send none, rather than an empty map.
	if _, err := c.Request(reqCtx, "svc.do", map[string]any{}); err != nil {
		t.Fatalf("Request: %v", err)
	}
	mu.Lock()
	seen = gotHeaders
	mu.Unlock()
	if len(seen) != 0 {
		t.Errorf("plain Request sent headers %v", seen)
	}
}

// TestClientLevelStreamActions covers the responder-side helpers storageagent
// uses: they emit the right frames rather than going through StreamResponder.
func TestClientLevelStreamActions(t *testing.T) {
	var mu sync.Mutex
	var frames []map[string]any
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m map[string]any
			if json.Unmarshal(data, &m) == nil {
				mu.Lock()
				frames = append(frames, m)
				mu.Unlock()
			}
		}
	}))
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())
	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()
	waitConnected(t, c)

	if err := c.StreamReady("reply.inbox.x"); err != nil {
		t.Fatalf("StreamReady: %v", err)
	}
	if err := c.StreamSend("reply.inbox.x", []byte("chunk")); err != nil {
		t.Fatalf("StreamSend: %v", err)
	}
	if err := c.StreamEnd("reply.inbox.x", ""); err != nil {
		t.Fatalf("StreamEnd: %v", err)
	}
	if err := c.StreamEnd("reply.inbox.y", "boom"); err != nil {
		t.Fatalf("StreamEnd(err): %v", err)
	}

	waitFor(t, 3*time.Second, "gateway did not see all four frames", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(frames) >= 4
	})
	mu.Lock()
	defer mu.Unlock()
	if frames[0]["action"] != "stream_ready" || frames[0]["subject"] != "reply.inbox.x" {
		t.Errorf("frame 0 = %v", frames[0])
	}
	if frames[1]["action"] != "stream_send" ||
		frames[1]["data"] != base64.StdEncoding.EncodeToString([]byte("chunk")) {
		t.Errorf("frame 1 = %v", frames[1])
	}
	if frames[2]["action"] != "stream_end" {
		t.Errorf("frame 2 = %v", frames[2])
	}
	if _, hasErr := frames[2]["error"]; hasErr {
		t.Errorf("a clean StreamEnd must not carry an error: %v", frames[2])
	}
	if frames[3]["error"] != "boom" {
		t.Errorf("frame 3 should carry the error: %v", frames[3])
	}
}

// TestExportedIdentityHelpers covers what dan needs: the header names and a way
// to build an Identity from a raw header map.
func TestExportedIdentityHelpers(t *testing.T) {
	if HeaderIdentity != "cc-id" || HeaderAuth != "cc-auth" ||
		HeaderEmail != "cc-email" || HeaderSubject != "cc-sub" {
		t.Error("header constants must match the gateway's stamped names")
	}
	h := map[string]string{
		HeaderIdentity: "evan@miren.dev", HeaderAuth: "jwt",
		HeaderEmail: "evan@miren.dev", HeaderSubject: "user-1",
	}
	got := IdentityFromHeaders(h)
	want := Identity{ID: "evan@miren.dev", Auth: "jwt", Email: "evan@miren.dev", Subject: "user-1"}
	if got != want {
		t.Errorf("IdentityFromHeaders = %+v, want %+v", got, want)
	}
	if IdentityFromHeaders(nil) != (Identity{}) {
		t.Error("nil headers should give a zero Identity")
	}
}

// TestWaitConnected covers the gap that surfaced cutting inu over: Run connects
// asynchronously, so a caller that starts it and immediately issues a Request is
// racing the dial. The old client hid that by queueing into a channel that
// existed from construction; this one fails fast, so callers need a way to wait.
func TestWaitConnected(t *testing.T) {
	gw := &fakeGateway{reply: []byte(`{"ok":true}`)}
	srv := httptest.NewServer(gw.handler())
	defer srv.Close()

	c, _ := New(srv.URL, StaticToken("tok"), quietLogger())

	// Before Run, waiting must respect the caller's deadline rather than hang.
	early, cancelEarly := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelEarly()
	if err := c.WaitConnected(early); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitConnected before Run = %v, want DeadlineExceeded", err)
	}

	ctx := t.Context()
	go func() { _ = c.Run(ctx) }()

	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitConnected(waitCtx); err != nil {
		t.Fatalf("WaitConnected: %v", err)
	}

	// Having waited, a request must work immediately rather than race the dial.
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()
	if _, err := c.Request(reqCtx, "svc.do", map[string]any{}); err != nil {
		t.Errorf("Request straight after WaitConnected: %v", err)
	}

	// Already connected: returns immediately, even with an expired context.
	done, cancelDone := context.WithCancel(context.Background())
	cancelDone()
	if err := c.WaitConnected(done); err != nil {
		t.Errorf("WaitConnected when already up = %v, want nil", err)
	}
}
