// Package chitchat is the Go client for the chitchat gateway. It speaks the
// long-lived /v1/ws endpoint, so subscribe and publish flow over a single
// connection rather than a fresh HTTP call per message.
//
// The import path is miren.dev/chitchat/client but the package is chitchat, so
// call sites read chitchat.New, chitchat.Message, chitchat.Handler:
//
//	import chitchat "miren.dev/chitchat/client"
//
// A Client reconnects on its own with exponential backoff. Subscriptions
// survive reconnects: the client replays its subscription list on every connect
// and waits for the gateway to confirm each one, and a replay that isn't
// confirmed drops the connection so the backoff loop tries again.
//
// One wrinkle worth knowing: the socket is usable slightly before that
// confirmation lands. Publish succeeds and Subscribe sends during the replay
// window, and it's only the "chitchat connected" log line that waits for every
// subscription to be acked. Deferring the whole thing would be worse, since a
// subscription registered during the replay isn't in the replay snapshot and
// would never be sent at all.
package chitchat

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Errors returned by the client. Callers that care about the difference between
// "the socket is down", "the socket is backed up", and "nobody is listening on
// that subject" should compare with errors.Is.
var (
	// ErrNotConnected means the WebSocket is down right now. Publishing while
	// disconnected fails immediately rather than queueing, because a frame
	// buffered during an outage is almost always stale by the time a connection
	// comes back.
	ErrNotConnected = errors.New("chitchat: not connected")

	// ErrWriteQueueFull means the outbound queue for the current connection is
	// full: frames are being produced faster than the socket drains them.
	ErrWriteQueueFull = errors.New("chitchat: write queue full")

	// ErrNoResponders means nothing was subscribed to the subject you sent a
	// Request to. NATS answers an unroutable request with a synthetic empty
	// message carrying a "Status: 503" header, and the gateway relays it
	// verbatim, so this is a real answer rather than a timeout.
	ErrNoResponders = errors.New("chitchat: no responders")
)

// Message is what subscribers see when a published message arrives.
type Message struct {
	Subject      string            `json:"subject"`
	Data         []byte            `json:"-"` // decoded from base64
	Headers      map[string]string `json:"headers,omitempty"`
	ReplySubject string            `json:"reply_subject,omitempty"`

	// Identity is the gateway-verified identity of the sender, parsed from the
	// stamped cc-* headers. It's unspoofable: the gateway is the only path to
	// NATS and it strips any client-supplied cc-* headers before stamping its
	// own.
	Identity Identity
}

// Handler is invoked for every message on a subscribed subject. It runs on the
// client's read goroutine, so long-running work should start its own goroutine.
type Handler func(ctx context.Context, msg Message)

// Identity is a caller's gateway-verified identity. An API-key caller has ID
// and Auth set to "apikey" with Email and Subject empty; a JWT caller has ID
// set to the email (or subject) with the claims filled in.
type Identity struct {
	ID      string // cc-id: email, else JWT subject, else "apikey"
	Auth    string // cc-auth: "jwt" or "apikey"
	Email   string // cc-email: JWT email claim (JWT callers only)
	Subject string // cc-sub: JWT subject claim (JWT callers only)
}

// String renders the identity compactly for logging, e.g. "alice@x (jwt)".
func (id Identity) String() string {
	if id.ID == "" {
		return "(unverified)"
	}
	if id.Auth == "" {
		return id.ID
	}
	return id.ID + " (" + id.Auth + ")"
}

// Caller returns the sender's verified identity.
//
// This reads the cc-* headers rather than returning the Identity field, because
// the headers are the source of truth and Identity is only a convenience the
// read loop fills in on delivery. A Message built by hand (a test constructing
// one from headers, say) would otherwise report an empty caller, which is a
// quiet way to lose the identity that audit logging depends on.
func (m Message) Caller() Identity {
	if id := identityFromHeaders(m.Headers); id != (Identity{}) {
		return id
	}
	return m.Identity
}

// CallerID returns the gateway-stamped identity of whoever sent this message
// (the JWT email, else its subject, or "apikey"). Empty if absent.
func (m Message) CallerID() string { return m.ccHeader("cc-id") }

// CallerAuth returns "jwt" or "apikey" for the message's sender.
func (m Message) CallerAuth() string { return m.ccHeader("cc-auth") }

// CallerEmail returns the sender's JWT email when present.
func (m Message) CallerEmail() string { return m.ccHeader("cc-email") }

// CallerSubject returns the sender's JWT subject (`sub`) when present.
func (m Message) CallerSubject() string { return m.ccHeader("cc-sub") }

// ccHeader looks a header up case-insensitively, since header casing isn't
// guaranteed on the wire.
func (m Message) ccHeader(key string) string { return headerLookup(m.Headers, key) }

// headerLookup finds key case-insensitively. NATS header key casing isn't
// guaranteed to round-trip exactly, so an exact map hit is tried first and a
// case-folded scan second.
func headerLookup(h map[string]string, key string) string {
	if v, ok := h[key]; ok {
		return v
	}
	for k, v := range h {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// identityFromHeaders extracts the cc-* identity headers.
func identityFromHeaders(h map[string]string) Identity {
	return Identity{
		ID:      headerLookup(h, "cc-id"),
		Auth:    headerLookup(h, "cc-auth"),
		Email:   headerLookup(h, "cc-email"),
		Subject: headerLookup(h, "cc-sub"),
	}
}

// noResponders reports whether this message is NATS's synthetic no-responders
// reply: an empty body carrying a 503 status header.
func (m Message) noResponders() bool {
	return len(m.Data) == 0 && m.ccHeader("Status") == "503"
}

// TokenSource yields a chitchat bearer token. A long-lived client pulls a fresh
// token on every (re)connect rather than holding one, so a credential that
// expires (a 24h multipass token, say) gets refreshed without anyone noticing.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

// TokenSourceFunc adapts a function to a TokenSource.
type TokenSourceFunc func(ctx context.Context) (string, error)

// Token implements TokenSource.
func (f TokenSourceFunc) Token(ctx context.Context) (string, error) { return f(ctx) }

// StaticToken is a TokenSource that always returns the same token, for a fixed
// API key or in tests.
func StaticToken(t string) TokenSource {
	return TokenSourceFunc(func(context.Context) (string, error) { return t, nil })
}

// Defaults for the tunables exposed via Option.
const (
	defaultInboxPrefix   = "inbox."
	defaultWriteQueue    = 64
	defaultReplayTimeout = 30 * time.Second
	// httpTimeout bounds the one-shot HTTP calls (EnsureStream, Whoami), which
	// callers often make with a context that has no deadline of its own.
	httpTimeout = 30 * time.Second
)

// Option configures a Client.
type Option func(*Client)

// WithInboxPrefix namespaces this service's request/reply inboxes, e.g.
// "code.inbox.". It's for readability and tracing on the wire; uniqueness comes
// from the random suffix, not the prefix. Defaults to "inbox.".
func WithInboxPrefix(prefix string) Option {
	return func(c *Client) { c.inboxPrefix = prefix }
}

// WithWriteQueue sets how many outbound frames may be in flight per connection
// before sends start blocking (or failing, for Publish). Defaults to 64.
func WithWriteQueue(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.writeQueue = n
		}
	}
}

// WithReplayTimeout bounds how long a reconnect waits for the gateway to
// confirm every replayed subscription before giving up and retrying the whole
// connection. Defaults to 30s.
func WithReplayTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.replayTimeout = d
		}
	}
}

// Client maintains a single WebSocket connection to chitchat and reconnects
// with exponential backoff. Subscriptions and the handler map survive
// reconnects.
type Client struct {
	url    string
	tokens TokenSource
	logger *slog.Logger
	// http is ours rather than http.DefaultClient, so a stalled gateway can't
	// hang a caller that passed a context with no deadline.
	http *http.Client

	inboxPrefix   string
	writeQueue    int
	replayTimeout time.Duration

	mu         sync.Mutex
	conn       *websocket.Conn
	subs       map[string]subscription     // subject pattern -> subscription
	subAcks    map[string]chan struct{}    // subject -> waiter for its "subscribed" ack
	streams    map[string]*streamSub       // stream_id -> consumer (RequestStream)
	responders map[string]*StreamResponder // cancel subject -> live responder
	connected  bool

	// writeCh is the outbound queue for the *current* connection. It is nil
	// while disconnected and replaced wholesale on every connect, so a
	// reconnect can never inherit frames (or a full queue) from the connection
	// that just died.
	writeCh chan []byte
}

// subscription is a registered subject handler, optionally bound to a durable
// session. An empty sessionID is a plain core (at-most-once) subscription.
type subscription struct {
	handler   Handler
	sessionID string
}

// replaySub is one entry in the subscription snapshot taken at connect time.
type replaySub struct{ subject, sessionID string }

// subscribeAction builds a subscribe frame, adding session_id only when the
// subscription is durable.
func subscribeAction(subject, sessionID string) map[string]any {
	m := map[string]any{"action": "subscribe", "subject": subject}
	if sessionID != "" {
		m["session_id"] = sessionID
	}
	return m
}

// New constructs a Client. baseURL is the chitchat base URL (http(s)://host);
// the /v1/ws path is appended automatically. tokens supplies the bearer token,
// fetched fresh on every (re)connect and HTTP call; for a fixed key pass
// StaticToken(key).
func New(baseURL string, tokens TokenSource, logger *slog.Logger, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, errors.New("chitchat: baseURL is required")
	}
	if tokens == nil {
		return nil, errors.New("chitchat: a TokenSource is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{
		url:           strings.TrimRight(baseURL, "/"),
		tokens:        tokens,
		logger:        logger,
		inboxPrefix:   defaultInboxPrefix,
		writeQueue:    defaultWriteQueue,
		replayTimeout: defaultReplayTimeout,
		http:          &http.Client{Timeout: httpTimeout},
		subs:          map[string]subscription{},
		subAcks:       map[string]chan struct{}{},
		streams:       map[string]*streamSub{},
		responders:    map[string]*StreamResponder{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Subscribe registers a handler for the given subject. NATS wildcards (`*`,
// `>`) work. Calling Subscribe before Run is fine: the subscription is replayed
// when the connection comes up. This is a plain core subscription: at-most-once
// and live-only.
func (c *Client) Subscribe(subject string, handler Handler) {
	c.SubscribeWithSession(subject, "", handler)
}

// SubscribeWithSession is like Subscribe but binds the subscription to a
// durable session. The gateway backs it with a JetStream consumer keyed on
// (sessionID, subject), which gives catch-up on connect, resume on reconnect,
// and at-least-once delivery (messages are acked only after a successful write
// to the socket). A stream must already cover the subject, and every logical
// subscriber needs its own sessionID.
func (c *Client) SubscribeWithSession(subject, sessionID string, handler Handler) {
	c.mu.Lock()
	c.subs[subject] = subscription{handler: handler, sessionID: sessionID}
	connected := c.connected
	c.mu.Unlock()
	if connected {
		if err := c.sendAction(subscribeAction(subject, sessionID), true); err != nil {
			// The subscription stays registered, so the next reconnect replays
			// it. Log rather than swallow: a dropped subscribe is invisible
			// otherwise, which is exactly how this used to fail.
			c.logger.Warn("chitchat: subscribe failed", "subject", subject, "err", err)
		}
	}
}

// Unsubscribe drops a subscription registered via Subscribe.
func (c *Client) Unsubscribe(subject string) {
	c.mu.Lock()
	delete(c.subs, subject)
	connected := c.connected
	c.mu.Unlock()
	if connected {
		_ = c.sendAction(map[string]any{"action": "unsubscribe", "subject": subject}, false)
	}
}

// Publish sends a message to a subject. replySubject is optional: when set, the
// recipient sees it on the incoming message and can publish back to it, which
// is the request/reply pattern.
//
// Publish does not block. If the socket is down it returns ErrNotConnected, and
// if the outbound queue is saturated it returns ErrWriteQueueFull, rather than
// stalling the caller behind a backed-up connection.
func (c *Client) Publish(subject string, data []byte, headers map[string]string, replySubject string) error {
	msg := map[string]any{
		"action":  "publish",
		"subject": subject,
		"data":    base64.StdEncoding.EncodeToString(data),
	}
	if len(headers) > 0 {
		msg["headers"] = headers
	}
	if replySubject != "" {
		msg["reply_subject"] = replySubject
	}
	return c.sendAction(msg, false)
}

// PublishJSON marshals v and publishes the resulting bytes.
func (c *Client) PublishJSON(subject string, v any, replySubject string) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	return c.Publish(subject, raw, nil, replySubject)
}

// subscribeAndWait registers handler, sends the subscribe action, and blocks
// until the gateway confirms with a "subscribed" ack (which it sends only once
// the subscription is live on NATS) or ctx fires. Waiting for the ack before
// publishing keeps a publish from racing ahead of the subscription; the gateway
// documents this as required.
func (c *Client) subscribeAndWait(ctx context.Context, subject string, handler Handler) error {
	ack := make(chan struct{})
	c.mu.Lock()
	c.subs[subject] = subscription{handler: handler}
	c.subAcks[subject] = ack
	c.mu.Unlock()

	if err := c.sendAction(subscribeAction(subject, ""), true); err != nil {
		c.clearAck(subject)
		c.mu.Lock()
		delete(c.subs, subject)
		c.mu.Unlock()
		return err
	}
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		c.clearAck(subject)
		c.mu.Lock()
		delete(c.subs, subject)
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *Client) clearAck(subject string) {
	c.mu.Lock()
	delete(c.subAcks, subject)
	c.mu.Unlock()
}

// Request subscribes to a transient inbox, waits for the subscription to be
// confirmed, then publishes payload with that inbox as the reply subject and
// blocks until the reply arrives (or ctx fires). Mirrors the HTTP /v1/request
// semantics over the long-lived WebSocket.
//
// If nothing is subscribed to subject, this returns ErrNoResponders rather than
// waiting out the context.
func (c *Client) Request(ctx context.Context, subject string, payload any) ([]byte, error) {
	inbox, err := c.newInboxSubject()
	if err != nil {
		return nil, err
	}

	replyCh := make(chan Message, 1)
	// Wait for the inbox subscription to be live before publishing, otherwise
	// the responder could reply before our interest is registered and we'd miss
	// it entirely.
	if err := c.subscribeAndWait(ctx, inbox, func(_ context.Context, msg Message) {
		select {
		case replyCh <- msg:
		default:
		}
	}); err != nil {
		return nil, fmt.Errorf("subscribe inbox: %w", err)
	}
	defer c.Unsubscribe(inbox)

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	if err := c.Publish(subject, raw, nil, inbox); err != nil {
		return nil, err
	}

	select {
	case msg := <-replyCh:
		if msg.noResponders() {
			return nil, fmt.Errorf("%w: %s", ErrNoResponders, subject)
		}
		return msg.Data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// StreamSpec describes a JetStream stream to create or update via the gateway's
// HTTP API. A stream is the durable buffer that session subscriptions replay
// from; for a bounded broadcast buffer use Retention "limits" with
// MaxMsgsPerSubject set.
type StreamSpec struct {
	Name              string   `json:"name"`
	Subjects          []string `json:"subjects"`
	Retention         string   `json:"retention,omitempty"`
	MaxMsgsPerSubject int64    `json:"max_msgs_per_subject,omitempty"`
	MaxAgeSeconds     int64    `json:"max_age_seconds,omitempty"`
	Discard           string   `json:"discard,omitempty"`
}

// EnsureStream creates or updates a stream over the gateway's HTTP API. It's
// idempotent, so it's safe to call on every startup. This is an HTTP call and
// does not require Run to be active.
func (c *Client) EnsureStream(ctx context.Context, spec StreamSpec) error {
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}
	body, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/v1/streams", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("ensure stream %s: %s: %s", spec.Name, resp.Status, strings.TrimSpace(string(b)))
	}
	return nil
}

// Whoami asks the gateway for this client's own verified identity via the
// /v1/whoami HTTP endpoint. Useful for logging who a service authenticates as
// at startup. Does not require Run to be active.
func (c *Client) Whoami(ctx context.Context) (Identity, error) {
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return Identity{}, fmt.Errorf("token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/v1/whoami", nil)
	if err != nil {
		return Identity{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.http.Do(req)
	if err != nil {
		return Identity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return Identity{}, fmt.Errorf("whoami: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var body struct {
		Identity string `json:"identity"`
		Auth     string `json:"auth"`
		Email    string `json:"email"`
		Subject  string `json:"subject"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Identity{}, fmt.Errorf("whoami: decode: %w", err)
	}
	return Identity{ID: body.Identity, Auth: body.Auth, Email: body.Email, Subject: body.Subject}, nil
}

// WhoAmI is an alias for Whoami, kept because some callers spell it this way.
func (c *Client) WhoAmI(ctx context.Context) (Identity, error) { return c.Whoami(ctx) }

// Keepalive timings. We ping a little faster than the read deadline so a pong
// (or any frame) keeps the connection marked live; if none arrives within
// pongWait the read errors out and we reconnect.
const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	writeWait  = 10 * time.Second
)

// Backoff bounds for reconnection.
const (
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
	// stablePeriod is how long a connection must stay up to count as healthy;
	// reconnecting after a healthy connection resets the backoff.
	stablePeriod = 30 * time.Second
)

// reconnectDelay returns how long to wait before the next reconnect (sleep) and
// the backoff to carry into the attempt after that (next), given the previous
// backoff and how long the connection that just ended lasted. A connection that
// lasted at least stablePeriod resets the wait to initialBackoff.
func reconnectDelay(prev, connLasted time.Duration) (sleep, next time.Duration) {
	sleep = prev
	if connLasted >= stablePeriod {
		sleep = initialBackoff
	}
	next = min(sleep*2, maxBackoff)
	return sleep, next
}

// Run blocks until ctx is cancelled, maintaining the WebSocket connection with
// exponential backoff between reconnect attempts. A connection that stays up at
// least stablePeriod counts as healthy and resets the backoff, so an occasional
// drop reconnects fast while rapid flapping still backs off.
func (c *Client) Run(ctx context.Context) error {
	backoff := initialBackoff
	for {
		start := time.Now()
		err := c.connectAndServe(ctx)
		if ctx.Err() != nil {
			return nil
		}
		sleep, next := reconnectDelay(backoff, time.Since(start))
		backoff = next
		if err != nil {
			c.logger.Warn("chitchat connection lost", "err", err, "retry_in", sleep)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(sleep):
		}
	}
}

func (c *Client) connectAndServe(ctx context.Context) error {
	// Fetch a fresh token per connect; return the error so the reconnect/backoff
	// loop retries when the token source (multipass, say) is briefly down.
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return fmt.Errorf("token: %w", err)
	}
	wsURL, err := buildWSURL(c.url)
	if err != nil {
		return err
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, _, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// A brand new queue for this connection. Frames that piled up during the
	// outage die with the old channel instead of being delivered late, and the
	// replay below can never find the queue already full.
	ch := make(chan []byte, c.writeQueue)

	c.mu.Lock()
	c.conn = conn
	c.writeCh = ch
	c.connected = true
	replay := make([]replaySub, 0, len(c.subs))
	for subject, s := range c.subs {
		replay = append(replay, replaySub{subject: subject, sessionID: s.sessionID})
	}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.writeCh = nil
		c.connected = false
		c.mu.Unlock()
		// Streams are ephemeral and don't survive a reconnect, so anything in
		// flight has to be told rather than left hanging on a dead channel.
		c.failAllStreams(ErrStreamDisconnected)
		// The requester is gone too, so a responder still generating chunks is
		// working for nobody. This is what makes Canceled's "or disconnects"
		// true rather than aspirational.
		c.cancelAllResponders()
	}()

	// Read-side keepalive: require a pong (or any frame) within pongWait; the
	// pong handler pushes the deadline out whenever one arrives.
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	// stop is called by whichever goroutine fails first. Closing the conn
	// unblocks the peer's blocking call immediately rather than waiting out a
	// deadline; closing done lets the writer's select exit.
	done := make(chan struct{})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(done)
			_ = conn.Close()
		})
	}

	// The writer starts before the replay, so the replay has something draining
	// the queue underneath it. Starting it afterwards is what made a saturated
	// queue swallow every subscribe frame.
	writerDone := c.startWriter(ctx, conn, ch, done, stop)

	// Read loop has to be running before we wait for acks, since the acks
	// arrive as frames. It signals readDone when the connection ends.
	readErr := make(chan error, 1)
	go func() {
		readErr <- c.readLoop(ctx, conn)
		stop()
	}()

	if err := c.replaySubscriptions(ctx, replay); err != nil {
		stop()
		<-writerDone
		<-readErr
		return fmt.Errorf("replay subscriptions: %w", err)
	}

	// Log only once every subscription is confirmed live. "chitchat connected"
	// used to print before the replay, which made it true and useless: the
	// process could be connected, publishing happily, and subscribed to
	// nothing.
	c.logger.Info("chitchat connected", "url", c.url, "subscriptions", len(replay))

	err = <-readErr
	<-writerDone
	return err
}

// startWriter drains ch onto the socket and sends periodic pings. It returns a
// channel closed when the writer has exited.
func (c *Client) startWriter(ctx context.Context, conn *websocket.Conn, ch chan []byte, done chan struct{}, stop func()) chan struct{} {
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		ping := time.NewTicker(pingPeriod)
		defer ping.Stop()
		for {
			select {
			case <-ctx.Done():
				stop()
				return
			case <-done:
				return
			case <-ping.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
					stop()
					return
				}
			case frame, ok := <-ch:
				if !ok {
					return
				}
				_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
					stop()
					return
				}
			}
		}
	}()
	return writerDone
}

// readLoop reads frames until the connection fails. Each frame extends the read
// deadline; silence past pongWait surfaces as a read error and triggers a
// reconnect.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Now().Add(pongWait))
		c.handleFrame(ctx, raw)
	}
}

// replaySubscriptions re-registers every subscription on a fresh connection and
// waits for the gateway to confirm each one. Any failure is returned so the
// caller can tear the connection down and let the backoff loop try again, which
// is the whole point: a client that silently fails to resubscribe looks healthy
// from the outside while serving nothing.
func (c *Client) replaySubscriptions(ctx context.Context, replay []replaySub) error {
	if len(replay) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.replayTimeout)
	defer cancel()

	// Register every ack waiter up front, so an ack that arrives while we're
	// still sending the rest of the batch isn't missed.
	acks := make([]chan struct{}, len(replay))
	c.mu.Lock()
	for i, s := range replay {
		ack := make(chan struct{})
		c.subAcks[s.subject] = ack
		acks[i] = ack
	}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		for _, s := range replay {
			delete(c.subAcks, s.subject)
		}
		c.mu.Unlock()
	}()

	for _, s := range replay {
		// Each blocking send can wait out writeWait on a saturated queue, so
		// without this check a client with many subscriptions could spend
		// len(replay) * writeWait here and blow past replayTimeout entirely,
		// delaying the reconnect instead of retrying it.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("subscribe %s: %w", s.subject, err)
		}
		if err := c.sendAction(subscribeAction(s.subject, s.sessionID), true); err != nil {
			return fmt.Errorf("subscribe %s: %w", s.subject, err)
		}
	}
	for i, ack := range acks {
		select {
		case <-ack:
		case <-ctx.Done():
			return fmt.Errorf("no ack for %s: %w", replay[i].subject, ctx.Err())
		}
	}
	return nil
}

func (c *Client) handleFrame(ctx context.Context, raw []byte) {
	var env struct {
		Type         string            `json:"type"`
		Subject      string            `json:"subject"`
		Data         string            `json:"data"`
		Headers      map[string]string `json:"headers,omitempty"`
		ReplySubject string            `json:"reply_subject,omitempty"`
		StreamID     string            `json:"stream_id,omitempty"`
		Error        string            `json:"error,omitempty"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		c.logger.Warn("chitchat: malformed frame", "err", err)
		return
	}
	switch env.Type {
	case "message":
		data, err := base64.StdEncoding.DecodeString(env.Data)
		if err != nil {
			c.logger.Warn("chitchat: bad base64", "err", err, "subject", env.Subject)
			return
		}
		handler := c.handlerFor(env.Subject)
		if handler == nil {
			c.logger.Debug("chitchat: no handler for subject", "subject", env.Subject)
			return
		}
		handler(ctx, Message{
			Subject:      env.Subject,
			Data:         data,
			Headers:      env.Headers,
			ReplySubject: env.ReplySubject,
			Identity:     identityFromHeaders(env.Headers),
		})
	case "subscribed":
		// Signal anyone waiting for this subscription to be confirmed live.
		c.mu.Lock()
		if ack, ok := c.subAcks[env.Subject]; ok {
			delete(c.subAcks, env.Subject)
			close(ack)
		}
		c.mu.Unlock()
	case "stream_started":
		// Informational: the consumer channel is already registered by stream_id.
	case "stream_chunk":
		data, err := base64.StdEncoding.DecodeString(env.Data)
		if err != nil {
			c.logger.Warn("chitchat: bad stream base64", "err", err, "stream_id", env.StreamID)
			return
		}
		c.deliverStream(env.StreamID, StreamFrame{Data: data}, false)
	case "stream_end":
		c.deliverStream(env.StreamID, StreamFrame{}, true)
	case "stream_error":
		c.deliverStream(env.StreamID, StreamFrame{Err: errors.New(env.Error)}, true)
	case "unsubscribed", "published":
		// nothing to do: successful action acks
	case "error":
		// A gateway that predates the streaming protocol rejects request_stream
		// with a generic error carrying no stream_id to correlate against, so
		// every open stream has to fail for the caller to fall back.
		if strings.Contains(env.Error, "unknown action: request_stream") {
			c.failAllStreams(ErrStreamingUnsupported)
			return
		}
		c.logger.Warn("chitchat: server error", "err", env.Error)
	}
}

// handlerFor returns the registered handler that matches a subject, using NATS
// token semantics: an exact match wins, then `*` matches exactly one token and
// a trailing `>` matches one or more remaining tokens.
//
// This has to agree with what the gateway will actually deliver. The gateway
// registers the real NATS subscription, so frames for `code.*.opened` arrive
// whether or not we can route them; a matcher that only understood `.>` dropped
// them on the floor with a Debug log, which is the same silent shape as the bug
// this client exists to fix.
func (c *Client) handlerFor(subject string) Handler {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.subs[subject]; ok {
		return s.handler
	}
	for pat, s := range c.subs {
		if subjectMatches(pat, subject) {
			return s.handler
		}
	}
	return nil
}

// subjectMatches reports whether a NATS subject pattern matches a concrete
// subject. `*` matches exactly one token; a trailing `>` matches all remaining
// tokens and must be the final token of the pattern.
func subjectMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	pt := strings.Split(pattern, ".")
	st := strings.Split(subject, ".")
	for i, tok := range pt {
		if tok == ">" {
			// `>` is only a wildcard as the last token, and it needs at least
			// one token to stand for.
			return i == len(pt)-1 && len(st) > i
		}
		if i >= len(st) {
			return false
		}
		if tok != "*" && tok != st[i] {
			return false
		}
	}
	// Every pattern token matched; it's a match only if the subject had no
	// extra tokens left over.
	return len(pt) == len(st)
}

// sendAction serializes an action and enqueues it on the current connection's
// outbound queue.
//
// block controls what happens when the queue is full. Subscribe and replay pass
// true and wait up to writeWait for room, because dropping a subscribe frame
// leaves the client connected and deaf. Publish passes false and fails fast, so
// a caller never stalls behind a backed-up socket.
func (c *Client) sendAction(action map[string]any, block bool) error {
	raw, err := json.Marshal(action)
	if err != nil {
		return err
	}

	c.mu.Lock()
	ch := c.writeCh
	c.mu.Unlock()

	// Disconnected: say so, rather than buffering into a queue with no drainer.
	if ch == nil {
		return ErrNotConnected
	}

	if !block {
		select {
		case ch <- raw:
			return nil
		default:
			return ErrWriteQueueFull
		}
	}

	t := time.NewTimer(writeWait)
	defer t.Stop()
	select {
	case ch <- raw:
		return nil
	case <-t.C:
		return ErrWriteQueueFull
	}
}

// jsonMarshal marshals a caller-supplied payload, wrapping the error so the
// caller can tell a bad payload from a transport failure.
func jsonMarshal(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return raw, nil
}

// newInboxSubject returns a fresh reply subject namespaced to this client.
func (c *Client) newInboxSubject() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return c.inboxPrefix + base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// buildWSURL turns the base URL into the WebSocket endpoint. The token is
// deliberately not in the query string: the gateway checks the Authorization
// header first and only falls back to `?token=` for browsers, which can't set
// headers on an upgrade. Putting it in the URL anyway would leak the credential
// into proxy and access logs for no benefit.
func buildWSURL(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/ws"
	return u.String(), nil
}
