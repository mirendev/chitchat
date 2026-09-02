package server

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"miren.dev/chitchat/internal/auth"
)

const (
	// pongWait is how long we wait for a pong (or any frame) before considering
	// the connection dead. pingPeriod must be shorter so a ping always lands
	// before the deadline. writeWait bounds a single write so a wedged client
	// can't block the NATS callback forever.
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	writeWait  = 10 * time.Second

	// subscribeFlushTimeout matches nats.go's Flush timeout. Keeping this as a
	// context-bound flush lets a closing websocket cancel subscription setup
	// instead of leaving its workers behind for the full timeout.
	subscribeFlushTimeout = 10 * time.Second

	// The reader and action dispatcher are deliberately separate. A bounded
	// queue keeps a client from growing memory without limit while still giving
	// the reader ample room to process websocket control frames during a NATS
	// round trip.
	wsActionQueueSize = 64

	// sessionInactiveThreshold is how long a durable session consumer survives
	// with no connection bound before JetStream cleans it up. A session that
	// reconnects within this window resumes from where it left off; one gone
	// longer gets a fresh consumer that replays whatever is still buffered.
	// A client can also tear a session down explicitly instead of waiting.
	sessionInactiveThreshold = 24 * time.Hour

	// sessionMetaKey / sessionMetaSubjectKey tag each durable session consumer
	// with the session id and subject it belongs to, so a session can be torn
	// down by finding its consumers without parsing the (hashed) durable name.
	sessionMetaKey        = "chitchat_session"
	sessionMetaSubjectKey = "chitchat_subject"

	// Streaming-response headers. All carry the reserved "cc-" prefix, so
	// buildStampedHeader strips any client-supplied copy — only the gateway sets
	// them, which makes the terminal marker unspoofable.
	//
	// headerStreamCancel tells a responder which subject to watch for a cancel
	// signal. headerStreamTerm ("ok"/"error") marks the final frame of a stream;
	// headerStreamErr carries the error message on an error terminal.
	headerStreamCancel = "cc-stream-cancel"
	headerStreamTerm   = "cc-stream-term"
	headerStreamErr    = "cc-stream-error"
	// headerStreamUpload (on a request_upload) tells the responder which subject to
	// read the streamed *request* body from; headerStreamReady is the responder's
	// signal — relayed to the requester as upload_ready — that it has subscribed
	// that subject and the requester may begin sending chunks. Both are gateway-set
	// (cc-prefixed, so stripped from client input) and therefore unspoofable.
	headerStreamUpload = "cc-stream-upload"
	headerStreamReady  = "cc-stream-ready"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type wsInbound struct {
	Action       string            `json:"action"`
	Subject      string            `json:"subject,omitempty"`
	Data         string            `json:"data,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	ReplySubject string            `json:"reply_subject,omitempty"`
	// SessionID, when set on a subscribe, makes the subscription durable: it is
	// backed by a JetStream consumer keyed on (session_id, subject) that
	// resumes and replays on reconnect. Empty means a plain core NATS sub.
	SessionID string `json:"session_id,omitempty"`
	// StreamID identifies an in-flight stream for cancel_stream.
	StreamID string `json:"stream_id,omitempty"`
	// Error carries the optional error terminal on a stream_end action.
	Error string `json:"error,omitempty"`
}

type wsOutbound struct {
	Type         string            `json:"type"`
	Subject      string            `json:"subject,omitempty"`
	Data         string            `json:"data,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Error        string            `json:"error,omitempty"`
	ReplySubject string            `json:"reply_subject,omitempty"`
	SessionID    string            `json:"session_id,omitempty"`
	Count        int               `json:"count,omitempty"`
	StreamID     string            `json:"stream_id,omitempty"`
	// UploadSubject is where a request_upload requester pushes its request body
	// (with stream_send/stream_end); carried on the upload_started ack.
	UploadSubject string `json:"upload_subject,omitempty"`

	// whoami response fields. WhoSubject carries the JWT `sub` claim under a
	// distinct key — Subject above already means the NATS subject.
	Identity   string `json:"identity,omitempty"`
	Auth       string `json:"auth,omitempty"`
	Email      string `json:"email,omitempty"`
	WhoSubject string `json:"who_subject,omitempty"`
}

type wsAction struct {
	msg         wsInbound
	invalidJSON bool
}

func (s *Server) websocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "err", err)
		return
	}

	// Capture the verified identity from the (authenticated) upgrade request
	// before we switch to a background context. WS actions aren't re-authed, so
	// this principal is reused for every publish on the connection.
	principal, _ := auth.FromContext(r.Context())

	ctx, cancel := context.WithCancel(context.Background())
	c := &wsConn{
		conn:           conn,
		server:         s,
		logger:         s.logger,
		ctx:            ctx,
		cancel:         cancel,
		principal:      principal,
		subs:           make(map[string]*nats.Subscription),
		coreSubPending: make(map[string]chan struct{}),
		jsConsumers:    make(map[string]*jsSub),
		streams:        make(map[string]*streamState),
	}
	c.run()
}

// jsSub is a live durable session subscription bound to this connection.
type jsSub struct {
	cc        jetstream.ConsumeContext
	sessionID string
}

// streamState is an in-flight streaming request initiated on this connection.
// The gateway owns the ephemeral inbox the responder streams to and the cancel
// subject the responder watches; both are torn down when the stream ends, is
// cancelled, or the connection drops.
type streamState struct {
	sub           *nats.Subscription // core sub on the ephemeral inbox
	cancelSubject string             // where a cancel signal is published
}

type wsConn struct {
	conn   *websocket.Conn
	server *Server
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	principal *auth.Principal // verified identity, captured at upgrade

	writeMu sync.Mutex
	mu      sync.Mutex
	subs    map[string]*nats.Subscription // core NATS subscriptions, keyed by subject

	coreSubMu      sync.Mutex
	coreSubPending map[string]chan struct{} // serializes concurrent requests for the same subject

	jsConsumers map[string]*jsSub       // durable session consumers, keyed by subject
	streams     map[string]*streamState // in-flight request_stream, keyed by stream_id
}

func (c *wsConn) run() {
	done := make(chan struct{})
	actions := make(chan wsAction, wsActionQueueSize)
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		c.dispatchActions(actions)
	}()

	defer func() {
		close(done)
		c.cancel()
		close(actions)
		<-dispatchDone
		c.mu.Lock()
		for subj, sub := range c.subs {
			sub.Unsubscribe()
			delete(c.subs, subj)
		}
		// Stop consuming but keep the durable consumer so its position survives
		// for the session's next reconnect.
		for subj, e := range c.jsConsumers {
			e.cc.Stop()
			delete(c.jsConsumers, subj)
		}
		// Cancel any in-flight streams so responders stop producing (and paying
		// for upstream work) when the requester disappears, then drop the inboxes.
		for id, st := range c.streams {
			c.server.nc.Publish(st.cancelSubject, nil)
			st.sub.Unsubscribe()
			delete(c.streams, id)
		}
		c.mu.Unlock()
		c.conn.Close()
	}()

	// Keepalive: a dead or wedged peer is detected within pongWait. The read
	// deadline is reset on every pong and on every inbound frame; the ping
	// loop keeps an otherwise-idle connection warm against intermediary
	// timeouts (load balancers, NAT) that would silently drop it.
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	go c.pingLoop(done)

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.logger.Debug("websocket read error", "err", err)
			}
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(pongWait))

		var msg wsInbound
		if err := json.Unmarshal(raw, &msg); err != nil {
			actions <- wsAction{invalidJSON: true}
			continue
		}
		actions <- wsAction{msg: msg}
	}
}

// dispatchActions preserves action ordering without making the websocket
// reader wait on NATS. Consecutive core subscriptions are independent, so they
// can register and flush concurrently. Before any later action runs, the
// dispatcher waits for all preceding subscriptions; a publish following a
// subscribe therefore keeps the same no-missed-message guarantee as before.
func (c *wsConn) dispatchActions(actions <-chan wsAction) {
	var coreSubscribes sync.WaitGroup
	for action := range actions {
		if c.ctx.Err() != nil {
			break
		}
		msg := action.msg
		if !action.invalidJSON && msg.Action == "subscribe" && msg.SessionID == "" {
			coreSubscribes.Add(1)
			go func() {
				defer coreSubscribes.Done()
				c.handleSubscribe(msg)
			}()
			continue
		}

		coreSubscribes.Wait()
		if c.ctx.Err() != nil {
			break
		}
		if action.invalidJSON {
			c.send(wsOutbound{Type: "error", Error: "invalid json"})
			continue
		}

		switch msg.Action {
		case "subscribe":
			c.handleSubscribe(msg)
		case "unsubscribe":
			c.handleUnsubscribe(msg)
		case "teardown":
			c.handleTeardown(msg)
		case "whoami":
			c.handleWhoami()
		case "publish":
			c.handlePublish(msg)
		case "request_stream":
			c.handleRequestStream(msg)
		case "request_upload":
			c.handleRequestUpload(msg)
		case "cancel_stream":
			c.handleCancelStream(msg)
		case "stream_send":
			c.handleStreamSend(msg)
		case "stream_ready":
			c.handleStreamReady(msg)
		case "stream_end":
			c.handleStreamEnd(msg)
		default:
			c.send(wsOutbound{Type: "error", Error: "unknown action: " + msg.Action})
		}
	}
	coreSubscribes.Wait()
}

func (c *wsConn) handleSubscribe(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}

	// A session_id upgrades the subscription to a durable JetStream-backed one.
	if msg.SessionID != "" {
		c.handleDurableSubscribe(msg)
		return
	}

	release, ok := c.acquireCoreSubscribe(msg.Subject)
	if !ok {
		return
	}
	defer release()

	c.mu.Lock()
	_, exists := c.subs[msg.Subject]
	c.mu.Unlock()

	if exists {
		c.send(wsOutbound{Type: "error", Subject: msg.Subject, Error: "already subscribed to " + msg.Subject})
		return
	}

	sub, err := c.server.nc.Subscribe(msg.Subject, func(m *nats.Msg) {
		out := wsOutbound{
			Type:         "message",
			Subject:      m.Subject,
			Data:         base64.StdEncoding.EncodeToString(m.Data),
			ReplySubject: m.Reply,
		}
		if len(m.Header) > 0 {
			out.Headers = make(map[string]string)
			for k := range m.Header {
				out.Headers[k] = m.Header.Get(k)
			}
		}
		c.send(out)
	})
	if err != nil {
		c.logger.Error("subscribe failed", "subject", msg.Subject, "err", err)
		c.send(wsOutbound{Type: "error", Subject: msg.Subject, Error: "subscribe failed: " + err.Error()})
		return
	}

	// A second concurrent subscribe may have passed the fast existence check
	// before this one registered. Claim the subject before the blocking flush so
	// exactly one worker owns it and later ordered actions can see it.
	c.mu.Lock()
	if _, exists := c.subs[msg.Subject]; exists {
		c.mu.Unlock()
		sub.Unsubscribe()
		c.send(wsOutbound{Type: "error", Subject: msg.Subject, Error: "already subscribed to " + msg.Subject})
		return
	}
	c.subs[msg.Subject] = sub
	c.mu.Unlock()

	// Flush so the SUB has reached the server before we confirm. Otherwise the
	// "subscribed" ack races ahead of the registered interest and messages
	// published in that window are silently dropped by core NATS.
	flushCtx, cancel := context.WithTimeout(c.ctx, subscribeFlushTimeout)
	defer cancel()
	if err := c.server.nc.FlushWithContext(flushCtx); err != nil {
		c.mu.Lock()
		if c.subs[msg.Subject] == sub {
			delete(c.subs, msg.Subject)
		}
		c.mu.Unlock()
		sub.Unsubscribe()
		c.logger.Error("subscribe flush failed", "subject", msg.Subject, "err", err)
		c.send(wsOutbound{Type: "error", Subject: msg.Subject, Error: "subscribe flush failed: " + err.Error()})
		return
	}

	c.send(wsOutbound{Type: "subscribed", Subject: msg.Subject})
}

// acquireCoreSubscribe lets different subjects set up concurrently while
// preserving response order for repeated requests on one subject. Without the
// per-subject gate, a duplicate worker could report its error before the first
// worker reported success, causing a replay waiter to reject a subscription
// that was about to become live.
func (c *wsConn) acquireCoreSubscribe(subject string) (func(), bool) {
	for {
		c.coreSubMu.Lock()
		pending, exists := c.coreSubPending[subject]
		if !exists {
			pending = make(chan struct{})
			c.coreSubPending[subject] = pending
			c.coreSubMu.Unlock()
			return func() {
				c.coreSubMu.Lock()
				delete(c.coreSubPending, subject)
				close(pending)
				c.coreSubMu.Unlock()
			}, true
		}
		c.coreSubMu.Unlock()

		select {
		case <-pending:
		case <-c.ctx.Done():
			return nil, false
		}
	}
}

// handleDurableSubscribe backs the subscription with a JetStream durable
// consumer keyed on (session_id, subject). Messages are pushed over the socket
// and acked only after a successful write, so the session resumes exactly where
// it left off when it reconnects with the same session_id.
func (c *wsConn) handleDurableSubscribe(msg wsInbound) {
	subject := msg.Subject
	sessionID := msg.SessionID

	c.mu.Lock()
	_, exists := c.jsConsumers[subject]
	c.mu.Unlock()
	if exists {
		c.send(wsOutbound{Type: "error", Subject: subject, Error: "already subscribed to " + subject})
		return
	}

	apiCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	// The subject must be captured by an existing stream — that stream is the
	// durable buffer. Admins size it (e.g. max_msgs_per_subject) via /v1/streams.
	streamName, err := c.server.js.StreamNameBySubject(apiCtx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			c.send(wsOutbound{Type: "error", Subject: subject, Error: "no durable stream captures subject " + subject + " (create one via POST /v1/streams)"})
			return
		}
		c.send(wsOutbound{Type: "error", Subject: subject, Error: "resolve stream failed: " + err.Error()})
		return
	}

	durable := durableName(sessionID, subject)
	cons, err := c.server.js.Consumer(apiCtx, streamName, durable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		// First time for this (session, subject): start from the beginning of the
		// buffer so a new session catches up on whatever is retained.
		cons, err = c.server.js.CreateConsumer(apiCtx, streamName, jetstream.ConsumerConfig{
			Durable:           durable,
			FilterSubject:     subject,
			AckPolicy:         jetstream.AckExplicitPolicy,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			InactiveThreshold: sessionInactiveThreshold,
			Metadata: map[string]string{
				sessionMetaKey:        sessionID,
				sessionMetaSubjectKey: subject,
			},
		})
	}
	if err != nil {
		c.send(wsOutbound{Type: "error", Subject: subject, Error: "consumer setup failed: " + err.Error()})
		return
	}

	cc, err := cons.Consume(
		func(m jetstream.Msg) {
			out := wsOutbound{
				Type:      "message",
				Subject:   m.Subject(),
				Data:      base64.StdEncoding.EncodeToString(m.Data()),
				SessionID: sessionID,
			}
			if hdrs := m.Headers(); len(hdrs) > 0 {
				out.Headers = make(map[string]string)
				for k := range hdrs {
					out.Headers[k] = hdrs.Get(k)
				}
			}
			if err := c.sendErr(out); err != nil {
				// Write failed: the connection is gone. Don't ack — the message is
				// redelivered to this session on reconnect. Tear the connection
				// down; run()'s cleanup stops this consumer.
				m.Nak()
				c.conn.Close()
				return
			}
			m.Ack()
		},
		jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, cerr error) {
			// The durable was deleted out from under us (e.g. an explicit
			// teardown from another connection or the HTTP endpoint). Drop the
			// local binding and tell the client the subscription is gone.
			if errors.Is(cerr, jetstream.ErrConsumerDeleted) || errors.Is(cerr, jetstream.ErrConsumerNotFound) {
				c.mu.Lock()
				delete(c.jsConsumers, subject)
				c.mu.Unlock()
				c.send(wsOutbound{Type: "unsubscribed", Subject: subject, SessionID: sessionID})
			}
		}),
	)
	if err != nil {
		c.send(wsOutbound{Type: "error", Subject: subject, Error: "consume failed: " + err.Error()})
		return
	}

	c.mu.Lock()
	c.jsConsumers[subject] = &jsSub{cc: cc, sessionID: sessionID}
	c.mu.Unlock()

	c.send(wsOutbound{Type: "subscribed", Subject: subject, SessionID: sessionID})
}

func (c *wsConn) handleUnsubscribe(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}

	c.mu.Lock()
	sub, isCore := c.subs[msg.Subject]
	if isCore {
		delete(c.subs, msg.Subject)
	}
	e, isDurable := c.jsConsumers[msg.Subject]
	if isDurable {
		delete(c.jsConsumers, msg.Subject)
	}
	c.mu.Unlock()

	if !isCore && !isDurable {
		c.send(wsOutbound{Type: "error", Error: "not subscribed to " + msg.Subject})
		return
	}

	if isCore {
		sub.Unsubscribe()
	}
	if isDurable {
		// Stop consuming; the durable consumer is kept so the session can resume.
		e.cc.Stop()
	}
	c.send(wsOutbound{Type: "unsubscribed", Subject: msg.Subject})
}

// handleTeardown explicitly deletes a session's durable consumer(s) instead of
// waiting for the inactivity timeout. With a subject it removes just that
// (session, subject) consumer; without one it removes every consumer belonging
// to the session. Live bindings on this connection are stopped first.
func (c *wsConn) handleTeardown(msg wsInbound) {
	if msg.SessionID == "" {
		c.send(wsOutbound{Type: "error", Error: "session_id is required"})
		return
	}

	c.mu.Lock()
	for subj, e := range c.jsConsumers {
		if e.sessionID == msg.SessionID && (msg.Subject == "" || subj == msg.Subject) {
			e.cc.Stop()
			delete(c.jsConsumers, subj)
		}
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	n, err := c.server.teardownSession(ctx, msg.SessionID, msg.Subject)
	if err != nil {
		c.send(wsOutbound{Type: "error", Error: "teardown failed: " + err.Error()})
		return
	}
	c.send(wsOutbound{Type: "torn_down", SessionID: msg.SessionID, Subject: msg.Subject, Count: n})
}

// teardownSession deletes durable session consumers. With a subject it deletes
// the single (session, subject) consumer; without one it scans every stream and
// deletes consumers tagged with this session id. Returns the number removed.
func (s *Server) teardownSession(ctx context.Context, sessionID, subject string) (int, error) {
	if subject != "" {
		streamName, err := s.js.StreamNameBySubject(ctx, subject)
		if err != nil {
			if errors.Is(err, jetstream.ErrStreamNotFound) {
				return 0, nil
			}
			return 0, err
		}
		if err := s.js.DeleteConsumer(ctx, streamName, durableName(sessionID, subject)); err != nil {
			if errors.Is(err, jetstream.ErrConsumerNotFound) {
				return 0, nil
			}
			return 0, err
		}
		return 1, nil
	}

	// Session-wide: collect matching consumers across all streams, then delete.
	type target struct{ stream, consumer string }
	var targets []target
	names := s.js.StreamNames(ctx)
	for name := range names.Name() {
		stream, err := s.js.Stream(ctx, name)
		if err != nil {
			continue
		}
		cl := stream.ListConsumers(ctx)
		for info := range cl.Info() {
			if info.Config.Metadata[sessionMetaKey] == sessionID {
				targets = append(targets, target{name, info.Name})
			}
		}
	}
	if err := names.Err(); err != nil {
		return 0, err
	}

	deleted := 0
	for _, t := range targets {
		if err := s.js.DeleteConsumer(ctx, t.stream, t.consumer); err != nil {
			if errors.Is(err, jetstream.ErrConsumerNotFound) {
				continue
			}
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (c *wsConn) handlePublish(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}

	data, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		c.send(wsOutbound{Type: "error", Error: "data must be base64 encoded"})
		return
	}

	m := &nats.Msg{
		Subject: msg.Subject,
		Data:    data,
		Reply:   msg.ReplySubject,
		Header:  buildStampedHeader(msg.Headers, c.principal),
	}

	if err := c.server.nc.PublishMsg(m); err != nil {
		c.send(wsOutbound{Type: "error", Error: "publish failed: " + err.Error()})
		return
	}

	c.send(wsOutbound{Type: "published", Subject: msg.Subject})
}

// --- streaming responses & requests ---
//
// A requester sends request_stream; the gateway mints an ephemeral inbox and a
// cancel subject, publishes the request with the reply set to the inbox, and
// relays everything that lands on the inbox to the requester as stream_chunk
// frames — until a frame carrying the cc-stream-term header (set only by the
// gateway, via stream_end) closes the stream. A responder streams with
// stream_send / stream_end against the reply subject it received. Ephemeral: a
// requester that disconnects loses the tail and re-requests; nothing is durable.
//
// request_upload additionally streams the *request* body: the gateway mints an
// upload subject (handed to the responder via cc-stream-upload, returned to the
// requester on upload_started) that the requester feeds with stream_send /
// stream_end. Because core NATS won't buffer chunks published before the
// responder's interest propagates, the responder sends stream_ready once it has
// subscribed the upload subject; the gateway relays that as upload_ready, and the
// requester only then begins sending. The response still comes back over the
// inbox, so request_upload is full bidirectional streaming.

// handleRequestStream begins a streaming request whose response is delivered as
// stream_chunk frames. See beginStream.
func (c *wsConn) handleRequestStream(msg wsInbound) { c.beginStream(msg, false) }

// handleRequestUpload begins a streaming request whose request body is also
// streamed (via an upload subject the requester feeds with stream_send /
// stream_end). See beginStream.
func (c *wsConn) handleRequestUpload(msg wsInbound) { c.beginStream(msg, true) }

// beginStream is the shared setup for request_stream and request_upload. It mints
// the ephemeral response inbox + cancel subject, subscribes the inbox to relay
// response frames to the requester, then publishes the initial request with the
// reply set to the inbox. When withUpload is set it also mints an upload subject,
// hands it to the responder via cc-stream-upload, and returns it on upload_started
// so the requester can stream the request body to it.
func (c *wsConn) beginStream(msg wsInbound, withUpload bool) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}
	data, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		c.send(wsOutbound{Type: "error", Error: "data must be base64 encoded"})
		return
	}

	// The requester supplies stream_id so it can register its handler before the
	// first frame arrives; fall back to a generated one for robustness.
	streamID := msg.StreamID
	if streamID == "" {
		streamID = "st_" + generateAckID()
	}
	inbox := "_stream." + generateAckID()
	cancelSubject := inbox + ".cancel"

	// Relay inbox messages to the requester. A frame stamped cc-stream-term is the
	// terminal (ok -> stream_end, error -> stream_error; both tear down); a frame
	// stamped cc-stream-ready becomes upload_ready; anything else is a chunk.
	sub, err := c.server.nc.Subscribe(inbox, func(m *nats.Msg) {
		out, terminal := streamFrameFor(streamID, m)
		c.send(out)
		if terminal {
			c.teardownStream(streamID, false)
		}
	})
	if err != nil {
		c.send(wsOutbound{Type: "error", Error: "stream subscribe failed: " + err.Error()})
		return
	}
	if err := c.server.nc.Flush(); err != nil {
		sub.Unsubscribe()
		c.send(wsOutbound{Type: "error", Error: "stream flush failed: " + err.Error()})
		return
	}

	c.mu.Lock()
	c.streams[streamID] = &streamState{sub: sub, cancelSubject: cancelSubject}
	c.mu.Unlock()

	// Tell the responder where to stream the response (Reply) and where to watch
	// for a cancel; for an upload, also where to read the request body from.
	header := buildStampedHeader(msg.Headers, c.principal)
	header.Set(headerStreamCancel, cancelSubject)
	uploadSubject := ""
	if withUpload {
		uploadSubject = "_upload." + generateAckID()
		header.Set(headerStreamUpload, uploadSubject)
	}
	pub := &nats.Msg{Subject: msg.Subject, Reply: inbox, Data: data, Header: header}
	if err := c.server.nc.PublishMsg(pub); err != nil {
		c.teardownStream(streamID, false)
		c.send(wsOutbound{Type: "error", Error: "stream publish failed: " + err.Error()})
		return
	}

	if withUpload {
		// The requester must wait for upload_ready before sending chunks.
		c.send(wsOutbound{Type: "upload_started", StreamID: streamID, UploadSubject: uploadSubject})
	} else {
		c.send(wsOutbound{Type: "stream_started", StreamID: streamID})
	}
}

// streamFrameFor maps an inbox message to the frame the requester should see. A
// message stamped with the gateway-only cc-stream-term header is terminal
// (ok -> stream_end, error -> stream_error); cc-stream-ready -> upload_ready (the
// responder is ready for the upload body); anything else is a chunk.
func streamFrameFor(streamID string, m *nats.Msg) (out wsOutbound, terminal bool) {
	switch m.Header.Get(headerStreamTerm) {
	case "ok":
		return wsOutbound{Type: "stream_end", StreamID: streamID}, true
	case "error":
		return wsOutbound{Type: "stream_error", StreamID: streamID, Error: m.Header.Get(headerStreamErr)}, true
	}
	if m.Header.Get(headerStreamReady) != "" {
		return wsOutbound{Type: "upload_ready", StreamID: streamID}, false
	}
	return wsOutbound{Type: "stream_chunk", StreamID: streamID, Data: base64.StdEncoding.EncodeToString(m.Data)}, false
}

// handleCancelStream signals the responder to stop, closes the requester's
// stream with a terminal frame, and tears the inbox down.
func (c *wsConn) handleCancelStream(msg wsInbound) {
	c.mu.Lock()
	_, ok := c.streams[msg.StreamID]
	c.mu.Unlock()
	if ok {
		c.send(wsOutbound{Type: "stream_end", StreamID: msg.StreamID})
	}
	c.teardownStream(msg.StreamID, true)
}

// handleStreamSend publishes one chunk to the reply subject the responder was
// handed. The subject is the ephemeral inbox; data is the chunk payload.
func (c *wsConn) handleStreamSend(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}
	data, err := base64.StdEncoding.DecodeString(msg.Data)
	if err != nil {
		c.send(wsOutbound{Type: "error", Error: "data must be base64 encoded"})
		return
	}
	m := &nats.Msg{Subject: msg.Subject, Data: data, Header: buildStampedHeader(msg.Headers, c.principal)}
	if err := c.server.nc.PublishMsg(m); err != nil {
		c.send(wsOutbound{Type: "error", Error: "stream_send failed: " + err.Error()})
	}
}

// handleStreamReady is sent by an upload responder once it has subscribed the
// upload subject. The gateway stamps the gateway-only cc-stream-ready header and
// publishes to the responder's reply inbox (msg.Subject); the requester sees it
// as upload_ready and only then begins streaming the request body — closing the
// race where chunks published before the responder's interest propagates are lost.
func (c *wsConn) handleStreamReady(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}
	header := buildStampedHeader(msg.Headers, c.principal)
	header.Set(headerStreamReady, "1")
	m := &nats.Msg{Subject: msg.Subject, Header: header}
	if err := c.server.nc.PublishMsg(m); err != nil {
		c.send(wsOutbound{Type: "error", Error: "stream_ready failed: " + err.Error()})
	}
}

// handleStreamEnd publishes the terminal frame: the gateway stamps cc-stream-term
// (and cc-stream-error on failure) so the requester side can close cleanly.
func (c *wsConn) handleStreamEnd(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}
	header := buildStampedHeader(msg.Headers, c.principal)
	if msg.Error != "" {
		header.Set(headerStreamTerm, "error")
		header.Set(headerStreamErr, msg.Error)
	} else {
		header.Set(headerStreamTerm, "ok")
	}
	m := &nats.Msg{Subject: msg.Subject, Header: header}
	if err := c.server.nc.PublishMsg(m); err != nil {
		c.send(wsOutbound{Type: "error", Error: "stream_end failed: " + err.Error()})
	}
}

// teardownStream drops the inbox subscription for a stream and, when signalCancel
// is set, publishes to its cancel subject so the responder stops producing. Safe
// to call for an unknown id (no-op).
func (c *wsConn) teardownStream(streamID string, signalCancel bool) {
	c.mu.Lock()
	st, ok := c.streams[streamID]
	if ok {
		delete(c.streams, streamID)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	if signalCancel {
		c.server.nc.Publish(st.cancelSubject, nil)
	}
	st.sub.Unsubscribe()
}

// handleWhoami reports the connection's verified identity. Strictly
// request/response — never sent unsolicited, so it can't preempt the
// "subscribed" ack that clients read as their first frame.
func (c *wsConn) handleWhoami() {
	out := wsOutbound{Type: "whoami"}
	if c.principal != nil {
		out.Identity = c.principal.Identity
		out.Auth = c.principal.Method
		out.Email = c.principal.Email
		out.WhoSubject = c.principal.Subject
	}
	c.send(out)
}

func (c *wsConn) pingLoop(done <-chan struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// WriteControl is safe to call concurrently with the data writer,
			// so a ping still goes out even if a data write is in flight.
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				return
			}
		case <-done:
			return
		}
	}
}

func (c *wsConn) send(msg wsOutbound) {
	if err := c.sendErr(msg); err != nil {
		c.logger.Debug("websocket write error", "err", err)
	}
}

func (c *wsConn) sendErr(msg wsOutbound) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	return c.conn.WriteJSON(msg)
}

// durableName derives a valid, deterministic JetStream consumer name from the
// caller's session id and the subject. Deterministic so the same (session,
// subject) rebinds the same consumer on reconnect; hashed so arbitrary session
// ids and wildcard subjects can't produce an invalid name.
func durableName(sessionID, subject string) string {
	h := sha256.Sum256([]byte(sessionID + "\x00" + subject))
	prefix := sanitizeName(sessionID)
	if len(prefix) > 24 {
		prefix = prefix[:24]
	}
	return "sess_" + prefix + "_" + hex.EncodeToString(h[:6])
}

func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
