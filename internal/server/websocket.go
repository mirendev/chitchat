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
)

const (
	// pongWait is how long we wait for a pong (or any frame) before considering
	// the connection dead. pingPeriod must be shorter so a ping always lands
	// before the deadline. writeWait bounds a single write so a wedged client
	// can't block the NATS callback forever.
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	writeWait  = 10 * time.Second

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
}

func (s *Server) websocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &wsConn{
		conn:        conn,
		server:      s,
		logger:      s.logger,
		ctx:         ctx,
		cancel:      cancel,
		subs:        make(map[string]*nats.Subscription),
		jsConsumers: make(map[string]*jsSub),
	}
	c.run()
}

// jsSub is a live durable session subscription bound to this connection.
type jsSub struct {
	cc        jetstream.ConsumeContext
	sessionID string
}

type wsConn struct {
	conn   *websocket.Conn
	server *Server
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	writeMu     sync.Mutex
	mu          sync.Mutex
	subs        map[string]*nats.Subscription // core NATS subscriptions, keyed by subject
	jsConsumers map[string]*jsSub             // durable session consumers, keyed by subject
}

func (c *wsConn) run() {
	done := make(chan struct{})
	defer func() {
		close(done)
		c.cancel()
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
		case "publish":
			c.handlePublish(msg)
		default:
			c.send(wsOutbound{Type: "error", Error: "unknown action: " + msg.Action})
		}
	}
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

	c.mu.Lock()
	_, exists := c.subs[msg.Subject]
	c.mu.Unlock()

	if exists {
		c.send(wsOutbound{Type: "error", Error: "already subscribed to " + msg.Subject})
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
		c.send(wsOutbound{Type: "error", Error: "subscribe failed: " + err.Error()})
		return
	}

	// Flush so the SUB has reached the server before we confirm. Otherwise the
	// "subscribed" ack races ahead of the registered interest and messages
	// published in that window are silently dropped by core NATS.
	if err := c.server.nc.Flush(); err != nil {
		sub.Unsubscribe()
		c.send(wsOutbound{Type: "error", Error: "subscribe flush failed: " + err.Error()})
		return
	}

	c.mu.Lock()
	c.subs[msg.Subject] = sub
	c.mu.Unlock()

	c.send(wsOutbound{Type: "subscribed", Subject: msg.Subject})
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
		c.send(wsOutbound{Type: "error", Error: "already subscribed to " + subject})
		return
	}

	apiCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	// The subject must be captured by an existing stream — that stream is the
	// durable buffer. Admins size it (e.g. max_msgs_per_subject) via /v1/streams.
	streamName, err := c.server.js.StreamNameBySubject(apiCtx, subject)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			c.send(wsOutbound{Type: "error", Error: "no durable stream captures subject " + subject + " (create one via POST /v1/streams)"})
			return
		}
		c.send(wsOutbound{Type: "error", Error: "resolve stream failed: " + err.Error()})
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
		c.send(wsOutbound{Type: "error", Error: "consumer setup failed: " + err.Error()})
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
		c.send(wsOutbound{Type: "error", Error: "consume failed: " + err.Error()})
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
	}
	if len(msg.Headers) > 0 {
		m.Header = make(nats.Header)
		for k, v := range msg.Headers {
			m.Header.Set(k, v)
		}
	}

	if err := c.server.nc.PublishMsg(m); err != nil {
		c.send(wsOutbound{Type: "error", Error: "publish failed: " + err.Error()})
		return
	}

	c.send(wsOutbound{Type: "published", Subject: msg.Subject})
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
