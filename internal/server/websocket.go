package server

import (
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
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
}

type wsOutbound struct {
	Type         string            `json:"type"`
	Subject      string            `json:"subject,omitempty"`
	Data         string            `json:"data,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Error        string            `json:"error,omitempty"`
	ReplySubject string            `json:"reply_subject,omitempty"`
}

func (s *Server) websocketHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "err", err)
		return
	}

	c := &wsConn{
		conn:   conn,
		server: s,
		subs:   make(map[string]*nats.Subscription),
		logger: s.logger,
	}
	c.run()
}

type wsConn struct {
	conn   *websocket.Conn
	server *Server
	logger *slog.Logger

	writeMu sync.Mutex
	mu      sync.Mutex
	subs    map[string]*nats.Subscription
}

func (c *wsConn) run() {
	defer func() {
		c.mu.Lock()
		for subj, sub := range c.subs {
			sub.Unsubscribe()
			delete(c.subs, subj)
		}
		c.mu.Unlock()
		c.conn.Close()
	}()

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.logger.Debug("websocket read error", "err", err)
			}
			return
		}

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

	c.mu.Lock()
	c.subs[msg.Subject] = sub
	c.mu.Unlock()

	c.send(wsOutbound{Type: "subscribed", Subject: msg.Subject})
}

func (c *wsConn) handleUnsubscribe(msg wsInbound) {
	if msg.Subject == "" {
		c.send(wsOutbound{Type: "error", Error: "subject is required"})
		return
	}

	c.mu.Lock()
	sub, exists := c.subs[msg.Subject]
	if exists {
		delete(c.subs, msg.Subject)
	}
	c.mu.Unlock()

	if !exists {
		c.send(wsOutbound{Type: "error", Error: "not subscribed to " + msg.Subject})
		return
	}

	sub.Unsubscribe()
	c.send(wsOutbound{Type: "unsubscribed", Subject: msg.Subject})
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

func (c *wsConn) send(msg wsOutbound) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.WriteJSON(msg); err != nil {
		c.logger.Debug("websocket write error", "err", err)
	}
}
