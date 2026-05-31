package server

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Server struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	authMW func(http.Handler) http.Handler
	logger *slog.Logger

	mu      sync.Mutex
	pending map[string]*pendingAck
}

type pendingAck struct {
	msg       jetstream.Msg
	expiresAt time.Time
}

func New(nc *nats.Conn, js jetstream.JetStream, authMW func(http.Handler) http.Handler, logger *slog.Logger) *Server {
	s := &Server{
		nc:      nc,
		js:      js,
		authMW:  authMW,
		logger:  logger,
		pending: make(map[string]*pendingAck),
	}
	go s.reapPendingAcks()
	return s
}

func (s *Server) Handler() http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/healthz", s.health).Methods(http.MethodGet)

	v1 := r.PathPrefix("/v1").Subrouter()
	v1.Use(mux.MiddlewareFunc(s.authMW))

	v1.HandleFunc("/publish", s.publish).Methods(http.MethodPost)
	v1.HandleFunc("/request", s.request).Methods(http.MethodPost)
	v1.HandleFunc("/subscribe", s.subscribe).Methods(http.MethodGet)
	v1.HandleFunc("/ws", s.websocketHandler).Methods(http.MethodGet)

	v1.HandleFunc("/streams", s.createStream).Methods(http.MethodPost)
	v1.HandleFunc("/streams", s.listStreams).Methods(http.MethodGet)
	v1.HandleFunc("/streams/{name}", s.getStream).Methods(http.MethodGet)
	v1.HandleFunc("/streams/{name}", s.deleteStream).Methods(http.MethodDelete)

	v1.HandleFunc("/streams/{name}/consumers", s.createConsumer).Methods(http.MethodPost)
	v1.HandleFunc("/streams/{name}/consumers", s.listConsumers).Methods(http.MethodGet)
	v1.HandleFunc("/streams/{name}/consumers/{consumer}/next", s.pullMessages).Methods(http.MethodPost)

	v1.HandleFunc("/ack", s.ackMessage).Methods(http.MethodPost)

	return r
}

// --- Health ---

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	if !s.nc.IsConnected() {
		http.Error(w, "nats disconnected", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// --- Core Messaging ---

type publishRequest struct {
	Subject string            `json:"subject"`
	Data    string            `json:"data"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.Subject == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subject is required"})
		return
	}

	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "data must be base64 encoded"})
		return
	}

	msg := &nats.Msg{
		Subject: req.Subject,
		Data:    data,
	}
	if len(req.Headers) > 0 {
		msg.Header = make(nats.Header)
		for k, v := range req.Headers {
			msg.Header.Set(k, v)
		}
	}

	if err := s.nc.PublishMsg(msg); err != nil {
		s.logger.Error("publish failed", "subject", req.Subject, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

type requestRequest struct {
	Subject   string            `json:"subject"`
	Data      string            `json:"data"`
	Headers   map[string]string `json:"headers,omitempty"`
	TimeoutMs int               `json:"timeout_ms,omitempty"`
}

type messageResponse struct {
	Subject string            `json:"subject"`
	Data    string            `json:"data"`
	Headers map[string]string `json:"headers,omitempty"`
}

func (s *Server) request(w http.ResponseWriter, r *http.Request) {
	var req requestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.Subject == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subject is required"})
		return
	}

	data, err := base64.StdEncoding.DecodeString(req.Data)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "data must be base64 encoded"})
		return
	}

	timeout := 5 * time.Second
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}

	replySubject := "_reply." + generateAckID()

	msg := &nats.Msg{
		Subject: req.Subject,
		Reply:   replySubject,
		Data:    data,
	}
	if len(req.Headers) > 0 {
		msg.Header = make(nats.Header)
		for k, v := range req.Headers {
			msg.Header.Set(k, v)
		}
	}

	// Subscribe to the reply subject before publishing so we don't miss it.
	replyCh := make(chan *nats.Msg, 1)
	sub, err := s.nc.Subscribe(replySubject, func(m *nats.Msg) {
		select {
		case replyCh <- m:
		default:
		}
	})
	if err != nil {
		s.logger.Error("reply subscribe failed", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	sub.AutoUnsubscribe(1)
	defer sub.Unsubscribe()

	if err := s.nc.PublishMsg(msg); err != nil {
		s.logger.Error("request failed", "subject", req.Subject, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
		return
	}

	select {
	case resp := <-replyCh:
		out := messageResponse{
			Subject: resp.Subject,
			Data:    base64.StdEncoding.EncodeToString(resp.Data),
		}
		if len(resp.Header) > 0 {
			out.Headers = make(map[string]string)
			for k := range resp.Header {
				out.Headers[k] = resp.Header.Get(k)
			}
		}
		writeJSON(w, http.StatusOK, out)
	case <-time.After(timeout):
		writeJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "request timed out"})
	case <-r.Context().Done():
		return
	}
}

// --- SSE Subscribe ---

func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	subject := r.URL.Query().Get("subject")
	if subject == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subject query parameter is required"})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	sub, err := s.nc.Subscribe(subject, func(msg *nats.Msg) {
		event := messageResponse{
			Subject: msg.Subject,
			Data:    base64.StdEncoding.EncodeToString(msg.Data),
		}
		if len(msg.Header) > 0 {
			event.Headers = make(map[string]string)
			for k := range msg.Header {
				event.Headers[k] = msg.Header.Get(k)
			}
		}

		data, err := json.Marshal(event)
		if err != nil {
			return
		}

		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	})
	if err != nil {
		s.logger.Error("subscribe failed", "subject", subject, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "subscribe failed"})
		return
	}

	<-r.Context().Done()
	sub.Unsubscribe()
}

// --- JetStream Streams ---

type createStreamRequest struct {
	Name          string   `json:"name"`
	Subjects      []string `json:"subjects"`
	Retention     string   `json:"retention,omitempty"`
	MaxAgeSeconds int      `json:"max_age_seconds,omitempty"`
	MaxBytes      int64    `json:"max_bytes,omitempty"`
	MaxMsgs       int64    `json:"max_msgs,omitempty"`
}

type streamInfoResponse struct {
	Name      string   `json:"name"`
	Subjects  []string `json:"subjects"`
	Messages  uint64   `json:"messages"`
	Bytes     uint64   `json:"bytes"`
	Consumers int      `json:"consumers"`
}

func (s *Server) createStream(w http.ResponseWriter, r *http.Request) {
	var req createStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	if len(req.Subjects) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "subjects is required"})
		return
	}

	cfg := jetstream.StreamConfig{
		Name:     req.Name,
		Subjects: req.Subjects,
	}

	switch req.Retention {
	case "workqueue":
		cfg.Retention = jetstream.WorkQueuePolicy
	case "interest":
		cfg.Retention = jetstream.InterestPolicy
	default:
		cfg.Retention = jetstream.LimitsPolicy
	}

	if req.MaxAgeSeconds > 0 {
		cfg.MaxAge = time.Duration(req.MaxAgeSeconds) * time.Second
	}
	if req.MaxBytes > 0 {
		cfg.MaxBytes = req.MaxBytes
	}
	if req.MaxMsgs > 0 {
		cfg.MaxMsgs = req.MaxMsgs
	}

	stream, err := s.js.CreateOrUpdateStream(r.Context(), cfg)
	if err != nil {
		s.logger.Error("create stream failed", "name", req.Name, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	info, err := stream.Info(r.Context())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"name": req.Name, "status": "created"})
		return
	}

	writeJSON(w, http.StatusOK, streamInfoFromJetStream(info))
}

func (s *Server) listStreams(w http.ResponseWriter, r *http.Request) {
	sl := s.js.StreamNames(r.Context())

	var streams []streamInfoResponse
	for name := range sl.Name() {
		stream, err := s.js.Stream(r.Context(), name)
		if err != nil {
			continue
		}
		info, err := stream.Info(r.Context())
		if err != nil {
			continue
		}
		streams = append(streams, streamInfoFromJetStream(info))
	}

	if sl.Err() != nil {
		s.logger.Error("list streams failed", "err", sl.Err())
	}

	if streams == nil {
		streams = []streamInfoResponse{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"streams": streams})
}

func (s *Server) getStream(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	stream, err := s.js.Stream(r.Context(), name)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	info, err := stream.Info(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, streamInfoFromJetStream(info))
}

func (s *Server) deleteStream(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if err := s.js.DeleteStream(r.Context(), name); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// --- JetStream Consumers ---

type createConsumerRequest struct {
	DurableName   string `json:"durable_name"`
	FilterSubject string `json:"filter_subject,omitempty"`
	AckPolicy     string `json:"ack_policy,omitempty"`
}

type consumerInfoResponse struct {
	Name          string `json:"name"`
	Stream        string `json:"stream"`
	FilterSubject string `json:"filter_subject,omitempty"`
	NumPending    uint64 `json:"num_pending"`
	NumAckPending int    `json:"num_ack_pending"`
}

func (s *Server) createConsumer(w http.ResponseWriter, r *http.Request) {
	streamName := mux.Vars(r)["name"]
	var req createConsumerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.DurableName == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "durable_name is required"})
		return
	}

	cfg := jetstream.ConsumerConfig{
		Durable:       req.DurableName,
		FilterSubject: req.FilterSubject,
	}

	switch req.AckPolicy {
	case "none":
		cfg.AckPolicy = jetstream.AckNonePolicy
	case "all":
		cfg.AckPolicy = jetstream.AckAllPolicy
	default:
		cfg.AckPolicy = jetstream.AckExplicitPolicy
	}

	stream, err := s.js.Stream(r.Context(), streamName)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	cons, err := stream.CreateOrUpdateConsumer(r.Context(), cfg)
	if err != nil {
		s.logger.Error("create consumer failed", "stream", streamName, "consumer", req.DurableName, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	info, err := cons.Info(r.Context())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]string{"name": req.DurableName, "status": "created"})
		return
	}

	writeJSON(w, http.StatusOK, consumerInfoFromJetStream(streamName, info))
}

func (s *Server) listConsumers(w http.ResponseWriter, r *http.Request) {
	streamName := mux.Vars(r)["name"]

	stream, err := s.js.Stream(r.Context(), streamName)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	cl := stream.ListConsumers(r.Context())

	var consumers []consumerInfoResponse
	for info := range cl.Info() {
		consumers = append(consumers, consumerInfoFromJetStream(streamName, info))
	}

	if cl.Err() != nil {
		s.logger.Error("list consumers failed", "stream", streamName, "err", cl.Err())
	}

	if consumers == nil {
		consumers = []consumerInfoResponse{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"consumers": consumers})
}

// --- Pull Messages ---

type pullRequest struct {
	Batch     int `json:"batch,omitempty"`
	TimeoutMs int `json:"timeout_ms,omitempty"`
}

type pulledMessage struct {
	Subject string            `json:"subject"`
	Data    string            `json:"data"`
	Seq     uint64            `json:"seq"`
	Headers map[string]string `json:"headers,omitempty"`
	AckID   string            `json:"ack_id"`
}

func (s *Server) pullMessages(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	streamName := vars["name"]
	consumerName := vars["consumer"]

	var req pullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	batch := req.Batch
	if batch <= 0 {
		batch = 1
	}
	if batch > 100 {
		batch = 100
	}

	timeout := 5 * time.Second
	if req.TimeoutMs > 0 {
		timeout = time.Duration(req.TimeoutMs) * time.Millisecond
	}

	stream, err := s.js.Stream(r.Context(), streamName)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "stream not found"})
		return
	}

	cons, err := stream.Consumer(r.Context(), consumerName)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "consumer not found"})
		return
	}

	msgs, err := cons.Fetch(batch, jetstream.FetchMaxWait(timeout))
	if err != nil {
		s.logger.Error("fetch failed", "stream", streamName, "consumer", consumerName, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	var messages []pulledMessage
	for msg := range msgs.Messages() {
		ackID := s.storePending(msg)

		pm := pulledMessage{
			Subject: msg.Subject(),
			Data:    base64.StdEncoding.EncodeToString(msg.Data()),
			AckID:   ackID,
		}

		meta, err := msg.Metadata()
		if err == nil {
			pm.Seq = meta.Sequence.Stream
		}

		hdrs := msg.Headers()
		if len(hdrs) > 0 {
			pm.Headers = make(map[string]string)
			for k := range hdrs {
				pm.Headers[k] = hdrs.Get(k)
			}
		}

		messages = append(messages, pm)
	}

	if messages == nil {
		messages = []pulledMessage{}
	}

	writeJSON(w, http.StatusOK, map[string]any{"messages": messages})
}

// --- Ack ---

type ackRequest struct {
	AckID string `json:"ack_id"`
}

func (s *Server) ackMessage(w http.ResponseWriter, r *http.Request) {
	var req ackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.AckID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ack_id is required"})
		return
	}

	s.mu.Lock()
	pa, ok := s.pending[req.AckID]
	if ok {
		delete(s.pending, req.AckID)
	}
	s.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "ack_id not found or expired"})
		return
	}

	if err := pa.msg.Ack(); err != nil {
		s.logger.Error("ack failed", "ack_id", req.AckID, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "ack failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// --- Pending Ack Management ---

func (s *Server) storePending(msg jetstream.Msg) string {
	id := generateAckID()
	s.mu.Lock()
	s.pending[id] = &pendingAck{
		msg:       msg,
		expiresAt: time.Now().Add(5 * time.Minute),
	}
	s.mu.Unlock()
	return id
}

func (s *Server) reapPendingAcks() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for id, pa := range s.pending {
			if now.After(pa.expiresAt) {
				pa.msg.Nak()
				delete(s.pending, id)
			}
		}
		s.mu.Unlock()
	}
}

func generateAckID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// --- Helpers ---

func streamInfoFromJetStream(info *jetstream.StreamInfo) streamInfoResponse {
	return streamInfoResponse{
		Name:      info.Config.Name,
		Subjects:  info.Config.Subjects,
		Messages:  info.State.Msgs,
		Bytes:     info.State.Bytes,
		Consumers: info.State.Consumers,
	}
}

func consumerInfoFromJetStream(streamName string, info *jetstream.ConsumerInfo) consumerInfoResponse {
	return consumerInfoResponse{
		Name:          info.Config.Durable,
		Stream:        streamName,
		FilterSubject: info.Config.FilterSubject,
		NumPending:    info.NumPending,
		NumAckPending: info.NumAckPending,
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

