// Command discovery is a copy-from reference for chitchat's service-discovery
// convention (see ../../docs/SERVICE_DISCOVERY.md). It is intentionally
// self-contained — it depends only on gorilla/websocket and the standard
// library, so you can lift whole functions into your own service.
//
// It does the two things every participating service needs:
//
//   - advertise(): heartbeats this process's descriptor into the catalog.
//   - discover():  durably subscribes to the whole catalog and keeps a live,
//     TTL-expiring view of every other service — the "all see all" fan-out.
//
// Run it to watch the catalog. Run TWO copies and each one's catalog will list
// BOTH instances, because each discoverer uses its own unique session_id and so
// gets its own independent JetStream consumer that receives every descriptor.
//
//	CHITCHAT_URL=http://127.0.0.1:8080 CHITCHAT_TOKEN=… go run ./examples/discovery
//
// Defaults: CHITCHAT_URL=https://chitchat.miren.garden, token from `multipass token`.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// Tune these for your service. heartbeat < ttl < the gateway's
// DISCOVERY_MAX_AGE_SECONDS (default 120).
const (
	serviceName       = "example"
	heartbeatInterval = 30 * time.Second
	ttl               = 90 * time.Second
)

// ---------------------------------------------------------------------------
// Descriptor — the advertisement document. Copy these structs verbatim; they
// match the schema in docs/SERVICE_DISCOVERY.md.
// ---------------------------------------------------------------------------

type Descriptor struct {
	SchemaVersion    int               `json:"schema_version"`
	Service          string            `json:"service"`
	Instance         string            `json:"instance"`
	Version          string            `json:"version,omitempty"`
	Description      string            `json:"description,omitempty"`
	TS               string            `json:"ts"` // RFC3339, refreshed every heartbeat
	TTLSeconds       int               `json:"ttl_seconds"`
	HeartbeatSeconds int               `json:"heartbeat_interval_seconds"`
	RPCs             []RPC             `json:"rpcs"`
	Entities         []Entity          `json:"entities,omitempty"`
	Events           []Event           `json:"events,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

type RPC struct {
	Name        string `json:"name"`
	Subject     string `json:"subject"`
	Description string `json:"description,omitempty"`
	TimeoutMs   int    `json:"timeout_ms,omitempty"`
	// Addressing is "service" (default), "instance", or "entity". For instance/
	// entity RPCs, SubjectTemplate carries the routable form with a {instance} or
	// {entity} placeholder; Subject is the un-suffixed stem.
	Addressing      string          `json:"addressing,omitempty"`
	SubjectTemplate string          `json:"subject_template,omitempty"`
	EntityType      string          `json:"entity_type,omitempty"`
	PinnedSubject   string          `json:"pinned_subject,omitempty"`
	RequestSchema   json.RawMessage `json:"request_schema"`
	ResponseSchema  json.RawMessage `json:"response_schema"`
}

// Entity declares a type of placed entity the service addresses by id. Placement
// (which instance hosts which entity) is service-defined; this just describes it.
type Entity struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	IDFormat    string `json:"id_format,omitempty"`
	ListRPC     string `json:"list_rpc,omitempty"`
	PlaceRPC    string `json:"place_rpc,omitempty"`
}

type Event struct {
	Name        string          `json:"name"`
	Subject     string          `json:"subject"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}

	instance := "inst-" + randHex(4)
	log.Printf("starting as %s/%s", serviceName, instance)

	// Catch SIGINT and SIGTERM — containers send SIGTERM on shutdown, and we
	// want discover()'s session teardown to run on either.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); advertise(ctx, cfg, instance) }()
	go func() { defer wg.Done(); discover(ctx, cfg) }()
	go func() { defer wg.Done(); serveWidget(ctx, cfg, instance) }()
	wg.Wait()
}

// sampleWidget is the one entity id this example "hosts", so an entity-addressed
// call resolves end-to-end without a real placement registry. The id is
// self-describing: <instance>:<local>.
func sampleWidget(instance string) string { return instance + ":w1" }

// resolveEntity substitutes a known entity id into an RPC's subject_template.
// Copy this to call entity-addressed RPCs you discover.
func resolveEntity(subjectTemplate, entityID string) string {
	return strings.Replace(subjectTemplate, "{entity}", entityID, 1)
}

// ===========================================================================
// ADVERTISE — heartbeat this process's descriptor into the catalog.
// Copy this to make your service show up in discovery.
// ===========================================================================

func advertise(ctx context.Context, cfg config, instance string) {
	subject := fmt.Sprintf("discovery.service.%s.%s", serviceName, instance)

	// Build the static parts once; only `ts` changes per heartbeat.
	desc := Descriptor{
		SchemaVersion:    1,
		Service:          serviceName,
		Instance:         instance,
		Version:          "0.1.0",
		Description:      "Reference discovery example.",
		TTLSeconds:       int(ttl.Seconds()),
		HeartbeatSeconds: int(heartbeatInterval.Seconds()),
		RPCs: []RPC{
			{
				Name:           "echo",
				Subject:        serviceName + ".echo",
				Description:    "Echo the request payload back.",
				TimeoutMs:      3000,
				PinnedSubject:  serviceName + ".echo." + instance,
				RequestSchema:  json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`),
				ResponseSchema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`),
			},
			{
				// Entity-addressed: the caller substitutes a widget id (e.g.
				// "<instance>:w1") into the template; only the hosting instance
				// subscribes to that suffix, so it routes directly.
				Name:            "widget.poke",
				Subject:         serviceName + ".widget.poke", // un-suffixed stem
				SubjectTemplate: serviceName + ".widget.poke.{entity}",
				Addressing:      "entity",
				EntityType:      "widget",
				Description:     "Poke a specific widget.",
				TimeoutMs:       3000,
				RequestSchema:   json.RawMessage(`{"type":"object"}`),
				ResponseSchema:  json.RawMessage(`{"type":"object","properties":{"poked":{"type":"string"}}}`),
			},
		},
		Entities: []Entity{{
			Type:        "widget",
			Description: "A placed widget addressed by id.",
			IDFormat:    "<instance>:<local>",
			ListRPC:     "widget.list", // not implemented here; declared for shape
		}},
	}

	beat := func() {
		desc.TS = time.Now().UTC().Format(time.RFC3339)
		body, _ := json.Marshal(desc)
		if err := cfg.publish(subject, body); err != nil {
			log.Printf("advertise: %v", err)
		}
	}

	beat() // announce immediately on startup
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			beat()
		}
	}
}

// ===========================================================================
// SERVE — handle an entity-addressed RPC for the one widget this instance hosts.
// Only this instance subscribes to "<service>.widget.poke.<instance>:w1", so the
// subject suffix routes the call directly here (exactly one subscriber).
// ===========================================================================

func serveWidget(ctx context.Context, cfg config, instance string) {
	subject := resolveEntity(serviceName+".widget.poke.{entity}", sampleWidget(instance))

	for ctx.Err() == nil {
		conn, err := cfg.dial()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		go func() { <-ctx.Done(); conn.Close() }()
		conn.WriteJSON(map[string]any{"action": "subscribe", "subject": subject})

		for {
			var m struct {
				Type         string `json:"type"`
				ReplySubject string `json:"reply_subject"`
			}
			if err := conn.ReadJSON(&m); err != nil {
				break
			}
			if m.Type == "message" && m.ReplySubject != "" {
				reply, _ := json.Marshal(map[string]string{"poked": sampleWidget(instance)})
				conn.WriteJSON(map[string]any{
					"action":  "publish",
					"subject": m.ReplySubject,
					"data":    base64.StdEncoding.EncodeToString(reply),
				})
			}
		}
		conn.Close()
		time.Sleep(time.Second)
	}
}

// ===========================================================================
// DISCOVER — subscribe to the whole catalog and keep a live view.
//
// The key to "all see all": each discoverer uses its OWN unique session_id.
// That gives this process its own durable JetStream consumer, which receives
// EVERY descriptor (snapshot on connect via DeliverAll, then live updates).
// If two discoverers shared a session_id they would share one consumer and
// COMPETE for messages — never do that.
// ===========================================================================

func discover(ctx context.Context, cfg config) {
	cat := newCatalog()

	// Background sweep: drop instances we haven't heard from within their TTL.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if cat.expire() {
					cat.render()
				}
			}
		}
	}()

	// One unique session_id for this process. Reused across reconnects so a
	// reconnect resumes instead of re-snapshotting; a fresh process gets a
	// fresh id and a full snapshot.
	sessionID := "disco-" + randHex(8)

	// Reconnect loop — a discoverer should survive gateway restarts / blips.
	backoff := time.Second
	for ctx.Err() == nil {
		err := runDiscover(ctx, cfg, sessionID, cat)
		if ctx.Err() != nil {
			break
		}
		log.Printf("discover: disconnected (%v); reconnecting in %s", err, backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}

	// Be a good citizen: free our durable consumer on clean shutdown instead of
	// waiting for the gateway's 24h inactivity timeout.
	cfg.teardownSession(sessionID)
}

func runDiscover(ctx context.Context, cfg config, sessionID string, cat *catalog) error {
	conn, err := cfg.dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() { <-ctx.Done(); conn.Close() }()

	// Keepalive: detect a dead gateway and keep the connection warm.
	const pongWait = 60 * time.Second
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		t := time.NewTicker(pongWait * 9 / 10)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
			case <-pingDone:
				return
			}
		}
	}()

	// Durable subscribe to the WHOLE catalog with our unique session_id.
	if err := conn.WriteJSON(map[string]any{
		"action":     "subscribe",
		"subject":    "discovery.service.>",
		"session_id": sessionID,
	}); err != nil {
		return err
	}

	for {
		var msg struct {
			Type    string            `json:"type"`
			Subject string            `json:"subject"`
			Data    string            `json:"data"`
			Error   string            `json:"error"`
			Headers map[string]string `json:"headers"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			return err
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))

		switch msg.Type {
		case "subscribed":
			log.Printf("discover: subscribed to catalog as %s", sessionID)
		case "error":
			return fmt.Errorf("subscribe rejected: %s", msg.Error)
		case "message":
			raw, err := base64.StdEncoding.DecodeString(msg.Data)
			if err != nil {
				continue
			}
			var d Descriptor
			if err := json.Unmarshal(raw, &d); err != nil || d.SchemaVersion != 1 {
				continue // ignore garbage / unknown schema versions
			}
			// The descriptor BODY (service/instance) is self-asserted, but the
			// gateway stamps the publisher's VERIFIED identity onto the message
			// headers (cc-id / cc-auth) and strips any client-supplied copy — so
			// these are trustworthy. See ../../README.md#ambient-identity.
			cat.upsert(d, msg.Headers["cc-id"], msg.Headers["cc-auth"])
			cat.render()
		}
	}
}

// ===========================================================================
// catalog — the in-memory live view, keyed by instance. Each descriptor is
// soft state; an instance is dropped once it goes silent past its TTL.
// ===========================================================================

type catalog struct {
	mu    sync.Mutex
	items map[string]catalogEntry // key: "service/instance"

	// firstPublisher records the verified identity (cc-id) that first advertised
	// each service name. It implements a trust-on-first-use (TOFU) check: if a
	// later descriptor claims the same service from a DIFFERENT verified
	// publisher, that's a possible impersonation. A real system would replace
	// this with an explicit policy (an allowlist of service -> allowed
	// identities); TOFU just shows how the verified header makes such a check
	// possible at all.
	firstPublisher map[string]string // service -> cc-id
}

type catalogEntry struct {
	desc      Descriptor
	seen      time.Time // when we last received a heartbeat for it
	publisher string    // verified cc-id of whoever published this descriptor
	auth      string    // verified cc-auth (jwt|apikey)
	suspect   bool      // service claimed by a different publisher than first seen
}

func newCatalog() *catalog {
	return &catalog{items: map[string]catalogEntry{}, firstPublisher: map[string]string{}}
}

func (c *catalog) upsert(d Descriptor, publisher, auth string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TOFU: remember the first verified publisher of each service; flag any
	// later descriptor for that service that comes from a different identity.
	suspect := false
	if first, ok := c.firstPublisher[d.Service]; ok {
		suspect = publisher != first
	} else if publisher != "" {
		c.firstPublisher[d.Service] = publisher
	}

	c.items[d.Service+"/"+d.Instance] = catalogEntry{
		desc:      d,
		seen:      time.Now(),
		publisher: publisher,
		auth:      auth,
		suspect:   suspect,
	}
}

// expire drops entries past their TTL; returns true if anything changed.
func (c *catalog) expire() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	changed := false
	for k, e := range c.items {
		if time.Since(e.seen) > time.Duration(e.desc.TTLSeconds)*time.Second {
			delete(c.items, k)
			changed = true
		}
	}
	return changed
}

func (c *catalog) render() {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.items))
	for k := range c.items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	fmt.Fprintf(&b, "\n=== catalog: %d instance(s) ===\n", len(keys))
	for _, k := range keys {
		e := c.items[k]
		subs := make([]string, len(e.desc.RPCs))
		for i, r := range e.desc.RPCs {
			subs[i] = r.Subject
		}
		// Show the VERIFIED publisher (gateway-stamped), not the self-asserted
		// service name, so impersonation is visible.
		by := e.publisher
		if by == "" {
			by = "(unverified)"
		} else {
			by = fmt.Sprintf("%s/%s", by, e.auth)
		}
		flag := ""
		if e.suspect {
			flag = "  ⚠ IMPERSONATION? published by a different identity than this service was first seen with"
		}
		fmt.Fprintf(&b, "  %-24s v%-8s by %-28s rpcs=[%s]%s\n",
			k, e.desc.Version, by, strings.Join(subs, ", "), flag)
	}
	fmt.Print(b.String())
}

// ===========================================================================
// callRPC — invoke a discovered RPC. Take subject + timeout from a descriptor's
// RPC entry. Copy this to call services you discover. (Not exercised by main,
// which only advertises + discovers, but included as reference.)
// ===========================================================================

func (cfg config) callRPC(subject string, payload any, timeout time.Duration) (json.RawMessage, error) {
	body, _ := json.Marshal(payload)
	reqBody, _ := json.Marshal(map[string]any{
		"subject":    subject,
		"data":       base64.StdEncoding.EncodeToString(body),
		"timeout_ms": int(timeout.Milliseconds()),
	})

	resp, err := cfg.do(http.MethodPost, "/v1/request", reqBody)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusGatewayTimeout {
		return nil, errors.New("rpc timed out (instance reached but did not reply)")
	}

	var out struct {
		Data    string            `json:"data"`
		Headers map[string]string `json:"headers"`
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	// "No responders": nobody is subscribed to that subject (service down).
	if out.Data == "" && out.Headers["Status"] == "503" {
		return nil, errors.New("no instance is listening on " + subject)
	}
	data, err := base64.StdEncoding.DecodeString(out.Data)
	if err != nil {
		return nil, fmt.Errorf("decode data: %w", err)
	}
	return json.RawMessage(data), nil
}

// ===========================================================================
// config + small HTTP/WS helpers (copy as needed; same shape as the other
// chitchat clients in cmd/).
// ===========================================================================

type config struct {
	baseURL string
	token   string
}

func loadConfig() (config, error) {
	base := os.Getenv("CHITCHAT_URL")
	if base == "" {
		base = "https://chitchat.miren.garden"
	}
	token, err := resolveToken(os.Getenv("CHITCHAT_TOKEN"))
	if err != nil {
		return config{}, err
	}
	return config{baseURL: strings.TrimRight(base, "/"), token: token}, nil
}

func (cfg config) do(method, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest(method, cfg.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.token)
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func (cfg config) publish(subject string, data []byte) error {
	body, _ := json.Marshal(map[string]any{
		"subject": subject,
		"data":    base64.StdEncoding.EncodeToString(data),
	})
	resp, err := cfg.do(http.MethodPost, "/v1/publish", body)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("publish %s: %s", subject, resp.Status)
	}
	return nil
}

func (cfg config) teardownSession(sessionID string) {
	resp, err := cfg.do(http.MethodDelete, "/v1/sessions/"+url.PathEscape(sessionID), nil)
	if err == nil {
		resp.Body.Close()
	}
}

func (cfg config) dial() (*websocket.Conn, error) {
	u, err := url.Parse(cfg.baseURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), http.Header{
		"Authorization": {"Bearer " + cfg.token},
	})
	return conn, err
}

func resolveToken(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	out, err := exec.Command("multipass", "token").Output()
	if err != nil {
		return "", fmt.Errorf("set CHITCHAT_TOKEN or install the multipass CLI: %w", err)
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", errors.New("multipass token returned empty output")
	}
	return tok, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
