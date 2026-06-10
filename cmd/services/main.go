// Command services lists the services advertised in chitchat's discovery
// catalog (see docs/SERVICE_DISCOVERY.md). It subscribes to discovery.service.>
// with a fresh session_id so JetStream replays the whole current catalog, groups
// the descriptors by service, and prints each service's RPCs, instances, and the
// gateway-verified publisher identity (cc-id / cc-auth).
//
//	services                 # one-shot list of all advertised services
//	services geo billing     # only these services
//	services --json          # machine-readable
//	services -w              # watch: re-render live as services come and go
//
// Environment: CHITCHAT_URL (default https://chitchat.miren.garden),
// CHITCHAT_TOKEN (default from `multipass token`).
package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = pongWait * 9 / 10
)

type Descriptor struct {
	SchemaVersion int    `json:"schema_version"`
	Service       string `json:"service"`
	Instance      string `json:"instance"`
	Version       string `json:"version"`
	Description   string `json:"description"`
	RPCs          []struct {
		Name            string `json:"name"`
		Subject         string `json:"subject"`
		Description     string `json:"description"`
		TimeoutMs       int    `json:"timeout_ms"`
		Addressing      string `json:"addressing"`
		SubjectTemplate string `json:"subject_template"`
		EntityType      string `json:"entity_type"`
	} `json:"rpcs"`
	Entities []struct {
		Type        string `json:"type"`
		Description string `json:"description"`
		IDFormat    string `json:"id_format"`
		ListRPC     string `json:"list_rpc"`
		PlaceRPC    string `json:"place_rpc"`
	} `json:"entities"`
}

func main() {
	jsonOut := flag.Bool("json", false, "output JSON")
	watch := flag.Bool("watch", false, "watch the catalog live instead of a one-shot list")
	flag.BoolVar(watch, "w", false, "shorthand for --watch")
	settle := flag.Duration("settle", 600*time.Millisecond, "one-shot: stop after this much idle")
	flag.Parse()
	filters := flag.Args()

	baseURL := os.Getenv("CHITCHAT_URL")
	if baseURL == "" {
		baseURL = "https://chitchat.miren.garden"
	}
	token, err := resolveToken(os.Getenv("CHITCHAT_TOKEN"))
	if err != nil {
		fail(err)
	}
	c := &client{baseURL: strings.TrimRight(baseURL, "/"), token: token}
	sessionID := "svc-" + randHex(8)

	if *watch {
		runWatch(c, sessionID, filters, *jsonOut)
		return
	}

	cat, err := c.snapshot(sessionID, *settle)
	c.teardown(sessionID) // free the durable consumer; one-shot doesn't resume
	if err != nil {
		fail(err)
	}
	render(cat, filters, *jsonOut)
}

// snapshot subscribes, drains the DeliverAll replay until the stream goes idle
// for `settle`, and returns the catalog. Good for a one-shot list.
func (c *client) snapshot(sessionID string, settle time.Duration) (*catalog, error) {
	conn, err := c.dial()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := conn.WriteJSON(subscribeMsg(sessionID)); err != nil {
		return nil, err
	}

	cat := newCatalog()
	conn.SetReadDeadline(time.Now().Add(settle))
	for {
		var m wsMessage
		if err := conn.ReadJSON(&m); err != nil {
			break // idle: snapshot drained
		}
		conn.SetReadDeadline(time.Now().Add(settle))
		if m.Type == "error" {
			return nil, fmt.Errorf("subscribe rejected: %s", m.Error)
		}
		if m.Type == "message" {
			cat.add(&m)
		}
	}
	return cat, nil
}

// runWatch keeps a live view, re-rendering on every change and expiring stale
// instances, reconnecting on drops.
func runWatch(c *client, sessionID string, filters []string, jsonOut bool) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	defer c.teardown(sessionID)

	cat := newCatalog()
	changes := make(chan struct{}, 1)
	notify := func() {
		select {
		case changes <- struct{}{}:
		default:
		}
	}

	// Expiry sweep.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for range t.C {
			if cat.expire() {
				notify()
			}
		}
	}()

	// Reader loop with reconnect.
	go func() {
		for {
			conn, err := c.dial()
			if err == nil {
				conn.SetReadDeadline(time.Now().Add(pongWait))
				conn.SetPongHandler(func(string) error {
					conn.SetReadDeadline(time.Now().Add(pongWait))
					return nil
				})
				go pingLoop(conn)
				conn.WriteJSON(subscribeMsg(sessionID))
				for {
					var m wsMessage
					if err := conn.ReadJSON(&m); err != nil {
						break
					}
					conn.SetReadDeadline(time.Now().Add(pongWait))
					if m.Type == "message" {
						cat.add(&m)
						notify()
					}
				}
				conn.Close()
			}
			time.Sleep(time.Second)
		}
	}()

	render(cat, filters, jsonOut)
	for {
		select {
		case <-changes:
			if !jsonOut {
				fmt.Print("\033[H\033[2J") // clear screen between renders
			}
			render(cat, filters, jsonOut)
		case <-stop:
			return
		}
	}
}

// --- catalog ---

type catalog struct {
	items map[string]*entry // key: service/instance
}

type entry struct {
	desc      Descriptor
	publisher string
	auth      string
	seen      time.Time
	ttl       time.Duration
}

func newCatalog() *catalog { return &catalog{items: map[string]*entry{}} }

func (c *catalog) add(m *wsMessage) {
	raw, err := base64.StdEncoding.DecodeString(m.Data)
	if err != nil {
		return
	}
	var d Descriptor
	if err := json.Unmarshal(raw, &d); err != nil || d.SchemaVersion != 1 {
		return
	}
	ttl := time.Duration(jsonInt(raw, "ttl_seconds")) * time.Second
	if ttl <= 0 {
		ttl = 90 * time.Second
	}
	c.items[d.Service+"/"+d.Instance] = &entry{
		desc:      d,
		publisher: m.Headers["cc-id"],
		auth:      m.Headers["cc-auth"],
		seen:      time.Now(),
		ttl:       ttl,
	}
}

func (c *catalog) expire() bool {
	changed := false
	for k, e := range c.items {
		if time.Since(e.seen) > e.ttl {
			delete(c.items, k)
			changed = true
		}
	}
	return changed
}

// --- rendering ---

type svcView struct {
	Service    string       `json:"service"`
	Instances  []string     `json:"instances"`
	Publishers []string     `json:"publishers"`
	RPCs       []rpcView    `json:"rpcs"`
	Entities   []entityView `json:"entities,omitempty"`
}

type rpcView struct {
	Subject     string `json:"subject"`
	Description string `json:"description,omitempty"`
	TimeoutMs   int    `json:"timeout_ms,omitempty"`
	// Addressing is "" (service) or "instance"/"entity". For the latter, Route is
	// the subject_template (with {instance}/{entity}) and EntityType names the type.
	Addressing string `json:"addressing,omitempty"`
	Route      string `json:"route,omitempty"`
	EntityType string `json:"entity_type,omitempty"`
}

type entityView struct {
	Type     string `json:"type"`
	IDFormat string `json:"id_format,omitempty"`
	ListRPC  string `json:"list_rpc,omitempty"`
	PlaceRPC string `json:"place_rpc,omitempty"`
}

func render(c *catalog, filters []string, jsonOut bool) {
	// Group instances by service.
	byService := map[string][]*entry{}
	for _, e := range c.items {
		if len(filters) > 0 && !contains(filters, e.desc.Service) {
			continue
		}
		byService[e.desc.Service] = append(byService[e.desc.Service], e)
	}

	services := make([]string, 0, len(byService))
	for s := range byService {
		services = append(services, s)
	}
	sort.Strings(services)

	views := make([]svcView, 0, len(services))
	for _, s := range services {
		entries := byService[s]
		sort.Slice(entries, func(i, j int) bool { return entries[i].desc.Instance < entries[j].desc.Instance })

		v := svcView{Service: s}
		rpcs := map[string]rpcView{}
		ents := map[string]entityView{}
		pubs := map[string]bool{}
		for _, e := range entries {
			v.Instances = append(v.Instances, e.desc.Instance)
			p := e.publisher
			if p == "" {
				p = "(unverified)"
			} else if e.auth != "" {
				p = p + " (" + e.auth + ")"
			}
			pubs[p] = true
			for _, r := range e.desc.RPCs {
				rv := rpcView{
					Subject: r.Subject, Description: r.Description, TimeoutMs: r.TimeoutMs,
					Addressing: r.Addressing, Route: r.SubjectTemplate, EntityType: r.EntityType,
				}
				// Key on the routable form so a template shows once across instances.
				key := r.SubjectTemplate
				if key == "" {
					key = r.Subject
				}
				rpcs[key] = rv
			}
			for _, ent := range e.desc.Entities {
				ents[ent.Type] = entityView{Type: ent.Type, IDFormat: ent.IDFormat, ListRPC: ent.ListRPC, PlaceRPC: ent.PlaceRPC}
			}
		}
		for p := range pubs {
			v.Publishers = append(v.Publishers, p)
		}
		sort.Strings(v.Publishers)
		v.RPCs = sortedValues(rpcs, func(r rpcView) string { return r.Subject + r.Route })
		v.Entities = sortedValues(ents, func(e entityView) string { return e.Type })
		views = append(views, v)
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(views)
		return
	}

	if len(views) == 0 {
		fmt.Println("no services advertised")
		return
	}
	for _, v := range views {
		plural := "s"
		if len(v.Instances) == 1 {
			plural = ""
		}
		fmt.Printf("%s — %d instance%s, published by %s\n",
			v.Service, len(v.Instances), plural, strings.Join(v.Publishers, ", "))
		for _, r := range v.RPCs {
			// Show the routable form: template for entity/instance RPCs, else subject.
			shown := r.Subject
			if r.Route != "" {
				shown = r.Route
			}
			line := "  " + shown
			switch r.Addressing {
			case "entity":
				line += "  [entity:" + r.EntityType + "]"
			case "instance":
				line += "  [instance]"
			}
			if r.Description != "" {
				line += "  — " + r.Description
			}
			if r.TimeoutMs > 0 {
				line += fmt.Sprintf("  (timeout %dms)", r.TimeoutMs)
			}
			fmt.Println(line)
		}
		for _, ent := range v.Entities {
			line := "  entity " + ent.Type
			if ent.IDFormat != "" {
				line += " (id " + ent.IDFormat + ")"
			}
			var via []string
			if ent.ListRPC != "" {
				via = append(via, "list="+ent.ListRPC)
			}
			if ent.PlaceRPC != "" {
				via = append(via, "place="+ent.PlaceRPC)
			}
			if len(via) > 0 {
				line += "  " + strings.Join(via, " ")
			}
			fmt.Println(line)
		}
		fmt.Printf("  instances: %s\n\n", strings.Join(v.Instances, ", "))
	}
}

// sortedValues returns a map's values sorted by the given key function.
func sortedValues[V any](m map[string]V, key func(V) string) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return key(out[i]) < key(out[j]) })
	return out
}

// --- transport helpers (self-contained; mirror the other cmd/ clients) ---

type client struct {
	baseURL, token string
}

type wsMessage struct {
	Type    string            `json:"type"`
	Data    string            `json:"data"`
	Error   string            `json:"error"`
	Headers map[string]string `json:"headers"`
}

func subscribeMsg(sessionID string) map[string]any {
	return map[string]any{"action": "subscribe", "subject": "discovery.service.>", "session_id": sessionID}
}

func pingLoop(conn *websocket.Conn) {
	t := time.NewTicker(pingPeriod)
	defer t.Stop()
	for range t.C {
		if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
			return
		}
	}
}

func (c *client) dial() (*websocket.Conn, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), http.Header{"Authorization": {"Bearer " + c.token}})
	return conn, err
}

func (c *client) teardown(sessionID string) {
	req, err := http.NewRequest(http.MethodDelete, c.baseURL+"/v1/sessions/"+url.PathEscape(sessionID), nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

// jsonInt pulls a top-level integer field out of a raw JSON object without a
// dedicated struct field (used for ttl_seconds).
func jsonInt(raw []byte, key string) int {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return 0
	}
	var n int
	if v, ok := m[key]; ok {
		json.Unmarshal(v, &n)
	}
	return n
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
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

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
