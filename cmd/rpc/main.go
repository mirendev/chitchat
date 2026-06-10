// Command rpc is a general-purpose CLI for the RPCs that services advertise in
// chitchat's discovery catalog (see docs/SERVICE_DISCOVERY.md). It can list the
// available RPCs, describe an RPC's input/output schemas, and call one — the
// same way any service would.
//
//	rpc list [filter...]            # list advertised RPCs (optionally by service)
//	rpc describe <subject>          # show an RPC's request/response schemas
//	rpc call <subject> [payload]    # call an RPC; payload from arg, @file, or stdin
//
//	# entity/instance-addressed RPCs: resolve the subject_template
//	rpc call sandbox.exec --entity a1:enc-x '{"cmd":["ls"]}'
//	rpc call geo.lookup --instance geo-1 '{"address":"123 Main St"}'
//
// Flags: --json (list/describe), --instance / --entity / --timeout (call).
// Environment: CHITCHAT_URL (default https://chitchat.miren.garden),
// CHITCHAT_TOKEN (default from `multipass token`).
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type Descriptor struct {
	SchemaVersion int    `json:"schema_version"`
	Service       string `json:"service"`
	Instance      string `json:"instance"`
	RPCs          []RPC  `json:"rpcs"`
}

type RPC struct {
	Name            string          `json:"name"`
	Subject         string          `json:"subject"`
	Description     string          `json:"description"`
	TimeoutMs       int             `json:"timeout_ms"`
	Addressing      string          `json:"addressing"`
	SubjectTemplate string          `json:"subject_template"`
	EntityType      string          `json:"entity_type"`
	RequestSchema   json.RawMessage `json:"request_schema"`
	ResponseSchema  json.RawMessage `json:"response_schema"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "list", "ls":
		cmdList(args)
	case "describe", "desc", "show":
		cmdDescribe(args)
	case "call":
		cmdCall(args)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: rpc <command> [args]

  list [filter...]            list advertised RPCs (optionally filter by service)
  describe <subject>          show an RPC's request/response schemas
  call <subject> [payload]    call an RPC (payload: arg, @file, '-' for stdin, or piped stdin)

call flags: --instance <id>, --entity <id> (resolve a subject_template), --timeout <dur>
list/describe flags: --json
`)
}

// --- list ---

func cmdList(args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "output JSON")
	flags, filters := splitArgs(args, nil)
	fs.Parse(flags)

	descs := newClient().snapshot()

	// Collect RPCs grouped by service, deduped by callable form.
	type item struct {
		Service     string `json:"service"`
		Name        string `json:"name"`
		Call        string `json:"call"` // subject or subject_template
		Addressing  string `json:"addressing"`
		EntityType  string `json:"entity_type,omitempty"`
		Description string `json:"description,omitempty"`
	}
	byKey := map[string]item{}
	for _, d := range descs {
		if len(filters) > 0 && !contains(filters, d.Service) {
			continue
		}
		for _, r := range d.RPCs {
			it := item{
				Service: d.Service, Name: r.Name, Call: callForm(r),
				Addressing: addressing(r), EntityType: r.EntityType, Description: r.Description,
			}
			byKey[d.Service+"\x00"+it.Call] = it
		}
	}
	items := make([]item, 0, len(byKey))
	for _, it := range byKey {
		items = append(items, it)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Service != items[j].Service {
			return items[i].Service < items[j].Service
		}
		return items[i].Call < items[j].Call
	})

	if *jsonOut {
		writeJSON(items)
		return
	}
	if len(items) == 0 {
		fmt.Println("no RPCs advertised")
		return
	}
	var svc string
	for _, it := range items {
		if it.Service != svc {
			svc = it.Service
			fmt.Println(svc)
		}
		tag := it.Addressing
		if it.Addressing == "entity" {
			tag = "entity:" + it.EntityType
		}
		line := fmt.Sprintf("  %-34s %-16s", it.Call, tag)
		if it.Description != "" {
			line += " " + it.Description
		}
		fmt.Println(strings.TrimRight(line, " "))
	}
}

// --- describe ---

func cmdDescribe(args []string) {
	fs := flag.NewFlagSet("describe", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "output JSON")
	flags, pos := splitArgs(args, nil)
	fs.Parse(flags)
	if len(pos) != 1 {
		fail(errors.New("usage: rpc describe <subject>"))
	}
	target := pos[0]

	descs := newClient().snapshot()
	r, service := findRPC(descs, target)
	if r == nil {
		fail(fmt.Errorf("no advertised RPC matches %q (try `rpc list`)", target))
	}

	if *jsonOut {
		writeJSON(map[string]any{
			"service": service, "name": r.Name, "subject": r.Subject,
			"call": callForm(*r), "addressing": addressing(*r), "entity_type": r.EntityType,
			"timeout_ms": r.TimeoutMs, "description": r.Description,
			"request_schema": r.RequestSchema, "response_schema": r.ResponseSchema,
		})
		return
	}

	fmt.Printf("%s  (%s)\n", callForm(*r), addressing(*r))
	if r.Description != "" {
		fmt.Println("  " + r.Description)
	}
	fmt.Printf("  service: %s\n", service)
	if r.EntityType != "" {
		fmt.Printf("  entity:  %s\n", r.EntityType)
	}
	if r.TimeoutMs > 0 {
		fmt.Printf("  timeout: %dms\n", r.TimeoutMs)
	}
	fmt.Println("  request:")
	fmt.Println(indentSchema(r.RequestSchema, "    "))
	fmt.Println("  response:")
	fmt.Println(indentSchema(r.ResponseSchema, "    "))
}

// --- call ---

func cmdCall(args []string) {
	fs := flag.NewFlagSet("call", flag.ExitOnError)
	instance := fs.String("instance", "", "instance id (resolves {instance} in subject_template)")
	entity := fs.String("entity", "", "entity id (resolves {entity} in subject_template)")
	timeout := fs.Duration("timeout", 0, "override the RPC's timeout")
	flags, pos := splitArgs(args, map[string]bool{"instance": true, "entity": true, "timeout": true})
	fs.Parse(flags)
	if len(pos) < 1 {
		fail(errors.New("usage: rpc call <subject> [payload]"))
	}
	target := pos[0]

	payload, err := readPayload(pos[1:])
	if err != nil {
		fail(err)
	}

	c := newClient()

	// Look up the RPC for addressing + the default timeout (best-effort; an
	// unknown subject can still be called raw).
	subject := target
	timeoutMs := 5000
	if r, _ := findRPC(c.snapshot(), target); r != nil {
		if r.TimeoutMs > 0 {
			timeoutMs = r.TimeoutMs
		}
		if *instance != "" || *entity != "" {
			if r.SubjectTemplate == "" {
				fail(fmt.Errorf("RPC %q is not instance/entity-addressed (no subject_template)", target))
			}
			if *entity != "" {
				subject = strings.Replace(r.SubjectTemplate, "{entity}", *entity, 1)
			} else {
				subject = strings.Replace(r.SubjectTemplate, "{instance}", *instance, 1)
			}
		}
	} else if *instance != "" || *entity != "" {
		fail(fmt.Errorf("no advertised RPC matches %q, can't resolve --instance/--entity", target))
	}
	if *timeout > 0 {
		timeoutMs = int(timeout.Milliseconds())
	}

	out, status, err := c.call(subject, payload, timeoutMs)
	if err != nil {
		fail(err)
	}
	switch status {
	case "503":
		fail(fmt.Errorf("no instance is serving %q (no responders)", subject))
	case "504":
		fail(fmt.Errorf("request to %q timed out after %dms", subject, timeoutMs))
	}
	os.Stdout.Write(prettyJSON(out))
	fmt.Println()
}

func readPayload(rest []string) ([]byte, error) {
	if len(rest) == 0 {
		// Read stdin if it's piped; otherwise default to an empty object.
		if fi, _ := os.Stdin.Stat(); fi != nil && fi.Mode()&os.ModeCharDevice == 0 {
			return io.ReadAll(os.Stdin)
		}
		return []byte("{}"), nil
	}
	arg := rest[0]
	switch {
	case arg == "-":
		return io.ReadAll(os.Stdin)
	case strings.HasPrefix(arg, "@"):
		return os.ReadFile(arg[1:])
	default:
		return []byte(arg), nil
	}
}

// --- catalog helpers ---

func addressing(r RPC) string {
	if r.Addressing == "" {
		return "service"
	}
	return r.Addressing
}

// callForm is the subject a caller routes with: the template for instance/entity
// RPCs, else the plain subject.
func callForm(r RPC) string {
	if r.SubjectTemplate != "" {
		return r.SubjectTemplate
	}
	return r.Subject
}

// findRPC locates an advertised RPC by its subject (stem), its callable form, or
// its service-qualified name. Returns the RPC and its service.
func findRPC(descs []Descriptor, target string) (*RPC, string) {
	for _, d := range descs {
		for i := range d.RPCs {
			r := d.RPCs[i]
			if target == r.Subject || target == callForm(r) ||
				target == r.Name || target == d.Service+"."+r.Name {
				return &r, d.Service
			}
		}
	}
	return nil, ""
}

// --- transport (self-contained; mirrors the other cmd/ clients) ---

type client struct {
	baseURL, token string
}

func newClient() *client {
	base := os.Getenv("CHITCHAT_URL")
	if base == "" {
		base = "https://chitchat.miren.garden"
	}
	token, err := resolveToken(os.Getenv("CHITCHAT_TOKEN"))
	if err != nil {
		fail(err)
	}
	return &client{baseURL: strings.TrimRight(base, "/"), token: token}
}

// snapshot subscribes to the discovery catalog with a fresh session_id, drains
// the DeliverAll replay until idle, and returns the descriptors.
func (c *client) snapshot() []Descriptor {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		fail(err)
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), http.Header{"Authorization": {"Bearer " + c.token}})
	if err != nil {
		fail(fmt.Errorf("connect: %w", err))
	}
	defer conn.Close()

	session := "rpc-" + randHex(8)
	conn.WriteJSON(map[string]any{"action": "subscribe", "subject": "discovery.service.>", "session_id": session})
	defer c.teardown(session)

	var descs []Descriptor
	const settle = 600 * time.Millisecond
	conn.SetReadDeadline(time.Now().Add(settle))
	for {
		var m struct {
			Type, Data, Error string
		}
		if err := conn.ReadJSON(&m); err != nil {
			break
		}
		conn.SetReadDeadline(time.Now().Add(settle))
		if m.Type == "error" {
			fail(fmt.Errorf("subscribe rejected: %s", m.Error))
		}
		if m.Type == "message" {
			raw, err := base64.StdEncoding.DecodeString(m.Data)
			if err != nil {
				continue
			}
			var d Descriptor
			if json.Unmarshal(raw, &d) == nil && d.SchemaVersion == 1 {
				descs = append(descs, d)
			}
		}
	}
	return descs
}

// call sends a request and returns (responseData, statusHeader, err). statusHeader
// is "503" for no-responders, "504" for a gateway timeout, else "".
func (c *client) call(subject string, payload []byte, timeoutMs int) ([]byte, string, error) {
	body, _ := json.Marshal(map[string]any{
		"subject": subject, "timeout_ms": timeoutMs,
		"data": base64.StdEncoding.EncodeToString(payload),
	})
	req, _ := http.NewRequest(http.MethodPost, c.baseURL+"/v1/request", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusGatewayTimeout {
		return nil, "504", nil
	}
	var out struct {
		Data    string            `json:"data"`
		Headers map[string]string `json:"headers"`
		Error   string            `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, "", fmt.Errorf("decode response: %s", strings.TrimSpace(string(raw)))
	}
	if out.Error != "" {
		return nil, "", errors.New(out.Error)
	}
	if out.Data == "" && out.Headers["Status"] == "503" {
		return nil, "503", nil
	}
	data, err := base64.StdEncoding.DecodeString(out.Data)
	if err != nil {
		return nil, "", fmt.Errorf("decode data: %w", err)
	}
	return data, "", nil
}

func (c *client) teardown(session string) {
	req, err := http.NewRequest(http.MethodDelete, c.baseURL+"/v1/sessions/"+url.PathEscape(session), nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
}

// --- small helpers ---

func indentSchema(raw json.RawMessage, prefix string) string {
	if len(raw) == 0 {
		return prefix + "(none)"
	}
	var buf bytes.Buffer
	if json.Indent(&buf, raw, prefix, "  ") != nil {
		return prefix + string(raw)
	}
	return prefix + buf.String()
}

func prettyJSON(raw []byte) []byte {
	var buf bytes.Buffer
	if json.Indent(&buf, raw, "", "  ") != nil {
		return raw
	}
	return buf.Bytes()
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// splitArgs separates flags (with their values) from positionals so flags may
// appear after the subject — Go's flag package stops at the first positional.
// valueFlags names the flags that take a following value (e.g. "entity").
func splitArgs(args []string, valueFlags map[string]bool) (flags, pos []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" { // everything after is positional
			pos = append(pos, args[i+1:]...)
			break
		}
		if len(a) > 1 && strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			name := strings.TrimLeft(a, "-")
			if !strings.Contains(a, "=") && valueFlags[name] && i+1 < len(args) {
				i++
				flags = append(flags, args[i])
			}
			continue
		}
		pos = append(pos, a)
	}
	return
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
