package main

import (
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
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type wsMsg struct {
	Action       string            `json:"action,omitempty"`
	Type         string            `json:"type,omitempty"`
	Subject      string            `json:"subject,omitempty"`
	Data         string            `json:"data,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	ReplySubject string            `json:"reply_subject,omitempty"`
	Error        string            `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: req <subject> [json-file|-]\n")
		fmt.Fprintf(os.Stderr, "\nSend a JSON document to a subject and wait for a reply.\n")
		fmt.Fprintf(os.Stderr, "Reads from stdin if no file is given or if '-' is specified.\n")
		fmt.Fprintf(os.Stderr, "\nEnvironment:\n")
		fmt.Fprintf(os.Stderr, "  CHITCHAT_URL    gateway URL (default: https://chitchat.miren.garden)\n")
		fmt.Fprintf(os.Stderr, "  CHITCHAT_TOKEN  API key or JWT (default: from \"multipass token\")\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		fmt.Fprintf(os.Stderr, "  -t <seconds>    timeout (default: 30)\n")
		os.Exit(1)
	}

	args := os.Args[1:]
	timeout := 30 * time.Second

	// Parse -t flag.
	for i := 0; i < len(args)-1; i++ {
		if args[i] == "-t" {
			var secs int
			if _, err := fmt.Sscanf(args[i+1], "%d", &secs); err == nil && secs > 0 {
				timeout = time.Duration(secs) * time.Second
			}
			args = append(args[:i], args[i+2:]...)
			break
		}
	}

	if len(args) < 1 {
		log.Fatal("subject is required")
	}

	subject := args[0]

	// Read JSON payload.
	var payload []byte
	var err error
	if len(args) < 2 || args[1] == "-" {
		payload, err = io.ReadAll(os.Stdin)
	} else {
		payload, err = os.ReadFile(args[1])
	}
	if err != nil {
		log.Fatalf("read input: %v", err)
	}

	// Validate it's JSON.
	if !json.Valid(payload) {
		log.Fatal("input is not valid JSON")
	}

	baseURL := os.Getenv("CHITCHAT_URL")
	if baseURL == "" {
		baseURL = "https://chitchat.miren.garden"
	}

	token, err := resolveToken(os.Getenv("CHITCHAT_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}

	conn, err := dial(baseURL, token)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	inbox := "inbox." + generateID()

	// Subscribe to inbox.
	if err := writeMsg(conn, wsMsg{Action: "subscribe", Subject: inbox}); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	resp, err := readMsg(conn)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	if resp.Type == "error" {
		log.Fatalf("subscribe failed: %s", resp.Error)
	}

	// Publish with reply_subject.
	encoded := base64.StdEncoding.EncodeToString(payload)
	if err := writeMsg(conn, wsMsg{
		Action:       "publish",
		Subject:      subject,
		Data:         encoded,
		ReplySubject: inbox,
	}); err != nil {
		log.Fatalf("publish: %v", err)
	}
	resp, err = readMsg(conn)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	if resp.Type == "error" {
		log.Fatalf("publish failed: %s", resp.Error)
	}

	// Wait for reply.
	conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		resp, err = readMsg(conn)
		if err != nil {
			log.Fatalf("timeout waiting for reply: %v", err)
		}
		if resp.Type == "message" && resp.Subject == inbox {
			break
		}
	}

	data, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		log.Fatalf("decode reply: %v", err)
	}

	// Pretty-print if it's JSON, otherwise raw.
	var pretty json.RawMessage
	if json.Unmarshal(data, &pretty) == nil {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(pretty)
	} else {
		os.Stdout.Write(data)
		os.Stdout.Write([]byte("\n"))
	}
}

func dial(baseURL, token string) (*websocket.Conn, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/ws"

	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), header)
	return conn, err
}

func writeMsg(conn *websocket.Conn, msg wsMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

func readMsg(conn *websocket.Conn) (wsMsg, error) {
	var msg wsMsg
	err := conn.ReadJSON(&msg)
	return msg, err
}

func resolveToken(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	out, err := exec.Command("multipass", "token").Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("multipass token: %s", strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("multipass token: %w (set CHITCHAT_TOKEN or install multipass CLI)", err)
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", errors.New("multipass token returned empty output")
	}
	return tok, nil
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
