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
	"sync"

	"github.com/gorilla/websocket"
)

type wsOutbound struct {
	Action  string            `json:"action"`
	Subject string            `json:"subject,omitempty"`
	Data    string            `json:"data,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type wsInbound struct {
	Type    string            `json:"type"`
	Subject string            `json:"subject,omitempty"`
	Data    string            `json:"data,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Error   string            `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: pipe <key>\n")
		fmt.Fprintf(os.Stderr, "\nCreates a bidirectional pipe keyed on an arbitrary string.\n")
		fmt.Fprintf(os.Stderr, "Stdin is sent to all other instances with the same key,\n")
		fmt.Fprintf(os.Stderr, "and their stdin appears on your stdout.\n")
		fmt.Fprintf(os.Stderr, "\nEnvironment:\n")
		fmt.Fprintf(os.Stderr, "  CHITCHAT_URL    gateway URL (default: https://chitchat.miren.garden)\n")
		fmt.Fprintf(os.Stderr, "  CHITCHAT_TOKEN  API key or JWT (default: from \"multipass token\")\n")
		os.Exit(1)
	}

	key := os.Args[1]
	subject := "pipe." + key

	baseURL := os.Getenv("CHITCHAT_URL")
	if baseURL == "" {
		baseURL = "https://chitchat.miren.garden"
	}

	token, err := resolveToken(os.Getenv("CHITCHAT_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}

	id := generateID()

	conn, err := dial(baseURL, token)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	if err := sendJSON(conn, wsOutbound{Action: "subscribe", Subject: subject}); err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	// Wait for subscribe confirmation.
	var resp wsInbound
	if err := conn.ReadJSON(&resp); err != nil {
		log.Fatalf("read subscribe response: %v", err)
	}
	if resp.Type == "error" {
		log.Fatalf("subscribe failed: %s", resp.Error)
	}

	fmt.Fprintf(os.Stderr, "pipe: connected on key %q\n", key)

	var wg sync.WaitGroup
	wg.Add(2)

	// Read from websocket, write to stdout.
	go func() {
		defer wg.Done()
		for {
			var msg wsInbound
			if err := conn.ReadJSON(&msg); err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					return
				}
				log.Fatalf("read: %v", err)
			}
			if msg.Type != "message" {
				continue
			}
			if msg.Headers["X-Pipe-Sender"] == id {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(msg.Data)
			if err != nil {
				continue
			}
			os.Stdout.Write(data)
		}
	}()

	// Read from stdin, publish to websocket.
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			n, err := os.Stdin.Read(buf)
			if n > 0 {
				encoded := base64.StdEncoding.EncodeToString(buf[:n])
				werr := sendJSON(conn, wsOutbound{
					Action:  "publish",
					Subject: subject,
					Data:    encoded,
					Headers: map[string]string{"X-Pipe-Sender": id},
				})
				if werr != nil {
					log.Fatalf("publish: %v", werr)
				}
			}
			if err == io.EOF {
				conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			if err != nil {
				log.Fatalf("stdin: %v", err)
			}
		}
	}()

	wg.Wait()
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

func sendJSON(conn *websocket.Conn, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
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
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
