package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	writeWait  = 10 * time.Second

	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

type wsMsg struct {
	Action  string `json:"action,omitempty"`
	Type    string `json:"type,omitempty"`
	Subject string `json:"subject,omitempty"`
	Data    string `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
}

func main() {
	subject := "announce.>"
	if len(os.Args) > 1 {
		subject = os.Args[1]
	}

	baseURL := os.Getenv("CHITCHAT_URL")
	if baseURL == "" {
		baseURL = "https://chitchat.miren.garden"
	}

	token, err := resolveToken(os.Getenv("CHITCHAT_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Fprintf(os.Stderr, "listening on %s\n", subject)

	// Reconnect loop: the connection can drop (intermediary timeout, network
	// blip, gateway restart) and we want subscriptions to survive it. Each
	// attempt redials and resubscribes; backoff resets once we're connected.
	backoff := minBackoff
	for {
		connected, err := listen(ctx, baseURL, token, subject)
		if ctx.Err() != nil {
			return
		}
		if connected {
			backoff = minBackoff
		}
		fmt.Fprintf(os.Stderr, "disconnected: %v; reconnecting in %s\n", err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// listen runs one connection's lifetime. It returns whether the subscription
// was successfully established (so the caller can reset backoff) and the error
// that ended the connection.
func listen(ctx context.Context, baseURL, token, subject string) (bool, error) {
	conn, err := dial(baseURL, token)
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	// Close the connection when the context is cancelled so a blocked read unwinds.
	go func() {
		<-ctx.Done()
		conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(writeWait))
		conn.Close()
	}()

	// Keepalive: detect a dead/wedged gateway within pongWait and refresh the
	// read deadline on every pong and inbound frame.
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	msg, _ := json.Marshal(wsMsg{Action: "subscribe", Subject: subject})
	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		return false, fmt.Errorf("subscribe: %w", err)
	}

	var resp wsMsg
	if err := conn.ReadJSON(&resp); err != nil {
		return false, fmt.Errorf("read subscribe response: %w", err)
	}
	if resp.Type == "error" {
		return false, fmt.Errorf("subscribe failed: %s", resp.Error)
	}
	conn.SetReadDeadline(time.Now().Add(pongWait))

	done := make(chan struct{})
	defer close(done)
	go pingLoop(conn, done)

	for {
		var m wsMsg
		if err := conn.ReadJSON(&m); err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return true, errors.New("connection closed")
			}
			return true, err
		}
		conn.SetReadDeadline(time.Now().Add(pongWait))
		if m.Type != "message" {
			continue
		}

		data, err := base64.StdEncoding.DecodeString(m.Data)
		if err != nil {
			continue
		}

		// Pretty-print JSON payloads, raw otherwise.
		var pretty json.RawMessage
		if json.Unmarshal(data, &pretty) == nil {
			formatted, _ := json.MarshalIndent(pretty, "  ", "  ")
			fmt.Printf("[%s]\n  %s\n", m.Subject, formatted)
		} else {
			fmt.Printf("[%s] %s\n", m.Subject, data)
		}
	}
}

func pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				return
			}
		case <-done:
			return
		}
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
