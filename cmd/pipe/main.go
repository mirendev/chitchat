package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c := &client{
		baseURL: baseURL,
		token:   token,
		subject: subject,
		id:      generateID(),
		ctx:     ctx,
	}

	if err := c.connect(); err != nil {
		log.Fatalf("connect: %v", err)
	}
	fmt.Fprintf(os.Stderr, "pipe: connected on key %q\n", key)

	// Close the live connection when shutting down so blocked reads/writes unwind.
	go func() {
		<-ctx.Done()
		if ws, _ := c.current(); ws != nil {
			ws.Close()
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.readLoop() }()
	go func() { defer wg.Done(); c.writeLoop(cancel) }()
	wg.Wait()
}

// client owns a single live websocket connection and transparently reconnects
// (and resubscribes) when it drops, so the pipe survives intermediary timeouts,
// network blips, and gateway restarts.
type client struct {
	baseURL, token, subject, id string
	ctx                         context.Context

	mu      sync.Mutex
	ws      *websocket.Conn
	gen     uint64
	pingEnd chan struct{}

	reMu sync.Mutex // serializes reconnects across the read and write loops
}

// connect dials, subscribes, installs keepalive handlers, and swaps the new
// connection in as current.
func (c *client) connect() error {
	ws, err := dial(c.baseURL, c.token)
	if err != nil {
		return err
	}

	ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	ws.SetWriteDeadline(time.Now().Add(writeWait))
	if err := ws.WriteJSON(wsOutbound{Action: "subscribe", Subject: c.subject}); err != nil {
		ws.Close()
		return err
	}
	var resp wsInbound
	if err := ws.ReadJSON(&resp); err != nil {
		ws.Close()
		return fmt.Errorf("read subscribe response: %w", err)
	}
	if resp.Type == "error" {
		ws.Close()
		return fmt.Errorf("subscribe failed: %s", resp.Error)
	}
	ws.SetReadDeadline(time.Now().Add(pongWait))

	pingEnd := make(chan struct{})
	go pingLoop(ws, pingEnd)

	c.mu.Lock()
	c.ws = ws
	c.gen++
	c.pingEnd = pingEnd
	c.mu.Unlock()
	return nil
}

func (c *client) current() (*websocket.Conn, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ws, c.gen
}

// reconnect replaces the connection identified by staleGen. If another loop
// already reconnected (the generation moved on), it returns immediately.
func (c *client) reconnect(staleGen uint64) error {
	c.reMu.Lock()
	defer c.reMu.Unlock()

	if _, gen := c.current(); gen != staleGen {
		return nil
	}

	c.mu.Lock()
	old, oldPing := c.ws, c.pingEnd
	c.ws, c.pingEnd = nil, nil
	c.mu.Unlock()
	if oldPing != nil {
		close(oldPing)
	}
	if old != nil {
		old.Close()
	}

	backoff := minBackoff
	for {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		if err := c.connect(); err == nil {
			fmt.Fprintln(os.Stderr, "pipe: reconnected")
			return nil
		} else {
			fmt.Fprintf(os.Stderr, "pipe: reconnect failed: %v; retrying in %s\n", err, backoff)
		}
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// readLoop reads from the websocket and writes payloads to stdout, reconnecting on error.
func (c *client) readLoop() {
	for {
		if c.ctx.Err() != nil {
			return
		}
		ws, gen := c.current()
		if ws == nil {
			if err := c.reconnect(gen); err != nil {
				return
			}
			continue
		}

		var msg wsInbound
		if err := ws.ReadJSON(&msg); err != nil {
			if c.ctx.Err() != nil {
				return
			}
			if err := c.reconnect(gen); err != nil {
				return
			}
			continue
		}
		ws.SetReadDeadline(time.Now().Add(pongWait))

		if msg.Type != "message" {
			continue
		}
		if msg.Headers["X-Pipe-Sender"] == c.id {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(msg.Data)
		if err != nil {
			continue
		}
		os.Stdout.Write(data)
	}
}

// writeLoop reads stdin and publishes each chunk, reconnecting and retrying on error.
func (c *client) writeLoop(cancel context.CancelFunc) {
	defer cancel()

	buf := make([]byte, 32*1024)
	for {
		n, err := os.Stdin.Read(buf)
		if n > 0 {
			out := wsOutbound{
				Action:  "publish",
				Subject: c.subject,
				Data:    base64.StdEncoding.EncodeToString(buf[:n]),
				Headers: map[string]string{"X-Pipe-Sender": c.id},
			}
			for {
				if c.ctx.Err() != nil {
					return
				}
				ws, gen := c.current()
				if ws != nil {
					ws.SetWriteDeadline(time.Now().Add(writeWait))
					if werr := ws.WriteJSON(out); werr == nil {
						break
					}
				}
				if rerr := c.reconnect(gen); rerr != nil {
					return
				}
			}
		}
		if err == io.EOF {
			if ws, _ := c.current(); ws != nil {
				ws.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(writeWait))
			}
			return
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "pipe: stdin: %v\n", err)
			return
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

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
