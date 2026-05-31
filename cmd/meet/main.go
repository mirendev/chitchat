package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"miren.dev/mflags"
)

type GlobalFlags struct {
	_        struct{} `group:"Global Options"`
	Endpoint string   `long:"endpoint" env:"CHITCHAT_URL" default:"https://chitchat.miren.garden" usage:"Chitchat gateway URL"`
	Token    string   `long:"token" env:"CHITCHAT_TOKEN" usage:"API key or JWT (default: from \"multipass token\")"`
}

type PingConfig struct {
	GlobalFlags
}

type PostConfig struct {
	GlobalFlags
	Channel  string   `long:"channel" short:"c" required:"true" usage:"Slack channel ID"`
	ThreadTS string   `long:"thread" short:"t" usage:"Thread timestamp — reply in a thread"`
	Blocks   string   `long:"blocks" usage:"JSON file with Block Kit blocks (- for stdin)"`
	Text     []string `rest:"true" usage:"Message text"`
}

type UpdateConfig struct {
	GlobalFlags
	Channel string   `long:"channel" short:"c" required:"true" usage:"Slack channel ID"`
	TS      string   `long:"ts" required:"true" usage:"Timestamp of the message to update"`
	Blocks  string   `long:"blocks" usage:"JSON file with Block Kit blocks (- for stdin)"`
	Text    []string `rest:"true" usage:"New message text"`
}

func main() {
	d := mflags.NewDispatcher("meet")

	d.Dispatch("ping", mflags.Infer(runPing, mflags.WithUsage("Check if meet is available")))
	d.Dispatch("post", mflags.Infer(runPost,
		mflags.WithUsage("Post a message to Slack"),
		mflags.WithExamples(
			mflags.Example{Name: "Simple message", Body: "meet post -c C08RJNM3V1U Hello world"},
			mflags.Example{Name: "Thread reply", Body: "meet post -c C08RJNM3V1U -t 1717012300.000100 Still working..."},
			mflags.Example{Name: "With blocks", Body: "meet post -c C08RJNM3V1U --blocks blocks.json Fallback text"},
			mflags.Example{Name: "From stdin", Body: "echo 'Deploy complete' | meet post -c C08RJNM3V1U"},
		),
	))
	d.Dispatch("update", mflags.Infer(runUpdate,
		mflags.WithUsage("Update an existing Slack message"),
		mflags.WithExamples(
			mflags.Example{Name: "Update text", Body: "meet update -c C08RJNM3V1U --ts 1717012345.000100 Updated text"},
		),
	))

	if err := d.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runPing(c *PingConfig) error {
	resp, err := request(&c.GlobalFlags, "meet.ping", map[string]any{})
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func runPost(c *PostConfig) error {
	text := strings.Join(c.Text, " ")
	if text == "" && c.Blocks == "" {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		text = strings.TrimSpace(string(b))
	}

	payload := map[string]any{
		"channel": c.Channel,
	}
	if text != "" {
		payload["text"] = text
	}
	if c.ThreadTS != "" {
		payload["thread_ts"] = c.ThreadTS
	}
	if c.Blocks != "" {
		blocks, err := readBlocks(c.Blocks)
		if err != nil {
			return err
		}
		payload["blocks"] = blocks
	}

	if text == "" && c.Blocks == "" {
		return errors.New("text or --blocks is required")
	}

	resp, err := request(&c.GlobalFlags, "meet.slack.post", payload)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

func runUpdate(c *UpdateConfig) error {
	text := strings.Join(c.Text, " ")

	payload := map[string]any{
		"channel": c.Channel,
		"ts":      c.TS,
	}
	if text != "" {
		payload["text"] = text
	}
	if c.Blocks != "" {
		blocks, err := readBlocks(c.Blocks)
		if err != nil {
			return err
		}
		payload["blocks"] = blocks
	}

	if text == "" && c.Blocks == "" {
		return errors.New("text or --blocks is required")
	}

	resp, err := request(&c.GlobalFlags, "meet.slack.update", payload)
	if err != nil {
		return err
	}
	printJSON(resp)
	return nil
}

// --- Chitchat request-reply ---

func request(g *GlobalFlags, subject string, payload any) (json.RawMessage, error) {
	token, err := resolveToken(g.Token)
	if err != nil {
		return nil, err
	}

	endpoint := g.Endpoint
	if endpoint == "" {
		endpoint = "https://chitchat.miren.garden"
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	reqBody := map[string]any{
		"subject":    subject,
		"data":       base64.StdEncoding.EncodeToString(payloadJSON),
		"timeout_ms": 10000,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := strings.TrimRight(endpoint, "/") + "/v1/request"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			return nil, fmt.Errorf("%s: %s", resp.Status, errResp.Error)
		}
		return nil, fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}

	var result struct {
		Data string `json:"data"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	decoded, err := base64.StdEncoding.DecodeString(result.Data)
	if err != nil {
		return nil, fmt.Errorf("decode data: %w", err)
	}

	return json.RawMessage(decoded), nil
}

// --- Helpers ---

func readBlocks(path string) (json.RawMessage, error) {
	var data []byte
	var err error
	if path == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read blocks: %w", err)
	}
	if !json.Valid(data) {
		return nil, errors.New("blocks file is not valid JSON")
	}
	return json.RawMessage(data), nil
}

func printJSON(data json.RawMessage) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(data)
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
