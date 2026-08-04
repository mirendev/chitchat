package chitchat

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
)

// streamChunkSize is how much of the request body goes in each stream_send.
const streamChunkSize = 512 * 1024

// uploadState routes an in-flight streamed upload's response frames (keyed by
// stream_id) back to the blocked Upload call.
type uploadState struct {
	started chan string   // upload_started -> the gateway-minted upload subject
	ready   chan struct{} // upload_ready -> the responder has subscribed
	chunks  chan []byte   // response stream_chunk payloads
	done    chan error    // stream_end (nil) / stream_error (err)
}

// fail delivers a terminal error to a blocked Upload without blocking the
// caller. done is buffered, and a second terminal is redundant either way.
func (u *uploadState) fail(err error) {
	select {
	case u.done <- err:
	default:
	}
}

// Upload performs a streaming request_upload to subject: it sends meta as the
// request payload, streams r as the request body (pipelined stream_send chunks
// with no per-chunk reply), then returns the responder's reply bytes. The whole
// exchange is bounded by ctx.
//
// This exists for large bodies, where the staged request/reply write path would
// pay a handshake per part.
func (c *Client) Upload(ctx context.Context, subject string, meta []byte, r io.Reader) ([]byte, error) {
	sid, err := newStreamID()
	if err != nil {
		return nil, err
	}
	u := &uploadState{
		started: make(chan string, 1),
		ready:   make(chan struct{}, 1),
		chunks:  make(chan []byte, 8),
		done:    make(chan error, 1),
	}
	c.mu.Lock()
	c.uploads[sid] = u
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.uploads, sid)
		c.mu.Unlock()
	}()

	if err := c.sendActionCtx(ctx, map[string]any{
		"action":    "request_upload",
		"subject":   subject,
		"data":      base64.StdEncoding.EncodeToString(meta),
		"stream_id": sid,
	}); err != nil {
		return nil, err
	}

	// The gateway mints the upload subject and returns it on upload_started.
	var uploadSubject string
	select {
	case uploadSubject = <-u.started:
	case e := <-u.done:
		return nil, orStreamErr(e, "upload aborted before start")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Wait for the responder to subscribe (upload_ready) before streaming, so
	// core NATS doesn't drop the early chunks.
	select {
	case <-u.ready:
	case e := <-u.done:
		return nil, orStreamErr(e, "upload aborted before ready")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	buf := make([]byte, streamChunkSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			if err := c.sendActionCtx(ctx, map[string]any{
				"action":  "stream_send",
				"subject": uploadSubject,
				"data":    base64.StdEncoding.EncodeToString(buf[:n]),
			}); err != nil {
				return nil, err
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// Tell the responder the body is truncated rather than leaving it
			// waiting on a stream that will never end.
			_ = c.sendAction(map[string]any{
				"action": "stream_end", "subject": uploadSubject, "error": rerr.Error(),
			}, false)
			return nil, rerr
		}
	}
	if err := c.sendActionCtx(ctx, map[string]any{"action": "stream_end", "subject": uploadSubject}); err != nil {
		return nil, err
	}

	// Collect the responder's reply: stream_chunk(s) until the terminal.
	var resp bytes.Buffer
	for {
		select {
		case ch := <-u.chunks:
			resp.Write(ch)
		case e := <-u.done:
			if e != nil {
				return nil, e
			}
			for { // drain anything buffered ahead of the terminal
				select {
				case ch := <-u.chunks:
					resp.Write(ch)
				default:
					return resp.Bytes(), nil
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// uploadFor returns the in-flight upload for a stream id, if there is one.
func (c *Client) uploadFor(streamID string) *uploadState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.uploads[streamID]
}

// failAllUploads ends every in-flight upload with err. Uploads are ephemeral in
// the same way streams are: the gateway-minted subject dies with the connection,
// so a blocked Upload would otherwise wait out its context for a reply that can
// never arrive.
func (c *Client) failAllUploads(err error) {
	c.mu.Lock()
	uploads := c.uploads
	c.uploads = map[string]*uploadState{}
	c.mu.Unlock()
	for _, u := range uploads {
		u.fail(err)
	}
}

// orStreamErr returns e when it carries a reason, else a generic error with
// fallback. A nil terminal here means the stream ended before it should have.
func orStreamErr(e error, fallback string) error {
	if e != nil {
		return e
	}
	return errors.New("chitchat: " + fallback)
}
