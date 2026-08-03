package chitchat

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
)

// headerStreamCancel is the header (set only by the gateway) telling a responder
// which subject to watch for a cancel signal on a streaming request.
const headerStreamCancel = "cc-stream-cancel"

// streamBuffer bounds a consumer channel. A full buffer applies backpressure up
// the chain; a consumer that stops reading is unblocked via the done channel.
const streamBuffer = 256

// Sentinels distinguishing why a stream ended without a clean stream_end.
var (
	// ErrStreamingUnsupported means the gateway rejected request_stream because
	// it predates the streaming protocol. Callers may fall back to another path.
	ErrStreamingUnsupported = errors.New("chitchat: gateway does not support streaming")

	// ErrStreamDisconnected means the connection dropped mid-stream. Streams are
	// ephemeral, so the caller should re-request.
	ErrStreamDisconnected = errors.New("chitchat: connection lost mid-stream")
)

// StreamFrame is one delivery on a streaming response: either a data chunk or,
// as the final delivery, a non-nil Err. The channel is closed after the stream
// ends, cleanly or after an Err frame.
type StreamFrame struct {
	Data []byte
	Err  error
}

// streamSub is the consumer side of an in-flight RequestStream.
type streamSub struct {
	ch chan StreamFrame
	// done is closed when the consumer's context is cancelled, so the read loop
	// never blocks forwarding chunks to a consumer that has stopped reading.
	done chan struct{}
	// fin is closed once the stream reaches a terminal, so the cancel watcher
	// exits instead of parking until the caller's context eventually fires.
	fin chan struct{}

	// mu serializes delivery against closure. Today the callers happen not to
	// overlap (deliverStream and the ErrStreamingUnsupported path both run on
	// the read goroutine, and the disconnect path runs only after readLoop has
	// returned), but that's an ordering invariant three functions away from
	// this struct. Sending on a closed channel panics the process, so this
	// makes the safety local instead of emergent.
	mu     sync.Mutex
	closed bool
}

// deliver forwards a frame unless the stream has already ended. It gives up if
// the consumer's context is cancelled, so a consumer that walked away can't
// wedge the read loop.
func (s *streamSub) deliver(f StreamFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- f:
	case <-s.done:
	}
}

// finish ends the stream exactly once, delivering a final frame first when one
// is given. Reports whether this call was the one that closed it.
func (s *streamSub) finish(terminal *StreamFrame) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	if terminal != nil {
		sendTerminal(s.ch, *terminal)
	}
	s.closed = true
	close(s.fin)
	close(s.ch)
	return true
}

func newStreamSub() *streamSub {
	return &streamSub{
		ch:   make(chan StreamFrame, streamBuffer),
		done: make(chan struct{}),
		fin:  make(chan struct{}),
	}
}

// RequestStream issues a streaming request and returns a channel of frames. The
// gateway manages the ephemeral reply inbox, so the caller allocates no subject
// or session. Cancelling ctx cancels the stream and signals the responder to
// stop. The channel closes when the stream ends; a final frame with Err set
// reports an error terminal, including ErrStreamingUnsupported and
// ErrStreamDisconnected.
func (c *Client) RequestStream(ctx context.Context, subject string, payload any) (<-chan StreamFrame, error) {
	raw, err := jsonMarshal(payload)
	if err != nil {
		return nil, err
	}
	id, err := newStreamID()
	if err != nil {
		return nil, err
	}

	st := newStreamSub()
	c.mu.Lock()
	c.streams[id] = st
	c.mu.Unlock()

	action := map[string]any{
		"action":    "request_stream",
		"subject":   subject,
		"data":      base64.StdEncoding.EncodeToString(raw),
		"stream_id": id,
	}
	// Blocking send: this is the frame that opens the stream, and dropping it
	// would leave the caller holding a channel that never delivers anything.
	if err := c.sendAction(action, true); err != nil {
		// Only finish what we still own. ErrNotConnected is exactly the error a
		// dropped connection produces, and that same drop runs failAllStreams,
		// which may already have taken this stream out of the map and ended it
		// with ErrStreamDisconnected. finish is idempotent, so the worst case
		// here is cosmetic rather than a panic, but the owner check keeps us
		// from racing a second, emptier terminal over a real one.
		c.mu.Lock()
		_, owned := c.streams[id]
		delete(c.streams, id)
		c.mu.Unlock()
		if owned {
			st.finish(nil)
		}
		return nil, err
	}

	// On cancel: unblock local delivery and tell the gateway to cancel. The
	// gateway responds with a terminal frame, which closes the channel. Exits
	// on fin too, so a stream that ends normally doesn't leave this parked for
	// the life of the caller's context.
	go func() {
		select {
		case <-st.fin:
			return
		case <-ctx.Done():
		}
		close(st.done)
		_ = c.sendAction(map[string]any{"action": "cancel_stream", "stream_id": id}, false)
	}()

	return st.ch, nil
}

// deliverStream routes a frame to its consumer. On a terminal it removes the
// stream and closes the channel. A consumer whose done is closed (ctx
// cancelled) no longer blocks the read loop; pending chunks are dropped.
func (c *Client) deliverStream(id string, f StreamFrame, terminal bool) {
	c.mu.Lock()
	st, ok := c.streams[id]
	if ok && terminal {
		delete(c.streams, id)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	if f.Err != nil || len(f.Data) > 0 {
		st.deliver(f)
	}
	if terminal {
		st.finish(nil)
	}
}

// failAllStreams ends every open stream with err. Used when the gateway rejects
// streaming and when the connection drops, since streams are ephemeral.
func (c *Client) failAllStreams(err error) {
	c.mu.Lock()
	streams := c.streams
	c.streams = map[string]*streamSub{}
	c.mu.Unlock()
	for _, st := range streams {
		st.finish(&StreamFrame{Err: err})
	}
}

// sendTerminal puts the final frame on ch even if a slow consumer has filled the
// buffer, dropping the oldest chunk to make room. Letting the send fail instead
// would close the channel with no error on it, and the consumer would read a
// truncated stream as a clean end.
//
// Safe to spin: callers hold the stream's mutex, so nothing else can be putting
// frames on ch, and each retry either lands or frees a slot.
func sendTerminal(ch chan StreamFrame, f StreamFrame) {
	for {
		select {
		case ch <- f:
			return
		default:
		}
		select {
		case <-ch: // drop the oldest chunk
		default: // consumer drained concurrently; try again
		}
	}
}

// --- responder side ---

// Responder is the responder-side handle for a streaming reply, returned by
// StreamResponderFor. It's an interface so consumers can depend on it, and fake
// it, without the concrete type.
type Responder interface {
	// Send streams one chunk to the requester.
	Send(data []byte) error
	// End closes the stream successfully.
	End() error
	// Fail closes the stream with an error terminal.
	Fail(err error) error
	// Canceled fires when the requester cancels, or when the connection drops.
	Canceled() <-chan struct{}
	// Close releases the responder's cancel subscription without sending a
	// terminal frame. End and Fail already do this, so Close only matters for
	// the paths that don't reach either. Defer it.
	Close()
}

// StreamResponder streams a response back to a request received with the
// gateway's streaming markers. Obtain one via StreamResponderFor.
type StreamResponder struct {
	cc            *Client
	reply         string
	cancelSubject string
	canceled      chan struct{}
	once          sync.Once
	finishOnce    sync.Once
}

// StreamResponderFor returns a responder when msg is a streaming request, which
// means it carries the gateway-set cancel-subject header and a reply subject.
// ok is false for an ordinary request, so a handler can fall back to its
// non-streaming path.
//
// The responder holds a subscription on the cancel subject until End, Fail, or
// Close runs, so handlers should `defer r.Close()` to be sure it's released
// even on a panic or an early return.
func (c *Client) StreamResponderFor(msg Message) (Responder, bool) {
	if msg.ReplySubject == "" {
		return nil, false
	}
	cancelSubject := headerLookup(msg.Headers, headerStreamCancel)
	if cancelSubject == "" {
		return nil, false
	}
	r := &StreamResponder{cc: c, reply: msg.ReplySubject, cancelSubject: cancelSubject, canceled: make(chan struct{})}
	// Watch for a cancel signal so the handler can abort upstream work.
	c.Subscribe(cancelSubject, func(_ context.Context, _ Message) { r.markCanceled() })
	c.mu.Lock()
	c.responders[cancelSubject] = r
	c.mu.Unlock()
	return r, true
}

// cancelAllResponders fires Canceled on every live responder. Called when the
// connection drops: the requester is gone, so a handler still generating chunks
// is working for nobody.
func (c *Client) cancelAllResponders() {
	c.mu.Lock()
	live := make([]*StreamResponder, 0, len(c.responders))
	for _, r := range c.responders {
		live = append(live, r)
	}
	c.mu.Unlock()
	for _, r := range live {
		r.markCanceled()
	}
}

// Send streams one chunk to the requester. It blocks when the outbound queue is
// full, so a fast producer throttles instead of losing chunks.
func (r *StreamResponder) Send(data []byte) error {
	return r.cc.sendAction(map[string]any{
		"action":  "stream_send",
		"subject": r.reply,
		"data":    base64.StdEncoding.EncodeToString(data),
	}, true)
}

// End closes the stream successfully.
func (r *StreamResponder) End() error { return r.finish("") }

// Fail closes the stream with an error terminal.
func (r *StreamResponder) Fail(err error) error {
	msg := "stream failed"
	if err != nil {
		msg = err.Error()
	}
	return r.finish(msg)
}

// Canceled fires when the requester cancels, or when the connection drops.
// Handlers should select on it to abort upstream work.
func (r *StreamResponder) Canceled() <-chan struct{} { return r.canceled }

// Close releases the cancel subscription. Idempotent, and safe to call after
// End or Fail.
func (r *StreamResponder) Close() { r.release() }

// release drops the cancel subscription and deregisters the responder. Running
// twice is harmless; Unsubscribe on an unknown subject is a no-op.
func (r *StreamResponder) release() {
	r.cc.mu.Lock()
	delete(r.cc.responders, r.cancelSubject)
	r.cc.mu.Unlock()
	r.cc.Unsubscribe(r.cancelSubject)
}

// finish sends the terminal frame exactly once. A second End or Fail would
// otherwise put two stream_end frames on the same reply subject.
func (r *StreamResponder) finish(errMsg string) error {
	var err error
	sent := false
	r.finishOnce.Do(func() {
		sent = true
		r.release()
		action := map[string]any{"action": "stream_end", "subject": r.reply}
		if errMsg != "" {
			action["error"] = errMsg
		}
		err = r.cc.sendAction(action, true)
	})
	if !sent {
		return nil
	}
	return err
}

func (r *StreamResponder) markCanceled() { r.once.Do(func() { close(r.canceled) }) }

// --- helpers ---

func newStreamID() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "cs_" + base64.RawURLEncoding.EncodeToString(b[:]), nil
}
