# Chitchat

Inter-service messaging for the Miren garden cluster, powered by NATS with JetStream. Chitchat runs NATS internally and exposes an authenticated HTTP API so any service can publish, subscribe, and consume durable streams without a native NATS client.

## Deploy

```bash
cd chitchat
miren deploy -C garden
miren env set -C garden -a chitchat GATEWAY_API_KEY=$(openssl rand -base64 32)
```

## Authentication

All `/v1` endpoints require a Bearer token. The gateway supports two auth methods — both can be active simultaneously.

### API Key (always enabled)

```
Authorization: Bearer <GATEWAY_API_KEY>
```

Best for service-to-service communication. The key is set via `GATEWAY_API_KEY` env var.

### Multipass JWT (optional)

Enable by setting `MULTIPASS_BASE_URL` to your multipass instance (e.g. `https://multipass.miren.cloud`). The gateway fetches the JWKS and verifies JWTs issued by multipass.

```
Authorization: Bearer <multipass-jwt>
```

Optionally restrict which verified identities are accepted:

- `ALLOWED_DOMAIN` (e.g. `miren.dev`) — restricts identities that are **email addresses** to this domain.
- `ALLOWED_PREFIXES` (comma-separated) — admits **non-email service identities** whose email/subject starts with one of these prefixes (e.g. `org:org-Miren-P20syIg0MAcS:`). Service/workload tokens (like a cluster agent) have a structured identity with no `@`, so they're allowed via a prefix rather than the domain.

A token is accepted if its identity matches `ALLOWED_DOMAIN` **or** any `ALLOWED_PREFIXES` entry. With neither set, any signature-valid multipass token is accepted.

```bash
miren env set -C garden -a chitchat MULTIPASS_BASE_URL=https://multipass.miren.cloud
miren env set -C garden -a chitchat ALLOWED_DOMAIN=miren.dev
miren env set -C garden -a chitchat ALLOWED_PREFIXES=org:org-Miren-P20syIg0MAcS:
```

When both are configured, each request is checked against the API key first, then falls back to JWT verification.

The `/healthz` endpoint is unauthenticated. For WebSocket connections, pass the token via `?token=` query parameter or `Authorization` header.

### Ambient identity

The gateway makes the caller's verified identity an **ambient property**. On every message a caller sends (`publish`, `request`, WS `publish`), the gateway stamps these headers:

| Header | Value |
| --- | --- |
| `cc-id` | Canonical identity: the JWT email (else its subject), or `apikey` for API-key callers |
| `cc-auth` | `jwt` or `apikey` |
| `cc-email` | The JWT email, when present |
| `cc-sub` | The JWT subject (`sub`), when present |

The names are deliberately short — they ride on every message. Recipients read them from the message `headers` on any receive path (SSE, WebSocket, durable subscription, pull) to learn who sent a message — including who answered a request-reply, since the reply is stamped too.

**These headers are unspoofable.** The `cc-` prefix is reserved: the gateway strips any client-supplied `cc-*` header (in any casing) before stamping its own, and NATS is reachable only through the gateway — so a caller cannot assert an identity that isn't theirs.

To ask the gateway for your own identity:

```
GET /v1/whoami        ->  {"identity":"alice@example.com","auth":"jwt","email":"alice@example.com","subject":"user-123"}
```

Over a WebSocket, send `{"action":"whoami"}` and receive `{"type":"whoami","identity":"…","auth":"…","email":"…","who_subject":"…"}`.

## API Reference

### Health Check

```
GET /healthz
```

Returns `200 ok` when the gateway is connected to NATS, `503` otherwise.

---

### Publish

Send a message to a subject. Any subscriber listening on that subject (or a matching wildcard) receives it immediately.

```
POST /v1/publish
```

```json
{
  "subject": "orders.new",
  "data": "eyJpZCI6IDEyM30=",
  "headers": {
    "X-Source": "checkout-service"
  }
}
```

- `subject` — NATS subject (required)
- `data` — base64-encoded payload (required)
- `headers` — optional key-value headers

Response: `{"ok": true}`

---

### Request-Reply

Send a message and wait for a single response. Useful for RPC-style calls between services.

```
POST /v1/request
```

```json
{
  "subject": "rpc.validate-address",
  "data": "eyJjaXR5IjogIlNGIn0=",
  "timeout_ms": 3000
}
```

- `timeout_ms` — how long to wait for a reply (default: 5000)

Response:

```json
{
  "subject": "_INBOX.xyz",
  "data": "eyJ2YWxpZCI6IHRydWV9",
  "headers": {}
}
```

Returns `504` if no reply arrives before the timeout.

---

### Subscribe (SSE)

Open a streaming connection that receives messages in real time via Server-Sent Events. The connection stays open until the client disconnects.

```
GET /v1/subscribe?subject=orders.>
```

NATS wildcards work: `*` matches a single token, `>` matches one or more tokens.

Each message arrives as an SSE event:

```
data: {"subject":"orders.new","data":"eyJpZCI6IDEyM30=","headers":{}}

data: {"subject":"orders.shipped","data":"eyJpZCI6IDQ1Nn0="}
```

Example with curl:

```bash
curl -N https://chitchat.example.com/v1/subscribe?subject=orders.%3E \
  -H "Authorization: Bearer $KEY"
```

---

### WebSocket

A bidirectional WebSocket connection for subscribing, unsubscribing, and publishing — all over a single persistent connection. Ideal for services that need to manage multiple subscriptions or combine sending and receiving.

```
GET /v1/ws
```

Pass the auth token as a query parameter since browsers don't support custom headers on WebSocket upgrades:

```
wss://chitchat.example.com/v1/ws?token=<GATEWAY_API_KEY>
```

Or use the `Authorization` header if your client supports it (non-browser clients).

#### Client-to-server messages

**Subscribe:**

```json
{"action": "subscribe", "subject": "orders.>"}
```

Response: `{"type": "subscribed", "subject": "orders.>"}`

**Unsubscribe:**

```json
{"action": "unsubscribe", "subject": "orders.>"}
```

Response: `{"type": "unsubscribed", "subject": "orders.>"}`

**Publish:**

```json
{
  "action": "publish",
  "subject": "orders.new",
  "data": "eyJpZCI6IDEyM30=",
  "headers": {"X-Source": "checkout"}
}
```

Response: `{"type": "published", "subject": "orders.new"}`

**Publish with reply subject** (for request-reply patterns):

```json
{
  "action": "publish",
  "subject": "rpc.geocode",
  "data": "eyJhZGRyZXNzIjogIjEyMyBNYWluIn0=",
  "reply_subject": "my-inbox.abc123"
}
```

The responder sees `reply_subject` on the incoming message and publishes their response to that subject. Subscribe to your inbox subject first to receive the reply.

#### Server-to-client messages

When a message arrives on a subscribed subject:

```json
{
  "type": "message",
  "subject": "orders.new",
  "data": "eyJpZCI6IDEyM30=",
  "headers": {"X-Source": "checkout"}
}
```

If the message was sent with a reply subject (request-reply), the `reply_subject` field is included:

```json
{
  "type": "message",
  "subject": "rpc.geocode",
  "data": "eyJhZGRyZXNzIjogIjEyMyBNYWluIn0=",
  "reply_subject": "_INBOX.xK9z"
}
```

To reply, publish to the `reply_subject`:

```json
{"action": "publish", "subject": "_INBOX.xK9z", "data": "eyJsYXQiOiAzNy43fQ=="}
```

Errors:

```json
{"type": "error", "error": "subject is required"}
```

#### Example: request-reply over WebSocket

```bash
# Service B (responder) — subscribe and reply
websocat "wss://chitchat.miren.garden/v1/ws" -H "Authorization: Bearer $KEY"
{"action": "subscribe", "subject": "rpc.geocode"}
# receives: {"type":"message","subject":"rpc.geocode","data":"...","reply_subject":"_INBOX.xK9z"}
{"action": "publish", "subject": "_INBOX.xK9z", "data": "eyJsYXQiOiAzNy43fQ=="}

# Service A (requester) — subscribe to inbox, then send request
websocat "wss://chitchat.miren.garden/v1/ws" -H "Authorization: Bearer $KEY"
{"action": "subscribe", "subject": "my-inbox.>"}
{"action": "publish", "subject": "rpc.geocode", "data": "eyJhZGRyZXNzIjogIjEyMyBNYWluIn0=", "reply_subject": "my-inbox.req1"}
# receives: {"type":"message","subject":"my-inbox.req1","data":"eyJsYXQiOiAzNy43fQ=="}
```

#### Example: pub/sub

```bash
websocat "wss://chitchat.miren.garden/v1/ws" \
  -H "Authorization: Bearer $KEY"

# then type:
{"action": "subscribe", "subject": "events.>"}
{"action": "publish", "subject": "events.test", "data": "aGVsbG8="}
```

All subscriptions are automatically cleaned up when the WebSocket connection closes.

#### Durable subscriptions

By default a `subscribe` is a plain core NATS subscription: at-most-once, live-only, no replay. Add a **`session_id`** to make it durable, backed by JetStream:

```json
{"action": "subscribe", "subject": "announce.>", "session_id": "billing-svc"}
```

Response: `{"type": "subscribed", "subject": "announce.>", "session_id": "billing-svc"}`

When `session_id` is present, the gateway binds a **durable consumer** keyed on `(session_id, subject)` to the stream that captures the subject. This gives you:

- **Catch-up on connect** — a new session receives everything still retained in the stream buffer for that subject, not just messages sent after it connected.
- **Resume on reconnect** — reconnect with the same `session_id` and you pick up exactly where you left off. Messages are acked only after a successful write to your socket, so a drop mid-stream redelivers rather than loses.
- **Independent cursors** — each distinct `session_id` gets its own copy of every message (broadcast fan-out). Two connections sharing a `session_id` for the same subject is undefined — give every logical subscriber its own id.

Requirements and notes:

- **A stream must already capture the subject.** The stream *is* the durable buffer; size it with `max_msgs_per_subject` (see [Create or Update a Stream](#create-or-update-a-stream)). Subscribing with a `session_id` to a subject no stream captures returns an error.
- **Messages older than the buffer are gone.** If a session is offline long enough that retained messages age or fall out of a bounded buffer, those are skipped — the bound is the tradeoff. Size the buffer for your worst-case offline window.
- **Abandoned sessions are reclaimed.** A durable consumer with no connection bound for 24h is cleaned up automatically; a session returning after that gets a fresh consumer that replays whatever is still buffered.
- Omit `session_id` and the behavior is exactly the old core-NATS subscription — nothing changes for existing clients.

`unsubscribe` stops delivery but keeps the durable cursor so the session can resume later; the cursor is only reclaimed by the inactivity timeout above.

**Tearing a session down explicitly.** When a session is permanently done, delete its durable consumer(s) immediately instead of waiting out the 24h inactivity timeout — over the WebSocket:

```json
{"action": "teardown", "session_id": "billing-svc"}
```

Response: `{"type": "torn_down", "session_id": "billing-svc", "count": 1}` (`count` is the number of consumers removed). Add a `subject` to scope the teardown to one subscription instead of the whole session. Any live binding for that session on the current connection is stopped first; if the session is live on another connection, that connection is told `{"type": "unsubscribed", ...}`.

You can also tear down out-of-band over HTTP (useful when the client has crashed and a cleanup process wants to reclaim the session) — see [Delete a Session](#delete-a-session).

#### Keepalives & reconnection

The gateway runs an application-level keepalive on every WebSocket connection. Clients must cooperate or they will be disconnected.

**What the server does:**

- Sends a **ping every ~54 seconds**.
- Expects a **pong (or any frame) within 60 seconds**. If nothing arrives, the connection is considered dead and is closed, and its subscriptions are cleaned up.
- Enforces a **10-second write deadline** on each message it sends, so a stalled reader can't wedge the server.

**What clients must do:**

- **Respond to pings with pongs.** Most WebSocket libraries do this automatically (e.g. gorilla/websocket, browser `WebSocket`, `websocat`). If yours doesn't, send a pong control frame in reply to each ping.
- **Detect a dead gateway.** Set your own read deadline (refresh it on every pong and inbound frame) and send your own pings on an interval shorter than the deadline. This lets you notice an intermediary (load balancer, NAT) that has silently dropped the connection without a close frame — otherwise a TCP-level half-open connection can look alive while no messages flow.
- **Reconnect and resubscribe.** Subscriptions do **not** survive a dropped connection. On reconnect, redial and re-send every `subscribe` action. Use exponential backoff so a gateway restart doesn't cause a reconnect storm.

Recommended client settings (matching the server):

| Setting | Value |
| --- | --- |
| Ping interval | ~30–54s |
| Read deadline (pong wait) | 60s |
| Write deadline | 10s |
| Reconnect backoff | 1s → 30s, exponential |

**Wait for `subscribed` before relying on a subscription.** The gateway registers the subscription with NATS and confirms it has taken effect *before* sending the `{"type":"subscribed"}` reply. If you publish (or signal another service to publish) before receiving that confirmation, messages can be missed. Always wait for `subscribed`.

**Messages published while disconnected are lost.** Plain pub/sub is at-most-once (see [Delivery Guarantees](#delivery-guarantees)). Reconnecting resubscribes you for *future* messages but does not replay what was sent during the gap. If you need catch-up, use a JetStream stream and a durable consumer instead.

The bundled `announce` and `pipe` clients implement all of the above (keepalive ping/pong, deadlines, and auto-reconnect with resubscribe) and are good reference implementations.

---

### Create or Update a Stream

Streams provide durable, replayable message storage backed by JetStream. Messages published to matching subjects are persisted according to the stream's retention policy.

```
POST /v1/streams
```

```json
{
  "name": "ORDERS",
  "subjects": ["orders.>"],
  "retention": "limits",
  "max_age_seconds": 86400,
  "max_bytes": 1073741824,
  "max_msgs": 1000000
}
```

- `name` — stream name, uppercase by convention (required)
- `subjects` — which subjects to capture (required)
- `retention` — `"limits"` (default), `"workqueue"`, or `"interest"`
- `max_age_seconds` — auto-delete messages older than this
- `max_bytes` — cap total stream size
- `max_msgs` — cap total message count
- `max_msgs_per_subject` — cap messages **per subject** (the useful knob for a durable broadcast buffer, where each subject is an independent topic — e.g. keep the last 100 per subject)
- `discard` — `"old"` (default) drops the oldest messages when a limit is hit, so the stream is a bounded ring buffer; `"new"` rejects new writes once full

To make a bounded durable-broadcast buffer, create a `limits` stream over your broadcast subjects with `max_msgs_per_subject` set. Subscribers then opt into durability per-connection with a `session_id` (see [Durable subscriptions](#durable-subscriptions)).

Response:

```json
{
  "name": "ORDERS",
  "subjects": ["orders.>"],
  "messages": 0,
  "bytes": 0,
  "consumers": 0
}
```

### List Streams

```
GET /v1/streams
```

### Get Stream Info

```
GET /v1/streams/{name}
```

### Delete a Stream

```
DELETE /v1/streams/{name}
```

---

### Create a Consumer

Consumers track read position within a stream so services can process messages at their own pace without losing their place.

```
POST /v1/streams/{name}/consumers
```

```json
{
  "durable_name": "order-processor",
  "filter_subject": "orders.new",
  "ack_policy": "explicit"
}
```

- `durable_name` — consumer name, survives restarts (required)
- `filter_subject` — only receive messages matching this subject (optional, defaults to all stream subjects)
- `ack_policy` — `"explicit"` (default), `"none"`, or `"all"`

### List Consumers

```
GET /v1/streams/{name}/consumers
```

---

### Pull Messages

Fetch the next batch of messages from a consumer. Messages are held pending until acknowledged or they expire (5 minutes).

```
POST /v1/streams/{name}/consumers/{consumer}/next
```

```json
{
  "batch": 10,
  "timeout_ms": 5000
}
```

- `batch` — number of messages to fetch (default: 1, max: 100)
- `timeout_ms` — how long to wait for messages (default: 5000)

Response:

```json
{
  "messages": [
    {
      "subject": "orders.new",
      "data": "eyJpZCI6IDEyM30=",
      "seq": 42,
      "headers": {},
      "ack_id": "a1b2c3d4e5f6..."
    }
  ]
}
```

### Acknowledge a Message

After processing a message, acknowledge it so the consumer advances past it. Unacknowledged messages are redelivered after 5 minutes.

```
POST /v1/ack
```

```json
{
  "ack_id": "a1b2c3d4e5f6..."
}
```

---

### Delete a Session

Tear down a durable session's consumer(s) immediately, rather than waiting for the 24h inactivity timeout. Works whether or not the session is currently connected — handy for reclaiming sessions left behind by a crashed client.

```
DELETE /v1/sessions/{session_id}
```

Optional `?subject=` scopes the teardown to a single subscription; without it, every consumer belonging to the session is removed.

```bash
# Tear down the whole session
curl -X DELETE https://chitchat.example.com/v1/sessions/billing-svc \
  -H "Authorization: Bearer $KEY"

# Tear down just one subscription within the session
curl -X DELETE "https://chitchat.example.com/v1/sessions/billing-svc?subject=announce.%3E" \
  -H "Authorization: Bearer $KEY"
```

Response: `{"session_id": "billing-svc", "deleted": 1}` — `deleted` is the number of consumers removed (0 if the session had none). After teardown, a client that reconnects with the same `session_id` starts fresh and replays whatever is still in the stream buffer.

---

## Patterns

### Fire-and-forget notifications

Publish a message — any current subscriber gets it, otherwise it's gone:

```bash
curl -X POST https://chitchat.example.com/v1/publish \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"subject": "events.user.signup", "data": "eyJ1c2VyIjogImFsaWNlIn0="}'
```

### Durable work queue

Create a workqueue stream and a consumer. Multiple workers can pull and ack independently — each message is delivered to exactly one worker:

```bash
# Create stream
curl -X POST https://chitchat.example.com/v1/streams \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "JOBS", "subjects": ["jobs.>"], "retention": "workqueue"}'

# Create consumer
curl -X POST https://chitchat.example.com/v1/streams/JOBS/consumers \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"durable_name": "worker"}'

# Publish work
curl -X POST https://chitchat.example.com/v1/publish \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"subject": "jobs.resize-image", "data": "eyJwYXRoIjogIi9pbWcvYWJjLnBuZyJ9"}'

# Pull and process
curl -X POST https://chitchat.example.com/v1/streams/JOBS/consumers/worker/next \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"batch": 1, "timeout_ms": 10000}'

# Ack when done
curl -X POST https://chitchat.example.com/v1/ack \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"ack_id": "<ack_id from pull response>"}'
```

### RPC between services

**Via REST** (blocking request-reply):

```bash
curl -X POST https://chitchat.miren.garden/v1/request \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{"subject": "rpc.geocode", "data": "eyJhZGRyZXNzIjogIjEyMyBNYWluIFN0In0=", "timeout_ms": 3000}'
```

**Via WebSocket** (non-blocking, subscribe to inbox first):

```json
{"action": "subscribe", "subject": "my-inbox.>"}
{"action": "publish", "subject": "rpc.geocode", "data": "eyJhZGRyZXNzIjogIjEyMyBNYWluIFN0In0=", "reply_subject": "my-inbox.req1"}
```

The responder receives the message with `reply_subject` and publishes the response to that subject. See the WebSocket request-reply example above for the full flow.

### Service discovery

For a convention where services **advertise themselves and their RPCs** (with JSON Schemas) so other services can discover and call them — built on these same primitives plus an auto-provisioned `DISCOVERY` stream — see [docs/SERVICE_DISCOVERY.md](docs/SERVICE_DISCOVERY.md).

## Architecture

Chitchat is a single Miren app with two services:

- **nats** — runs the NATS server with JetStream, stores stream data on a persistent disk
- **web** — Go HTTP gateway that connects to NATS internally and exposes the REST/SSE API

The NATS server is not directly accessible from outside the app. All access goes through the authenticated HTTP gateway.

## Data Encoding

All `data` fields in requests and responses are **base64-encoded** (standard encoding, not URL-safe). This allows arbitrary binary payloads to be transported over JSON.

To encode: `echo -n '{"id": 123}' | base64` → `eyJpZCI6IDEyM30=`

To decode: `echo 'eyJpZCI6IDEyM30=' | base64 -d` → `{"id": 123}`

## Delivery Guarantees

- **Publish/Subscribe** (no stream): at-most-once. If no subscriber is listening, the message is dropped.
- **Durable WebSocket subscriptions** (`session_id`, with a capturing stream): at-least-once for as long as the message is retained in the stream buffer. The session catches up on connect and resumes from its last ack on reconnect. Messages that age out of the buffer before the session reads them are skipped.
- **JetStream streams**: at-least-once with explicit ack. Unacknowledged messages are redelivered after 5 minutes. If the gateway restarts, all pending acks are lost and those messages will be redelivered by NATS.
- **Workqueue retention**: each message is delivered to exactly one consumer (at-least-once within that consumer).
