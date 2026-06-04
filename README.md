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

Optionally set `ALLOWED_DOMAIN` (e.g. `miren.dev`) to restrict access to users with matching email addresses.

```bash
miren env set -C garden -a chitchat MULTIPASS_BASE_URL=https://multipass.miren.cloud
miren env set -C garden -a chitchat ALLOWED_DOMAIN=miren.dev
```

When both are configured, each request is checked against the API key first, then falls back to JWT verification.

The `/healthz` endpoint is unauthenticated. For WebSocket connections, pass the token via `?token=` query parameter or `Authorization` header.

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
- **JetStream streams**: at-least-once with explicit ack. Unacknowledged messages are redelivered after 5 minutes. If the gateway restarts, all pending acks are lost and those messages will be redelivered by NATS.
- **Workqueue retention**: each message is delivered to exactly one consumer (at-least-once within that consumer).
