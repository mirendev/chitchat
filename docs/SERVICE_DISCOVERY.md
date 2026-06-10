# Service Discovery

A convention for services to **advertise themselves and their RPCs** on chitchat, and for other
services to **discover and call them** — without hard-coding subjects and payload shapes out of
band.

This is a **convention, not a feature**. It is built entirely from chitchat's existing
primitives (publish, request-reply, durable WebSocket subscriptions, and a JetStream stream).
The one piece of supporting infrastructure — the `DISCOVERY` stream — is **provisioned
automatically by the gateway on startup**, so there is nothing to set up.

In one sentence: each service **heartbeats a descriptor** (who it is + what RPCs it offers, with
JSON Schemas) into a shared catalog stream; discoverers **subscribe to the catalog** to learn
what's live; and RPCs are invoked with the ordinary `/v1/request` endpoint.

---

## How it maps to chitchat primitives

| Discovery action | chitchat primitive |
|---|---|
| Advertise / heartbeat a descriptor | [`POST /v1/publish`](../README.md#publish) to `discovery.service.<service>.<instance>` |
| The catalog (last descriptor per instance) | The `DISCOVERY` JetStream stream (auto-provisioned) |
| Browse the catalog + live updates | Durable [`/v1/ws` subscribe](../README.md#durable-subscriptions) with a `session_id` to `discovery.service.>` |
| Invoke a discovered RPC | [`POST /v1/request`](../README.md#request-reply) to the RPC's subject |

Everything below is layered on those. No special endpoints exist for discovery.

---

## Subject namespace

| Subject | Who uses it | Purpose |
|---|---|---|
| `discovery.service.<service>.<instance>` | service → | Each instance heartbeats its descriptor here. One subject per instance. |
| `discovery.service.>` | ← discoverer | Subscribe for the whole catalog + live updates. |
| `discovery.service.<service>.>` | ← discoverer | Watch a single service's instances. |
| `<service>.<method>` | RPC caller → | Service-addressed RPC — any instance answers (see [fan-out](#multi-instance-rpcs--fan-out)). |
| `<service>.<method>.<instance>` | RPC caller → | Instance-addressed RPC — pins one named instance. |
| `<service>.<method>.<entity-id>` | RPC caller → | Entity-addressed RPC — routes to the one instance that hosts the entity (see [Entity-addressed RPCs](#entity-addressed-rpcs--placement)). |
| `<service>.ping` | RPC caller → | Recommended health probe (see [Health checks](#health-checks)). |

**Token rules.** `<service>` and `<instance>` must each be a single NATS subject token — no
dots, `*`, or `>` — lowercase-kebab by convention (e.g. `billing`, `geo-api`). A `.` in either
silently reshapes the subject tree and breaks the per-instance catalog slot, so sanitize them at
the service side. `<instance>` must be **stable for the life of the process** (e.g. the Miren
machine id, hostname, or a UUID generated once at boot) so its catalog slot is reused across
heartbeats instead of creating a new entry each beat.

### Addressing modes

An RPC subject can target three different scopes, all built from that single-token rule:

| Mode | Subject | Who subscribes | Use |
|---|---|---|---|
| **service** | `<service>.<method>` | any/all live instances (fan-out, first reply wins) | stateless / idempotent calls |
| **instance** | `<service>.<method>.<instance>` | exactly one named instance | pin a specific process |
| **entity** | `<service>.<method>.<entity-id>` | exactly the instance hosting that entity | address a *placed* entity |

`<entity-id>` is itself a **single NATS token** of the form `<instance>:<local>` — note the `:`,
**not** a `.`, so the whole id stays one token. Because the id embeds its host `<instance>`, the
hosting instance is the only subscriber to that suffix, so a caller holding the id routes to it
**directly, with no lookup** — the id *is* the route. See
[Entity-addressed RPCs & placement](#entity-addressed-rpcs--placement) for the full pattern.

**Reserved prefixes.** `_reply.*` is owned by the gateway (request-reply plumbing). The
`discovery.>` subtree is owned by this convention — don't publish unrelated traffic there.

---

## The descriptor

A service's advertisement is a single JSON document, base64-encoded into the `data` field of the
publish. Example:

```json
{
  "schema_version": 1,
  "service": "geo",
  "instance": "geo-1",
  "version": "1.4.0",
  "description": "Geocoding and reverse-geocoding.",
  "ts": "2026-06-05T12:00:00Z",
  "ttl_seconds": 90,
  "heartbeat_interval_seconds": 30,
  "rpcs": [
    {
      "name": "lookup",
      "subject": "geo.lookup",
      "description": "Geocode a street address to coordinates.",
      "timeout_ms": 3000,
      "request_schema": {
        "type": "object",
        "required": ["address"],
        "properties": { "address": { "type": "string" } }
      },
      "response_schema": {
        "type": "object",
        "required": ["lat", "lng"],
        "properties": {
          "lat": { "type": "number" },
          "lng": { "type": "number" }
        }
      }
    }
  ],
  "events": [
    {
      "name": "geocoded",
      "subject": "geo.events.geocoded",
      "description": "Emitted after each successful geocode.",
      "schema": { "type": "object", "properties": { "address": { "type": "string" } } }
    }
  ],
  "metadata": { "team": "maps", "region": "us-west" }
}
```

### Fields

| Field | Required | Notes |
|---|---|---|
| `schema_version` | yes | Convention version. Start at `1`. Discoverers ignore descriptors with a `schema_version` they don't understand. |
| `service` | yes | Service name (single token). |
| `instance` | yes | Stable per-process instance id (single token). |
| `ts` | yes | RFC 3339 timestamp, **refreshed on every heartbeat**. This is the liveness clock — do not cache a build-time value. |
| `ttl_seconds` | yes | A discoverer considers this instance dead once `now - ts > ttl_seconds`. |
| `heartbeat_interval_seconds` | yes | How often the service re-publishes. Informational; lets discoverers reason about staleness. |
| `rpcs` | yes (may be `[]`) | The RPCs this service offers. |
| `rpcs[].name` | yes | Short RPC name. |
| `rpcs[].subject` | yes | Subject to `POST /v1/request` against. |
| `rpcs[].request_schema` | yes | JSON Schema for the request body. Use `{}` or `true` for "any". |
| `rpcs[].response_schema` | yes | JSON Schema for the success response body. |
| `rpcs[].timeout_ms` | no | Suggested request timeout (default 5000). |
| `rpcs[].description` | no | Human description. |
| `rpcs[].addressing` | no | `"service"` (default), `"instance"`, or `"entity"` — see [Addressing modes](#addressing-modes). |
| `rpcs[].subject_template` | no | For instance/entity RPCs, the routable subject with one `{instance}` or `{entity}` placeholder (e.g. `sandbox.exec.{entity}`). The caller substitutes the id. |
| `rpcs[].entity_type` | no | For `addressing:"entity"`, the `entities[].type` this RPC targets. SHOULD be set, and `{entity}` SHOULD appear in `subject_template`. |
| `rpcs[].pinned_subject` | no | Self-pin shorthand: the instance-addressed subject for *this* descriptor's own instance (e.g. `geo.lookup.geo-1`). Equivalent to `addressing:"instance"` + `subject_template:"<subject>.{instance}"`; new services should prefer that general form. |
| `entities` | no | Entity *types* this service addresses by id — see [Entity-addressed RPCs & placement](#entity-addressed-rpcs--placement). Each: `type` (required, single token), `description`, `id_format`, `list_rpc`, `place_rpc` (the latter two name RPCs by their `name`). |
| `version` | no | Service build/semver. |
| `description` | no | Human description of the service. |
| `events` | no | Pub/sub topics the service emits: `{name, subject, schema, description}`. |
| `metadata` | no | Free-form key/value (region, machine id, docs URL, …). |

For instance- and entity-addressed RPCs, `subject` is the un-suffixed *stem* (a stable name, and
often the legacy "broadcast + owner answers" subject); `subject_template` is the form a caller
actually routes with. All new fields are optional — a descriptor without them is valid and means
every RPC is service-addressed.

JSON Schemas are **embedded, self-contained objects** (draft 2020-12 recommended) — not `$ref`
to external files, since there is no schema-hosting endpoint. If your schemas are large, see
[descriptor size](#caveats--limits).

### Formal schema for the descriptor envelope

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "chitchat service descriptor",
  "type": "object",
  "required": ["schema_version", "service", "instance", "ts", "ttl_seconds", "heartbeat_interval_seconds", "rpcs"],
  "properties": {
    "schema_version": { "type": "integer", "const": 1 },
    "service":  { "type": "string", "pattern": "^[a-z0-9][a-z0-9-]*$" },
    "instance": { "type": "string", "pattern": "^[a-z0-9][a-z0-9-]*$" },
    "version":  { "type": "string" },
    "description": { "type": "string" },
    "ts": { "type": "string", "format": "date-time" },
    "ttl_seconds": { "type": "integer", "minimum": 1 },
    "heartbeat_interval_seconds": { "type": "integer", "minimum": 1 },
    "rpcs": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name", "subject", "request_schema", "response_schema"],
        "properties": {
          "name": { "type": "string" },
          "subject": { "type": "string" },
          "addressing": { "enum": ["service", "instance", "entity"] },
          "subject_template": { "type": "string" },
          "entity_type": { "type": "string" },
          "pinned_subject": { "type": "string" },
          "description": { "type": "string" },
          "timeout_ms": { "type": "integer", "minimum": 1 },
          "request_schema": { "type": "object" },
          "response_schema": { "type": "object" }
        }
      }
    },
    "entities": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["type"],
        "properties": {
          "type": { "type": "string", "pattern": "^[a-z0-9][a-z0-9-]*$" },
          "description": { "type": "string" },
          "id_format": { "type": "string" },
          "list_rpc": { "type": "string" },
          "place_rpc": { "type": "string" }
        }
      }
    },
    "events": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["name", "subject"],
        "properties": {
          "name": { "type": "string" },
          "subject": { "type": "string" },
          "description": { "type": "string" },
          "schema": { "type": "object" }
        }
      }
    },
    "metadata": { "type": "object" }
  }
}
```

---

## The DISCOVERY stream

The catalog lives in a JetStream stream named `DISCOVERY`. **The gateway creates it
automatically on startup** — you do not create, configure, or delete it by hand. Its
configuration (managed in `cmd/gateway/main.go`) is equivalent to:

```jsonc
{
  "name": "DISCOVERY",
  "subjects": ["discovery.service.>"],
  "retention": "limits",
  "max_msgs_per_subject": 1,   // keep only the latest descriptor per instance
  "discard": "old",
  "max_age_seconds": 120       // overridable via DISCOVERY_MAX_AGE_SECONDS
}
```

- **`max_msgs_per_subject: 1`** makes each `discovery.service.<service>.<instance>` subject hold
  exactly one message — that instance's most recent descriptor. The stream is the live catalog.
- **`max_age_seconds`** purges a descriptor that hasn't been refreshed, so a dead instance ages
  out of the catalog. Override the default with the `DISCOVERY_MAX_AGE_SECONDS` env var on the
  gateway.

**Sizing invariant:** keep

```
heartbeat_interval_seconds  <  ttl_seconds  <  DISCOVERY_MAX_AGE_SECONDS
```

(defaults: `30 < 90 < 120`). `max_age` must exceed `ttl_seconds` so the stream never purges a
descriptor a live discoverer still considers valid. Note that `max_age` is only a **backstop
janitor** — the authoritative liveness signal is client-side TTL (`now - ts`), described under
[Liveness model](#liveness-model).

> Because the stream is always present, there is no "create it first" step and no
> stream-missing failure mode. Treat `DISCOVERY` as gateway-managed: don't hand-create or delete
> it.

---

## Advertising a service

On startup and then every `heartbeat_interval_seconds`, build your descriptor **with a fresh
`ts`**, base64-encode it, and publish it to your instance subject:

```bash
# DESC_B64 = base64 of the descriptor JSON, with "ts" set to now()
curl -X POST "$CHITCHAT_URL/v1/publish" \
  -H "Authorization: Bearer $CHITCHAT_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"subject\":\"discovery.service.geo.geo-1\",\"data\":\"$DESC_B64\"}"
```

Pseudocode for the loop:

```
instance = stable_id()                  # e.g. machine id / persisted UUID
every heartbeat_interval_seconds:
    desc.ts = now_rfc3339()
    publish("discovery.service.geo." + instance, base64(json(desc)))
```

That's the whole service side: a periodic fire-and-forget publish. There is no registration
handshake and no deregistration call — a stopped service simply stops heartbeating and ages out.

---

## Discovering services

Open a WebSocket and durably subscribe to the catalog. **Use a fresh `session_id` each time your
process starts** — this is the recommended default:

```jsonc
// connect: wss://chitchat.miren.garden/v1/ws  (Authorization: Bearer $TOKEN)
{"action": "subscribe", "subject": "discovery.service.>", "session_id": "disco-<random-per-boot>"}
// -> {"type":"subscribed", ...}
// -> {"type":"message", "subject":"discovery.service.geo.geo-1", "data":"<base64 descriptor>"}  (snapshot)
// -> ...one message per live instance, then live updates as services heartbeat...
```

For each `message`: base64-decode `data`, parse the descriptor, **group by `service`** and
**key by `instance`**. Maintain the catalog as soft state and **expire any instance where
`now - ts > ttl_seconds`**.

Why a fresh `session_id` per boot: a durable subscription replays the **full current catalog**
(`DeliverAll`) only for a *new* `session_id`. Reusing a stable `session_id` resumes from your
last acked position and delivers only *changes* since then — useful for long-lived gap-resume,
but it will not re-send the descriptor of an instance that has been quiet since before you last
ran. A fresh id per process start sidesteps that and always gives you a complete snapshot, then
live updates. (See [Liveness model](#liveness-model) for the small blind spots either way.)

A plain subscribe (no `session_id`) also works and needs nothing from the stream, but it only
shows instances as they next heartbeat — your catalog fills in within one `heartbeat_interval`
rather than immediately. The discoverer should mirror the WebSocket client shape in
[`cmd/announce/main.go`](../cmd/announce/main.go) (keepalive pings, reconnect, resubscribe).

> **Reference implementation.** [`examples/discovery`](../examples/discovery) is a self-contained,
> copy-from program that advertises a service and maintains the live catalog using exactly this
> pattern. Run two copies and each one's catalog lists both instances.
>
> **CLI.** [`cmd/services`](../cmd/services) lists the advertised services from the catalog —
> their RPCs, instances, and verified publisher. `services` for a one-shot list, `services -w`
> to watch live, `services --json` for scripting.

---

## Calling a discovered RPC

Pick the RPC from a descriptor and call its `subject` with `POST /v1/request`, sending a body
that satisfies the RPC's `request_schema` (base64-encoded). Use the descriptor's `timeout_ms`:

```bash
curl -X POST "$CHITCHAT_URL/v1/request" \
  -H "Authorization: Bearer $CHITCHAT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"subject":"geo.lookup","timeout_ms":3000,"data":"eyJhZGRyZXNzIjoiMTIzIE1haW4gU3QifQ=="}'
# data above is base64 of {"address":"123 Main St"}
```

- **`200` with a body** → the response `data` is base64 of either a success body matching
  `response_schema`, or an [error envelope](#error-envelope).
- **`200` with empty `data` and a `Status: 503` response header** → NATS "no responders": nothing
  is currently subscribed to that subject (the service is down or not listening). The `headers`
  map also echoes `Nats-Subject`. Treat this as *service unavailable* — it comes back immediately,
  not after the timeout.
- **`504`** → a responder exists but did not reply within `timeout_ms` (slow or stuck handler).

So a robust caller checks for an empty body with `headers.Status == "503"` (no instance
listening) in addition to a `504` (instance reached but silent).

To pin a specific instance (avoiding fan-out), call the RPC's `pinned_subject` if the descriptor
provides one, e.g. `geo.lookup.geo-1`.

---

## Entity-addressed RPCs & placement

Some services manage **entities** that live on a particular instance — e.g. `sandboxagent`'s
*enclaves* and *sandboxes*, each placed on one agent process. An RPC can be addressed to a
specific entity so it reaches exactly the instance that hosts it.

### The pattern

- **Subject:** `<service>.<method>.<entity-id>` (e.g. `sandbox.exec.a1:enc-mfrggzdf`).
- **Entity id:** a single NATS token `<instance>:<local>` — `:` not `.`. The id **embeds its host
  instance**, so the hosting instance is the only subscriber to that suffix.
- **Routing is lookup-free.** A caller holding the id sends straight to the suffix; the id *is*
  the route, so there's no registry to consult. And because exactly one instance subscribes, an
  entity-addressed call has **no fan-out** — no duplicate side effects, no first-reply-wins.

A service declares this in its descriptor: an `entities[]` entry for the type, and the relevant
RPCs marked `addressing:"entity"` with a `subject_template`:

```json
{
  "entities": [
    { "type": "sandbox", "id_format": "<instance>:<local>", "list_rpc": "enclave.list", "place_rpc": "pool.place" }
  ],
  "rpcs": [
    {
      "name": "sandbox.exec",
      "subject": "sandbox.exec",
      "subject_template": "sandbox.exec.{entity}",
      "addressing": "entity",
      "entity_type": "sandbox",
      "request_schema": { "type": "object", "required": ["cmd"] },
      "response_schema": { "type": "object" }
    }
  ]
}
```

A caller substitutes the id into the template (`strings.Replace(tmpl, "{entity}", id, 1)`) and
calls the result like any RPC. Instance-addressed RPCs work the same way with `{instance}` and
`addressing:"instance"`.

**A dead entity answers immediately.** If you target `<service>.<method>.<instance>:<local>` whose
host is gone, nothing is subscribed, so you get the `200` + empty body + `Status: 503` no-responders
reply right away (not a timeout) — a natural "that entity isn't here" signal with no registry.

### Finding entities and placing new ones

Routing to a *known* entity needs nothing more than its id. Two things still need discovery, and
both are **service-defined** (chitchat does not manage an entity catalog):

- **Enumeration** — listing the entities that exist. Offer a service-addressed `list_rpc` that any
  instance answers from its own replicated view, and name it in `entities[].list_rpc`.
- **Placement** — deciding which instance a *new* entity should be created on. Offer a
  service-addressed `place_rpc` that returns the chosen instance and the instance-addressed
  "create" subject to call (e.g. `{ "instance": "a1", "create_subject": "enclave.create.a1" }`),
  and name it in `entities[].place_rpc`.

To keep enumeration/placement live, a service typically heartbeats each instance's entity inventory
into **its own** bounded JetStream stream — the same shape as the service catalog: one subject per
instance (`<service>.advertise.<instance>`), `max_msgs_per_subject:1`, `discard:old`, and
`interval < ttl < max_age`. Instances durably subscribe to build an entity→instance registry that
answers `list_rpc` / `place_rpc`. This stream, its payload, and the placement algorithm are
entirely up to the service; the descriptor only *describes* them via `entities[]`.

> **Reference.** `sandboxagent` implements exactly this: entity-addressed `sandbox.<op>.<fullID>`
> and `enclave.{info,destroy}.<fullID>`; ids like `a1:enc-mfrggzdf`; a `SANDBOX_ADVERTISE` stream
> fed by `sandbox.advertise.<agentID>`; and a `sandbox.pool.place` RPC returning the target agent
> and its `enclave.create.<agent>` subject. [`examples/discovery`](../examples/discovery) shows a
> minimal version (a `widget` entity with `widget.poke.{entity}`), and
> [`cmd/services`](../cmd/services) displays entity-addressed RPCs and entity types.

### Trust

Entity ids and `entities[]` are **self-asserted** descriptor-body data, like `service`/`instance`.
Route using the self-describing id, but make authorization decisions from the gateway-verified
[`cc-*` identity headers](../README.md#ambient-identity) stamped on the entity-addressed request —
not from the id the caller chose. Entity-addressed and advertisement messages carry those verified
headers like any other, so the caller and the hosting instance are both attributable.

---

## Health checks

Liveness is already covered by heartbeats, but a service SHOULD also expose a lightweight
`<service>.ping` RPC for active probing (precedent: `meet.ping` in
[`cmd/meet/main.go`](../cmd/meet/main.go)). Recommended response:

```json
{ "ok": true, "service": "geo", "instance": "geo-1", "version": "1.4.0" }
```

You may advertise `ping` in `rpcs` like any other RPC; doing so makes it self-describing.

---

## Error envelope

The gateway returns HTTP `200` for any reply that arrives — success or application error are
**not** distinguished by status code. By convention, an RPC that fails replies with:

```json
{ "error": { "code": "invalid_address", "message": "address could not be parsed" } }
```

Callers should: decode the response `data`, and if it contains a top-level `error` object, treat
the call as failed. A success reply is the body described by the RPC's `response_schema` and has
no `error` field. `code` is a short stable string for programmatic handling; `message` is human
text.

---

## Liveness model

- **Source of truth = client-side TTL.** A discoverer holds an instance in its catalog only while
  `now - ts <= ttl_seconds`, where `ts` is from the most recent descriptor it received. This is
  authoritative and does not depend on any stream notification.
- **`max_age` is a backstop janitor.** It purges stale descriptors from the stream so a *new*
  snapshot subscriber doesn't see corpses. It does not notify existing discoverers; they expire
  via TTL.

Two small blind spots to be aware of (both ≤ one `heartbeat_interval`):

1. **Snapshot/plain-subscribe gap.** A plain (non-durable) subscriber learns of an instance only
   at its next heartbeat. A fresh-`session_id` durable snapshot avoids this — it gets current
   state immediately.
2. **Durable-resume gap.** A reused stable `session_id` gets only changes since its last ack; an
   instance that's alive but hasn't heartbeated in the resume window is invisible until its next
   beat. Prefer a fresh `session_id` per boot to avoid this.

---

## Multi-instance RPCs & fan-out

chitchat subscribes to RPC subjects with **plain NATS subscriptions — no queue groups**. So if
several live instances of a service each serve the same `<service>.<method>` subject, a request
published there fans out to **all** of them, and **all** of them reply. `POST /v1/request` takes
the **first** reply and discards the rest (first-reply-wins) — but the other instances still did
the work and ran their side effects.

This is fine for read-only/idempotent RPCs and a problem for ones with side effects. Mitigations,
all convention-only (no gateway changes):

- **Single owner.** Run exactly one instance per RPC subject, or have only one instance subscribe.
- **Instance- or entity-addressing.** Address a specific instance (`<service>.<method>.<instance>`)
  or a placed entity (`<service>.<method>.<entity-id>`) — exactly one instance subscribes, so the
  call is single-delivery by *routing*, no queue group needed. See
  [Entity-addressed RPCs](#entity-addressed-rpcs--placement). (`pinned_subject` is the self-pin form.)
- **Idempotent handlers.** Make the RPC safe to execute more than once (dedupe on a request id).
- **Accept first-wins.** For read-only lookups, the wasted duplicate work is harmless.

Native single-delivery load balancing would require queue-group support in the gateway — a
possible future enhancement, not part of this convention today.

---

## Versioning & evolution

- **`schema_version`** gates breaking changes to *this convention*. Bump it only when the
  descriptor format changes incompatibly; discoverers ignore descriptors whose `schema_version`
  they don't support.
- **Descriptor consumers must ignore unknown fields**, so new optional fields are additive.
- **RPC schema changes should be additive** (new optional properties) so existing callers keep
  working; a breaking RPC change is best modeled as a new RPC `name`/`subject`.

---

## Security & trust

The descriptor *body* is **self-asserted**. Any holder of a valid bearer token can publish a
descriptor whose `service`/`instance` fields claim to be any service, and can subscribe to any
RPC subject and answer first. The gateway has **no per-service authorization** — it won't stop
one service from impersonating another within the same trust domain.

However, the *transport* now attributes every descriptor. The gateway stamps a **verified
publisher identity** onto every message — `cc-id` / `cc-auth` (and `cc-email` / `cc-sub` for
multipass JWTs) — and strips any client-supplied copy, so these headers are unspoofable (see the
[Authentication](../README.md#authentication) section). Those headers ride with each descriptor
and are preserved across JetStream replay, so a discoverer can read them and **cross-check the
claimed `service` against the authenticated principal that actually published it** — a basic
anti-impersonation signal.

Still, treat the catalog as **advisory**: identity is now *attributable*, but there is no
per-service authz enforcing that "only principal X may advertise service Y." Use the catalog to
discover and route; make authorization decisions from the verified identity, not the descriptor
body.

---

## Caveats & limits

| Caveat | What to know |
|---|---|
| Multi-instance RPC fan-out | All live instances on a shared subject receive each request and reply; `/v1/request` is first-reply-wins; the others' side effects still run. See [above](#multi-instance-rpcs--fan-out). |
| Snapshot liveness gap | A plain subscribe learns of an instance only at its next heartbeat (≤ `heartbeat_interval`); a fresh-`session_id` durable snapshot is immediate. |
| Durable-resume gap | A reused `session_id` gets only post-ack changes; prefer a fresh `session_id` per boot for a full snapshot. |
| Descriptor size vs payload limit | NATS `max_payload` is 1 MB and applies to the stored descriptor too. Keep descriptors well under it. If schemas are large, keep them thin and expose a `<service>.schema` RPC that returns the full schema on demand, or put a schema URL in `metadata`. |
| Subject token collisions | `<service>`/`<instance>` must be single tokens; a `.` reshapes the tree and breaks the per-instance catalog slot. Sanitize at the service. |
| Entity-id token validity | An `<entity-id>` must stay a single NATS token — use `:` between `<instance>` and `<local>`, never `.`/`*`/`>`. A `.` splits the suffix onto a different subtree, the host never subscribes there, and the call lands nowhere. |
| Targeting a dead entity | An entity-addressed call whose host is gone has no subscriber → immediate `200` + empty body + `Status: 503` (no-responders), not a timeout. Treat it as "entity not here." |
| Trust | Descriptor *bodies* are self-claims; no per-service authz. But the gateway stamps a verified, unspoofable publisher identity (`cc-*`) on every descriptor, so the publisher is *attributable* — cross-check the claimed `service` against it. Advisory, not an authz boundary. |
| One `session_id` per discoverer | Two connections sharing a `session_id` for the same subject is undefined — give every logical discoverer its own id. |
| Gateway restart | Pending acks are lost on restart, so a discoverer may receive a duplicate descriptor — harmless, since descriptors are idempotent soft state. |
| `DISCOVERY` stream is gateway-managed | Auto-provisioned on startup; don't hand-create or delete it. Tune only via `DISCOVERY_MAX_AGE_SECONDS`. |

---

## Worked example

**Service `geo`, instance `geo-1`, one RPC `geo.lookup`.**

The gateway has already created the `DISCOVERY` stream on startup — nothing to set up.

**1. `geo-1` heartbeats its descriptor** every 30s (descriptor from [The descriptor](#the-descriptor), with a fresh `ts` each beat):

```bash
curl -X POST "$CHITCHAT_URL/v1/publish" \
  -H "Authorization: Bearer $CHITCHAT_TOKEN" -H "Content-Type: application/json" \
  -d "{\"subject\":\"discovery.service.geo.geo-1\",\"data\":\"$DESC_B64\"}"
```

**2. A discoverer browses the catalog** (fresh `session_id` → instant snapshot, then live):

```jsonc
// wss://chitchat.miren.garden/v1/ws
{"action":"subscribe","subject":"discovery.service.>","session_id":"disco-boot-7f3a"}
// <- {"type":"message","subject":"discovery.service.geo.geo-1","data":"<base64 descriptor>"}
// decode -> group by service "geo", key by instance "geo-1", drop if now-ts > ttl_seconds
```

**3. The discoverer calls the RPC** it found (`subject` and `timeout_ms` from the descriptor):

```bash
curl -X POST "$CHITCHAT_URL/v1/request" \
  -H "Authorization: Bearer $CHITCHAT_TOKEN" -H "Content-Type: application/json" \
  -d '{"subject":"geo.lookup","timeout_ms":3000,"data":"eyJhZGRyZXNzIjoiMTIzIE1haW4gU3QifQ=="}'
# 200 + data = base64 of {"lat":37.7,"lng":-122.4}   (success; or an error envelope on failure)
# 200 + empty data + Status:503 header              -> no geo instance is listening (service down)
# 504                                                -> an instance was reached but didn't reply in time
```

**4. `geo-1` stops** (crash or deploy). It stops heartbeating; discoverers drop it after
`ttl_seconds`, and its descriptor is purged from the stream after `DISCOVERY_MAX_AGE_SECONDS`,
so a fresh snapshot no longer lists it.
