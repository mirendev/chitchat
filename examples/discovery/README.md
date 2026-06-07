# Discovery example

A self-contained, copy-from reference for the [service discovery
convention](../../docs/SERVICE_DISCOVERY.md). Depends only on `gorilla/websocket`
and the standard library, so you can lift whole functions into your own service.

It does the two things every participating service needs:

- **`advertise()`** — heartbeats this process's descriptor into the catalog (`POST /v1/publish`).
- **`discover()`** — durably subscribes to the whole catalog with a **unique `session_id`** and
  keeps a live, TTL-expiring view of every service. The unique id is what makes it
  "all see all": each discoverer gets its own JetStream consumer and so receives every
  descriptor, instead of competing for them.

`callRPC()` is included as reference for invoking a discovered RPC (it's not called by `main`,
which only advertises + discovers).

## Run it

Against a local gateway:

```bash
CHITCHAT_URL=http://127.0.0.1:8080 CHITCHAT_TOKEN=yourtoken go run ./examples/discovery
```

Run it in **two terminals**. Each process prints a catalog, and each catalog lists **both**
instances — because each uses its own `session_id`. Stop one and watch it disappear from the
other's catalog after its TTL.

Defaults: `CHITCHAT_URL=https://chitchat.miren.garden`, token from `multipass token`.

## What to copy

| You want to… | Copy |
|---|---|
| Show your service in discovery | the `Descriptor`/`RPC`/`Event` structs + `advertise()` |
| Consume the catalog | `discover()` + `runDiscover()` + the `catalog` type |
| Call a service you discovered | `callRPC()` |

Tune `serviceName`, `heartbeatInterval`, and `ttl` at the top of `main.go`. Keep
`heartbeat < ttl < the gateway's DISCOVERY_MAX_AGE_SECONDS` (default 120).
