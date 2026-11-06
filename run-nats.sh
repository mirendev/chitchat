#!/bin/bash
set -e

# Fail with a clear message rather than nats-server's config parse error.
: "${NATS_TOKEN:?NATS_TOKEN is required: nats-server.conf uses it to authorize the gateway}"

mkdir -p /miren/data/local/jetstream

exec /usr/local/bin/nats-server -c /etc/nats/nats-server.conf
