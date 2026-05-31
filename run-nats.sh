#!/bin/bash
set -e

mkdir -p /miren/data/local/jetstream

exec /usr/local/bin/nats-server -c /etc/nats/nats-server.conf
