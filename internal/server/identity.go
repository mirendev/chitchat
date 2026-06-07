package server

import (
	"strings"

	"github.com/nats-io/nats.go"

	"miren.dev/chitchat/internal/auth"
)

// Gateway-owned, verified identity headers stamped onto every outbound message.
// Kept short (cc- prefix) because they ride on every message. Recipients can
// trust these because the gateway is the only path to NATS and it strips any
// client-supplied copy (see buildStampedHeader).
const (
	headerIdentity = "cc-id"
	headerAuth     = "cc-auth"
	headerEmail    = "cc-email"
	headerSubject  = "cc-sub"

	// reservedHeaderPrefix is matched case-insensitively against client header
	// keys. nats.Header is a plain case-sensitive map, so an exact-name strip
	// would let "Cc-Id" (or any other casing) slip through — we must compare on
	// the lowercased, trimmed prefix.
	reservedHeaderPrefix = "cc-"
)

// buildStampedHeader copies the client's headers minus any reserved cc-* keys
// (in any casing — anti-spoofing), then stamps the caller's
// verified identity from p. It always returns a non-nil header so identity is
// present even when the client sent no headers. A nil principal still strips
// reserved keys (fail closed) but stamps nothing.
func buildStampedHeader(clientHeaders map[string]string, p *auth.Principal) nats.Header {
	h := make(nats.Header)

	for k, v := range clientHeaders {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(k)), reservedHeaderPrefix) {
			continue // never let a client assert its own identity
		}
		h.Set(k, v)
	}

	if p != nil {
		h.Set(headerIdentity, p.Identity)
		h.Set(headerAuth, p.Method)
		if p.Email != "" {
			h.Set(headerEmail, p.Email)
		}
		if p.Subject != "" {
			h.Set(headerSubject, p.Subject)
		}
	}

	return h
}
