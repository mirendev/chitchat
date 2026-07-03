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

	// userHeaderPrefix carves out a caller-asserted end-user identity namespace
	// (cc-user-id / cc-user-email / cc-user-name) from the reserved cc- prefix.
	// Unlike the gateway-owned cc-{id,auth,email,sub}, these are NOT verified by
	// the gateway — a trusted service (e.g. inu-web) sets them to name the end
	// user it is acting on behalf of, and recipients read them as the subject
	// while cc-* remains the verified acting service. A future mode may have the
	// gateway verify a supplied user token and stamp these itself; the header
	// names are stable so that upgrade is transparent to recipients.
	userHeaderPrefix = "cc-user-"
)

// buildStampedHeader copies the client's headers minus the gateway-owned
// cc-{id,auth,email,sub} keys (in any casing — anti-spoofing), then stamps the
// caller's verified identity from p. Client-set cc-user-* headers are allowed
// through (a caller-asserted end-user identity; see userHeaderPrefix). It always
// returns a non-nil header so identity is present even when the client sent no
// headers. A nil principal still strips the reserved keys (fail closed) but
// stamps nothing.
func buildStampedHeader(clientHeaders map[string]string, p *auth.Principal) nats.Header {
	h := make(nats.Header)

	for k, v := range clientHeaders {
		lk := strings.ToLower(strings.TrimSpace(k))
		if strings.HasPrefix(lk, reservedHeaderPrefix) && !strings.HasPrefix(lk, userHeaderPrefix) {
			continue // never let a client assert the gateway-owned cc-* identity
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
