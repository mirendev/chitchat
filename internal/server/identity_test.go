package server

import (
	"strings"
	"testing"

	"miren.dev/chitchat/internal/auth"
)

func TestBuildStampedHeader(t *testing.T) {
	jwtPrincipal := &auth.Principal{
		Method:   auth.MethodJWT,
		Identity: "alice@example.com",
		Email:    "alice@example.com",
		Subject:  "user-123",
	}
	apiKeyPrincipal := &auth.Principal{
		Method:   auth.MethodAPIKey,
		Identity: auth.APIKeyIdentity,
	}

	t.Run("stamps verified identity with no client headers", func(t *testing.T) {
		h := buildStampedHeader(nil, jwtPrincipal)
		if got := h.Get(headerIdentity); got != "alice@example.com" {
			t.Errorf("identity = %q, want alice@example.com", got)
		}
		if got := h.Get(headerAuth); got != "jwt" {
			t.Errorf("auth = %q, want jwt", got)
		}
		if got := h.Get(headerEmail); got != "alice@example.com" {
			t.Errorf("email = %q, want alice@example.com", got)
		}
		if got := h.Get(headerSubject); got != "user-123" {
			t.Errorf("subject = %q, want user-123", got)
		}
	})

	t.Run("strips client-supplied reserved headers in any casing", func(t *testing.T) {
		client := map[string]string{
			"cc-id":         "admin@evil",
			"CC-ID":         "admin2@evil",
			"Cc-Auth":       "jwt",
			"  cc-email ":   "spoof@evil",
			"X-Pipe-Sender": "keep-me",
			"Foo":           "bar",
		}
		h := buildStampedHeader(client, apiKeyPrincipal)

		// Verified values win.
		if got := h.Get(headerIdentity); got != auth.APIKeyIdentity {
			t.Errorf("identity = %q, want %q", got, auth.APIKeyIdentity)
		}
		if got := h.Get(headerAuth); got != "apikey" {
			t.Errorf("auth = %q, want apikey", got)
		}
		// No spoofed value survives under ANY key/casing.
		for k, vs := range h {
			if strings.HasPrefix(strings.ToLower(k), reservedHeaderPrefix) {
				for _, v := range vs {
					if v == "admin@evil" || v == "admin2@evil" || v == "spoof@evil" {
						t.Errorf("spoofed value %q survived under key %q", v, k)
					}
				}
			}
		}
		// Non-reserved headers are preserved untouched.
		if got := h.Get("X-Pipe-Sender"); got != "keep-me" {
			t.Errorf("X-Pipe-Sender = %q, want keep-me", got)
		}
		if got := h.Get("Foo"); got != "bar" {
			t.Errorf("Foo = %q, want bar", got)
		}
	})

	t.Run("api-key principal omits email and subject", func(t *testing.T) {
		h := buildStampedHeader(nil, apiKeyPrincipal)
		if got := h.Get(headerEmail); got != "" {
			t.Errorf("email = %q, want empty", got)
		}
		if got := h.Get(headerSubject); got != "" {
			t.Errorf("subject = %q, want empty", got)
		}
	})

	t.Run("nil principal still strips reserved client headers", func(t *testing.T) {
		client := map[string]string{
			"cc-id": "admin@evil",
			"Keep":  "yes",
		}
		h := buildStampedHeader(client, nil)
		if got := h.Get(headerIdentity); got != "" {
			t.Errorf("identity = %q, want empty (stripped, none stamped)", got)
		}
		if got := h.Get("Keep"); got != "yes" {
			t.Errorf("Keep = %q, want yes", got)
		}
	})
}
