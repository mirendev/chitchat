package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// Auth methods, used as the value of Principal.Method and the
// cc-auth header.
const (
	MethodJWT    = "jwt"
	MethodAPIKey = "apikey"

	// APIKeyIdentity is the identity stamped for callers authenticated with the
	// shared API key (which carries no per-principal identity of its own).
	APIKeyIdentity = "apikey"
)

// Claims is the subset of multipass JWT claims we consume.
type Claims struct {
	jwt.RegisteredClaims
	Email string `json:"email,omitempty"`
}

// Identity returns the best-effort caller identity (email preferred, then sub).
func (c *Claims) Identity() string {
	if c.Email != "" {
		return c.Email
	}
	return c.Subject
}

// Principal is the verified identity of the current caller, derived from
// whichever auth method succeeded. It is what the gateway stamps onto outbound
// messages and reports via /v1/whoami.
type Principal struct {
	Method   string // MethodJWT or MethodAPIKey
	Identity string // canonical identity: email else subject (JWT), or "apikey"
	Email    string // JWT only, when present
	Subject  string // JWT only, when present
}

type ctxKey struct{}

// FromContext returns the verified principal for the current request, if any.
func FromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(*Principal)
	return p, ok
}

// Middleware accepts either a static API key or a multipass JWT (if configured).
// API key is checked first; if it doesn't match and a Verifier is present, the
// token is verified as a JWT.
func Middleware(apiKey string, verifier *Verifier, logger *slog.Logger) func(http.Handler) http.Handler {
	keyBytes := []byte(apiKey)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var token string

			if header := r.Header.Get("Authorization"); header != "" {
				var ok bool
				token, ok = strings.CutPrefix(header, "Bearer ")
				if !ok {
					writeError(w, http.StatusUnauthorized, "invalid authorization scheme")
					return
				}
			} else if q := r.URL.Query().Get("token"); q != "" {
				token = q
			} else {
				writeError(w, http.StatusUnauthorized, "missing authorization")
				return
			}

			// Try static API key first.
			if subtle.ConstantTimeCompare([]byte(token), keyBytes) == 1 {
				p := &Principal{Method: MethodAPIKey, Identity: APIKeyIdentity}
				ctx := context.WithValue(r.Context(), ctxKey{}, p)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			// Fall back to multipass JWT if configured.
			if verifier != nil {
				claims, err := verifier.Verify(token)
				if err != nil {
					logger.Debug("jwt verify failed", "err", err)
					writeError(w, http.StatusUnauthorized, "invalid token")
					return
				}
				p := &Principal{
					Method:   MethodJWT,
					Identity: claims.Identity(),
					Email:    claims.Email,
					Subject:  claims.Subject,
				}
				ctx := context.WithValue(r.Context(), ctxKey{}, p)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			writeError(w, http.StatusUnauthorized, "invalid token")
		})
	}
}

// Verifier holds a cached JWKS for verifying multipass-issued JWTs.
type Verifier struct {
	keys            keyfunc.Keyfunc
	allowedDomain   string
	allowedPrefixes []string
}

// NewVerifier fetches the JWKS at <multipassBaseURL>/.well-known/jwks.json
// and returns a Verifier with periodic auto-refresh.
//
// Authorization (when any restriction is configured): a token is accepted if its
// email is in allowedDomain, OR its email/subject starts with one of
// allowedPrefixes. The prefix list is how non-email service identities (e.g.
// "org:org-Miren-...:app:clusteragent:...") are admitted, since they have no
// @domain. With neither configured, all signature-valid tokens are accepted.
func NewVerifier(ctx context.Context, multipassBaseURL, allowedDomain string, allowedPrefixes []string) (*Verifier, error) {
	jwksURL := strings.TrimRight(multipassBaseURL, "/") + "/.well-known/jwks.json"
	k, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		return nil, fmt.Errorf("fetch jwks at %s: %w", jwksURL, err)
	}
	return &Verifier{keys: k, allowedDomain: allowedDomain, allowedPrefixes: allowedPrefixes}, nil
}

// Verify parses and verifies a Bearer JWT. Returns claims on success.
func (v *Verifier) Verify(tokenStr string) (*Claims, error) {
	var claims Claims
	tok, err := jwt.ParseWithClaims(tokenStr, &claims, v.keys.Keyfunc,
		jwt.WithValidMethods([]string{"ES256", "EdDSA", "RS256"}),
	)
	if err != nil {
		return nil, err
	}
	if !tok.Valid {
		return nil, errors.New("invalid token")
	}
	if err := v.authorize(claims.Email, claims.Subject); err != nil {
		return nil, err
	}
	return &claims, nil
}

// authorize applies the domain / prefix allowlist. Pure (no crypto) so it can be
// unit-tested directly.
func (v *Verifier) authorize(email, subject string) error {
	// No restrictions configured: any signature-valid token is allowed.
	if v.allowedDomain == "" && len(v.allowedPrefixes) == 0 {
		return nil
	}
	// Email in the allowed domain (human users).
	if v.allowedDomain != "" && strings.Contains(email, "@") &&
		strings.HasSuffix(strings.ToLower(email), "@"+strings.ToLower(v.allowedDomain)) {
		return nil
	}
	// Identity matches an allowed prefix (service/workload identities).
	for _, p := range v.allowedPrefixes {
		if p != "" && (strings.HasPrefix(email, p) || strings.HasPrefix(subject, p)) {
			return nil
		}
	}
	id := email
	if id == "" {
		id = subject
	}
	return fmt.Errorf("identity %q not allowed (domain=%q, prefixes=%v)", id, v.allowedDomain, v.allowedPrefixes)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
