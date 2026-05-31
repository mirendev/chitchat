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

type ctxKey struct{}

// FromContext returns the verified claims for the current request, if any.
func FromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*Claims)
	return c, ok
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
				next.ServeHTTP(w, r)
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
				ctx := context.WithValue(r.Context(), ctxKey{}, claims)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			writeError(w, http.StatusUnauthorized, "invalid token")
		})
	}
}

// Verifier holds a cached JWKS for verifying multipass-issued JWTs.
type Verifier struct {
	keys          keyfunc.Keyfunc
	allowedDomain string
}

// NewVerifier fetches the JWKS at <multipassBaseURL>/.well-known/jwks.json
// and returns a Verifier with periodic auto-refresh. If allowedDomain is
// non-empty, emails must match it.
func NewVerifier(ctx context.Context, multipassBaseURL, allowedDomain string) (*Verifier, error) {
	jwksURL := strings.TrimRight(multipassBaseURL, "/") + "/.well-known/jwks.json"
	k, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		return nil, fmt.Errorf("fetch jwks at %s: %w", jwksURL, err)
	}
	return &Verifier{keys: k, allowedDomain: allowedDomain}, nil
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
	if v.allowedDomain != "" && claims.Email != "" {
		if !strings.HasSuffix(strings.ToLower(claims.Email), "@"+strings.ToLower(v.allowedDomain)) {
			return nil, fmt.Errorf("email %s not in allowed domain %s", claims.Email, v.allowedDomain)
		}
	}
	return &claims, nil
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
