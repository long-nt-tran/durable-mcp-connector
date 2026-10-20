package auth

import (
	"bytes"
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

func tokenScopes(claims map[string]any) []string {
	var scopes []string
	for _, name := range []string{"scope", "scp"} {
		switch value := claims[name].(type) {
		case string:
			scopes = append(scopes, strings.Fields(value)...)
		case []any:
			for _, item := range value {
				if scope, ok := item.(string); ok {
					scopes = append(scopes, scope)
				}
			}
		}
	}
	return scopes
}

var signingAlgorithms = []jose.SignatureAlgorithm{
	jose.RS256, jose.RS384, jose.RS512,
	jose.PS256, jose.PS384, jose.PS512,
	jose.ES256, jose.ES384, jose.ES512,
	jose.EdDSA,
}

var registeredClaims = []string{"iss", "sub", "aud", "exp", "nbf", "iat", "jti"}

const (
	maxResponseBytes = 1 << 20
	maxLoggedChars   = 512
	maxURLBytes      = 512
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

var (
	discoveryRetryInterval = 30 * time.Second
	keyRefreshInterval     = time.Minute
	// keyMaxAge bounds how long a signing key the issuer has removed, such as a
	// compromised one, is still trusted, while the issuer's keys can be fetched.
	keyMaxAge = 5 * time.Minute
)

var errCannotVerify = errors.New("cannot verify access tokens")

type unavailableError struct {
	error
	// rateLimited reports that the request did not fetch the signing keys
	// because a fetch started less than keyRefreshInterval before it.
	rateLimited bool
}

type call struct {
	done     chan struct{}
	started  time.Time
	finished time.Time
	err      error
}

func (i *issuer) startCall(f func() error) *call {
	c := &call{done: make(chan struct{}), started: time.Now()}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				i.logger.Error("fetch from issuer panicked", "issuer", i.url, "error", fmt.Sprintf("panic: %v\n%s", r, debug.Stack()))
				c.err = fmt.Errorf("panic: %v", r)
			}
			c.finished = time.Now()
			close(c.done)
		}()
		c.err = f()
	}()
	return c
}

func (c *call) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *call) wait(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type issuer struct {
	url          string
	loopback     bool
	verifyConfig *oidc.Config
	logger       *slog.Logger
	mu           sync.Mutex
	discovery    *call
	jwksURL      string
	fetch        *call
	keys         []jose.JSONWebKey
	fetched      time.Time
}

func (i *issuer) discover(ctx context.Context) (string, error) {
	i.mu.Lock()
	if i.jwksURL != "" {
		defer i.mu.Unlock()
		return i.jwksURL, nil
	}
	c := i.discovery
	if c == nil || c.isDone() && time.Since(c.finished) >= discoveryRetryInterval {
		c = i.startCall(i.discoverJWKSURL)
		i.discovery = c
	}
	i.mu.Unlock()
	if err := c.wait(ctx); err != nil {
		return "", err
	}
	if c.err != nil {
		return "", c.err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.jwksURL, nil
}

func (i *issuer) discoverJWKSURL() error {
	var meta struct {
		Issuer  string `json:"issuer"`
		JWKSURL string `json:"jwks_uri"`
	}
	if err := i.get(strings.TrimSuffix(i.url, "/")+"/.well-known/openid-configuration", &meta); err != nil {
		return fmt.Errorf("discovering issuer %s: %w", i.url, err)
	}
	if meta.Issuer != i.url {
		return fmt.Errorf("discovering issuer %s: discovery document names issuer %.*q", i.url, maxLoggedChars, meta.Issuer)
	}
	if meta.JWKSURL == "" {
		return fmt.Errorf("discovering issuer %s: discovery document has no jwks_uri", i.url)
	}
	if len(meta.JWKSURL) > maxURLBytes {
		return fmt.Errorf("discovering issuer %s: jwks_uri is longer than %d bytes", i.url, maxURLBytes)
	}
	if u, err := url.Parse(meta.JWKSURL); err != nil || !i.allowedURL(u) || u.Host == "" {
		return fmt.Errorf("discovering issuer %s: jwks_uri %q must be an https URL (http only for a loopback host, when the issuer is on one)", i.url, meta.JWKSURL)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.jwksURL = meta.JWKSURL
	return nil
}

func (i *issuer) allowedURL(u *url.URL) bool {
	return u.Scheme == "https" || u.Scheme == "http" && i.loopback && loopbackHost(u.Hostname())
}

func (i *issuer) get(u string, v any) (err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("%.*s", maxLoggedChars, err)
		}
	}()
	client := *httpClient
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if !i.allowedURL(req.URL) {
			return errors.New("redirect must go to an https URL (http only for a loopback host, when the issuer is on one)")
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	resp, err := client.Get(u)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return fmt.Errorf("%s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxLoggedChars))
		return fmt.Errorf("%s: %s: %q", u, resp.Status, body)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("%s: %w", u, err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("%s: response is larger than %d bytes", u, maxResponseBytes)
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("%s: %w", u, err)
	}
	return nil
}

// signingKeys returns the keys that may have signed a token with header,
// fetching the issuer's keys when they are older than keyMaxAge or none match,
// at most once per keyRefreshInterval.
func (i *issuer) signingKeys(ctx context.Context, jwksURL string, header jose.Header) ([]crypto.PublicKey, error) {
	i.mu.Lock()
	keys, fetched := i.keys, i.fetched
	i.mu.Unlock()
	if time.Since(fetched) < keyMaxAge {
		if matching := matchingKeys(keys, header); len(matching) > 0 {
			return matching, nil
		}
	}
	i.mu.Lock()
	c := i.fetch
	rateLimited := c != nil && c.isDone()
	if c == nil || rateLimited && time.Since(c.started) >= keyRefreshInterval {
		c, rateLimited = i.startCall(func() error {
			err := i.fetchKeys(jwksURL)
			if err != nil {
				i.logger.Warn("cannot fetch signing keys", "issuer", i.url, "error", err)
			}
			return err
		}), false
		i.fetch = c
	}
	i.mu.Unlock()
	if err := c.wait(ctx); err != nil {
		return nil, unavailableError{error: err}
	}
	i.mu.Lock()
	keys = i.keys
	i.mu.Unlock()
	if matching := matchingKeys(keys, header); len(matching) > 0 {
		return matching, nil
	}
	switch {
	case c.err != nil:
		return nil, unavailableError{error: c.err, rateLimited: rateLimited}
	case rateLimited:
		// The key may be newer than the last fetch, so the token is rejected only if it has expired.
		return nil, unavailableError{error: fmt.Errorf("no signing key matches the token, and the keys were fetched less than %v ago", keyRefreshInterval), rateLimited: true}
	}
	return nil, errors.New("no signing key matches the token")
}

func (i *issuer) fetchKeys(jwksURL string) error {
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := i.get(jwksURL, &set); err != nil {
		return fmt.Errorf("fetching signing keys: %w", err)
	}
	var keys []jose.JSONWebKey
	for _, raw := range set.Keys {
		var k jose.JSONWebKey
		if err := k.UnmarshalJSON(raw); err != nil {
			var meta struct {
				KeyID string `json:"kid"`
			}
			json.Unmarshal(raw, &meta)
			i.logger.Warn("skipped signing key", "issuer", i.url, "kid", fmt.Sprintf("%.*s", maxLoggedChars, meta.KeyID), "error", fmt.Sprintf("%.*s", maxLoggedChars, err))
			continue
		}
		if !k.IsPublic() || k.Use == "enc" || k.Algorithm != "" && !slices.Contains(signingAlgorithms, jose.SignatureAlgorithm(k.Algorithm)) {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return fmt.Errorf("fetching signing keys: %s: no usable signing key", jwksURL)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.keys, i.fetched = keys, time.Now()
	return nil
}

// matchingKeys returns the keys with header's kid, or all keys if it has none,
// leaving out those whose alg differs from header's.
func matchingKeys(keys []jose.JSONWebKey, header jose.Header) []crypto.PublicKey {
	var matching []crypto.PublicKey
	for _, k := range keys {
		if header.KeyID != "" && k.KeyID != header.KeyID || k.Algorithm != "" && k.Algorithm != header.Algorithm {
			continue
		}
		matching = append(matching, k.Key)
	}
	return matching
}

func (i *issuer) verify(ctx context.Context, token string, jws *jose.JSONWebSignature) (*mcpauth.TokenInfo, error) {
	jwksURL, err := i.discover(ctx)
	if err != nil {
		return nil, unavailableError{error: err}
	}
	keys, err := i.signingKeys(ctx, jwksURL, jws.Signatures[0].Header)
	var unavailable unavailableError
	if errors.As(err, &unavailable) && unavailable.rateLimited && expired(jws.UnsafePayloadWithoutVerification()) {
		// A retry cannot make an expired token valid, so it gets the 401 that makes its client refresh it.
		return nil, errors.New("token is expired")
	}
	if err != nil {
		return nil, err
	}
	verified, err := oidc.NewVerifier(i.url, &oidc.StaticKeySet{PublicKeys: keys}, i.verifyConfig).Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	var registered jwt.Claims
	var claims map[string]any
	if err := verified.Claims(&registered); err != nil {
		return nil, err
	}
	if err := verified.Claims(&claims); err != nil {
		return nil, err
	}
	if registered.Expiry == nil {
		return nil, errors.New("token has no exp claim")
	}
	if registered.Subject == "" {
		return nil, errors.New("token has no sub claim")
	}
	if err := registered.Validate(jwt.Expected{}); err != nil {
		return nil, err
	}
	return &mcpauth.TokenInfo{
		UserID:     registered.Issuer + "#" + registered.Subject,
		Expiration: registered.Expiry.Time(),
		Scopes:     tokenScopes(claims),
		Extra:      claims,
	}, nil
}

// expired reports whether payload's exp claim, which is unverified, is past the
// leeway.
func expired(payload []byte) bool {
	var claims jwt.Claims
	return json.Unmarshal(payload, &claims) == nil && errors.Is(claims.Validate(jwt.Expected{}), jwt.ErrExpired)
}

// checkJSON rejects duplicate names before another JSON decoder reads the claims.
func checkJSON(d *json.Decoder, depth int) error {
	if depth > 128 {
		return errors.New("claims are nested too deeply")
	}
	value, err := d.Token()
	if err != nil {
		return errors.New("malformed claims")
	}
	delimiter, ok := value.(json.Delim)
	if !ok {
		return nil
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("malformed claims")
	}
	names := map[string]bool{}
	for d.More() {
		if delimiter == '{' {
			key, err := d.Token()
			if err != nil {
				return errors.New("malformed claims")
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("malformed claims")
			}
			if names[name] {
				return errors.New("duplicate claim name")
			}
			names[name] = true
		}
		if err := checkJSON(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

func unverifiedIssuer(payload []byte) (string, error) {
	d := json.NewDecoder(bytes.NewReader(payload))
	d.UseNumber()
	if err := checkJSON(d, 0); err != nil {
		return "", err
	}
	if _, err := d.Token(); err != io.EOF {
		return "", errors.New("malformed claims")
	}
	var claims map[string]json.RawMessage
	if err := json.Unmarshal(payload, &claims); err != nil || claims == nil {
		return "", errors.New("malformed claims")
	}
	for name := range claims {
		for _, registered := range registeredClaims {
			if name != registered && strings.EqualFold(name, registered) {
				return "", fmt.Errorf("claim name differs only in case from %q", registered)
			}
		}
	}
	raw, ok := claims["iss"]
	if !ok {
		return "", errors.New("token has no iss claim")
	}
	var iss string
	if err := json.Unmarshal(raw, &iss); err != nil {
		return "", errors.New("malformed claims")
	}
	return iss, nil
}

// newTokenVerifier returns a verifier for the configured trusted issuers.
func newTokenVerifier(issuers []IssuerConfig, logger *slog.Logger) mcpauth.TokenVerifier {
	algs := make([]string, len(signingAlgorithms))
	for k, alg := range signingAlgorithms {
		algs[k] = string(alg)
	}
	byIssuer := map[string]*issuer{}
	for _, iss := range issuers {
		u, err := url.Parse(iss.Issuer)
		byIssuer[iss.Issuer] = &issuer{url: iss.Issuer, loopback: err == nil && loopbackHost(u.Hostname()), verifyConfig: &oidc.Config{ClientID: iss.Audience, SupportedSigningAlgs: algs, SkipExpiryCheck: true}, logger: logger}
	}
	verify := func(ctx context.Context, token string) (string, *mcpauth.TokenInfo, error) {
		jws, err := jose.ParseSignedCompact(token, signingAlgorithms)
		if errors.As(err, new(*jose.ErrUnexpectedSignatureAlgorithm)) {
			return "", nil, errors.New("unsupported signature algorithm")
		} else if err != nil {
			return "", nil, errors.New("malformed token")
		}
		name, err := unverifiedIssuer(jws.UnsafePayloadWithoutVerification())
		if err != nil {
			return "", nil, err
		}
		iss, ok := byIssuer[name]
		if !ok {
			return "", nil, errors.New("unknown issuer")
		}
		info, err := iss.verify(ctx, token, jws)
		return iss.url, info, err
	}
	return func(ctx context.Context, token string, _ *http.Request) (*mcpauth.TokenInfo, error) {
		iss, info, err := verify(ctx, token)
		var attrs []any
		if iss != "" {
			attrs = append(attrs, "issuer", iss)
		}
		var unavailable unavailableError
		switch {
		case errors.As(err, &unavailable):
			level := slog.LevelError
			switch {
			case ctx.Err() != nil:
				level = slog.LevelDebug
			case unavailable.rateLimited:
				// Keeps tokens with made-up key IDs from flooding the log with errors.
				level = slog.LevelWarn
			}
			logger.Log(ctx, level, "cannot verify access token", append(attrs, "error", unavailable.error)...)
			return nil, errCannotVerify
		case err != nil:
			// A token rejected before a configured issuer checked it may come from anyone, so it logs at debug.
			level := slog.LevelWarn
			if iss == "" {
				level = slog.LevelDebug
			}
			logger.Log(ctx, level, "rejected access token", append(attrs, "reason", err)...)
			return nil, mcpauth.ErrInvalidToken
		}
		return info, nil
	}
}
