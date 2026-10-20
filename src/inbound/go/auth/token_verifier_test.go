package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc/oidctest"
	"github.com/go-jose/go-jose/v4"
	mcpauth "github.com/modelcontextprotocol/go-sdk/auth"
)

const (
	discoveryPath = "/.well-known/openid-configuration"
	keysPath      = "/keys"
)

type fakeIssuer struct {
	*httptest.Server
	mu        sync.Mutex
	oidc      oidctest.Server
	key       *rsa.PrivateKey
	kid       string
	requests  map[string]int
	overrides map[string]http.HandlerFunc
}

func newIssuer(t *testing.T) *fakeIssuer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return newIssuerOn(t, ln)
}

func newIssuerOn(t *testing.T, ln net.Listener) *fakeIssuer {
	t.Helper()
	f := &fakeIssuer{requests: map[string]int{}, overrides: map[string]http.HandlerFunc{}}
	f.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests[r.URL.Path]++
		override := f.overrides[r.URL.Path]
		f.mu.Unlock()
		if override != nil {
			override(w, r)
			return
		}
		f.serveOIDC(w, r)
	}))
	f.Listener.Close()
	f.Listener = ln
	f.Start()
	t.Cleanup(f.Close)
	f.oidc.SetIssuer(f.URL)
	f.rotate(t, "k1")
	return f
}

func (f *fakeIssuer) serveOIDC(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.oidc.ServeHTTP(w, r)
}

func (f *fakeIssuer) override(path string, h http.HandlerFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.overrides[path] = h
}

func (f *fakeIssuer) count(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[path]
}

func newKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func (f *fakeIssuer) rotate(t *testing.T, kid string) {
	key := newKey(t)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.key, f.kid = key, kid
	f.oidc.PublicKeys = []oidctest.PublicKey{{PublicKey: &key.PublicKey, KeyID: kid, Algorithm: "RS256"}}
}

func (f *fakeIssuer) claims(audience string) map[string]any {
	now := time.Now()
	return map[string]any{"iss": f.URL, "sub": "alice", "aud": audience, "exp": now.Add(time.Hour).Unix(), "iat": now.Unix()}
}

func (f *fakeIssuer) sign(t *testing.T, claims map[string]any) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	return oidctest.SignIDToken(f.key, f.kid, "RS256", marshal(t, claims))
}

func marshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

type logBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

var nop = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

func newHandler(a *Config, next http.Handler) (http.Handler, *logBuffer) {
	logs := &logBuffer{}
	return newBearerAuthHandler(a, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), next), logs
}

func request(ctx context.Context, h http.Handler, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/mcp", nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func serve(a *Config, token string) (*httptest.ResponseRecorder, *mcpauth.TokenInfo, string) {
	var info *mcpauth.TokenInfo
	h, logs := newHandler(a, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		info = mcpauth.TokenInfoFromContext(r.Context())
	}))
	w := request(context.Background(), h, token)
	return w, info, logs.String()
}

func setInterval(t *testing.T, v *time.Duration, d time.Duration) {
	old := *v
	*v = d
	t.Cleanup(func() { *v = old })
}

func newAuthConfig(issuers ...string) *Config {
	a := &Config{Resource: "https://mcp.example.com/mcp"}
	for _, iss := range issuers {
		a.Issuers = append(a.Issuers, IssuerConfig{Issuer: iss, Audience: "connector"})
	}
	return a
}

func TestRequireToken(t *testing.T) {
	idp, second := newIssuer(t), newIssuer(t)
	a := newAuthConfig(idp.URL, second.URL)
	a.Issuers[1].Audience = "second-audience"

	with := func(changes map[string]any) map[string]any {
		claims := idp.claims("connector")
		for k, v := range changes {
			if v == nil {
				delete(claims, k)
			} else {
				claims[k] = v
			}
		}
		return claims
	}
	unsigned := func(header string, claims map[string]any) string {
		enc := base64.RawURLEncoding
		return enc.EncodeToString([]byte(header)) + "." + enc.EncodeToString([]byte(marshal(t, claims))) + "."
	}
	signRaw := func(format string) string {
		return oidctest.SignIDToken(idp.key, idp.kid, "RS256", fmt.Sprintf(format, idp.URL, time.Now().Add(time.Hour).Unix()))
	}

	tests := []struct {
		name, token, issuer, reason string
	}{
		{"not a JWT", "opaque-token", "", "malformed token"},
		{"no exp", idp.sign(t, with(map[string]any{"exp": nil})), idp.URL, "token has no exp claim"},
		{"no sub", idp.sign(t, with(map[string]any{"sub": nil})), idp.URL, "token has no sub claim"},
		{"another issuer's audience", idp.sign(t, with(map[string]any{"aud": "second-audience"})), idp.URL, `expected audience "connector"`},
		{"another issuer's iss", idp.sign(t, second.claims("second-audience")), second.URL, ""},
		{"alg the key does not allow", oidctest.SignIDToken(idp.key, idp.kid, "PS256", marshal(t, with(nil))), idp.URL, "no signing key matches the token"},
		{"unknown issuer", idp.sign(t, with(map[string]any{"iss": "https://unknown.example/" + strings.Repeat("a", 100000)})), "", "unknown issuer"},
		{"alg HS256", oidctest.SignIDToken([]byte("0123456789abcdef0123456789abcdef"), "k1", "HS256", marshal(t, with(nil))), "", "unsupported signature algorithm"},
		{"long alg", unsigned(`{"alg":"`+strings.Repeat("A", 100000)+`","kid":"k1"}`, with(nil)), "", "unsupported signature algorithm"},
		{"duplicate sub", signRaw(`{"iss":%q,"aud":"connector","exp":%d,"sub":"alice","sub":"mallory"}`), "", "duplicate claim name"},
		{"sub with long s", signRaw(`{"iss":%q,"aud":"connector","exp":%d,"sub":"alice","ſub":"mallory"}`), "", `claim name differs only in case from "sub"`},
		{"aud in another case", signRaw(`{"iss":%q,"AUD":"connector","aud":"someone-else","exp":%d,"sub":"alice"}`), "", `claim name differs only in case from "aud"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, info, logs := serve(a, tt.token)
			if w.Code != http.StatusUnauthorized || info != nil || w.Body.String() != "invalid token\n" {
				t.Errorf("status = %d, body %q; want 401 and %q", w.Code, w.Body, "invalid token\n")
			}
			if got, want := w.Header().Get("WWW-Authenticate"), `Bearer resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp", error="invalid_token"`; got != want {
				t.Errorf("WWW-Authenticate = %q, want %q", got, want)
			}
			logged := `level=DEBUG msg="rejected access token" reason=`
			if tt.issuer != "" {
				logged = `level=WARN msg="rejected access token" issuer=` + tt.issuer + " reason="
			}
			if reason := strings.Trim(strconv.Quote(tt.reason), `"`); !strings.Contains(logs, logged) || !strings.Contains(logs, reason) {
				t.Errorf("log %q does not contain %q and %q", logs, logged, reason)
			}
			if len(logs) >= 1000 || tt.issuer == "" && strings.Contains(logs, "issuer=") {
				t.Errorf("log is %d bytes, want under 1000 and no unconfigured issuer: %.200q", len(logs), logs)
			}
		})
	}

	for _, iss := range []struct {
		idp      *fakeIssuer
		audience string
	}{{idp, "connector"}, {second, "second-audience"}} {
		claims := iss.idp.claims(iss.audience)
		claims["groups"] = []any{"ops"}
		w, info, _ := serve(a, iss.idp.sign(t, claims))
		if w.Code != http.StatusOK || info == nil {
			t.Fatalf("%s: status = %d, want 200; body %q", iss.idp.URL, w.Code, w.Body)
		}
		var extra map[string]any
		if err := json.Unmarshal([]byte(marshal(t, claims)), &extra); err != nil {
			t.Fatal(err)
		}
		if exp := time.Unix(claims["exp"].(int64), 0); info.UserID != iss.idp.URL+"#alice" || !info.Expiration.Equal(exp) || !reflect.DeepEqual(info.Extra, extra) {
			t.Errorf("token info = %+v, want caller %s alice, Expiration %v and Extra %#v", info, iss.idp.URL, exp, extra)
		}
	}
}

func TestRequireTokenDiscoversIssuerOnUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	h, logs := newHandler(newAuthConfig("http://"+addr), nop)

	token := func(f *fakeIssuer) string {
		now := time.Now()
		return f.sign(t, map[string]any{"iss": "http://" + addr, "sub": "alice", "aud": "connector", "exp": now.Add(time.Hour).Unix()})
	}

	down := &fakeIssuer{}
	down.rotate(t, "k1")
	w := request(context.Background(), h, token(down))
	if w.Code != http.StatusInternalServerError || w.Body.String() != "cannot verify access tokens\n" {
		t.Errorf("issuer down: status = %d, body %q; want 500 and %q", w.Code, w.Body, "cannot verify access tokens\n")
	}
	if want := `level=ERROR msg="cannot verify access token" issuer=http://` + addr; !strings.Contains(logs.String(), want) {
		t.Errorf("log %q does not contain %q", logs, want)
	}

	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	up := newIssuerOn(t, ln)
	if w := request(context.Background(), h, token(up)); w.Code != http.StatusInternalServerError || w.Body.String() != "cannot verify access tokens\n" {
		t.Errorf("within the retry interval: status = %d, body %q; want 500", w.Code, w.Body)
	}
	if n := up.count(discoveryPath); n != 0 {
		t.Errorf("discovery requests within the retry interval = %d, want 0", n)
	}

	setInterval(t, &discoveryRetryInterval, 0)
	if w := request(context.Background(), h, token(up)); w.Code != http.StatusOK {
		t.Errorf("issuer up: status = %d, want 200; body %q", w.Code, w.Body)
	}
	if n := up.count(discoveryPath); n != 1 {
		t.Errorf("discovery requests = %d, want 1", n)
	}
}

func TestRequireTokenSharesDiscovery(t *testing.T) {
	blocked, other := newIssuer(t), newIssuer(t)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	blocked.override(discoveryPath, func(w http.ResponseWriter, r *http.Request) {
		<-gate
		blocked.serveOIDC(w, r)
	})
	h, logs := newHandler(newAuthConfig(blocked.URL, other.URL), nop)

	const waiters = 5
	codes := make(chan int, waiters)
	for range waiters {
		token := blocked.sign(t, blocked.claims("connector"))
		go func() { codes <- request(context.Background(), h, token).Code }()
	}
	for deadline := time.Now().Add(5 * time.Second); blocked.count(discoveryPath) == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for discovery")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan int, 1)
	token := blocked.sign(t, blocked.claims("connector"))
	go func() { cancelled <- request(ctx, h, token).Code }()
	cancel()
	select {
	case code := <-cancelled:
		if code != http.StatusInternalServerError {
			t.Errorf("cancelled request: status = %d, want 500", code)
		}
		if strings.Contains(logs.String(), "level=ERROR") {
			t.Errorf("log %q contains an error for the cancelled request", logs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request still waiting on discovery")
	}

	if w := request(context.Background(), h, other.sign(t, other.claims("connector"))); w.Code != http.StatusOK {
		t.Errorf("other issuer: status = %d, want 200; body %q", w.Code, w.Body)
	}

	release()
	for range waiters {
		if code := <-codes; code != http.StatusOK {
			t.Errorf("waiter: status = %d, want 200", code)
		}
	}
	if n := blocked.count(discoveryPath); n != 1 {
		t.Errorf("discovery requests = %d, want 1", n)
	}
}

func TestRequireTokenKeyRefresh(t *testing.T) {
	idp := newIssuer(t)
	h, logs := newHandler(newAuthConfig(idp.URL), nop)
	k1 := idp.sign(t, idp.claims("connector"))
	expiredClaims := idp.claims("connector")
	expiredClaims["exp"] = time.Now().Add(-time.Hour).Unix()
	expiredK1 := idp.sign(t, expiredClaims)
	claims, other := marshal(t, idp.claims("connector")), newKey(t)
	forged, unknownKey := oidctest.SignIDToken(other, "k1", "RS256", claims), oidctest.SignIDToken(other, "unknown", "RS256", claims)
	check := func(name, token string, want, fetches int) {
		t.Helper()
		if w, n := request(context.Background(), h, token), idp.count(keysPath); w.Code != want || n != fetches {
			t.Errorf("%s: status = %d, JWKS requests = %d; want %d and %d", name, w.Code, n, want, fetches)
		}
	}

	check("first token", k1, http.StatusOK, 1)
	setInterval(t, &keyRefreshInterval, 0)
	check("cached key", k1, http.StatusOK, 1)
	check("bad signature", forged, http.StatusUnauthorized, 1)
	setInterval(t, &keyRefreshInterval, time.Hour)
	check("unknown key within the interval", unknownKey, http.StatusInternalServerError, 1)
	setInterval(t, &keyMaxAge, 0)
	check("keys past max age within the interval", k1, http.StatusOK, 1)
	if want := `level=WARN msg="cannot verify access token" issuer=` + idp.URL; !strings.Contains(logs.String(), want) || strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("log %q does not contain %q, or contains an error", logs, want)
	}

	idp.rotate(t, "k2")
	k2 := idp.sign(t, idp.claims("connector"))
	check("rotated key within the interval", k2, http.StatusInternalServerError, 1)
	setInterval(t, &keyRefreshInterval, 0)
	check("removed key", k1, http.StatusUnauthorized, 2)
	check("rotated key", k2, http.StatusOK, 3)
	check("unknown key", unknownKey, http.StatusUnauthorized, 4)
	setInterval(t, &keyRefreshInterval, time.Hour)
	check("expired token with a removed key within the interval", expiredK1, http.StatusUnauthorized, 4)
}

func TestRequireTokenKeyFetchFailure(t *testing.T) {
	idp := newIssuer(t)
	h, logs := newHandler(newAuthConfig(idp.URL), nop)
	cached := idp.sign(t, idp.claims("connector"))
	if w := request(context.Background(), h, cached); w.Code != http.StatusOK {
		t.Fatalf("before the failure: status = %d, want 200; body %q", w.Code, w.Body)
	}
	idp.override(keysPath, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "jwks unavailable", http.StatusInternalServerError)
	})
	idp.rotate(t, "k2")
	for _, tt := range []struct {
		interval time.Duration
		level    string
	}{{0, "ERROR"}, {time.Hour, "WARN"}} {
		setInterval(t, &keyRefreshInterval, tt.interval)
		logged := len(logs.String())
		w := request(context.Background(), h, idp.sign(t, idp.claims("connector")))
		if w.Code != http.StatusInternalServerError || w.Body.String() != "cannot verify access tokens\n" {
			t.Errorf("refresh interval %v: status = %d, body %q; want 500 and %q", tt.interval, w.Code, w.Body, "cannot verify access tokens\n")
		}
		if want := "level=" + tt.level + ` msg="cannot verify access token" issuer=` + idp.URL; !strings.Contains(logs.String()[logged:], want) {
			t.Errorf("refresh interval %v: log %q does not contain %q", tt.interval, logs.String()[logged:], want)
		}
	}
	if w := request(context.Background(), h, cached); w.Code != http.StatusOK {
		t.Errorf("cached key after the failure: status = %d, want 200; body %q", w.Code, w.Body)
	}
	if n, failures := idp.count(keysPath), strings.Count(logs.String(), `level=WARN msg="cannot fetch signing keys" issuer=`+idp.URL); n != 2 || failures != 1 {
		t.Errorf("JWKS requests = %d, logged fetch failures = %d; want 2 and 1", n, failures)
	}
}

func TestRequireTokenSkipsUnusableKeys(t *testing.T) {
	idp := newIssuer(t)
	k1, err := json.Marshal(jose.JSONWebKey{Key: &idp.key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"})
	if err != nil {
		t.Fatal(err)
	}
	var unusable []string
	for _, k := range []jose.JSONWebKey{{Algorithm: "RSA-OAEP"}, {Use: "enc"}} {
		k.Key, k.KeyID = &idp.key.PublicKey, "k1"
		b, err := json.Marshal(k)
		if err != nil {
			t.Fatal(err)
		}
		unusable = append(unusable, string(b))
	}
	unusable = append(unusable, `{"kty":"oct","kid":"k1","k":"c2VjcmV0"}`, `{"kty":"EC","crv":"secp256k1","kid":"k1","x":"`+strings.Repeat("A", 43)+`","y":"`+strings.Repeat("A", 43)+`"}`)
	for _, tt := range []struct {
		name string
		keys []string
		want int
	}{
		{"usable key among unusable ones", append(unusable, string(k1)), http.StatusOK},
		{"no usable key", unusable, http.StatusInternalServerError},
	} {
		set := `{"keys":[` + strings.Join(tt.keys, ",") + `]}`
		idp.override(keysPath, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, set)
		})
		if w, _, _ := serve(newAuthConfig(idp.URL), idp.sign(t, idp.claims("connector"))); w.Code != tt.want {
			t.Errorf("%s: status = %d, want %d; body %q", tt.name, w.Code, tt.want, w.Body)
		}
	}
}

type handlerTransport struct{ http.Handler }

func (h handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Result(), nil
}

func TestRequireTokenLimitsIssuerResponses(t *testing.T) {
	const remote = "https://idp.example"
	padding := strings.Repeat(" ", 1<<20)
	errorBody := strings.Repeat("x", 600)
	for _, tt := range []struct {
		name      string
		discovery func(idp *fakeIssuer) string
		keys      func(idp *fakeIssuer) string
		redirect  bool
	}{
		{name: "discovery over 1 MiB", discovery: func(idp *fakeIssuer) string { return discovery(remote, remote+keysPath) + padding }},
		{name: "key set over 1 MiB", keys: func(idp *fakeIssuer) string { return keySet(t, idp) + padding }},
		{name: "jwks_uri over http on a loopback host from a remote issuer", discovery: func(idp *fakeIssuer) string { return discovery(remote, "http://localhost/moved") }},
		{name: "key set from a remote issuer redirected over http to a loopback host", redirect: true},
		{name: "discovery names another issuer", discovery: func(idp *fakeIssuer) string { return discovery("https://other.example", remote+keysPath) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			idp := newIssuer(t)
			idp.oidc.SetIssuer(remote)
			old := httpClient.Transport
			httpClient.Transport = handlerTransport{idp.Config.Handler}
			t.Cleanup(func() { httpClient.Transport = old })
			for path, body := range map[string]func(*fakeIssuer) string{discoveryPath: tt.discovery, keysPath: tt.keys} {
				if body != nil {
					idp.override(path, func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, body(idp)) })
				}
			}
			if tt.redirect {
				idp.override(keysPath, func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "http://localhost/moved", http.StatusFound)
				})
			}
			idp.override("/moved", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, keySet(t, idp)) })
			claims := idp.claims("connector")
			claims["iss"] = remote
			if w, _, logs := serve(newAuthConfig(remote), idp.sign(t, claims)); w.Code != http.StatusInternalServerError {
				t.Errorf("status = %d, want 500; body %q, log %.300q", w.Code, w.Body, logs)
			}
			if n := idp.count("/moved"); n != 0 {
				t.Errorf("requests to the loopback host = %d, want 0", n)
			}
		})
	}

	for _, tt := range []struct {
		name, path, reason string
		respond            func(w http.ResponseWriter, idp *fakeIssuer)
	}{
		{name: "error body", path: discoveryPath, reason: "502 Bad Gateway", respond: func(w http.ResponseWriter, _ *fakeIssuer) { http.Error(w, errorBody, http.StatusBadGateway) }},
		{name: "long status line", path: discoveryPath, reason: "502 xxx", respond: func(w http.ResponseWriter, _ *fakeIssuer) {
			if conn, buf, err := http.NewResponseController(w).Hijack(); err == nil {
				defer conn.Close()
				fmt.Fprintf(buf, "HTTP/1.1 502 %s\r\n\r\n", strings.Repeat("x", 1<<20))
				buf.Flush()
			}
		}},
		{name: "long issuer", path: discoveryPath, respond: func(w http.ResponseWriter, idp *fakeIssuer) {
			io.WriteString(w, discovery("https://idp.example/"+errorBody, idp.URL+keysPath))
		}},
		{name: "long jwks_uri", path: discoveryPath, respond: func(w http.ResponseWriter, idp *fakeIssuer) {
			io.WriteString(w, discovery(idp.URL, idp.URL+"/"+errorBody))
		}},
		{name: "long kid and crv of malformed keys", path: keysPath, reason: "skipped signing key", respond: func(w http.ResponseWriter, _ *fakeIssuer) {
			io.WriteString(w, `{"keys":[{"kty":"RSA","kid":"`+errorBody+`"},{"kty":"EC","crv":"`+errorBody+`"}]}`)
		}},
		{name: "redirect to a long http URL", path: keysPath, reason: "https URL", respond: func(w http.ResponseWriter, _ *fakeIssuer) {
			w.Header().Set("Location", "http://idp.example/"+errorBody)
			w.WriteHeader(http.StatusFound)
		}},
		{name: "redirect to a long unparsable URL", path: keysPath, respond: func(w http.ResponseWriter, _ *fakeIssuer) {
			w.Header().Set("Location", "/%zz"+errorBody)
			w.WriteHeader(http.StatusFound)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			idp := newIssuer(t)
			idp.override(tt.path, func(w http.ResponseWriter, _ *http.Request) { tt.respond(w, idp) })
			w, _, logs := serve(newAuthConfig(idp.URL), idp.sign(t, idp.claims("connector")))
			if w.Code != http.StatusInternalServerError || strings.Contains(logs, strings.Repeat("x", 513)) || !strings.Contains(logs, tt.reason) {
				t.Errorf("status = %d, log %.1000q; want 500 and a log with %q and at most 512 bytes of each value from the issuer", w.Code, logs, tt.reason)
			}
		})
	}
}

func discovery(issuer, jwksURL string) string {
	return fmt.Sprintf(`{"issuer":%q,"jwks_uri":%q}`, issuer, jwksURL)
}

func keySet(t *testing.T, idp *fakeIssuer) string {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	return marshal(t, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &idp.key.PublicKey, KeyID: idp.kid, Algorithm: "RS256", Use: "sig"}}})
}

func TestRequireTokenWithScopes(t *testing.T) {
	idp := newIssuer(t)
	a := newAuthConfig(idp.URL)
	a.ScopesSupported = []string{"openid", "temporal.read"}
	for token, want := range map[string]string{
		"":             `Bearer resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp", scope="openid temporal.read"`,
		"opaque-token": `Bearer resource_metadata="https://mcp.example.com/.well-known/oauth-protected-resource/mcp", scope="openid temporal.read", error="invalid_token"`,
	} {
		if w, _, _ := serve(a, token); w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") != want {
			t.Errorf("token %q: status = %d, WWW-Authenticate = %q; want 401 and %q", token, w.Code, w.Header().Get("WWW-Authenticate"), want)
		}
	}
	if w, info, _ := serve(a, idp.sign(t, idp.claims("connector"))); w.Code != http.StatusOK || info == nil {
		t.Errorf("valid token: status = %d, token info %+v; want 200 and the token's info", w.Code, info)
	}
}

func TestRequireTokenClockSkew(t *testing.T) {
	idp := newIssuer(t)
	a := newAuthConfig(idp.URL)
	now := time.Now()
	tests := []struct {
		name, claim string
		at          time.Time
		want        int
	}{
		{"expired within leeway", "exp", now.Add(-30 * time.Second), http.StatusOK},
		{"expired beyond leeway", "exp", now.Add(-75 * time.Second), http.StatusUnauthorized},
		{"not yet valid within leeway", "nbf", now.Add(30 * time.Second), http.StatusOK},
		{"not yet valid beyond leeway", "nbf", now.Add(75 * time.Second), http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims := idp.claims("connector")
			claims[tt.claim] = tt.at.Unix()
			if w, _, _ := serve(a, idp.sign(t, claims)); w.Code != tt.want {
				t.Errorf("status = %d, want %d; body %q", w.Code, tt.want, w.Body)
			}
		})
	}
}

type panicOn string

func (p panicOn) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == string(p) {
		panic("boom-value")
	}
	return http.DefaultTransport.RoundTrip(r)
}

func TestRequireTokenFetchPanic(t *testing.T) {
	idp := newIssuer(t)
	h, logs := newHandler(newAuthConfig(idp.URL), nop)
	setInterval(t, &discoveryRetryInterval, 0)
	setInterval(t, &keyRefreshInterval, 0)
	client := httpClient
	t.Cleanup(func() { httpClient = client })
	for _, path := range []string{discoveryPath, keysPath} {
		httpClient = &http.Client{Transport: panicOn(path)}
		logged := len(logs.String())
		if w := request(context.Background(), h, idp.sign(t, idp.claims("connector"))); w.Code != http.StatusInternalServerError || w.Body.String() != "cannot verify access tokens\n" {
			t.Errorf("panic fetching %s: status = %d, body %q; want 500 and %q", path, w.Code, w.Body, "cannot verify access tokens\n")
		}
		if want := `level=ERROR msg="fetch from issuer panicked" issuer=` + idp.URL + ` error="panic: boom-value\ngoroutine `; !strings.Contains(logs.String()[logged:], want) {
			t.Errorf("panic fetching %s: log %q does not contain %q", path, logs.String()[logged:], want)
		}
	}
	httpClient = client
	if w := request(context.Background(), h, idp.sign(t, idp.claims("connector"))); w.Code != http.StatusOK {
		t.Errorf("after the panics: status = %d, want 200; body %q", w.Code, w.Body)
	}
	setInterval(t, &discoveryRetryInterval, time.Hour)
	setInterval(t, &keyRefreshInterval, time.Hour)
	for _, path := range []string{discoveryPath, keysPath} {
		h, _ := newHandler(newAuthConfig(idp.URL), nop)
		httpClient = &http.Client{Transport: panicOn(path)}
		request(context.Background(), h, idp.sign(t, idp.claims("connector")))
		httpClient = client
		fetches := idp.count(path)
		if w := request(context.Background(), h, idp.sign(t, idp.claims("connector"))); w.Code != http.StatusInternalServerError || idp.count(path) != fetches {
			t.Errorf("request within the retry interval after a panic fetching %s: status = %d, %d new fetches; want 500 and none", path, w.Code, idp.count(path)-fetches)
		}
	}
}
