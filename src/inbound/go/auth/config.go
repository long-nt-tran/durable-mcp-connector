// Package auth provides OAuth resource-server authentication for inbound HTTP requests.
package auth

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strings"
)

// IssuerConfig identifies a trusted token issuer and this connector's API audience.
type IssuerConfig struct {
	Issuer   string `yaml:"issuer"`
	Audience string `yaml:"audience"`
}

// Config configures the HTTP resource server. It contains no client secret.
type Config struct {
	Resource             string         `yaml:"resource"`
	Issuers              []IssuerConfig `yaml:"issuers"`
	AuthorizationServers []string       `yaml:"authorization_servers,omitempty"`
	// ScopesSupported advertises scopes. RequiredScopes enforces access.
	ScopesSupported []string `yaml:"scopes_supported,omitempty"`
	RequiredScopes  []string `yaml:"required_scopes,omitempty"`
}

// Validate checks URLs, audiences, issuer uniqueness, and scope syntax.
func (a Config) Validate() error {
	var errs []error
	if err := validateAuthURL("auth: resource", a.Resource); err != nil {
		errs = append(errs, err)
	}
	if len(a.Issuers) == 0 {
		errs = append(errs, errors.New("auth: issuers: at least one issuer is required"))
	}
	seen := map[string]bool{}
	for n, iss := range a.Issuers {
		label := fmt.Sprintf("auth: issuers[%d]", n)
		if err := validateAuthURL(label+": issuer", iss.Issuer); err != nil {
			errs = append(errs, err)
		}
		if seen[iss.Issuer] {
			errs = append(errs, fmt.Errorf("%s: duplicate issuer", label))
		}
		seen[iss.Issuer] = true
		if strings.TrimSpace(iss.Audience) == "" {
			errs = append(errs, fmt.Errorf("%s: audience is required", label))
		}
	}
	for n, server := range a.AuthorizationServers {
		if err := validateAuthURL(fmt.Sprintf("auth: authorization_servers[%d]", n), server); err != nil {
			errs = append(errs, err)
		}
	}
	for _, scopes := range []struct {
		name   string
		values []string
	}{{"scopes_supported", a.ScopesSupported}, {"required_scopes", a.RequiredScopes}} {
		for n, scope := range scopes.values {
			if scope == "" || strings.ContainsFunc(scope, func(r rune) bool { return r <= ' ' || r == '"' || r == '\\' || r > '~' }) {
				errs = append(errs, fmt.Errorf("auth: %s[%d]: invalid scope", scopes.name, n))
			}
		}
	}
	return errors.Join(errs...)
}

func validateAuthURL(label, value string) error {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" && (u.Scheme != "http" || !loopbackHost(u.Hostname())) ||
		u.Host == "" || u.User != nil || strings.ContainsAny(value, "?#") {
		return fmt.Errorf("%s must be an https URL (http only for a loopback host), with a host and no user info, query or fragment", label)
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	return err == nil && addr.IsLoopback()
}
