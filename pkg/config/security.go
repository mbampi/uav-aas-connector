package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// SecurityConfig holds transport security (TLS) and authentication settings
// shared by every exporter. Every field is optional and defaults to the
// protocol's historical insecure behavior, so existing configs keep working
// unchanged; set tlsEnabled/username explicitly to opt in per exporter.
//
// Interpretation is protocol-specific:
//   - OPC UA client/server: TLSEnabled switches the secure channel from None
//     to Basic256Sha256 (Sign+Encrypt); CertFile/KeyFile are the endpoint's
//     own certificate/key.
//   - MQTT/gRPC: TLSEnabled builds a standard tls.Config for the connection;
//     CAFile pins a custom CA, InsecureSkipVerify disables server
//     certificate verification (testing only).
//   - HTTP: TLSEnabled serves over HTTPS using CertFile/KeyFile.
//
// Username/Password enable protocol-native authentication (OPC UA user
// token, MQTT credentials, gRPC per-RPC basic auth, HTTP basic auth) when
// set; anonymous/unauthenticated access is used otherwise.
type SecurityConfig struct {
	TLSEnabled         bool   `yaml:"tlsEnabled,omitempty"`
	CertFile           string `yaml:"certFile,omitempty"`
	KeyFile            string `yaml:"keyFile,omitempty"`
	CAFile             string `yaml:"caFile,omitempty"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify,omitempty"`

	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

// BuildTLSConfig builds a *tls.Config for protocols that consume one
// directly (MQTT, gRPC). It returns a nil config, nil error if TLS is
// disabled.
func (s SecurityConfig) BuildTLSConfig() (*tls.Config, error) {
	if !s.TLSEnabled {
		return nil, nil
	}

	tlsCfg := &tls.Config{InsecureSkipVerify: s.InsecureSkipVerify}

	switch {
	case s.CertFile != "" && s.KeyFile != "":
		cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS certificate/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	case s.CertFile != "" || s.KeyFile != "":
		return nil, fmt.Errorf("certFile and keyFile must both be set, or both left empty")
	}

	if s.CAFile != "" {
		caPEM, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file '%s': %w", s.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA certificate '%s'", s.CAFile)
		}
		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}
