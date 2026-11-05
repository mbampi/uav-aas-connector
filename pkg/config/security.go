package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// SecurityConfig holds TLS and authentication settings shared by every exporter.
// All fields are optional; by default there is no TLS and access is anonymous.
//
//   - OPC UA: TLSEnabled uses Basic256Sha256 (Sign+Encrypt) with CertFile/KeyFile.
//   - MQTT/gRPC: TLSEnabled builds a tls.Config. CAFile sets a custom CA and
//     InsecureSkipVerify disables verification (testing only).
//   - HTTP: TLSEnabled serves HTTPS using CertFile/KeyFile.
//
// Username/Password enable the protocol's own authentication when set.
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
