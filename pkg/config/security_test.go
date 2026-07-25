package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestCert writes a throwaway self-signed cert/key pair to dir and
// returns their paths.
func generateTestCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certFile = filepath.Join(dir, "cert.pem")
	certOut, err := os.Create(certFile)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}))
	require.NoError(t, certOut.Close())

	keyFile = filepath.Join(dir, "key.pem")
	keyOut, err := os.Create(keyFile)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}))
	require.NoError(t, keyOut.Close())

	return certFile, keyFile
}

func TestSecurityConfig_BuildTLSConfig_Disabled(t *testing.T) {
	cfg := SecurityConfig{}

	tlsCfg, err := cfg.BuildTLSConfig()

	assert.NoError(t, err)
	assert.Nil(t, tlsCfg)
}

func TestSecurityConfig_BuildTLSConfig_WithCertAndKey(t *testing.T) {
	certFile, keyFile := generateTestCert(t, t.TempDir())
	cfg := SecurityConfig{TLSEnabled: true, CertFile: certFile, KeyFile: keyFile}

	tlsCfg, err := cfg.BuildTLSConfig()

	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	assert.Len(t, tlsCfg.Certificates, 1)
}

func TestSecurityConfig_BuildTLSConfig_OnlyOneOfCertKey(t *testing.T) {
	cfg := SecurityConfig{TLSEnabled: true, CertFile: "cert.pem"}

	_, err := cfg.BuildTLSConfig()

	assert.ErrorContains(t, err, "certFile and keyFile must both be set")
}

func TestSecurityConfig_BuildTLSConfig_InvalidCAFile(t *testing.T) {
	cfg := SecurityConfig{TLSEnabled: true, CAFile: filepath.Join(t.TempDir(), "missing-ca.pem")}

	_, err := cfg.BuildTLSConfig()

	assert.ErrorContains(t, err, "failed to read CA file")
}

func TestSecurityConfig_BuildTLSConfig_NoCertRequiredWhenTLSOnly(t *testing.T) {
	// TLS can be enabled purely to verify the server (e.g. via a system or
	// custom root CA) without presenting a client certificate.
	cfg := SecurityConfig{TLSEnabled: true, InsecureSkipVerify: true}

	tlsCfg, err := cfg.BuildTLSConfig()

	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	assert.Empty(t, tlsCfg.Certificates)
	assert.True(t, tlsCfg.InsecureSkipVerify)
}
