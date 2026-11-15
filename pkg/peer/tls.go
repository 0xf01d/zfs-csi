package peer

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSFiles points at a PEM certificate, key and CA. Empty means "plain TCP",
// which is only appropriate for local tests or a trusted network.
type TLSFiles struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// ServerConfig builds a mutual-TLS server config.
func (f TLSFiles) ServerConfig() (*tls.Config, error) {
	if f.CertFile == "" && f.KeyFile == "" && f.CAFile == "" {
		return nil, nil
	}
	if f.CertFile == "" || f.KeyFile == "" {
		return nil, fmt.Errorf("tls: cert and key are both required")
	}
	cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load key pair: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if f.CAFile != "" {
		pool, err := loadCA(f.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ClientConfig builds the matching mutual-TLS client config.
func (f TLSFiles) ClientConfig() (*tls.Config, error) {
	if f.CertFile == "" && f.KeyFile == "" && f.CAFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if f.CertFile != "" || f.KeyFile != "" {
		if f.CertFile == "" || f.KeyFile == "" {
			return nil, fmt.Errorf("tls: cert and key are both required")
		}
		cert, err := tls.LoadX509KeyPair(f.CertFile, f.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if f.CAFile != "" {
		pool, err := loadCA(f.CAFile)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func loadCA(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("tls: read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tls: no certificates in %s", path)
	}
	return pool, nil
}
