package peer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
)

func writeCert(t *testing.T, dir, name string, der []byte, key *ecdsa.PrivateKey) string {
	t.Helper()
	certPath := filepath.Join(dir, name+".crt")
	keyPath := filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath
}

func TestMutualTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caPath := writeCert(t, dir, "ca", caDER, caKey)

	leaf := func(name string, serial int64, server bool) (string, string) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		}
		if server {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			tmpl.DNSNames = []string{"localhost"}
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		} else {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return writeCert(t, dir, name, der, key), filepath.Join(dir, name+".key")
	}
	serverCert, serverKey := leaf("server", 2, true)
	clientCert, clientKey := leaf("client", 3, false)

	serverFiles := TLSFiles{CertFile: serverCert, KeyFile: serverKey, CAFile: caPath}
	serverTLS, err := serverFiles.ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	clientFiles := TLSFiles{CertFile: clientCert, KeyFile: clientKey, CAFile: caPath}
	clientTLS, err := clientFiles.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}

	h := NewServer(ServerConfig{NodeName: "n1", Pool: "tank/k8s", Backend: backend.NewFake()}).Handler()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h, TLSConfig: serverTLS}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() { _ = srv.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := NewClient("https://"+ln.Addr().String(), "n1", clientTLS)
	if err := client.Health(ctx); err != nil {
		t.Fatalf("mutual TLS health: %v", err)
	}

	// No client certificate: the server must refuse the handshake.
	noCert, err := TLSFiles{CAFile: caPath}.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	bad := NewClient("https://"+ln.Addr().String(), "n1", noCert)
	if err := bad.Health(ctx); err == nil {
		t.Fatal("server accepted a client without a certificate")
	}
}
