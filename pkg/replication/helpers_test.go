package replication

import (
	"net/http/httptest"
	"testing"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/peer"
)

func startRemote(t *testing.T, node string, b backend.Backend) (*peer.Client, *httptest.Server) {
	t.Helper()
	srv := peer.NewServer(peer.ServerConfig{NodeName: node, Pool: "tank/k8s", Backend: b})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return peer.NewClient(ts.URL, node, nil), ts
}
