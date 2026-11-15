package driver

import (
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/mount"
	"github.com/0xf01d/zfs-csi/pkg/peer"
	"github.com/0xf01d/zfs-csi/pkg/replication"
)

type fakeMounter struct {
	mu     sync.Mutex
	mounts map[string]string
	ro     map[string]bool
}

func newFakeMounter() *fakeMounter {
	return &fakeMounter{mounts: map[string]string{}, ro: map[string]bool{}}
}

func (f *fakeMounter) Mount(source, target, fstype string, options []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mounts[target] = source
	return nil
}

func (f *fakeMounter) BindMount(source, target string, readOnly bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mounts[target] = source
	f.ro[target] = readOnly
	return nil
}

func (f *fakeMounter) Unmount(target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.mounts, target)
	delete(f.ro, target)
	return nil
}

func (f *fakeMounter) IsMountPoint(target string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.mounts[target]
	return ok, nil
}

func (f *fakeMounter) MakeDir(path string) error { return nil }

var _ mount.Mounter = (*fakeMounter)(nil)

func startPeer(t *testing.T, node, pool string, b backend.Backend) (*peer.Client, *httptest.Server) {
	t.Helper()
	srv := peer.NewServer(peer.ServerConfig{NodeName: node, Pool: pool, Backend: b})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return peer.NewClient(ts.URL, node, nil), ts
}

func testController(t *testing.T, pool string, nodes ...string) (*Controller, map[string]*backend.Fake) {
	t.Helper()
	fakes := map[string]*backend.Fake{}
	var peers []replication.Peer
	for _, n := range nodes {
		b := backend.NewFake()
		fakes[n] = b
		client, _ := startPeer(t, n, pool, b)
		peers = append(peers, replication.Peer{Node: n, Client: client})
	}
	c := NewController("test.csi", pool, "topology.test/node", peers, nodes)
	c.Logf = t.Logf
	return c, fakes
}
