//go:build linux

// Package e2e contains the real-ZFS test. It is skipped unless ZFS is
// available and either ZFS_CSI_E2E_POOL_A/B are set or the test runs as root
// so it can build two temporary file-backed pools.
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/naming"
	"github.com/0xf01d/zfs-csi/pkg/peer"
	"github.com/0xf01d/zfs-csi/pkg/replication"
)

func pools(t *testing.T) (string, string) {
	t.Helper()
	if os.Getenv("ZFS_CSI_E2E") == "" {
		t.Skip("set ZFS_CSI_E2E=1 to run the real ZFS end-to-end test")
	}
	if !backend.IsZFSAvailable() {
		t.Skip("ZFS is not available")
	}
	a, b := os.Getenv("ZFS_CSI_E2E_POOL_A"), os.Getenv("ZFS_CSI_E2E_POOL_B")
	if a != "" && b != "" {
		return a, b
	}
	if os.Geteuid() != 0 {
		t.Skip("root is required to create temporary zpools; or set ZFS_CSI_E2E_POOL_A/B")
	}
	dir := t.TempDir()
	a = fmt.Sprintf("zfscsiA%d", os.Getpid())
	b = fmt.Sprintf("zfscsiB%d", os.Getpid())
	for _, p := range []struct{ name, file string }{{a, dir + "/a.img"}, {b, dir + "/b.img"}} {
		if out, err := exec.Command("truncate", "-s", "512M", p.file).CombinedOutput(); err != nil {
			t.Fatalf("truncate: %v: %s", err, out)
		}
		if out, err := exec.Command("zpool", "create", "-f", p.name, p.file).CombinedOutput(); err != nil {
			t.Fatalf("zpool create: %v: %s", err, out)
		}
	}
	t.Cleanup(func() {
		_ = exec.Command("zpool", "destroy", "-f", a).Run()
		_ = exec.Command("zpool", "destroy", "-f", b).Run()
	})
	return a, b
}

func mountDataset(t *testing.T, ds, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("mount", "-t", "zfs", ds, dir).CombinedOutput(); err != nil {
		t.Fatalf("mount %s: %v: %s", ds, err, out)
	}
	t.Cleanup(func() { _ = exec.Command("umount", dir).Run() })
}

func writeSample(t *testing.T, root string) {
	t.Helper()
	files := map[string]string{
		"a.txt":       strings.Repeat("alpha\n", 1024),
		"dir/b.txt":   strings.Repeat("bravo\n", 2048),
		"dir/c.bin":   strings.Repeat("charlie", 4096),
		"empty":       "",
		"deep/x/y.md": "# snapshot test\n",
	}
	for name, content := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func modifySample(t *testing.T, root string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("changed alpha\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "dir/new.txt"), []byte("new file\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "dir/c.bin")); err != nil {
		t.Fatal(err)
	}
}

func treeHash(root string) (string, error) {
	h := sha256.New()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			_, _ = io.WriteString(h, "d "+rel+"\n")
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, _ = io.WriteString(h, "f "+rel+"\n")
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		return nil
	})
	return hex.EncodeToString(h.Sum(nil)), err
}

func TestIncrementalSnapshotToSecondPool(t *testing.T) {
	poolA, poolB := pools(t)
	z := backend.NewZFS("")
	src := naming.Dataset(poolA, fmt.Sprintf("pvc-e2e-%d", time.Now().UnixNano()))
	dst := naming.Dataset(poolB, fmt.Sprintf("pvc-e2e-%d", time.Now().UnixNano()))
	defer z.DestroyDataset(src, true)
	defer z.DestroyDataset(dst, true)

	if err := z.CreateDataset(src); err != nil {
		t.Fatal(err)
	}
	mntA := filepath.Join(t.TempDir(), "src")
	mountDataset(t, src, mntA)
	writeSample(t, mntA)
	if err := z.Snapshot(src, "vs-a"); err != nil {
		t.Fatal(err)
	}
	modifySample(t, mntA)
	if err := z.Snapshot(src, "vs-b"); err != nil {
		t.Fatal(err)
	}
	want, err := treeHash(mntA)
	if err != nil {
		t.Fatal(err)
	}

	var stream bytes.Buffer
	if err := z.SendIncremental(naming.Snapshot(src, "vs-a"), naming.Snapshot(src, "vs-b"), &stream); err != nil {
		t.Fatal(err)
	}
	t.Logf("incremental stream: %d bytes", stream.Len())
	if err := z.Receive(bytes.NewReader(stream.Bytes()), dst); err != nil {
		t.Fatal(err)
	}
	mntB := filepath.Join(t.TempDir(), "dst")
	mountDataset(t, dst, mntB)
	got, err := treeHash(mntB)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("tree hash mismatch: got %s want %s", got, want)
	}
}

func TestReplicatorPeerPathWithRealZFS(t *testing.T) {
	poolA, poolB := pools(t)
	zA := backend.NewZFS("")
	zB := backend.NewZFS("")
	vol := fmt.Sprintf("pvc-peer-%d", time.Now().UnixNano())
	src := naming.Dataset(poolA, vol)
	dst := naming.Dataset(poolB, vol)
	defer zA.DestroyDataset(src, true)
	defer zB.DestroyDataset(dst, true)

	if err := zA.CreateDataset(src); err != nil {
		t.Fatal(err)
	}
	mntA := filepath.Join(t.TempDir(), "src")
	mountDataset(t, src, mntA)
	writeSample(t, mntA)
	if err := zA.Snapshot(src, "vs-a"); err != nil {
		t.Fatal(err)
	}
	modifySample(t, mntA)
	if err := zA.Snapshot(src, "vs-b"); err != nil {
		t.Fatal(err)
	}
	want, err := treeHash(mntA)
	if err != nil {
		t.Fatal(err)
	}

	srv := peer.NewServer(peer.ServerConfig{NodeName: "node-b", Pool: poolB, Backend: zB})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	client := peer.NewClient(ts.URL, "node-b", nil)
	mgr := replication.New(zA, poolA, "node-a", []replication.Peer{{Node: "node-b", Client: client}}, 10, 5*time.Minute)
	mgr.Logf = t.Logf
	if err := mgr.NotifySnapshot(context.Background(), vol, "vs-b"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := zB.DatasetExists(dst); !ok {
		t.Fatal("peer did not receive dataset")
	}
	mntB := filepath.Join(t.TempDir(), "dst")
	mountDataset(t, dst, mntB)
	got, err := treeHash(mntB)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("peer tree hash mismatch: got %s want %s", got, want)
	}
}
