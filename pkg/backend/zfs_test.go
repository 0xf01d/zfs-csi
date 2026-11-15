package backend

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/naming"
)

// These tests only run where a real ZFS pool is available. The sandbox does
// not have ZFS, so they skip. Set ZFS_CSI_TEST_POOL=tank/k8s on a real node.
func testPool(t *testing.T) (string, *ZFS) {
	t.Helper()
	if !IsZFSAvailable() {
		t.Skip("ZFS is not available (/dev/zfs or zfs binary missing)")
	}
	pool := os.Getenv("ZFS_CSI_TEST_POOL")
	if pool == "" {
		t.Skip("ZFS_CSI_TEST_POOL is not set")
	}
	return pool, NewZFS("")
}

func tempDataset(pool, suffix string) string {
	return fmt.Sprintf("%s/zfs-csi-test-%d-%d-%s", strings.TrimSuffix(pool, "/"), os.Getpid(), time.Now().UnixNano(), suffix)
}

func TestZFSLifecycle(t *testing.T) {
	pool, z := testPool(t)
	ds := tempDataset(pool, "life")
	clone := ds + "-clone"
	defer z.DestroyDataset(ds, true)
	defer z.DestroyDataset(clone, true)

	if err := z.CreateDataset(ds); err != nil {
		t.Fatal(err)
	}
	if err := z.CreateDataset(ds); err != nil {
		t.Fatalf("create should be idempotent: %v", err)
	}
	ok, err := z.DatasetExists(ds)
	if err != nil || !ok {
		t.Fatalf("exists = %v, %v", ok, err)
	}
	mp, err := z.Mountpoint(ds)
	if err != nil || mp != "legacy" {
		t.Fatalf("mountpoint = %q, %v", mp, err)
	}
	if err := z.Snapshot(ds, "vs-1"); err != nil {
		t.Fatal(err)
	}
	if err := z.Snapshot(ds, "vs-1"); err != nil {
		t.Fatalf("snapshot should be idempotent: %v", err)
	}
	snaps, err := z.ListSnapshots(ds)
	if err != nil || len(snaps) != 1 || snaps[0].Name != "vs-1" {
		t.Fatalf("snapshots = %+v, %v", snaps, err)
	}
	if err := z.Clone(naming.Snapshot(ds, "vs-1"), clone); err != nil {
		t.Fatal(err)
	}
	if err := z.SetQuota(clone, 16<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := z.UsedBytes(clone); err != nil {
		t.Fatal(err)
	}
	if err := z.DestroyDataset(clone, true); err != nil {
		t.Fatal(err)
	}
	if err := z.DestroySnapshot(ds, "vs-1"); err != nil {
		t.Fatal(err)
	}
	if err := z.DestroyDataset(ds, true); err != nil {
		t.Fatal(err)
	}
	if err := z.DestroyDataset(ds, true); err != nil {
		t.Fatalf("destroy should be idempotent: %v", err)
	}
}

func TestZFSIncrementalSendReceive(t *testing.T) {
	pool, z := testPool(t)
	src := tempDataset(pool, "src")
	dst := tempDataset(pool, "dst")
	defer z.DestroyDataset(src, true)
	defer z.DestroyDataset(dst, true)

	if err := z.CreateDataset(src); err != nil {
		t.Fatal(err)
	}
	if err := z.Snapshot(src, "rep-1"); err != nil {
		t.Fatal(err)
	}
	// Change metadata between snapshots so the incremental stream is not
	// empty, without needing a mount.
	cmd := exec.Command("zfs", "set", "quota=32M", src)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("zfs set: %v: %s", err, out)
	}
	if err := z.Snapshot(src, "rep-2"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := z.SendIncremental(naming.Snapshot(src, "rep-1"), naming.Snapshot(src, "rep-2"), &buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("empty incremental stream")
	}
	if err := z.Receive(bytes.NewReader(buf.Bytes()), dst); err != nil {
		t.Fatal(err)
	}
	ok, err := z.DatasetExists(dst)
	if err != nil || !ok {
		t.Fatalf("received dataset exists = %v, %v", ok, err)
	}
	snaps, err := z.ListSnapshots(dst)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range snaps {
		if s.Name == "rep-2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("received snapshots = %+v", snaps)
	}
}
