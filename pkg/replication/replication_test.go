package replication

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
)

func testManager(t *testing.T, b backend.Backend, node string, peers ...Peer) *Manager {
	t.Helper()
	m := New(b, "tank/k8s", node, peers, 10, 10*time.Second)
	m.Logf = t.Logf
	return m
}

func TestReplicateFullThenIncremental(t *testing.T) {
	ctx := context.Background()
	a := backend.NewFake()
	b := backend.NewFake()
	if err := a.SeedSnapshot("tank/k8s/vol", "vs-1", 10); err != nil {
		t.Fatal(err)
	}
	clientB, _ := startRemote(t, "node-b", b)
	ma := testManager(t, a, "node-a", Peer{Node: "node-b", Client: clientB})

	if err := ma.NotifySnapshot(ctx, "vol", "vs-1"); err != nil {
		t.Fatal(err)
	}
	ok, _ := b.DatasetExists("tank/k8s/vol")
	if !ok {
		t.Fatal("full replication did not create remote dataset")
	}
	if got := string(b.LastReceived("tank/k8s/vol")); !strings.Contains(got, "-> tank/k8s/vol@vs-1") {
		t.Fatalf("full stream = %q", got)
	}
	if err := a.SetUsed("tank/k8s/vol", 20); err != nil {
		t.Fatal(err)
	}
	if err := a.Snapshot("tank/k8s/vol", "vs-2"); err != nil {
		t.Fatal(err)
	}
	if err := ma.NotifySnapshot(ctx, "vol", "vs-2"); err != nil {
		t.Fatal(err)
	}
	stream := string(b.LastReceived("tank/k8s/vol"))
	if !strings.Contains(stream, "tank/k8s/vol@vs-1 -> tank/k8s/vol@vs-2") {
		t.Fatalf("expected incremental stream, got %q", stream)
	}
	// Already replicated: no new stream.
	before := string(b.LastReceived("tank/k8s/vol"))
	if err := ma.NotifySnapshot(ctx, "vol", "vs-2"); err != nil {
		t.Fatal(err)
	}
	if after := string(b.LastReceived("tank/k8s/vol")); after != before {
		t.Fatalf("idempotent replication sent again: %q", after)
	}
}

func TestEnsureLocalFailoverImportsLatest(t *testing.T) {
	ctx := context.Background()
	a := backend.NewFake()
	b := backend.NewFake()
	if err := a.SeedSnapshot("tank/k8s/vol", "vs-old", 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Snapshot("tank/k8s/vol", "vs-new"); err != nil {
		t.Fatal(err)
	}
	clientA, _ := startRemote(t, "node-a", a)
	mb := testManager(t, b, "node-b", Peer{Node: "node-a", Client: clientA})

	if err := mb.EnsureLocal(ctx, "vol", ""); err != nil {
		t.Fatal(err)
	}
	ok, _ := b.DatasetExists("tank/k8s/vol")
	if !ok {
		t.Fatal("failover did not import dataset")
	}
	snaps, _ := b.ListSnapshots("tank/k8s/vol")
	if len(snaps) != 1 || snaps[0].Name != "vs-new" {
		t.Fatalf("imported snapshots = %+v, want newest only", snaps)
	}
}

func TestEnsureLocalImportsNamedSnapshotIntoNewVolume(t *testing.T) {
	ctx := context.Background()
	a := backend.NewFake()
	b := backend.NewFake()
	if err := a.SeedSnapshot("tank/k8s/source", "vs-1", 7); err != nil {
		t.Fatal(err)
	}
	clientA, _ := startRemote(t, "node-a", a)
	mb := testManager(t, b, "node-b", Peer{Node: "node-a", Client: clientA})

	if err := mb.EnsureLocal(ctx, "restored", "source@vs-1"); err != nil {
		t.Fatal(err)
	}
	ok, _ := b.DatasetExists("tank/k8s/restored")
	if !ok {
		t.Fatal("restored dataset missing")
	}
	if stream := string(b.LastReceived("tank/k8s/restored")); !strings.Contains(stream, "-> tank/k8s/source@vs-1") {
		t.Fatalf("stream = %q", stream)
	}
}

func TestEnsureLocalClonesLocally(t *testing.T) {
	ctx := context.Background()
	b := backend.NewFake()
	if err := b.SeedSnapshot("tank/k8s/source", "vs-1", 7); err != nil {
		t.Fatal(err)
	}
	m := testManager(t, b, "node-b")
	if err := m.EnsureLocal(ctx, "local-clone", "source@vs-1"); err != nil {
		t.Fatal(err)
	}
	ok, _ := b.DatasetExists("tank/k8s/local-clone")
	if !ok {
		t.Fatal("local clone missing")
	}
	if len(b.LastReceived("tank/k8s/local-clone")) != 0 {
		t.Fatal("local clone should not stream data")
	}
}

func TestSnapshotNowCreatesRepSnapshot(t *testing.T) {
	ctx := context.Background()
	a := backend.NewFake()
	if err := a.CreateDataset("tank/k8s/vol"); err != nil {
		t.Fatal(err)
	}
	m := testManager(t, a, "node-a")
	if err := m.SnapshotNow(ctx, "vol"); err != nil {
		t.Fatal(err)
	}
	snaps, _ := a.ListSnapshots("tank/k8s/vol")
	if len(snaps) != 1 || !strings.HasPrefix(snaps[0].Name, "rep-") {
		t.Fatalf("snapshots = %+v", snaps)
	}
}

func TestEnsureLocalCatchesUpExistingDataset(t *testing.T) {
	ctx := context.Background()
	a := backend.NewFake()
	b := backend.NewFake()
	if err := a.SeedSnapshot("tank/k8s/vol", "vs-old", 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Snapshot("tank/k8s/vol", "vs-new"); err != nil {
		t.Fatal(err)
	}
	if err := b.SeedSnapshot("tank/k8s/vol", "vs-old", 1); err != nil {
		t.Fatal(err)
	}
	clientA, _ := startRemote(t, "node-a", a)
	mb := testManager(t, b, "node-b", Peer{Node: "node-a", Client: clientA})

	if err := mb.EnsureLocal(ctx, "vol", ""); err != nil {
		t.Fatal(err)
	}
	snaps, _ := b.ListSnapshots("tank/k8s/vol")
	if !hasSnapshot(snaps, "vs-new") {
		t.Fatalf("did not catch up: %+v", snaps)
	}
	if stream := string(b.LastReceived("tank/k8s/vol")); !strings.Contains(stream, "tank/k8s/vol@vs-old -> tank/k8s/vol@vs-new") {
		t.Fatalf("expected incremental catch-up, got %q", stream)
	}
}
