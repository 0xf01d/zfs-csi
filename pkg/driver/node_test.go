package driver

import (
	"context"
	"testing"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/replication"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
)

func testNode(t *testing.T) (*Node, *backend.Fake, *fakeMounter) {
	t.Helper()
	b := backend.NewFake()
	rep := replication.New(b, "tank/k8s", "node-a", nil, 2, 0)
	m := newFakeMounter()
	n := NewNode("test.csi", "node-a", "tank/k8s", "topology.test/node", 0, b, m, rep)
	n.Logf = t.Logf
	return n, b, m
}

func TestNodeStagePublishUnpublish(t *testing.T) {
	ctx := context.Background()
	n, b, m := testNode(t)

	stageReq := &csi.NodeStageVolumeRequest{
		VolumeId:          "pvc-1",
		StagingTargetPath: "/staging/pvc-1",
		VolumeCapability:  mountCap(),
		VolumeContext:     map[string]string{ContextPool: "tank/k8s"},
	}
	if _, err := n.NodeStageVolume(ctx, stageReq); err != nil {
		t.Fatal(err)
	}
	ok, _ := b.DatasetExists("tank/k8s/pvc-1")
	if !ok {
		t.Fatal("stage did not create dataset")
	}
	if m.mounts["/staging/pvc-1"] != "tank/k8s/pvc-1" {
		t.Fatalf("staging mount = %+v", m.mounts)
	}
	// Idempotent.
	if _, err := n.NodeStageVolume(ctx, stageReq); err != nil {
		t.Fatal(err)
	}

	pub := &csi.NodePublishVolumeRequest{
		VolumeId:          "pvc-1",
		StagingTargetPath: "/staging/pvc-1",
		TargetPath:        "/pods/pvc-1",
		VolumeCapability:  mountCap(),
		VolumeContext:     map[string]string{ContextPool: "tank/k8s"},
	}
	if _, err := n.NodePublishVolume(ctx, pub); err != nil {
		t.Fatal(err)
	}
	if m.mounts["/pods/pvc-1"] != "/staging/pvc-1" {
		t.Fatalf("bind mount = %+v", m.mounts)
	}
	pub.Readonly = true
	if _, err := n.NodePublishVolume(ctx, pub); err != nil {
		t.Fatal(err)
	}

	if _, err := n.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: "pvc-1", TargetPath: "/pods/pvc-1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.mounts["/pods/pvc-1"]; ok {
		t.Fatal("target still mounted")
	}
	if _, err := n.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: "pvc-1", StagingTargetPath: "/staging/pvc-1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.mounts["/staging/pvc-1"]; ok {
		t.Fatal("staging still mounted")
	}
}

func TestNodePublishWithoutExplicitStage(t *testing.T) {
	ctx := context.Background()
	n, b, m := testNode(t)
	if _, err := n.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		VolumeId:          "pvc-2",
		StagingTargetPath: "/staging/pvc-2",
		TargetPath:        "/pods/pvc-2",
		VolumeCapability:  mountCap(),
	}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := b.DatasetExists("tank/k8s/pvc-2"); !ok {
		t.Fatal("publish did not ensure dataset")
	}
	if m.mounts["/pods/pvc-2"] != "/staging/pvc-2" {
		t.Fatalf("mounts = %+v", m.mounts)
	}
}

func TestNodeStageClonesSourceSnapshot(t *testing.T) {
	ctx := context.Background()
	n, b, _ := testNode(t)
	if err := b.SeedSnapshot("tank/k8s/source", "vs-snap", 123); err != nil {
		t.Fatal(err)
	}
	_, err := n.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{
		VolumeId:          "pvc-clone",
		StagingTargetPath: "/staging/pvc-clone",
		VolumeCapability:  mountCap(),
		VolumeContext:     map[string]string{ContextSourceSnapshot: "source@vs-snap"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ok, _ := b.DatasetExists("tank/k8s/pvc-clone")
	if !ok {
		t.Fatal("clone dataset missing")
	}
}

func TestNodeGetInfoAndStats(t *testing.T) {
	ctx := context.Background()
	n, _, _ := testNode(t)
	info, err := n.NodeGetInfo(ctx, &csi.NodeGetInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if info.NodeId != "node-a" || info.AccessibleTopology.Segments["topology.test/node"] != "node-a" {
		t.Fatalf("info = %+v", info)
	}
	stats, err := n.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{
		VolumeId:   "pvc-1",
		VolumePath: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Usage) == 0 || stats.Usage[0].Total == 0 {
		t.Fatalf("stats = %+v", stats)
	}
}
