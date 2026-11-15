package driver

import (
	"context"
	"testing"

	"github.com/0xf01d/zfs-csi/pkg/naming"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
)

func mountCap() *csi.VolumeCapability {
	return &csi.VolumeCapability{
		AccessType: &csi.VolumeCapability_Mount{Mount: &csi.VolumeCapability_MountVolume{}},
		AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER},
	}
}

func topologyReq(key, node string) *csi.TopologyRequirement {
	return &csi.TopologyRequirement{
		Requisite: []*csi.Topology{{Segments: map[string]string{key: node}}},
	}
}

func createReq(name, node string) *csi.CreateVolumeRequest {
	req := &csi.CreateVolumeRequest{
		Name:               name,
		CapacityRange:      &csi.CapacityRange{RequiredBytes: 1 << 20},
		VolumeCapabilities: []*csi.VolumeCapability{mountCap()},
	}
	if node != "" {
		req.AccessibilityRequirements = topologyReq("topology.test/node", node)
	}
	return req
}

func TestCreateVolumeLateBinding(t *testing.T) {
	ctx := context.Background()
	c, fakes := testController(t, "tank/k8s", "node-a", "node-b")

	resp, err := c.CreateVolume(ctx, createReq("pvc-123", ""))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.VolumeId != "pvc-123" {
		t.Fatalf("volume id = %q", resp.Volume.VolumeId)
	}
	for node, b := range fakes {
		ok, _ := b.DatasetExists(naming.Dataset("tank/k8s", "pvc-123"))
		if ok {
			t.Fatalf("dataset was created on %s before scheduling", node)
		}
	}
	if got := len(resp.Volume.AccessibleTopology); got != 2 {
		t.Fatalf("topologies = %d, want 2", got)
	}

	resp, err = c.CreateVolume(ctx, createReq("pvc-123", "node-a"))
	if err != nil {
		t.Fatal(err)
	}
	ok, _ := fakes["node-a"].DatasetExists(naming.Dataset("tank/k8s", "pvc-123"))
	if !ok {
		t.Fatal("dataset not created on selected node")
	}
	ok, _ = fakes["node-b"].DatasetExists(naming.Dataset("tank/k8s", "pvc-123"))
	if ok {
		t.Fatal("dataset unexpectedly created on wrong node")
	}
	if resp.Volume.VolumeContext[ContextDataset] != "tank/k8s/pvc-123" {
		t.Fatalf("dataset context = %q", resp.Volume.VolumeContext[ContextDataset])
	}
}

func TestSnapshotLifecycleAndList(t *testing.T) {
	ctx := context.Background()
	c, fakes := testController(t, "tank/k8s", "node-a")
	if _, err := c.CreateVolume(ctx, createReq("pvc-1", "node-a")); err != nil {
		t.Fatal(err)
	}
	snap, err := c.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: "pvc-1"})
	if err != nil {
		t.Fatal(err)
	}
	want := "pvc-1@vs-snap-1"
	if snap.Snapshot.SnapshotId != want {
		t.Fatalf("snapshot id = %q, want %q", snap.Snapshot.SnapshotId, want)
	}
	if !snap.Snapshot.ReadyToUse {
		t.Fatal("snapshot not ready")
	}
	snaps, err := fakes["node-a"].ListSnapshots("tank/k8s/pvc-1")
	if err != nil || len(snaps) != 1 || snaps[0].Name != "vs-snap-1" {
		t.Fatalf("backend snapshots = %+v, err=%v", snaps, err)
	}
	// Idempotent.
	again, err := c.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap-1", SourceVolumeId: "pvc-1"})
	if err != nil || again.Snapshot.SnapshotId != want {
		t.Fatalf("idempotent snapshot: %v %v", again, err)
	}
	list, err := c.ListSnapshots(ctx, &csi.ListSnapshotsRequest{SourceVolumeId: "pvc-1"})
	if err != nil || len(list.Entries) != 1 {
		t.Fatalf("list snapshots: %+v %v", list, err)
	}
	if _, err := c.DeleteSnapshot(ctx, &csi.DeleteSnapshotRequest{SnapshotId: want}); err != nil {
		t.Fatal(err)
	}
	snaps, _ = fakes["node-a"].ListSnapshots("tank/k8s/pvc-1")
	if len(snaps) != 0 {
		t.Fatalf("snapshot not deleted: %+v", snaps)
	}
}

func TestCreateVolumeFromSnapshotOnAnotherNode(t *testing.T) {
	ctx := context.Background()
	c, fakes := testController(t, "tank/k8s", "node-a", "node-b")
	if _, err := c.CreateVolume(ctx, createReq("source", "node-a")); err != nil {
		t.Fatal(err)
	}
	snap, err := c.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{Name: "snap", SourceVolumeId: "source"})
	if err != nil {
		t.Fatal(err)
	}
	req := createReq("restored", "node-b")
	req.VolumeContentSource = &csi.VolumeContentSource{
		Type: &csi.VolumeContentSource_Snapshot{
			Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: snap.Snapshot.SnapshotId},
		},
	}
	resp, err := c.CreateVolume(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Volume.ContentSource.GetSnapshot().GetSnapshotId() != snap.Snapshot.SnapshotId {
		t.Fatal("content source not preserved")
	}
	ok, _ := fakes["node-b"].DatasetExists("tank/k8s/restored")
	if !ok {
		t.Fatal("restored dataset missing on node-b")
	}
	if got := string(fakes["node-b"].LastReceived("tank/k8s/restored")); got == "" {
		t.Fatal("node-b did not receive a stream")
	}
	// The source snapshot also exists locally on node-a (clone path).
	req2 := createReq("restored-local", "node-a")
	req2.VolumeContentSource = req.VolumeContentSource
	if _, err := c.CreateVolume(ctx, req2); err != nil {
		t.Fatal(err)
	}
	ok, _ = fakes["node-a"].DatasetExists("tank/k8s/restored-local")
	if !ok {
		t.Fatal("local clone missing")
	}
}

func TestDeleteVolumeExpandsAndCrossPeer(t *testing.T) {
	ctx := context.Background()
	c, fakes := testController(t, "tank/k8s", "node-a", "node-b")
	if _, err := c.CreateVolume(ctx, createReq("pvc-x", "node-a")); err != nil {
		t.Fatal(err)
	}
	if err := fakes["node-b"].CreateDataset("tank/k8s/pvc-x"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{
		VolumeId:      "pvc-x",
		CapacityRange: &csi.CapacityRange{RequiredBytes: 42 << 20},
	}); err != nil {
		t.Fatal(err)
	}
	if got := fakes["node-a"].Quota("tank/k8s/pvc-x"); got != 42<<20 {
		t.Fatalf("quota = %d", got)
	}
	if _, err := c.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: "pvc-x"}); err != nil {
		t.Fatal(err)
	}
	for node, b := range fakes {
		ok, _ := b.DatasetExists("tank/k8s/pvc-x")
		if ok {
			t.Fatalf("dataset still exists on %s", node)
		}
	}
}

func TestControllerCapabilities(t *testing.T) {
	c, _ := testController(t, "tank/k8s", "node-a")
	resp, err := c.ControllerGetCapabilities(context.Background(), &csi.ControllerGetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Capabilities) < 6 {
		t.Fatalf("only %d capabilities", len(resp.Capabilities))
	}
}
