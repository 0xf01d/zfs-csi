package driver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/naming"
	"github.com/0xf01d/zfs-csi/pkg/peer"
	"github.com/0xf01d/zfs-csi/pkg/replication"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// ContextSourceSnapshot is passed from controller to node so a fresh node
	// can clone or import the volume's source snapshot.
	ContextSourceSnapshot = "zfs-csi/sourceSnapshot"
	// ContextPool and ContextDataset describe the backing dataset.
	ContextPool     = "zfs-csi/pool"
	ContextDataset  = "zfs-csi/dataset"
	ContextCapacity = "zfs-csi/capacityBytes"
)

// Controller implements the CSI Controller service.
type Controller struct {
	csi.UnimplementedControllerServer
	DriverName  string
	Pool        string
	TopologyKey string
	Peers       []replication.Peer
	Nodes       []string
	Logf        func(string, ...any)
}

func NewController(driverName, pool, topologyKey string, peers []replication.Peer, nodes []string) *Controller {
	return &Controller{
		DriverName:  driverName,
		Pool:        pool,
		TopologyKey: topologyKey,
		Peers:       peers,
		Nodes:       nodes,
		Logf:        log.Printf,
	}
}

func (c *Controller) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

func (c *Controller) client(node string) *peer.Client {
	for _, p := range c.Peers {
		if p.Node == node {
			return p.Client
		}
	}
	return nil
}

func (c *Controller) allNodes() []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range c.Nodes {
		if n != "" && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	for _, p := range c.Peers {
		if p.Node != "" && !seen[p.Node] {
			seen[p.Node] = true
			out = append(out, p.Node)
		}
	}
	sort.Strings(out)
	return out
}

func (c *Controller) topologies(node string) []*csi.Topology {
	nodes := c.allNodes()
	if node != "" {
		nodes = []string{node}
	}
	out := make([]*csi.Topology, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, &csi.Topology{Segments: map[string]string{c.TopologyKey: n}})
	}
	return out
}

func (c *Controller) chooseNode(req *csi.CreateVolumeRequest) string {
	reqs := req.GetAccessibilityRequirements()
	if reqs == nil {
		return ""
	}
	known := map[string]bool{}
	for _, n := range c.allNodes() {
		known[n] = true
	}
	for _, list := range [][]*csi.Topology{reqs.GetRequisite(), reqs.GetPreferred()} {
		for _, t := range list {
			n := t.GetSegments()[c.TopologyKey]
			if n != "" && known[n] {
				return n
			}
		}
	}
	return ""
}

func (c *Controller) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	for _, cap := range req.GetVolumeCapabilities() {
		if cap.GetBlock() != nil {
			return nil, status.Error(codes.InvalidArgument, "block volumes are not supported; this driver mounts ZFS datasets")
		}
	}
	volume := naming.SafeComponent(req.GetName())
	capacity := int64(0)
	if req.GetCapacityRange() != nil {
		capacity = req.GetCapacityRange().GetRequiredBytes()
	}
	if capacity <= 0 {
		capacity = 1 << 30
	}

	sourceSnapshot := ""
	if src := req.GetVolumeContentSource().GetSnapshot(); src != nil {
		sourceSnapshot = src.GetSnapshotId()
		if _, _, err := naming.SplitSnapshotHandle(sourceSnapshot); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	target := c.chooseNode(req)
	if target != "" {
		client := c.client(target)
		if client == nil {
			return nil, status.Errorf(codes.ResourceExhausted, "node %q is not configured as a peer", target)
		}
		exists, err := c.volumeExists(ctx, volume)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if !exists {
			if err := c.provisionOn(ctx, client, target, volume, sourceSnapshot, capacity); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
	} else if sourceSnapshot != "" {
		// Late binding: the node plugin will clone or import when the pod is
		// actually scheduled. Nothing to do here yet.
		c.logf("CreateVolume %s deferred: no topology selected", volume)
	}

	ctxMap := map[string]string{
		ContextPool:     c.Pool,
		ContextDataset:  naming.Dataset(c.Pool, volume),
		ContextCapacity: strconv.FormatInt(capacity, 10),
	}
	if sourceSnapshot != "" {
		ctxMap[ContextSourceSnapshot] = sourceSnapshot
	}
	vol := &csi.Volume{
		VolumeId:           volume,
		CapacityBytes:      capacity,
		VolumeContext:      ctxMap,
		AccessibleTopology: c.topologies(""),
	}
	if sourceSnapshot != "" {
		vol.ContentSource = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{SnapshotId: sourceSnapshot},
			},
		}
	}
	return &csi.CreateVolumeResponse{Volume: vol}, nil
}

func (c *Controller) provisionOn(ctx context.Context, client *peer.Client, node, volume, sourceSnapshot string, capacity int64) error {
	if sourceSnapshot == "" {
		if err := client.CreateDataset(ctx, volume, ""); err != nil {
			return err
		}
		return c.setQuota(ctx, client, volume, capacity)
	}
	sourceVolume, snap, err := naming.SplitSnapshotHandle(sourceSnapshot)
	if err != nil {
		return err
	}
	st, err := client.Status(ctx, sourceVolume)
	if err == nil && st.Exists && peer.SnapshotExists(st, snap) {
		full := naming.Snapshot(naming.Dataset(c.Pool, sourceVolume), snap)
		if err := client.Clone(ctx, volume, full); err != nil {
			return err
		}
		return c.setQuota(ctx, client, volume, capacity)
	}
	// The source snapshot must be pulled from another node.
	for _, p := range c.Peers {
		src, err := p.Client.Status(ctx, sourceVolume)
		if err != nil || !src.Exists || !peer.SnapshotExists(src, snap) {
			continue
		}
		if err := client.Import(ctx, volume, sourceVolume, snap, "", p.Client.BaseURL); err != nil {
			return err
		}
		return c.setQuota(ctx, client, volume, capacity)
	}
	return fmt.Errorf("source snapshot %s not found on any node", sourceSnapshot)
}

func (c *Controller) setQuota(ctx context.Context, client *peer.Client, volume string, capacity int64) error {
	if capacity <= 0 {
		return nil
	}
	return client.Expand(ctx, volume, capacity)
}

func (c *Controller) volumeExists(ctx context.Context, volume string) (bool, error) {
	var firstErr error
	for _, p := range c.Peers {
		st, err := p.Client.Status(ctx, volume)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if st.Exists {
			return true, nil
		}
	}
	return false, firstErr
}

func (c *Controller) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if len(c.Peers) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no storage nodes configured; cannot delete the dataset")
	}
	var errs []error
	for _, p := range c.Peers {
		if err := p.Client.DeleteDataset(ctx, req.GetVolumeId()); err != nil && !errors.Is(err, backend.ErrNotFound) {
			errs = append(errs, fmt.Errorf("%s: %w", p.Node, err))
		}
	}
	if len(errs) > 0 {
		return nil, status.Error(codes.Internal, errors.Join(errs...).Error())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

func (c *Controller) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and node id are required")
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{}}, nil
}

func (c *Controller) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (c *Controller) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if len(req.GetVolumeCapabilities()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	var confirmed []*csi.VolumeCapability
	for _, cap := range req.GetVolumeCapabilities() {
		if cap.GetBlock() != nil {
			return &csi.ValidateVolumeCapabilitiesResponse{
				Message: "only filesystem volumes are supported",
			}, nil
		}
		confirmed = append(confirmed, cap)
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeContext:      req.GetVolumeContext(),
			VolumeCapabilities: confirmed,
		},
	}, nil
}

func (c *Controller) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	if req.GetSourceVolumeId() == "" || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "source volume id and name are required")
	}
	volume := req.GetSourceVolumeId()
	snap := naming.SnapshotName(naming.SnapshotPrefixVolume, req.GetName())

	owner, st, err := c.findOwner(ctx, volume)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if owner == nil {
		if len(c.Peers) == 0 {
			return nil, status.Error(codes.Unavailable, "no storage nodes configured")
		}
		owner = &c.Peers[0]
		if err := owner.Client.CreateDataset(ctx, volume, ""); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		st = &peer.DatasetStatus{Volume: volume, Exists: true}
	}
	// Idempotence: if the snapshot already exists, report it.
	for _, s := range st.Snapshots {
		if s.Name == snap {
			return snapshotResponse(volume, snap, s.Creation, s.UsedBytes), nil
		}
	}
	if err := owner.Client.CreateSnapshot(ctx, volume, snap); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	created := time.Now().UTC()
	used := int64(0)
	if after, err := owner.Client.Status(ctx, volume); err == nil {
		for _, s := range after.Snapshots {
			if s.Name == snap {
				created = s.Creation
				used = s.UsedBytes
			}
		}
	}
	return snapshotResponse(volume, snap, created, used), nil
}

func snapshotResponse(volume, snap string, created time.Time, used int64) *csi.CreateSnapshotResponse {
	if created.IsZero() {
		created = time.Now().UTC()
	}
	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     naming.SnapshotHandle(volume, snap),
			SourceVolumeId: volume,
			CreationTime:   timestamppb.New(created),
			SizeBytes:      used,
			ReadyToUse:     true,
		},
	}
}

func (c *Controller) findOwner(ctx context.Context, volume string) (*replication.Peer, *peer.DatasetStatus, error) {
	var firstErr error
	for i := range c.Peers {
		st, err := c.Peers[i].Client.Status(ctx, volume)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if st.Exists {
			return &c.Peers[i], &st, nil
		}
	}
	return nil, nil, firstErr
}

func (c *Controller) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot id is required")
	}
	volume, snap, err := naming.SplitSnapshotHandle(req.GetSnapshotId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(c.Peers) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "no storage nodes configured; cannot delete the snapshot")
	}
	var errs []error
	for _, p := range c.Peers {
		if err := p.Client.DeleteSnapshot(ctx, volume, snap); err != nil && !errors.Is(err, backend.ErrNotFound) {
			errs = append(errs, fmt.Errorf("%s: %w", p.Node, err))
		}
	}
	if len(errs) > 0 {
		return nil, status.Error(codes.Internal, errors.Join(errs...).Error())
	}
	return &csi.DeleteSnapshotResponse{}, nil
}

func (c *Controller) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	volumes := map[string]bool{}
	if req.GetSourceVolumeId() != "" {
		volumes[req.GetSourceVolumeId()] = true
	} else {
		for _, p := range c.Peers {
			vs, err := p.Client.Volumes(ctx)
			if err != nil {
				continue
			}
			for _, v := range vs {
				volumes[v] = true
			}
		}
	}
	seen := map[string]*csi.Snapshot{}
	for volume := range volumes {
		for _, p := range c.Peers {
			st, err := p.Client.Status(ctx, volume)
			if err != nil || !st.Exists {
				continue
			}
			for _, s := range st.Snapshots {
				id := naming.SnapshotHandle(volume, s.Name)
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = &csi.Snapshot{
					SnapshotId:     id,
					SourceVolumeId: volume,
					CreationTime:   timestamppb.New(s.Creation),
					SizeBytes:      s.UsedBytes,
					ReadyToUse:     true,
				}
			}
		}
	}
	entries := make([]*csi.ListSnapshotsResponse_Entry, 0, len(seen))
	for _, s := range seen {
		entries = append(entries, &csi.ListSnapshotsResponse_Entry{Snapshot: s})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Snapshot.SnapshotId < entries[j].Snapshot.SnapshotId })
	return &csi.ListSnapshotsResponse{Entries: entries}, nil
}

func (c *Controller) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	seen := map[string]bool{}
	var entries []*csi.ListVolumesResponse_Entry
	for _, p := range c.Peers {
		vols, err := p.Client.Volumes(ctx)
		if err != nil {
			continue
		}
		for _, v := range vols {
			if seen[v] {
				continue
			}
			seen[v] = true
			used := int64(0)
			if st, err := p.Client.Status(ctx, v); err == nil {
				used = st.UsedBytes
			}
			entries = append(entries, &csi.ListVolumesResponse_Entry{
				Volume: &csi.Volume{
					VolumeId:           v,
					CapacityBytes:      used,
					AccessibleTopology: c.topologies(""),
				},
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Volume.VolumeId < entries[j].Volume.VolumeId })
	return &csi.ListVolumesResponse{Entries: entries}, nil
}

func (c *Controller) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetCapacityRange().GetRequiredBytes() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "volume id and required bytes are required")
	}
	bytes := req.GetCapacityRange().GetRequiredBytes()
	var errs []error
	found := false
	for _, p := range c.Peers {
		st, err := p.Client.Status(ctx, req.GetVolumeId())
		if err != nil || !st.Exists {
			continue
		}
		found = true
		if err := p.Client.Expand(ctx, req.GetVolumeId(), bytes); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", p.Node, err))
		}
	}
	if len(errs) > 0 {
		return nil, status.Error(codes.Internal, errors.Join(errs...).Error())
	}
	if !found {
		return nil, status.Error(codes.NotFound, "volume not found on any node")
	}
	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         bytes,
		NodeExpansionRequired: false,
	}, nil
}

func (c *Controller) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	caps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
	out := make([]*csi.ControllerServiceCapability, 0, len(caps))
	for _, cap := range caps {
		out = append(out, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{Type: cap},
			},
		})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: out}, nil
}
