package driver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"syscall"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/mount"
	"github.com/0xf01d/zfs-csi/pkg/naming"
	"github.com/0xf01d/zfs-csi/pkg/replication"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Node implements the CSI Node service.
type Node struct {
	csi.UnimplementedNodeServer
	DriverName  string
	NodeID      string
	Pool        string
	TopologyKey string
	MaxVolumes  int64
	Backend     backend.Backend
	Mounter     mount.Mounter
	Replicator  *replication.Manager
	Logf        func(string, ...any)
	mu          sync.Mutex
	volumeLocks map[string]*sync.Mutex
}

func NewNode(driverName, nodeID, pool, topologyKey string, maxVolumes int64, b backend.Backend, m mount.Mounter, r *replication.Manager) *Node {
	return &Node{
		DriverName:  driverName,
		NodeID:      nodeID,
		Pool:        pool,
		TopologyKey: topologyKey,
		MaxVolumes:  maxVolumes,
		Backend:     b,
		Mounter:     m,
		Replicator:  r,
		Logf:        log.Printf,
		volumeLocks: make(map[string]*sync.Mutex),
	}
}

func (n *Node) logf(format string, args ...any) {
	if n.Logf != nil {
		n.Logf(format, args...)
	}
}

func (n *Node) lock(volume string) func() {
	n.mu.Lock()
	l, ok := n.volumeLocks[volume]
	if !ok {
		l = &sync.Mutex{}
		n.volumeLocks[volume] = l
	}
	n.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (n *Node) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "staging target path is required")
	}
	if err := n.stage(ctx, req.GetVolumeId(), req.GetStagingTargetPath(), req.GetVolumeCapability(), req.GetVolumeContext()); err != nil {
		return nil, err
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *Node) stage(ctx context.Context, volume, staging string, cap *csi.VolumeCapability, vctx map[string]string) error {
	if cap != nil && cap.GetBlock() != nil {
		return status.Error(codes.InvalidArgument, "block volumes are not supported")
	}
	if staging == "" {
		return status.Error(codes.InvalidArgument, "staging target path is required")
	}
	unlock := n.lock(volume)
	defer unlock()

	source := vctx[ContextSourceSnapshot]
	if n.Replicator != nil {
		if err := n.Replicator.EnsureLocal(ctx, volume, source); err != nil {
			return status.Errorf(codes.Internal, "ensure dataset: %v", err)
		}
	}
	ds := naming.Dataset(n.Pool, volume)
	if value := vctx[ContextCapacity]; value != "" {
		capacity, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid capacity in volume context: %v", err)
		}
		if capacity > 0 {
			if err := n.Backend.SetQuota(ds, capacity); err != nil {
				return status.Errorf(codes.Internal, "set quota: %v", err)
			}
		}
	}

	mounted, err := n.Mounter.IsMountPoint(staging)
	if err != nil {
		return status.Errorf(codes.Internal, "check mountpoint: %v", err)
	}
	if mounted {
		return nil
	}

	mp, err := n.Backend.Mountpoint(ds)
	if err != nil {
		return status.Errorf(codes.Internal, "mountpoint of %s: %v", ds, err)
	}
	var flags []string
	if cap != nil && cap.GetMount() != nil {
		flags = append(flags, cap.GetMount().GetMountFlags()...)
	}
	if mp != "" && mp != "legacy" && mp != "none" {
		if ok, _ := n.Mounter.IsMountPoint(mp); ok {
			return n.Mounter.BindMount(mp, staging, hasRO(flags))
		}
	}
	if err := n.Mounter.Mount(ds, staging, "zfs", flags); err != nil {
		return status.Errorf(codes.Internal, "mount %s at %s: %v", ds, staging, err)
	}
	return nil
}

func (n *Node) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and staging target path are required")
	}
	unlock := n.lock(req.GetVolumeId())
	defer unlock()
	mounted, err := n.Mounter.IsMountPoint(req.GetStagingTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check mountpoint: %v", err)
	}
	if !mounted {
		return &csi.NodeUnstageVolumeResponse{}, nil
	}
	if err := n.Mounter.Unmount(req.GetStagingTargetPath()); err != nil {
		return nil, status.Errorf(codes.Internal, "unmount staging: %v", err)
	}
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *Node) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if err := n.stage(ctx, req.GetVolumeId(), req.GetStagingTargetPath(), req.GetVolumeCapability(), req.GetVolumeContext()); err != nil {
		return nil, err
	}
	mounted, err := n.Mounter.IsMountPoint(req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check target mountpoint: %v", err)
	}
	if mounted {
		return &csi.NodePublishVolumeResponse{}, nil
	}
	readOnly := req.GetReadonly()
	if cap := req.GetVolumeCapability(); cap != nil && cap.GetMount() != nil && hasRO(cap.GetMount().GetMountFlags()) {
		readOnly = true
	}
	if err := n.Mounter.BindMount(req.GetStagingTargetPath(), req.GetTargetPath(), readOnly); err != nil {
		return nil, status.Errorf(codes.Internal, "bind mount: %v", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *Node) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and target path are required")
	}
	mounted, err := n.Mounter.IsMountPoint(req.GetTargetPath())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check target mountpoint: %v", err)
	}
	if mounted {
		if err := n.Mounter.Unmount(req.GetTargetPath()); err != nil {
			return nil, status.Errorf(codes.Internal, "unmount target: %v", err)
		}
	}
	// Take one last snapshot and push it while the pod is going away. The
	// call is asynchronous so kubelet is not blocked, but it starts before
	// the dataset can be used elsewhere.
	if n.Replicator != nil {
		n.Replicator.TriggerAsync(req.GetVolumeId(), true)
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func hasRO(flags []string) bool {
	for _, f := range flags {
		if f == "ro" || f == "readonly" {
			return true
		}
	}
	return false
}

func (n *Node) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId:            n.NodeID,
		MaxVolumesPerNode: n.MaxVolumes,
		AccessibleTopology: &csi.Topology{
			Segments: map[string]string{n.TopologyKey: n.NodeID},
		},
	}, nil
}

func (n *Node) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	caps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		csi.NodeServiceCapability_RPC_GET_VOLUME_STATS,
		csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
	}
	out := make([]*csi.NodeServiceCapability, 0, len(caps))
	for _, c := range caps {
		out = append(out, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{Type: c},
			},
		})
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: out}, nil
}

func (n *Node) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id and volume path are required")
	}
	path := req.GetVolumePath()
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, status.Errorf(codes.NotFound, "path %s does not exist", path)
		}
		return nil, status.Errorf(codes.Internal, "stat %s: %v", path, err)
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return nil, status.Errorf(codes.Internal, "statfs %s: %v", path, err)
	}
	total := int64(st.Blocks) * int64(st.Bsize)
	avail := int64(st.Bavail) * int64(st.Bsize)
	used := int64(st.Blocks-st.Bfree) * int64(st.Bsize)
	inodes := int64(st.Files)
	inodesFree := int64(st.Ffree)
	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{Available: avail, Total: total, Used: used, Unit: csi.VolumeUsage_BYTES},
			{Available: inodesFree, Total: inodes, Used: inodes - inodesFree, Unit: csi.VolumeUsage_INODES},
		},
	}, nil
}

func (n *Node) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	bytes := req.GetCapacityRange().GetRequiredBytes()
	if bytes <= 0 {
		used, err := n.Backend.UsedBytes(naming.Dataset(n.Pool, req.GetVolumeId()))
		if err != nil {
			return nil, status.Errorf(codes.Internal, "used bytes: %v", err)
		}
		bytes = used
	}
	return &csi.NodeExpandVolumeResponse{CapacityBytes: bytes}, nil
}

// EnsureDataset is a small helper used by tests and by future direct callers.
func (n *Node) EnsureDataset(ctx context.Context, volume, sourceSnapshot string) error {
	if n.Replicator == nil {
		return fmt.Errorf("no replicator configured")
	}
	return n.Replicator.EnsureLocal(ctx, volume, sourceSnapshot)
}
