// Package naming derives every ZFS name from the CSI identifiers.
//
// Nothing is stored in a local database. Given the pool and a volume handle,
// any node can reconstruct the dataset path. Given a volume handle and a
// snapshot name, any node can reconstruct the snapshot path.
package naming

import (
	"fmt"
	"strings"
)

const (
	// SnapshotPrefixVolume marks snapshots requested through the Kubernetes
	// VolumeSnapshot API.
	SnapshotPrefixVolume = "vs"
	// SnapshotPrefixReplication marks snapshots taken by the replicator.
	SnapshotPrefixReplication = "rep"
)

// Dataset returns the ZFS dataset name for a volume handle.
func Dataset(pool, volume string) string {
	return strings.TrimSuffix(pool, "/") + "/" + strings.TrimPrefix(volume, "/")
}

// Snapshot returns the ZFS snapshot name "dataset@snapshot".
func Snapshot(dataset, snap string) string {
	return dataset + "@" + snap
}

// SnapshotHandle is the CSI snapshot ID. It is self describing, so any node
// can find both the dataset and the ZFS snapshot from it.
func SnapshotHandle(volume, snap string) string {
	return volume + "@" + snap
}

// SplitSnapshotHandle splits a CSI snapshot ID into volume handle and ZFS
// snapshot name.
func SplitSnapshotHandle(handle string) (volume, snap string, err error) {
	i := strings.LastIndex(handle, "@")
	if i <= 0 || i == len(handle)-1 {
		return "", "", fmt.Errorf("invalid snapshot handle %q", handle)
	}
	return handle[:i], handle[i+1:], nil
}

// SnapshotName builds a ZFS-safe snapshot name.
func SnapshotName(prefix, uid string) string {
	return prefix + "-" + SafeComponent(uid)
}

// SafeComponent turns an arbitrary CSI name into a single ZFS-safe path
// component.
func SafeComponent(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.', r == ':':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := b.String()
	if out == "" {
		return "unknown"
	}
	return out
}

// ValidDataset reports whether name is a plausible absolute ZFS dataset path.
func ValidDataset(name string) bool {
	if name == "" || strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return false
	}
	if strings.ContainsAny(name, "@ \t\n\r") || strings.Contains(name, "..") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" {
			return false
		}
	}
	return true
}

// ValidSnapshot reports whether snap is a ZFS-safe snapshot component.
func ValidSnapshot(snap string) bool {
	if snap == "" || strings.ContainsAny(snap, "@/ \t\n\r*?[]") || strings.Contains(snap, "..") {
		return false
	}
	return true
}
