// Package backend defines the small set of ZFS primitives the driver needs.
//
// There is deliberately exactly one production implementation, the ZFS CLI
// backend in zfs.go. The fake in memory backend exists only for tests.
package backend

import (
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned when a dataset, snapshot or mountpoint is absent.
var ErrNotFound = errors.New("not found")

// Snapshot describes one ZFS snapshot below a dataset.
type SnapshotInfo struct {
	Name      string    `json:"name"`
	Dataset   string    `json:"dataset"`
	Creation  time.Time `json:"creation"`
	UsedBytes int64     `json:"usedBytes"`
}

// Backend is the complete surface the driver has towards the storage system.
// All methods must be idempotent where the operation has an obvious "already
// done" meaning (create, destroy, snapshot, clone).
type Backend interface {
	CreateDataset(name string) error
	DestroyDataset(name string, recursive bool) error
	Snapshot(ds, snap string) error
	DestroySnapshot(ds, snap string) error
	Clone(snap, target string) error
	ListSnapshots(ds string) ([]SnapshotInfo, error)
	SendIncremental(from, to string, w io.Writer) error
	Receive(r io.Reader, target string) error
	Mountpoint(ds string) (string, error)
	UsedBytes(ds string) (int64, error)

	// Extensions used by failover, garbage collection and expansion.
	DatasetExists(ds string) (bool, error)
	ListDatasets(root string) ([]string, error)
	SetQuota(ds string, bytes int64) error
}
