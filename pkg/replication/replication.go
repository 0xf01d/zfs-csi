// Package replication pushes snapshots of local datasets to peer nodes.
//
// The unit is a dataset, never a mounted block device. For every snapshot the
// replicator finds the newest snapshot both sides share, then streams
// "zfs send -i <base> <latest>" into the peer's "zfs receive". If there is no
// common base it sends a full stream. Restarting after a crash only requires
// listing snapshots again; there is no local database.
package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/naming"
	"github.com/0xf01d/zfs-csi/pkg/peer"
)

// Peer is one replication destination.
type Peer struct {
	Node   string
	Client *peer.Client
}

// Manager replicates datasets belonging to one pool.
type Manager struct {
	Backend backend.Backend
	Pool    string
	Node    string
	Peers   []Peer
	Keep    int
	Timeout time.Duration
	Logf    func(format string, args ...any)
	mu      sync.Mutex
	perVol  map[string]*sync.Mutex
}

func New(b backend.Backend, pool, node string, peers []Peer, keep int, timeout time.Duration) *Manager {
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	return &Manager{
		Backend: b,
		Pool:    pool,
		Node:    node,
		Peers:   peers,
		Keep:    keep,
		Timeout: timeout,
		Logf:    log.Printf,
		perVol:  make(map[string]*sync.Mutex),
	}
}

func (m *Manager) lock(volume string) func() {
	m.mu.Lock()
	l, ok := m.perVol[volume]
	if !ok {
		l = &sync.Mutex{}
		m.perVol[volume] = l
	}
	m.mu.Unlock()
	l.Lock()
	return l.Unlock
}

func (m *Manager) logf(format string, args ...any) {
	if m.Logf != nil {
		m.Logf(format, args...)
	}
}

// NotifySnapshot replicates an already existing snapshot (for example the
// @vs-<uid> snapshot that Kubernetes asked for).
func (m *Manager) NotifySnapshot(ctx context.Context, volume, snapshot string) error {
	unlock := m.lock(volume)
	defer unlock()
	return m.replicate(ctx, volume, snapshot)
}

// SnapshotNow takes a @rep-<timestamp> snapshot and replicates it.
func (m *Manager) SnapshotNow(ctx context.Context, volume string) error {
	unlock := m.lock(volume)
	defer unlock()
	snap := naming.SnapshotName(naming.SnapshotPrefixReplication, fmt.Sprintf("%d", time.Now().UTC().UnixNano()))
	ds := naming.Dataset(m.Pool, volume)
	exists, err := m.Backend.DatasetExists(ds)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("volume %s does not exist on %s", volume, m.Node)
	}
	if err := m.Backend.Snapshot(ds, snap); err != nil {
		return err
	}
	return m.replicate(ctx, volume, snap)
}

// NotifyAsync replicates one named snapshot in the background.
func (m *Manager) NotifyAsync(volume, snapshot string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), m.Timeout)
		defer cancel()
		if err := m.NotifySnapshot(ctx, volume, snapshot); err != nil {
			m.logf("replication %s@%s: %v", volume, snapshot, err)
		}
	}()
}

// TriggerAsync is the fire-and-forget version used by CSI RPCs.
func (m *Manager) TriggerAsync(volume string, takeSnapshot bool) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), m.Timeout)
		defer cancel()
		var err error
		if takeSnapshot {
			err = m.SnapshotNow(ctx, volume)
		} else {
			err = m.NotifySnapshot(ctx, volume, "")
		}
		if err != nil {
			m.logf("replication %s: %v", volume, err)
		}
	}()
}

func (m *Manager) replicate(ctx context.Context, volume, latest string) error {
	ds := naming.Dataset(m.Pool, volume)
	exists, err := m.Backend.DatasetExists(ds)
	if err != nil || !exists {
		if err != nil {
			return err
		}
		return fmt.Errorf("dataset %s does not exist", ds)
	}
	local, err := m.Backend.ListSnapshots(ds)
	if err != nil {
		return err
	}
	if len(local) == 0 {
		return fmt.Errorf("dataset %s has no snapshots", ds)
	}
	if latest == "" {
		latest = newest(local).Name
	}
	if !hasSnapshot(local, latest) {
		return fmt.Errorf("snapshot %s@%s does not exist", ds, latest)
	}

	var errs []error
	for _, p := range m.Peers {
		if err := m.replicateTo(ctx, p, volume, latest, local); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", p.Node, err))
			continue
		}
	}
	if len(errs) == 0 {
		m.prune(ctx, volume, local)
	}
	return errors.Join(errs...)
}

func (m *Manager) replicateTo(ctx context.Context, p Peer, volume, latest string, local []backend.SnapshotInfo) error {
	remote, err := p.Client.Status(ctx, volume)
	if err != nil {
		return err
	}
	if remote.Exists && hasSnapshot(remote.Snapshots, latest) {
		return nil
	}
	base := ""
	if remote.Exists {
		if common, ok := peer.CommonSnapshot(local, remote.Snapshots); ok {
			if common.Name == latest {
				return nil
			}
			base = common.Name
		}
	}
	m.logf("replicating %s to %s: from=%q to=%q", volume, p.Node, base, latest)
	pr, pw := io.Pipe()
	sendErr := make(chan error, 1)
	go func() {
		to := naming.Snapshot(naming.Dataset(m.Pool, volume), latest)
		from := ""
		if base != "" {
			from = naming.Snapshot(naming.Dataset(m.Pool, volume), base)
		}
		err := m.Backend.SendIncremental(from, to, pw)
		pw.CloseWithError(err)
		sendErr <- err
	}()
	recvErr := p.Client.Receive(ctx, volume, base, pr)
	_ = pr.Close()
	se := <-sendErr
	if recvErr != nil {
		return recvErr
	}
	return se
}

func (m *Manager) prune(ctx context.Context, volume string, local []backend.SnapshotInfo) {
	if m.Keep <= 0 {
		return
	}
	var reps []backend.SnapshotInfo
	for _, s := range local {
		if strings.HasPrefix(s.Name, naming.SnapshotPrefixReplication+"-") {
			reps = append(reps, s)
		}
	}
	if len(reps) <= m.Keep {
		return
	}
	sort.Slice(reps, func(i, j int) bool { return reps[i].Creation.Before(reps[j].Creation) })
	drop := reps[:len(reps)-m.Keep]
	ds := naming.Dataset(m.Pool, volume)
	for _, s := range drop {
		if err := m.Backend.DestroySnapshot(ds, s.Name); err != nil {
			m.logf("prune local %s@%s: %v", ds, s.Name, err)
		}
		for _, p := range m.Peers {
			if err := p.Client.DeleteSnapshot(ctx, volume, s.Name); err != nil {
				m.logf("prune %s %s@%s: %v", p.Node, ds, s.Name, err)
			}
		}
	}
}

// Run replicates every local volume periodically. An interval <= 0 means the
// manager is purely event driven.
func (m *Manager) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.replicateAll(ctx)
		}
	}
}

func (m *Manager) replicateAll(ctx context.Context) {
	datasets, err := m.Backend.ListDatasets(m.Pool)
	if err != nil {
		m.logf("list datasets: %v", err)
		return
	}
	for _, ds := range datasets {
		if !strings.HasPrefix(ds, strings.TrimSuffix(m.Pool, "/")+"/") {
			continue
		}
		volume := strings.TrimPrefix(ds, strings.TrimSuffix(m.Pool, "/")+"/")
		if err := m.SnapshotNow(ctx, volume); err != nil {
			m.logf("periodic snapshot %s: %v", volume, err)
		}
	}
}

// EnsureLocal makes sure the volume dataset exists on this node. It is the
// failover path:
//
//   - if sourceSnapshot is given and is local, clone it (instant, no copy);
//   - if it is on a peer, import it (full stream);
//   - if no source is given, ask peers for the newest snapshot of this volume
//     and import that; only a volume no node has ever seen is created empty.
func (m *Manager) EnsureLocal(ctx context.Context, volume, sourceSnapshot string) error {
	unlock := m.lock(volume)
	defer unlock()

	ds := naming.Dataset(m.Pool, volume)
	exists, err := m.Backend.DatasetExists(ds)
	if err != nil {
		return err
	}
	if exists {
		if sourceSnapshot != "" {
			return nil
		}
		// The dataset exists but a peer may have newer snapshots (for
		// example while this node was down). Catch up before the pod starts.
		return m.catchUp(ctx, volume)
	}

	if sourceSnapshot != "" {
		sourceVolume, snap, err := naming.SplitSnapshotHandle(sourceSnapshot)
		if err != nil {
			return err
		}
		if m.hasLocalSnapshot(sourceVolume, snap) {
			src := naming.Snapshot(naming.Dataset(m.Pool, sourceVolume), snap)
			return m.Backend.Clone(src, ds)
		}
		p, ok := m.findPeerSnapshot(ctx, sourceVolume, snap)
		if !ok {
			return fmt.Errorf("source snapshot %s@%s not found locally or on any peer", sourceVolume, snap)
		}
		m.logf("importing %s@%s from %s for %s", sourceVolume, snap, p.Node, volume)
		return m.importFrom(ctx, p, volume, sourceVolume, "", snap)
	}

	// Failover of an existing volume. Find the newest snapshot any peer has.
	p, st, snap, ok := m.newestRemote(ctx, volume)
	if ok {
		m.logf("failover import %s@%s from %s", volume, snap.Name, p.Node)
		return m.importFrom(ctx, p, volume, volume, "", snap.Name)
	}
	_ = st

	return m.Backend.CreateDataset(ds)
}

func (m *Manager) catchUp(ctx context.Context, volume string) error {
	ds := naming.Dataset(m.Pool, volume)
	local, err := m.Backend.ListSnapshots(ds)
	if err != nil && !errors.Is(err, backend.ErrNotFound) {
		return err
	}
	p, st, remote, ok := m.newestRemote(ctx, volume)
	if !ok {
		return nil
	}
	if hasSnapshot(local, remote.Name) {
		return nil
	}
	if len(local) > 0 && !remote.Creation.After(newest(local).Creation) {
		return nil
	}
	base := ""
	if common, ok := peer.CommonSnapshot(local, st.Snapshots); ok {
		base = common.Name
	}
	m.logf("catch-up %s: from=%q to=%q via %s", volume, base, remote.Name, p.Node)
	return m.importFrom(ctx, p, volume, volume, base, remote.Name)
}

func (m *Manager) newestRemote(ctx context.Context, volume string) (Peer, peer.DatasetStatus, backend.SnapshotInfo, bool) {
	type candidate struct {
		peer Peer
		stat peer.DatasetStatus
		snap backend.SnapshotInfo
	}
	var best *candidate
	for _, p := range m.Peers {
		st, err := p.Client.Status(ctx, volume)
		if err != nil {
			m.logf("failover query %s: %v", p.Node, err)
			continue
		}
		if !st.Exists {
			continue
		}
		snap, ok := peer.LatestSnapshot(st)
		if !ok {
			// A peer with a dataset but no snapshot is useless for a
			// dataset-level transfer. Take one, then use it.
			name := naming.SnapshotName(naming.SnapshotPrefixReplication, fmt.Sprintf("%d", time.Now().UTC().UnixNano()))
			if err := p.Client.CreateSnapshot(ctx, volume, name); err != nil {
				m.logf("failover snapshot on %s: %v", p.Node, err)
				continue
			}
			snap = backend.SnapshotInfo{Name: name, Dataset: naming.Dataset(m.Pool, volume), Creation: time.Now()}
			if st, err = p.Client.Status(ctx, volume); err != nil {
				continue
			}
			ok = true
		}
		if ok && (best == nil || snap.Creation.After(best.snap.Creation)) {
			best = &candidate{peer: p, stat: st, snap: snap}
		}
	}
	if best == nil {
		return Peer{}, peer.DatasetStatus{}, backend.SnapshotInfo{}, false
	}
	return best.peer, best.stat, best.snap, true
}

func (m *Manager) importFrom(ctx context.Context, p Peer, targetVolume, sourceVolume, base, snapshot string) error {
	stream, err := p.Client.Send(ctx, sourceVolume, base, snapshot)
	if err != nil {
		return err
	}
	defer stream.Close()
	target := naming.Dataset(m.Pool, targetVolume)
	exists, err := m.Backend.DatasetExists(target)
	if err != nil {
		return err
	}
	if exists && base == "" {
		if err := m.Backend.DestroyDataset(target, true); err != nil {
			return err
		}
	}
	return m.Backend.Receive(stream, target)
}

func (m *Manager) hasLocalSnapshot(volume, snap string) bool {
	snaps, err := m.Backend.ListSnapshots(naming.Dataset(m.Pool, volume))
	if err != nil {
		return false
	}
	return hasSnapshot(snaps, snap)
}

func (m *Manager) findPeerSnapshot(ctx context.Context, volume, snap string) (Peer, bool) {
	for _, p := range m.Peers {
		st, err := p.Client.Status(ctx, volume)
		if err != nil {
			continue
		}
		if st.Exists && peer.SnapshotExists(st, snap) {
			return p, true
		}
	}
	return Peer{}, false
}

func newest(snaps []backend.SnapshotInfo) backend.SnapshotInfo {
	best := snaps[0]
	for _, s := range snaps[1:] {
		if s.Creation.After(best.Creation) {
			best = s
		}
	}
	return best
}

func hasSnapshot(snaps []backend.SnapshotInfo, name string) bool {
	for _, s := range snaps {
		if s.Name == name {
			return true
		}
	}
	return false
}
