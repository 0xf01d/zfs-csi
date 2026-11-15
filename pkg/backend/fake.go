package backend

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Fake is an in-memory Backend used by unit tests. It does not implement ZFS
// data semantics; it only tracks datasets, snapshots and streams.
type Fake struct {
	mu        sync.Mutex
	datasets  map[string]*fakeDataset
	received  map[string][]byte
	clockBase time.Time
	clockTick int64
}

type fakeDataset struct {
	used      int64
	quota     int64
	snapshots map[string]SnapshotInfo
}

// NewFake returns an empty fake backend.
func NewFake() *Fake {
	return &Fake{
		datasets:  make(map[string]*fakeDataset),
		received:  make(map[string][]byte),
		clockBase: time.Unix(1700000000, 0).UTC(),
	}
}

func (f *Fake) now() time.Time {
	f.clockTick++
	return f.clockBase.Add(time.Duration(f.clockTick) * time.Second)
}

func (f *Fake) get(name string) (*fakeDataset, bool) {
	d, ok := f.datasets[name]
	return d, ok
}

func (f *Fake) CreateDataset(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.get(name); ok {
		return nil
	}
	f.datasets[name] = &fakeDataset{snapshots: make(map[string]SnapshotInfo)}
	return nil
}

func (f *Fake) DestroyDataset(name string, recursive bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.get(name); ok {
		delete(f.datasets, name)
		return nil
	}
	if !recursive {
		return ErrNotFound
	}
	for n := range f.datasets {
		if strings.HasPrefix(n, name+"/") {
			delete(f.datasets, n)
		}
	}
	return nil
}

func (f *Fake) Snapshot(ds, snap string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.get(ds)
	if !ok {
		return ErrNotFound
	}
	if _, ok := d.snapshots[snap]; ok {
		return nil
	}
	d.snapshots[snap] = SnapshotInfo{
		Name:      snap,
		Dataset:   ds,
		Creation:  f.now(),
		UsedBytes: d.used,
	}
	return nil
}

func (f *Fake) DestroySnapshot(ds, snap string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.get(ds)
	if !ok {
		return nil
	}
	delete(d.snapshots, snap)
	return nil
}

func (f *Fake) Clone(snap, target string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ds, name, err := splitSnapshot(snap)
	if err != nil {
		return err
	}
	src, ok := f.get(ds)
	if !ok {
		return ErrNotFound
	}
	info, ok := src.snapshots[name]
	if !ok {
		return ErrNotFound
	}
	if _, ok := f.get(target); ok {
		return nil
	}
	f.datasets[target] = &fakeDataset{
		used:      info.UsedBytes,
		snapshots: make(map[string]SnapshotInfo),
	}
	return nil
}

func (f *Fake) ListSnapshots(ds string) ([]SnapshotInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.get(ds)
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]SnapshotInfo, 0, len(d.snapshots))
	for _, s := range d.snapshots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Creation.Equal(out[j].Creation) {
			return out[i].Name < out[j].Name
		}
		return out[i].Creation.Before(out[j].Creation)
	})
	return out, nil
}

func (f *Fake) SendIncremental(from, to string, w io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ds, name, err := splitSnapshot(to)
	if err != nil {
		return err
	}
	d, ok := f.get(ds)
	if !ok {
		return ErrNotFound
	}
	if _, ok := d.snapshots[name]; !ok {
		return ErrNotFound
	}
	if from != "" {
		fds, fname, err := splitSnapshot(from)
		if err != nil {
			return err
		}
		fd, ok := f.get(fds)
		if !ok {
			return ErrNotFound
		}
		if _, ok := fd.snapshots[fname]; !ok {
			return ErrNotFound
		}
	}
	_, err = fmt.Fprintf(w, "fake-send %s -> %s\n", from, to)
	return err
}

func (f *Fake) Receive(r io.Reader, target string) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.get(target); !ok {
		f.datasets[target] = &fakeDataset{snapshots: make(map[string]SnapshotInfo)}
	}
	f.received[target] = append([]byte(nil), b...)
	// The fake stream is a text marker, not real ZFS data. If it names the
	// destination snapshot, create it so replication tests can observe what
	// arrived.
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "fake-send ") {
			continue
		}
		parts := strings.SplitN(strings.TrimPrefix(line, "fake-send "), " -> ", 2)
		if len(parts) != 2 {
			continue
		}
		i := strings.LastIndex(parts[1], "@")
		if i <= 0 || i == len(parts[1])-1 {
			continue
		}
		snap := parts[1][i+1:]
		d := f.datasets[target]
		d.snapshots[snap] = SnapshotInfo{
			Name:      snap,
			Dataset:   target,
			Creation:  f.now(),
			UsedBytes: d.used,
		}
	}
	return nil
}

func (f *Fake) Mountpoint(ds string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.get(ds); !ok {
		return "", ErrNotFound
	}
	return "/fake/" + ds, nil
}

func (f *Fake) UsedBytes(ds string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.get(ds)
	if !ok {
		return 0, ErrNotFound
	}
	return d.used, nil
}

func (f *Fake) DatasetExists(ds string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.get(ds)
	return ok, nil
}

func (f *Fake) ListDatasets(root string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for name := range f.datasets {
		if name == root || strings.HasPrefix(name, root+"/") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (f *Fake) SetQuota(ds string, bytes int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.get(ds)
	if !ok {
		return ErrNotFound
	}
	d.quota = bytes
	return nil
}

// Test helpers.

// SeedSnapshot creates a dataset (if needed) and a snapshot with a fixed used
// size. It is meant for tests.
func (f *Fake) SeedSnapshot(ds, snap string, used int64) error {
	if err := f.CreateDataset(ds); err != nil {
		return err
	}
	f.mu.Lock()
	f.datasets[ds].used = used
	f.mu.Unlock()
	return f.Snapshot(ds, snap)
}

// SetUsed sets the used bytes of a dataset for tests.
func (f *Fake) SetUsed(ds string, used int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.get(ds)
	if !ok {
		return ErrNotFound
	}
	d.used = used
	return nil
}

// LastReceived returns the bytes most recently written to target by Receive.
func (f *Fake) LastReceived(target string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.received[target]...)
}

// Quota returns the quota set for a dataset.
func (f *Fake) Quota(ds string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if d, ok := f.get(ds); ok {
		return d.quota
	}
	return 0
}

var _ Backend = (*Fake)(nil)
