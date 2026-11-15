package backend

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/naming"
)

// ZFS is the production Backend. It shells out to zfs(8) on purpose: fewer
// dependencies, no libzfs ABI, and the exact commands are visible in logs.
type ZFS struct {
	binary string
}

// NewZFS returns a backend using the zfs binary at path. An empty path means
// "zfs" from PATH.
func NewZFS(path string) *ZFS {
	if path == "" {
		path = "zfs"
	}
	return &ZFS{binary: path}
}

func (z *ZFS) run(stdin io.Reader, stdout io.Writer, args ...string) error {
	cmd := exec.Command(z.binary, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}
	if stdout != nil {
		cmd.Stdout = stdout
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("zfs %s: %s", strings.Join(args, " "), msg)
	}
	return nil
}

func (z *ZFS) output(args ...string) (string, error) {
	var out bytes.Buffer
	if err := z.run(nil, &out, args...); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func isMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "no such") ||
		strings.Contains(msg, "not found")
}

func (z *ZFS) CreateDataset(name string) error {
	if !naming.ValidDataset(name) {
		return fmt.Errorf("invalid dataset name %q", name)
	}
	exists, err := z.DatasetExists(name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	err = z.run(nil, nil, "create", "-o", "mountpoint=legacy", name)
	if err != nil {
		// A concurrent create is still success.
		if ok, _ := z.DatasetExists(name); ok {
			return nil
		}
	}
	return err
}

func (z *ZFS) DestroyDataset(name string, recursive bool) error {
	if !naming.ValidDataset(name) {
		return fmt.Errorf("invalid dataset name %q", name)
	}
	args := []string{"destroy"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, name)
	err := z.run(nil, nil, args...)
	if err != nil && isMissing(err) {
		return nil
	}
	return err
}

func (z *ZFS) Snapshot(ds, snap string) error {
	if !naming.ValidDataset(ds) || !naming.ValidSnapshot(snap) {
		return fmt.Errorf("invalid snapshot %s@%s", ds, snap)
	}
	full := naming.Snapshot(ds, snap)
	err := z.run(nil, nil, "snapshot", full)
	if err != nil && (strings.Contains(strings.ToLower(err.Error()), "exists") ||
		strings.Contains(strings.ToLower(err.Error()), "already")) {
		return nil
	}
	return err
}

func (z *ZFS) DestroySnapshot(ds, snap string) error {
	if !naming.ValidDataset(ds) || !naming.ValidSnapshot(snap) {
		return fmt.Errorf("invalid snapshot %s@%s", ds, snap)
	}
	full := naming.Snapshot(ds, snap)
	err := z.run(nil, nil, "destroy", full)
	if err == nil {
		return nil
	}
	if isMissing(err) {
		return nil
	}
	// A snapshot used as a clone origin cannot be destroyed immediately.
	// Defer it: ZFS removes it once the last clone is gone. This keeps
	// DeleteSnapshot successful after a PVC was restored from the snapshot.
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "clone") || strings.Contains(msg, "dependent") {
		if derr := z.run(nil, nil, "destroy", "-d", full); derr == nil {
			return nil
		}
	}
	return err
}

func (z *ZFS) Clone(snap, target string) error {
	ds, _, err := splitSnapshot(snap)
	if err != nil {
		return err
	}
	if !naming.ValidDataset(ds) || !naming.ValidDataset(target) {
		return fmt.Errorf("invalid clone %s -> %s", snap, target)
	}
	exists, err := z.DatasetExists(target)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	err = z.run(nil, nil, "clone", "-o", "mountpoint=legacy", snap, target)
	if err != nil {
		if ok, _ := z.DatasetExists(target); ok {
			return nil
		}
	}
	return err
}

func splitSnapshot(full string) (dataset, snap string, err error) {
	i := strings.LastIndex(full, "@")
	if i <= 0 || i == len(full)-1 {
		return "", "", fmt.Errorf("invalid snapshot name %q", full)
	}
	return full[:i], full[i+1:], nil
}

func (z *ZFS) ListSnapshots(ds string) ([]SnapshotInfo, error) {
	if !naming.ValidDataset(ds) {
		return nil, fmt.Errorf("invalid dataset name %q", ds)
	}
	out, err := z.output("list", "-H", "-p", "-o", "name,creation,used", "-t", "snapshot", "-r", ds)
	if err != nil {
		if isMissing(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var snaps []SnapshotInfo
	prefix := ds + "@"
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 || !strings.HasPrefix(fields[0], prefix) {
			continue
		}
		name := strings.TrimPrefix(fields[0], prefix)
		if strings.Contains(name, "/") {
			continue
		}
		creation, _ := strconv.ParseInt(fields[1], 10, 64)
		used, _ := strconv.ParseInt(fields[2], 10, 64)
		snaps = append(snaps, SnapshotInfo{
			Name:      name,
			Dataset:   ds,
			Creation:  time.Unix(creation, 0).UTC(),
			UsedBytes: used,
		})
	}
	return snaps, nil
}

func (z *ZFS) SendIncremental(from, to string, w io.Writer) error {
	if _, _, err := splitSnapshot(to); err != nil {
		return err
	}
	args := []string{"send"}
	if from != "" {
		if _, _, err := splitSnapshot(from); err != nil {
			return err
		}
		args = append(args, "-i", from)
	}
	args = append(args, to)
	return z.run(nil, w, args...)
}

func (z *ZFS) Receive(r io.Reader, target string) error {
	if !naming.ValidDataset(target) {
		return fmt.Errorf("invalid receive target %q", target)
	}
	return z.run(r, nil, "receive", "-F", target)
}

func (z *ZFS) Mountpoint(ds string) (string, error) {
	if !naming.ValidDataset(ds) {
		return "", fmt.Errorf("invalid dataset name %q", ds)
	}
	out, err := z.output("get", "-H", "-o", "value", "mountpoint", ds)
	if err != nil {
		if isMissing(err) {
			return "", ErrNotFound
		}
		return "", err
	}
	return out, nil
}

func (z *ZFS) UsedBytes(ds string) (int64, error) {
	if !naming.ValidDataset(ds) {
		return 0, fmt.Errorf("invalid dataset name %q", ds)
	}
	out, err := z.output("get", "-H", "-p", "-o", "value", "used", ds)
	if err != nil {
		if isMissing(err) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	n, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse used bytes %q: %w", out, err)
	}
	return n, nil
}

func (z *ZFS) DatasetExists(ds string) (bool, error) {
	if !naming.ValidDataset(ds) {
		return false, fmt.Errorf("invalid dataset name %q", ds)
	}
	_, err := z.output("list", "-H", "-o", "name", ds)
	if err != nil {
		if isMissing(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (z *ZFS) ListDatasets(root string) ([]string, error) {
	if !naming.ValidDataset(root) {
		return nil, fmt.Errorf("invalid dataset name %q", root)
	}
	out, err := z.output("list", "-H", "-o", "name", "-r", "-t", "filesystem", root)
	if err != nil {
		if isMissing(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	var ds []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		ds = append(ds, line)
	}
	return ds, nil
}

func (z *ZFS) SetQuota(ds string, bytes int64) error {
	if !naming.ValidDataset(ds) {
		return fmt.Errorf("invalid dataset name %q", ds)
	}
	value := "none"
	if bytes > 0 {
		value = strconv.FormatInt(bytes, 10)
	}
	return z.run(nil, nil, "set", "quota="+value, ds)
}

var _ Backend = (*ZFS)(nil)

// IsZFSAvailable is a cheap runtime probe used by tests and the binary to
// avoid pretending ZFS exists.
func IsZFSAvailable() bool {
	_, err := exec.LookPath("zfs")
	if err != nil {
		return false
	}
	_, err = os.Stat("/dev/zfs")
	return err == nil
}
