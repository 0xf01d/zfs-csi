package mount

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsMountPoint(t *testing.T) {
	m := NewLinux()
	if ok, err := m.IsMountPoint("/"); err != nil || !ok {
		t.Fatalf("root mountpoint: %v %v", ok, err)
	}
	dir := t.TempDir()
	sub := filepath.Join(dir, "not-mounted")
	if err := m.MakeDir(sub); err != nil {
		t.Fatal(err)
	}
	if ok, err := m.IsMountPoint(sub); err != nil || ok {
		t.Fatalf("temp dir reported as mountpoint: %v %v", ok, err)
	}
	// Ensure the parser handles the directory existing but not mounted.
	if _, err := os.Stat(sub); err != nil {
		t.Fatal(err)
	}
}
