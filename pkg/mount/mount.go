// Package mount contains the small amount of mount plumbing the node plugin
// needs. It is an interface so the driver can be tested without root.
package mount

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Mounter is the node's filesystem surface.
type Mounter interface {
	Mount(source, target, fstype string, options []string) error
	BindMount(source, target string, readOnly bool) error
	Unmount(target string) error
	IsMountPoint(target string) (bool, error)
	MakeDir(path string) error
}

// Linux is the real mounter.
type Linux struct{}

func NewLinux() *Linux { return &Linux{} }

func (l *Linux) MakeDir(path string) error {
	return os.MkdirAll(path, 0o750)
}

func (l *Linux) Mount(source, target, fstype string, options []string) error {
	if err := l.MakeDir(target); err != nil {
		return err
	}
	args := []string{}
	if fstype != "" {
		args = append(args, "-t", fstype)
	}
	if len(options) > 0 {
		args = append(args, "-o", strings.Join(options, ","))
	}
	args = append(args, source, target)
	return run("mount", args...)
}

func (l *Linux) BindMount(source, target string, readOnly bool) error {
	if err := l.MakeDir(target); err != nil {
		return err
	}
	if err := run("mount", "--bind", source, target); err != nil {
		return err
	}
	if readOnly {
		if err := run("mount", "-o", "remount,bind,ro", target); err != nil {
			_ = run("umount", target)
			return err
		}
	}
	return nil
}

func (l *Linux) Unmount(target string) error {
	var err error
	for i := 0; i < 5; i++ {
		err = run("umount", target)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
	}
	// Last resort, same as kubelet: lazy detach.
	if e := run("umount", "-l", target); e == nil {
		return nil
	}
	return err
}

func (l *Linux) IsMountPoint(target string) (bool, error) {
	clean := filepath.Clean(target)
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("read mountinfo: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		if unescape(fields[4]) == clean {
			return true, nil
		}
	}
	return false, nil
}

func unescape(s string) string {
	r := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return r.Replace(s)
}

func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), msg)
	}
	return nil
}

var _ Mounter = (*Linux)(nil)
