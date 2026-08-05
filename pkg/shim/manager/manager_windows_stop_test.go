//go:build windows

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package manager

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/pkg/shim"
)

// bundleCtx returns a context carrying bundle path dir, the way the shim
// framework supplies it from the -bundle flag.
func bundleCtx(dir string) context.Context {
	return context.WithValue(context.Background(), shim.OptsKey{}, shim.Opts{BundlePath: dir})
}

// seedBundle populates dir with the artifacts the shim writes into a bundle,
// plus a file that must survive cleanup. It returns the paths that must be
// removed and the paths that must remain.
func seedBundle(t *testing.T, dir string) (removed, kept []string) {
	t.Helper()

	rootfs := filepath.Join(dir, "rootfs")
	if err := os.MkdirAll(filepath.Join(rootfs, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir rootfs: %v", err)
	}
	removed = append(removed, rootfs)

	for _, name := range []string{
		"merged_fs_gpt.vmdk",
		"merged_fs_gpt_header.bin",
		"merged_fs_gpt_pad.bin",
		"merged_fs_b.vmdk",
	} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		removed = append(removed, p)
	}

	// containerd owns these; cleanup must not touch them.
	for _, name := range []string{"config.json", "shim.pid", "bootstrap.json"} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("1"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		kept = append(kept, p)
	}

	return removed, kept
}

func assertBundleCleaned(t *testing.T, removed, kept []string) {
	t.Helper()
	for _, p := range removed {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("expected %s to be removed, stat err = %v", filepath.Base(p), err)
		}
	}
	for _, p := range kept {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected %s to survive cleanup: %v", filepath.Base(p), err)
		}
	}
}

// TestRemoveBundleArtifacts verifies that cleanup removes the rootfs directory
// and every VMDK descriptor and auxiliary blob the shim wrote, and nothing else.
// A single leftover descriptor is enough to make containerd's bundle removal
// fail and wedge the container id, so the coverage here is deliberately exact.
func TestRemoveBundleArtifacts(t *testing.T) {
	dir := t.TempDir()
	removed, kept := seedBundle(t, dir)

	removeBundleArtifacts(bundleCtx(dir))

	assertBundleCleaned(t, removed, kept)
}

func TestRemoveBundleArtifactsNoBundlePath(t *testing.T) {
	// Must not panic or touch the working directory when the context carries
	// no bundle path.
	removeBundleArtifacts(context.Background())
}

// TestRemoveBundleArtifactsBudgetIsGlobal guards the sum that matters: cleanup
// must stay inside bundleRemoveWindow no matter how many artifacts are locked,
// because Stop's total (shim wait + cleanup) has to fit inside containerd's 5s
// cleanup timeout. Retrying per-target instead would multiply by target count.
func TestRemoveBundleArtifactsBudgetIsGlobal(t *testing.T) {
	dir := t.TempDir()

	// Many artifacts, all of them undeletable: a directory that is not empty
	// and whose child is held open cannot be removed on Windows.
	var held []*os.File
	for _, name := range []string{
		"merged_fs_gpt.vmdk", "merged_fs_gpt_header.bin", "merged_fs_gpt_pad.bin",
		"merged_fs_a.vmdk", "merged_fs_b.vmdk", "merged_fs_c.vmdk",
	} {
		p := filepath.Join(dir, name)
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		held = append(held, f)
	}
	t.Cleanup(func() {
		for _, f := range held {
			f.Close()
		}
	})

	start := time.Now()
	removeBundleArtifacts(bundleCtx(dir))
	elapsed := time.Since(start)

	// Generous headroom for slow CI, but far below targets × window.
	if limit := bundleRemoveWindow + 2*time.Second; elapsed > limit {
		t.Errorf("cleanup took %s, beyond the %s bound", elapsed, limit)
	}
}

// TestStopCleansBundleWhenShimAlreadyGone covers the common path: the shim
// exited and removed its own pid file, so Stop has nothing to terminate but
// must still clear the bundle artifacts.
func TestStopCleansBundleWhenShimAlreadyGone(t *testing.T) {
	dir := t.TempDir()
	removed, kept := seedBundle(t, dir)

	// Drop the pid file so Stop takes the "already exited" branch.
	pidFile := filepath.Join(dir, "shim.pid")
	if err := os.Remove(pidFile); err != nil {
		t.Fatalf("remove shim.pid: %v", err)
	}
	kept = filterOut(kept, pidFile)

	status, err := manager{}.Stop(bundleCtx(dir), "test-id")
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if status.ExitStatus != 128+9 {
		t.Errorf("ExitStatus = %d, want %d", status.ExitStatus, 128+9)
	}

	assertBundleCleaned(t, removed, kept)
}

// TestStopCleansBundleWhenPidIsStale covers a stale pid file pointing at a pid
// that is no longer in the process table: Stop must still succeed and clean up
// rather than erroring out and leaving the bundle behind.
func TestStopCleansBundleWhenPidIsStale(t *testing.T) {
	dir := t.TempDir()
	removed, kept := seedBundle(t, dir)

	// Start and reap a process so its pid is almost certainly gone.
	cmd := exec.Command("cmd.exe", "/c", "exit", "0")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start helper process: %v", err)
	}
	pid := cmd.Process.Pid
	_, _ = cmd.Process.Wait()

	if err := os.WriteFile(filepath.Join(dir, "shim.pid"),
		[]byte(strconv.Itoa(pid)), 0o644); err != nil {
		t.Fatalf("write shim.pid: %v", err)
	}

	if _, err := (manager{}).Stop(bundleCtx(dir), "test-id"); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	assertBundleCleaned(t, removed, kept)
}

// TestStopTerminatesLiveShimAndCleansBundle verifies the full path: a live
// process is terminated, waited for, and the bundle is cleaned.
func TestStopTerminatesLiveShimAndCleansBundle(t *testing.T) {
	dir := t.TempDir()
	removed, kept := seedBundle(t, dir)

	cmd := exec.Command("ping", "-n", "60", "127.0.0.1")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start helper process: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	if err := os.WriteFile(filepath.Join(dir, "shim.pid"),
		[]byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
		t.Fatalf("write shim.pid: %v", err)
	}

	status, err := manager{}.Stop(bundleCtx(dir), "test-id")
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if status.Pid != cmd.Process.Pid {
		t.Errorf("Pid = %d, want %d", status.Pid, cmd.Process.Pid)
	}

	assertBundleCleaned(t, removed, kept)
}

func filterOut(paths []string, drop string) []string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if p != drop {
			out = append(out, p)
		}
	}
	return out
}
