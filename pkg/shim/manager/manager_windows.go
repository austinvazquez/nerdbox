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
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	winio "github.com/Microsoft/go-winio"
	bootapi "github.com/containerd/containerd/api/runtime/bootstrap/v1"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/log"
	"golang.org/x/sys/windows"

	options "github.com/containerd/nerdbox/api/runtime/options/v1"
	"github.com/containerd/nerdbox/pkg/shim/watchdog"
)

// stopWaitDefault bounds how long Stop waits for the shim process to fully
// exit after TerminateProcess, replacing an unconditional windows.INFINITE
// wait.
//
// This is deliberately NOT derived from watchdog.Timeout. Stop runs inside
// a fresh re-exec of this binary with "-action delete" — a different OS
// process from the long-running shim server manager.Start spawned earlier
// — so it never sees the env vars Start forwarded to that other process,
// including any per-sandbox watchdog.EnvTimeout override (see
// api/runtime/options/v1); there is no channel for that value to reach
// here. Tying this wait to watchdog.Timeout's own default (30s) would also
// just recreate the problem below with a bigger number.
//
// The real constraint this needs to fit inside is containerd's own
// cleanupAfterDeadShim (core/runtime/v2/shim.go), which invokes this
// binary via exec.CommandContext under io.containerd.timeout.shim.cleanup
// — 5s by default — and kills the whole process outright when that
// expires, with no chance for anything in it to log or return an error. A
// plain context.Context cannot cross that process boundary, so ctx here
// never carries that deadline (the ctx.Deadline() clamp below is a no-op
// for this caller; it is kept only for some other caller that does pass a
// bounded ctx directly). stopWaitDefault is sized to fit under that common
// 5s budget, with a safety margin, so this call's own diagnostic log line
// and returned error get a real chance to run instead of racing — and
// losing to — that outer kill.
//
// Known limitation: if a sandbox's WatchdogTimeout override is set higher
// than this, Stop can time out and report an error to containerd well
// before that shim's own watchdog fires — Stop has no way to learn the
// override, so it cannot wait that long itself. The shim's own self-exit
// still happens independently, on its own schedule, regardless of what
// Stop reports back.
const stopWaitDefault = 3 * time.Second

// stopWaitSafetyMargin is subtracted from ctx's remaining deadline, for a
// caller that does pass one, so this call's own diagnostic logging/error
// has a chance to finish before that deadline's own enforcement (if any)
// kills this process outright.
const stopWaitSafetyMargin = 500 * time.Millisecond

// maxStopWait caps the wait passed to WaitForSingleObject regardless of
// ctx: waitTimeout.Milliseconds() is truncated into a uint32, and
// windows.INFINITE is itself 0xFFFFFFFF, so an unbounded or sufficiently
// large value could wrap into an accidental infinite wait — exactly the
// hang this exists to remove. No legitimate deadline needs to exceed this.
const maxStopWait = 24 * time.Hour

func newCommand(ctx context.Context, id, containerdAddress, containerdTTRPCAddress string, debug bool) (*exec.Cmd, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	args := []string{
		"-namespace", ns,
		"-id", id,
		"-address", containerdAddress,
	}
	if debug {
		args = append(args, "-debug")
	}
	cmd := exec.Command(self, args...)
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), "GOMAXPROCS=4")
	cmd.Env = append(cmd.Env, "OTEL_SERVICE_NAME=containerd-shim-"+id)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}
	return cmd, nil
}

// shimPipeAddress generates a named pipe address for the shim based on the
// containerd address, namespace, and grouping ID — mirroring the Unix socket
// address derivation in CreateSocketAddress.
func shimPipeAddress(ctx context.Context, containerdAddress, grouping string) (string, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return "", err
	}
	path := filepath.Join(containerdAddress, ns, grouping)
	d := sha256.Sum256([]byte(path))
	return fmt.Sprintf(`\\.\pipe\containerd-shim-%x`, d[:16]), nil
}

func (manager) Start(ctx context.Context, bparams *bootapi.BootstrapParams) (_ *bootapi.BootstrapResult, retErr error) {
	id := bparams.InstanceID
	debug := bparams.LogLevel <= bootapi.LogLevel_LOG_LEVEL_DEBUG

	cmd, err := newCommand(ctx, id, bparams.ContainerdGrpcAddress, bparams.ContainerdTtrpcAddress, debug)
	if err != nil {
		return nil, err
	}
	grouping := id
	sp, err := readSpec()
	if err != nil {
		// See the identical comment in manager_unix.go's Start: the sandbox
		// bundle has no config.json when containerd's shim sandboxer
		// creates a sandbox via CRI, and grouping-by-annotation is an
		// optional convenience, not something Start should fail over.
		if !os.IsNotExist(err) {
			return nil, err
		}
		sp = &spec{}
	}
	for _, group := range groupLabels {
		if groupID, ok := sp.Annotations[group]; ok {
			grouping = groupID
			break
		}
	}

	// Generate a named pipe address for the shim TTRPC socket.
	address, err := shimPipeAddress(ctx, bparams.ContainerdGrpcAddress, grouping)
	if err != nil {
		return nil, err
	}

	// Pass the pipe address to the child shim process via environment variable.
	// The shim's serveListener reads TTRPC_SOCKET to know where to listen.
	cmd.Env = append(cmd.Env, "TTRPC_SOCKET="+address)

	if opts, err := watchdogOptions(bparams); err != nil {
		log.G(ctx).WithError(err).Warn("failed to read watchdog runtime options; using defaults")
	} else if opts != nil {
		switch {
		case opts.GetDisableWatchdog():
			cmd.Env = append(cmd.Env, watchdog.EnvDisable+"=1")
		case opts.GetWatchdogTimeout().AsDuration() > 0:
			cmd.Env = append(cmd.Env, watchdog.EnvTimeout+"="+opts.GetWatchdogTimeout().AsDuration().String())
		}
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	defer func() {
		if retErr != nil {
			cmd.Process.Kill()
		}
	}()
	// Capture the shim exit error so we can detect an early crash while
	// waiting for the pipe. The channel is buffered so the goroutine never
	// blocks even if we return before reading from it.
	shimExit := make(chan error, 1)
	go func() {
		shimExit <- cmd.Wait()
	}()

	if err = shim.WritePidFile(filepath.Join(bundlePath(ctx), "shim.pid"), cmd.Process.Pid); err != nil {
		return nil, err
	}

	// Wait for the child shim to create the TTRPC named pipe.
	// On Unix, the socket is pre-created via fd passing and exists before
	// the child starts. On Windows, the child creates the pipe after startup,
	// so we must wait for it before returning the address to containerd.
	if err := waitForShimPipe(ctx, address, shimExit,
		shimPipeReadyTimeout,
		shimPipeDialPerAttempt,
		shimPipeRetryDelay,
	); err != nil {
		return nil, err
	}
	return &bootapi.BootstrapResult{
		Version:  3,
		Address:  address,
		Protocol: "ttrpc",
	}, nil
}

const (
	shimPipeReadyTimeout   = 10 * time.Second
	shimPipeDialPerAttempt = 1 * time.Second
	shimPipeRetryDelay     = 10 * time.Millisecond
)

// waitForShimPipe polls a named pipe address with a short per-attempt DialPipe timeout
// until the pipe is reachable, the caller's context is done, the shim signals it has stopped,
// or readyTimeout elapses — whichever comes first.
//
// A short per-attempt timeout prevents a single DialPipe from consuming the
// whole budget when the pipe exists but the shim goroutine has not yet called
// Accept(). Errors that indicate the pipe is not yet ready (not-exist, per-attempt timeout, busy)
// are retried; any other error is fatal.
func waitForShimPipe(ctx context.Context, address string, shimExit <-chan error, readyTimeout, perAttempt, retryDelay time.Duration) error {
	timer := time.NewTimer(readyTimeout)
	defer timer.Stop()

	shimExitErr := func(exitErr error) error {
		// If the shim exited before creating the pipe, report its exit
		// error immediately rather than continuing to poll until timeout.
		if exitErr == nil {
			exitErr = errors.New("exit code 0")
		}
		return fmt.Errorf("shim exited before creating pipe: %w", exitErr)
	}

	// checkCancel does a non-blocking probe of the three cancel cases.
	// Returns a non-nil error if a cancel/exit/timeout case is pending,
	// nil otherwise. Running this at the top of each iteration gives the
	// cancel cases precedence over the DialPipe attempt that follows —
	// Go's select picks randomly among ready cases, so without this guard
	// a just-fired cancel could lose to a backoff timer that fired in the
	// same tick.
	checkCancel := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exitErr := <-shimExit:
			return shimExitErr(exitErr)
		case <-timer.C:
			return fmt.Errorf("timed out waiting for shim pipe %s", address)
		default:
			return nil
		}
	}

	// sleepCancel waits up to backoff, returning early with a cancel error
	// if any cancel case fires during the wait.
	sleepCancel := func(backoff time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exitErr := <-shimExit:
			return shimExitErr(exitErr)
		case <-timer.C:
			return fmt.Errorf("timed out waiting for shim pipe %s", address)
		case <-time.After(backoff):
			return nil
		}
	}

	for {
		if err := checkCancel(); err != nil {
			return err
		}

		dialTimeout := perAttempt
		conn, err := winio.DialPipe(address, &dialTimeout)
		if err == nil {
			conn.Close()
			return nil
		}

		// ERROR_PIPE_BUSY is handled internally by go-winio's tryDialPipe
		// loop and surfaces as winio.ErrTimeout once the per-attempt timeout
		// deadline fires; the explicit ERROR_PIPE_BUSY branch is a guard.
		retryable := os.IsNotExist(err) ||
			errors.Is(err, winio.ErrTimeout) ||
			errors.Is(err, windows.ERROR_PIPE_BUSY)
		if !retryable {
			return fmt.Errorf("waiting for shim pipe %s: %w", address, err)
		}

		log.G(ctx).WithError(err).Debug("shim pipe not ready; backing off before retry")

		// Backoff + jitter (up to 100% of base delay)
		backoff := retryDelay + time.Duration(rand.Int64N(int64(retryDelay)))
		if err := sleepCancel(backoff); err != nil {
			return err
		}
	}
}

// watchdogOptions extracts *options.Options from bparams.Extensions, set by
// containerd from the sandbox/container's configured runtime options (see
// api/runtime/options/v1). Returns (nil, nil) if the caller never set one —
// an absent extension is the common case, not an error.
func watchdogOptions(bparams *bootapi.BootstrapParams) (*options.Options, error) {
	var opts options.Options
	found, err := bparams.FindExtension(&opts)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &opts, nil
}

// bundlePath extracts the bundle path from the context. The shim framework
// stores it as shim.Opts{BundlePath: ...} via the -bundle flag.
func bundlePath(ctx context.Context) string {
	if o, ok := ctx.Value(shim.OptsKey{}).(shim.Opts); ok {
		return o.BundlePath
	}
	return ""
}

// removeRootfs removes the rootfs directory from the bundle so that
// containerd's bundle cleanup doesn't attempt a bind filter unmount.
// On Windows, Unmount calls bindfilter.RemoveFileBinding which fails with
// ERROR_ACCESS_DENIED on directories that were never bind filter mounts
// (nerdbox uses VM-based virtio block devices instead). Removing the
// directory makes UnmountAll a no-op.
func removeRootfs(ctx context.Context) {
	bp := bundlePath(ctx)
	if bp == "" {
		return
	}
	if err := os.RemoveAll(filepath.Join(bp, "rootfs")); err != nil {
		log.G(ctx).WithError(err).WithField("component", "shim-manager").Warn("failed to remove bundle rootfs")
	}
}

func (manager) Stop(ctx context.Context, id string) (shim.StopStatus, error) {
	// Must run once we're confident the shim is actually gone, to ensure
	// containerd's bundle cleanup is successful — see [removeRootfs]. Not
	// unconditional: on the timeout/wait-failure paths below, the process
	// may still be alive and holding open handles under the bundle, so
	// running removal concurrently with it would race file deletion
	// against a process that's still using those files. confirmedGone
	// defaults to true because every *other* return path in this function
	// (pid file already gone, OpenProcess says the pid is stale, or the
	// wait below actually observes the exit) has already confirmed that.
	confirmedGone := true
	defer func() {
		if confirmedGone {
			removeRootfs(ctx)
		}
	}()

	p, err := os.ReadFile(filepath.Join(bundlePath(ctx), "shim.pid"))
	if err != nil {
		if os.IsNotExist(err) {
			// The shim already exited and cleaned up its pid file.
			return shim.StopStatus{
				ExitedAt:   time.Now(),
				ExitStatus: 128 + 9,
			}, nil
		}
		return shim.StopStatus{}, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(p)))
	if err != nil {
		return shim.StopStatus{}, err
	}

	// Open the shim process with the rights needed to terminate it, wait for
	// it to exit, and read its exit code. If OpenProcess fails with
	// ERROR_INVALID_PARAMETER the PID is no longer in the process table —
	// the shim has already exited.
	h, err := windows.OpenProcess(
		windows.PROCESS_TERMINATE|windows.SYNCHRONIZE,
		false,
		uint32(pid),
	)
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
			// Process already gone.
			return shim.StopStatus{
				ExitedAt:   time.Now(),
				ExitStatus: 128 + 9,
				Pid:        pid,
			}, nil
		}
		return shim.StopStatus{}, fmt.Errorf("open shim process: %w", err)
	}
	defer windows.CloseHandle(h)

	// Best-effort: tell pid's watchdog.Listen goroutine (if any) that it is
	// about to be killed, so it can capture a goroutine dump and, if
	// TerminateProcess/the OS's own teardown doesn't finish it off first,
	// self-terminate. A no-op if the shim never called Listen. See
	// [watchdog] for why this exists — a thread wedged deep in a
	// hypervisor call can leave this same class of hang with no way to
	// diagnose it from the outside, since TerminateProcess never runs any
	// code in the target process. Arm's own error (as opposed to "no such
	// event", which is the expected case whenever pid never called Listen)
	// is worth logging, but not worth failing Stop over.
	if err := watchdog.Arm(pid); err != nil {
		log.G(ctx).WithError(err).WithField("pid", pid).Warn("failed to arm shim watchdog")
	}

	// Terminate the shim. ERROR_ACCESS_DENIED is returned when the process
	// has already exited but the handle is still open; WaitForSingleObject
	// below will return immediately in that case.
	if err := windows.TerminateProcess(h, uint32(128+9)); err != nil && !errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return shim.StopStatus{}, fmt.Errorf("terminate shim process: %w", err)
	}

	// Block until the process has fully exited, bounded rather than
	// INFINITE: TerminateProcess is unconditional, but a thread executing
	// in kernel/hypervisor code at the moment of termination can make the
	// OS's own teardown of that thread — and therefore this wait — take far
	// longer than expected. Time out and report it rather than hang the
	// caller forever; the process is still marked for termination
	// regardless, so a timeout here does not leave it running (though we
	// can no longer be sure it has actually exited — see confirmedGone
	// above).
	//
	// See stopWaitDefault's doc for why this is a small fixed value rather
	// than derived from watchdog.Timeout, and for the ctx.Deadline() clamp
	// below being a no-op for Stop's usual caller.
	waitTimeout := min(stopWaitDefault, maxStopWait)
	if dl, ok := ctx.Deadline(); ok {
		if remaining := time.Until(dl) - stopWaitSafetyMargin; remaining < waitTimeout {
			waitTimeout = max(remaining, 0)
		}
	}
	if status, err := windows.WaitForSingleObject(h, uint32(waitTimeout.Milliseconds())); err != nil {
		confirmedGone = false
		return shim.StopStatus{}, fmt.Errorf("wait for shim process: %w", err)
	} else if status == uint32(windows.WAIT_TIMEOUT) {
		confirmedGone = false
		log.G(ctx).WithFields(log.Fields{
			"component":   "shim-manager",
			"pid":         pid,
			"reason":      "stop_wait_timeout",
			"duration_ms": waitTimeout.Milliseconds(),
		}).Warn("shim process did not exit within timeout after termination")
		return shim.StopStatus{}, fmt.Errorf("shim process %d did not exit within %s after termination", pid, waitTimeout)
	}

	return shim.StopStatus{
		ExitedAt:   time.Now(),
		ExitStatus: 128 + 9,
		Pid:        pid,
	}, nil
}
