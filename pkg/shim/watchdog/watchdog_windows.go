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

package watchdog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/containerd/log"
	"golang.org/x/sys/windows"
)

// eventName returns the session-local Win32 event name for pid's watchdog.
//
// Local, not Global: the shim server ([Listen], the creator) and the
// delete-subprocess ([Arm], the opener) are always spawned by the same
// containerd process tree and so always share one Windows session -
// there is no cross-session need here.
func eventName(pid int) string {
	return fmt.Sprintf(`Local\nerdbox-shim-watchdog-%d`, pid)
}

// securityAttributes grants access to the event's owner — this accounts
// for both the unprivileged case and plus Administrators and SYSTEM so a
// deployment that does run elevated or as a service still works. OW
// resolves against the security descriptor's owner field, which
// defaults to the creating token's user, so it correctly matches Arm's
// same-user token without requiring either side to be privileged.
func securityAttributes() (*windows.SecurityAttributes, error) {
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;GA;;;OW)(A;;GA;;;BA)(A;;GA;;;SY)")
	if err != nil {
		return nil, err
	}
	// InheritHandle deliberately left at its zero value (not inheritable):
	// nothing needs a child process of the shim to see this handle, and
	// making it inheritable would leak it into every child the shim spawns.
	sa := &windows.SecurityAttributes{SecurityDescriptor: sd}
	sa.Length = uint32(unsafe.Sizeof(*sa))
	return sa, nil
}

// osExit is os.Exit by default; overridden in tests so a simulated arm does
// not kill the test binary.
var osExit = os.Exit

// Listen creates this process's watchdog event and spawns a goroutine that
// waits on it, looping so a repeated [Arm] (e.g. a retried Stop) is handled
// each time. Call once, early in the shim server's main — safe to call in
// the short-lived shim's start/delete roles too, since nothing will ever
// [Arm] those processes' pids and the event is reclaimed when they exit.
//
// See the package doc for why this exists.
func Listen(ctx context.Context) error {
	sa, err := securityAttributes()
	if err != nil {
		return fmt.Errorf("build watchdog event security attributes: %w", err)
	}
	name := eventName(os.Getpid())
	namep, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return fmt.Errorf("encode watchdog event name %s: %w", name, err)
	}
	// CreateEvent returns a valid handle — to the pre-existing object — even
	// when the name is already taken, but the x/sys/windows wrapper still
	// sets err to ERROR_ALREADY_EXISTS in that case to signal "opened, not
	// created". That's the expected outcome of a pid getting reused within
	// one process's lifetime (notably: this package's own tests, which all
	// listen under the real, constant os.Getpid() of the test binary), not
	// a failure — the returned handle is just as usable either way. Only a
	// zero handle is a genuine failure.
	h, err := windows.CreateEvent(sa, 0, 0, namep)
	if h == 0 {
		return fmt.Errorf("create watchdog event %s: %w", name, err)
	}

	go func() {
		defer windows.CloseHandle(h)
		for {
			if _, err := windows.WaitForSingleObject(h, windows.INFINITE); err != nil {
				log.G(ctx).WithError(err).Warn("nerdbox: watchdog wait failed; watchdog disabled")
				return
			}
			armed(ctx)
		}
	}()
	return nil
}

// exitTimerStarted guards against a retried Arm (containerd may retry Stop
// on a slow shim) restacking a second, independent exit timer: without this,
// two arms within one timeout window would each schedule their own
// AfterFunc, and whichever fired first — not necessarily the intended
// deadline — would win. The first countdown is authoritative; later arms
// within the same process lifetime are logged at Debug but otherwise ignored.
var exitTimerStarted atomic.Bool

// armed runs when the watchdog event fires and starts the exit timer:
// if the process is still alive after timeout, output a diagnostic stack
// dump of the Go thread's before exiting.
func armed(ctx context.Context) {
	if !exitTimerStarted.CompareAndSwap(false, true) {
		log.G(ctx).WithField("component", "watchdog").Debug("nerdbox: watchdog already armed; ignoring repeat arm")
		return
	}

	log.G(ctx).WithFields(log.Fields{
		"component":  "watchdog",
		"reason":     "armed_by_manager",
		"timeout_ms": Timeout.Milliseconds(),
	}).Debug("nerdbox: shutdown watchdog armed")

	time.AfterFunc(Timeout, func() {
		entry := log.G(ctx).WithFields(log.Fields{
			"component": "watchdog",
			"reason":    "watchdog_timeout_exceeded",
		})
		if path, err := writeGoroutineDump(); err != nil {
			entry = entry.WithError(err)
		} else {
			entry = entry.WithField("dump_path", path)
		}
		entry.Error("nerdbox: process still alive after watchdog timeout; terminating")
		osExit(1)
	})
}

// Arm signals pid's watchdog event, telling that process's [Listen] goroutine
// to start its exit timer. Returns nil both when the signal was sent and
// when pid never called Listen (no such event exists yet — the expected
// case for any Stop caller that isn't a nerdbox shim, so it is not treated
// as an error). Any other failure (for example an ACL/token mismatch) is
// returned so the caller can decide whether it is worth logging;
// TerminateProcess remains the primary teardown mechanism regardless of
// whether Arm succeeds.
func Arm(pid int) error {
	namep, err := windows.UTF16PtrFromString(eventName(pid))
	if err != nil {
		return fmt.Errorf("encode watchdog event name: %w", err)
	}
	h, err := windows.OpenEvent(windows.EVENT_MODIFY_STATE, false, namep)
	if err != nil {
		if errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
			return nil
		}
		return fmt.Errorf("open watchdog event: %w", err)
	}
	defer windows.CloseHandle(h)
	if err := windows.SetEvent(h); err != nil {
		return fmt.Errorf("set watchdog event: %w", err)
	}
	return nil
}

// writeGoroutineDump captures every goroutine's stack and writes it to a
// process-owned temp file (mode 0600, os.CreateTemp's default), returning
// its path for the caller to log.
func writeGoroutineDump() (string, error) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	f, err := os.CreateTemp("", "nerdbox-watchdog-*.stacks.log")
	if err != nil {
		return "", fmt.Errorf("create goroutine dump file: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(buf); err != nil {
		return "", fmt.Errorf("write goroutine dump: %w", err)
	}
	return f.Name(), nil
}
