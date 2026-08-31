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
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func withTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	orig := Timeout
	Timeout = d
	t.Cleanup(func() { Timeout = orig })
}

func stubExit(t *testing.T) *int32 {
	t.Helper()
	var code int32 = -1
	orig := osExit
	osExit = func(c int) { atomic.StoreInt32(&code, int32(c)) }
	t.Cleanup(func() { osExit = orig })
	return &code
}

// resetExitTimerStarted lets each test exercise armed()'s first-arm-wins
// gate independently of what earlier tests in this binary already did.
func resetExitTimerStarted(t *testing.T) {
	t.Helper()
	orig := exitTimerStarted.Load()
	exitTimerStarted.Store(false)
	t.Cleanup(func() { exitTimerStarted.Store(orig) })
}

func TestArmWithoutListenIsNoop(t *testing.T) {
	// A pid with no Listen goroutine (or no process at all) has no event to
	// open; Arm must not panic, block, or report an error for this expected
	// case.
	if err := Arm(int(^uint32(0) >> 1)); err != nil { // an implausible pid
		t.Fatalf("Arm() error = %v, want nil for a pid that never called Listen", err)
	}
}

func TestListenArmTriggersExitAfterTimeout(t *testing.T) {
	withTimeout(t, 100*time.Millisecond)
	resetExitTimerStarted(t)
	code := stubExit(t)

	if err := Listen(context.Background()); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	if err := Arm(os.Getpid()); err != nil {
		t.Fatalf("Arm() error = %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(code) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("osExit was not called with 1 within the deadline; got %d", atomic.LoadInt32(code))
}

func TestListenWithoutArmDoesNotExit(t *testing.T) {
	withTimeout(t, 50*time.Millisecond)
	code := stubExit(t)

	if err := Listen(context.Background()); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if got := atomic.LoadInt32(code); got != -1 {
		t.Fatalf("osExit called with code %d; want no call", got)
	}
}

func TestArmedTwiceDoesNotRestackExitTimer(t *testing.T) {
	// Each armed() call schedules a fixed-duration timer from its own call
	// time, so even without the fix the *earliest* exit would still land on
	// schedule — the actual defect a restacked second timer causes is an
	// extra, redundant timer (and, in production, a second osExit call
	// racing a process that's already mid-exit). Assert on call count, not
	// timing, to catch that directly.
	withTimeout(t, 50*time.Millisecond)
	resetExitTimerStarted(t)
	ctx := context.Background()

	var calls atomic.Int32
	orig := osExit
	osExit = func(int) { calls.Add(1) }
	t.Cleanup(func() { osExit = orig })

	armed(ctx) // t=0: starts the one-shot 50ms countdown.
	time.Sleep(20 * time.Millisecond)
	armed(ctx) // t=20ms: must not schedule a second timer.

	time.Sleep(200 * time.Millisecond) // past both a 50ms and a 70ms deadline.

	if got := calls.Load(); got != 1 {
		t.Fatalf("osExit called %d times; want exactly 1 (a second armed() call must not restack a second exit timer)", got)
	}
}

func TestWriteGoroutineDump(t *testing.T) {
	path, err := writeGoroutineDump()
	if err != nil {
		t.Fatalf("writeGoroutineDump() error = %v", err)
	}
	defer os.Remove(path)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat dump file: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("dump file is empty")
	}
}
