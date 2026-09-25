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
	"os"
	"time"
)

// Timeout is how long the watchdog waits, once armed, before concluding the
// process is unrecoverably wedged and terminating it. A consumer may
// override this before calling [Listen].
var Timeout = 30 * time.Second

const (
	// EnvDisable, if set to any non-empty value, tells the shim server not
	// to call Listen at all. Set by the shim manager's Start from the
	// sandbox's runtime options (see api/runtime/options/v1.Options); read
	// by ConfigureFromEnv, which the shim server's main should call before
	// deciding whether to call Listen.
	EnvDisable = "NERDBOX_WATCHDOG_DISABLE"

	// EnvTimeout, if set to a value accepted by time.ParseDuration,
	// overrides Timeout. Same producer/consumer as EnvDisable. Ignored if
	// EnvDisable is also set.
	EnvTimeout = "NERDBOX_WATCHDOG_TIMEOUT"
)

// ConfigureFromEnv applies [EnvTimeout] to [Timeout] and reports whether
// [EnvDisable] says the watchdog should be skipped entirely. Call once,
// before [Listen]; an invalid timeout value is ignored falling back to
// the default configuration rather than treated as fatal, since a misconfigured
// knob should not be why the shim fails to start.
func ConfigureFromEnv() (disabled bool) {
	if os.Getenv(EnvDisable) != "" {
		return true
	}
	if v := os.Getenv(EnvTimeout); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			Timeout = d
		}
	}
	return false
}
