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
	"testing"
	"time"
)

func withEnv(t *testing.T, key, value string) {
	t.Helper()
	t.Setenv(key, value)
}

func withRestoredTimeout(t *testing.T) {
	t.Helper()
	orig := Timeout
	t.Cleanup(func() { Timeout = orig })
}

func TestConfigureFromEnvDefault(t *testing.T) {
	withRestoredTimeout(t)
	Timeout = 30 * time.Second

	if disabled := ConfigureFromEnv(); disabled {
		t.Fatal("ConfigureFromEnv() = disabled, want enabled with neither env var set")
	}
	if Timeout != 30*time.Second {
		t.Fatalf("Timeout = %s, want unchanged 30s", Timeout)
	}
}

func TestConfigureFromEnvDisable(t *testing.T) {
	withRestoredTimeout(t)
	withEnv(t, EnvDisable, "1")

	if disabled := ConfigureFromEnv(); !disabled {
		t.Fatal("ConfigureFromEnv() = enabled, want disabled with EnvDisable set")
	}
}

func TestConfigureFromEnvTimeoutOverride(t *testing.T) {
	withRestoredTimeout(t)
	withEnv(t, EnvTimeout, "45s")

	if disabled := ConfigureFromEnv(); disabled {
		t.Fatal("ConfigureFromEnv() = disabled, want enabled")
	}
	if Timeout != 45*time.Second {
		t.Fatalf("Timeout = %s, want 45s", Timeout)
	}
}

func TestConfigureFromEnvInvalidTimeoutIgnored(t *testing.T) {
	withRestoredTimeout(t)
	Timeout = 30 * time.Second
	withEnv(t, EnvTimeout, "not-a-duration")

	if disabled := ConfigureFromEnv(); disabled {
		t.Fatal("ConfigureFromEnv() = disabled, want enabled")
	}
	if Timeout != 30*time.Second {
		t.Fatalf("Timeout = %s, want unchanged 30s for an invalid override", Timeout)
	}
}

func TestConfigureFromEnvDisableWinsOverTimeout(t *testing.T) {
	withRestoredTimeout(t)
	withEnv(t, EnvDisable, "1")
	withEnv(t, EnvTimeout, "45s")

	if disabled := ConfigureFromEnv(); !disabled {
		t.Fatal("ConfigureFromEnv() = enabled, want disabled")
	}
}
