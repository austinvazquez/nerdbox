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

// Package watchdog lets a nerdbox shim server self-terminate when the
// host-side manager (see [github.com/containerd/nerdbox/pkg/shim/manager])
// has given up waiting on it to exit gracefully.
//
// A VM backend's own shutdown path can block forever on a call with no
// cancellation path back to Go — for example a vCPU thread wedged inside a
// hypervisor call. The shim manager's stop is designed to be the last line of
// defense against that, but a thread stuck deep enough in the kernel/hypervisor
// can make even that wait take far longer than expected, and today it has no
// timeout at all. [Listen], called once by the shim server at startup, gives
// the process a second independent path to notice it has been told to die
// and exit on its own - with a goroutine dump captured first for postmortem -
// rather than depending solely on the external kill completing promptly. The
// trade-off is when a shim is killed by its watchdog; none of its cleanup will
// run on exit. Any leftover resources will have their reference count decremented
// and should be cleaned up by containerd on the next invocation.
//
// This is Windows-only. Unix shim processes already receive SIGKILL, which
// the kernel enforces without the target's cooperation, so there is no
// equivalent gap to fill there.
package watchdog
