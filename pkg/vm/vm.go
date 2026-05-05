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

// Package vm defines the hypervisor-agnostic interface used by nerdbox to
// manage the lifecycle of a microVM and its guest agent (vminitd).
//
// A [Manager] is the entry point for creating VM instances. Each [Instance]
// represents one running VM and exposes:
//
//   - configuration methods (SetCPUAndMemory, AddFS, AddDisk, AddNIC) that
//     must be called before [Instance.Start];
//   - lifecycle methods ([Instance.Start] and [Instance.Shutdown]);
//   - communication channels ([Instance.Client] for the TTRPC control plane
//     and [Instance.StartStream] for raw byte streams) that are valid only
//     after a successful Start.
package vm

import (
	"context"
	"io"
	"net"

	"github.com/containerd/ttrpc"
)

// NetworkMode selects the host-side transport that backs a virtio-net
// interface inside the guest. The choice determines the framing semantics
// observed by both the host networking helper (for example passt or
// gvproxy) and the guest kernel.
type NetworkMode int

const (
	// NetworkModeUnixgram backs the NIC with an AF_UNIX SOCK_DGRAM socket.
	// This is the mode used by passt-style helpers that exchange complete
	// L2 frames as datagrams.
	NetworkModeUnixgram NetworkMode = iota

	// NetworkModeUnixstream backs the NIC with an AF_UNIX SOCK_STREAM
	// socket. This is the mode used by gvproxy and vfkit-compatible
	// helpers that frame L2 packets over a stream connection.
	NetworkModeUnixstream
)

// Manager creates [Instance] objects. A Manager is typically obtained from a
// containerd plugin and is safe to share across goroutines.
type Manager interface {
	// NewInstance constructs a new VM instance whose runtime state
	// (sockets, FIFOs, console pipes, etc.) is rooted at the directory
	// given by state. The state directory must already exist and be
	// writable. The returned instance is not yet running; configure it via
	// the SetCPUAndMemory / AddFS / AddDisk / AddNIC methods and then call
	// [Instance.Start].
	NewInstance(ctx context.Context, state string) (Instance, error)
}

// StartOpts is the resolved configuration consumed by [Instance.Start]. It
// is normally constructed indirectly by passing [StartOpt] values to Start.
type StartOpts struct {
	// InitArgs are appended to the command line of the guest init process
	// (vminitd). They are interpreted by vminitd, not by the kernel.
	InitArgs []string

	// ConsoleWriter, if non-nil, receives a copy of the guest serial
	// console output in addition to the implementation's default sink
	// (typically os.Stderr). Useful for capturing boot logs in tests.
	ConsoleWriter io.Writer
}

// StartOpt mutates a [StartOpts] value. Options are applied in order.
type StartOpt func(*StartOpts)

// WithInitArgs appends args to [StartOpts.InitArgs]. It can be called
// multiple times; each call adds to the existing slice.
func WithInitArgs(args ...string) StartOpt {
	return func(o *StartOpts) {
		o.InitArgs = append(o.InitArgs, args...)
	}
}

// WithConsoleWriter sets [StartOpts.ConsoleWriter] so that guest console
// output is duplicated to w. Passing nil disables the extra sink.
func WithConsoleWriter(w io.Writer) StartOpt {
	return func(o *StartOpts) {
		o.ConsoleWriter = w
	}
}

// MountConfig is the resolved configuration for a filesystem or block
// device attachment, produced by applying [MountOpt] values.
type MountConfig struct {
	// Readonly mounts the filesystem or disk read-only inside the guest.
	Readonly bool

	// Vmdk indicates that the backing file is a VMDK image rather than a
	// raw block device. Only meaningful for [Instance.AddDisk].
	Vmdk bool
}

// MountOpt mutates a [MountConfig] value. Options are applied in order.
type MountOpt func(*MountConfig)

// Instance represents a single VM. Methods on Instance are safe for
// concurrent use, but the lifecycle is strict:
//
//   - Configuration methods (SetCPUAndMemory, AddFS, AddDisk, AddNIC) must
//     be called before [Instance.Start]; calling them after Start returns
//     an error.
//   - [Instance.Client] and [Instance.StartStream] return useful values only
//     after Start has succeeded.
//   - [Instance.Shutdown] tears down the VM and releases resources; the
//     instance is not reusable after Shutdown.
type Instance interface {
	// SetCPUAndMemory configures the number of vCPUs and RAM (in MiB)
	// that will be exposed to the guest when the VM starts. It must be
	// called before [Instance.Start].
	SetCPUAndMemory(ctx context.Context, cpu uint8, ram uint32) error

	// AddFS attaches a host directory mountPath to the guest as a
	// virtio-fs share identified by tag. The guest mounts the share by
	// referring to tag. Must be called before [Instance.Start].
	AddFS(ctx context.Context, tag, mountPath string, opts ...MountOpt) error

	// AddDisk attaches a host file at mountPath to the guest as a virtio
	// block device identified by blockID. Use [WithReadOnly] for
	// read-only attachment and [WithVmdk] when the file is a VMDK image.
	// Must be called before [Instance.Start].
	AddDisk(ctx context.Context, blockID, mountPath string, opts ...MountOpt) error

	// AddNIC attaches a virtio-net interface to the guest. endpoint is
	// the path to the host-side AF_UNIX socket that bridges packets to
	// the network helper, mac is the link-layer address presented to the
	// guest, mode selects the socket framing (see [NetworkMode]), and
	// features and flags are libkrun-specific tuning bits. Must be called
	// before [Instance.Start].
	AddNIC(ctx context.Context, endpoint string, mac net.HardwareAddr, mode NetworkMode, features, flags uint32) error

	// Start boots the VM and waits for the guest agent (vminitd) to
	// connect back over the TTRPC control channel. Configuration methods
	// must not be called after Start. Returns an error if the VM exits or
	// the guest fails to connect within an implementation-defined
	// timeout.
	Start(ctx context.Context, opts ...StartOpt) error

	// Client returns the TTRPC client connected to the guest agent. The
	// returned client is valid only after a successful [Instance.Start]
	// and until [Instance.Shutdown] is called. The client is shared and
	// must not be closed by callers.
	Client() *ttrpc.Client

	// Shutdown stops the VM, closes the TTRPC connection, and releases
	// associated host resources. It is safe to call Shutdown on a VM that
	// failed to start. The instance is not reusable after Shutdown
	// returns.
	Shutdown(context.Context) error

	// StartStream opens a raw bidirectional byte stream into the VM,
	// identified by streamID. The guest side claims the stream by
	// presenting the same ID; see docs/vsock-streaming.md for the
	// coordination protocol. The returned net.Conn is owned by the
	// caller, who must Close it when done.
	//
	// If the underlying transport is not yet available (for example, the
	// VM is still booting), StartStream retries for an
	// implementation-defined window before returning an error wrapping
	// errdefs.ErrUnavailable.
	//
	// TODO: Consider making this interface optional, a per RPC implementation
	// is possible but likely less efficient.
	StartStream(ctx context.Context, streamID string) (net.Conn, error)
}

// WithReadOnly sets [MountConfig.Readonly] so the filesystem or disk is
// mounted read-only inside the guest.
func WithReadOnly() MountOpt {
	return func(o *MountConfig) {
		o.Readonly = true
	}
}

// WithVmdk sets [MountConfig.Vmdk] so [Instance.AddDisk] interprets the
// backing file as a VMDK image. It has no effect on filesystem mounts.
func WithVmdk() MountOpt {
	return func(o *MountConfig) {
		o.Vmdk = true
	}
}
