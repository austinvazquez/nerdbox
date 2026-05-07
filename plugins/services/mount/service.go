//go:build linux

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

package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"

	ctrMount "github.com/containerd/containerd/v2/core/mount"
	cplugins "github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/containerd/errdefs/pkg/errgrpc"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/plugin/registry"
	"github.com/containerd/ttrpc"

	api "github.com/containerd/nerdbox/api/services/mount/v1"
)

func init() {
	registry.Register(&plugin.Registration{
		Type: cplugins.TTRPCPlugin,
		ID:   "mount",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			return &service{}, nil
		},
	})
}

type service struct {
	mu     sync.Mutex
	mounts []*api.MountSpec // in-VM mounts, in mount order
}

func (s *service) RegisterTTRPC(server *ttrpc.Server) error {
	api.RegisterTTRPCMountService(server, s)
	return nil
}

func (s *service) MountAll(ctx context.Context, r *api.MountAllRequest) (*api.MountAllResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, m := range r.Mounts {
		log.G(ctx).WithFields(log.Fields{
			"type":    m.Type,
			"source":  m.Source,
			"target":  m.Target,
			"options": m.Options,
		}).Info("mounting filesystem")

		i := slices.IndexFunc(s.mounts, func(e *api.MountSpec) bool { return e.Target == m.Target })
		if i >= 0 {
			if mountSpecsEqual(s.mounts[i], m) {
				log.G(ctx).WithField("target", m.Target).Debug("mount already exists with matching spec; skipping")
				continue
			}
			return nil, errgrpc.ToGRPC(fmt.Errorf("target %s already mounted with a different spec: %w", m.Target, errdefs.ErrAlreadyExists))
		}

		if err := os.MkdirAll(m.Target, 0700); err != nil {
			return nil, errgrpc.ToGRPC(fmt.Errorf("failed to create mount target directory %s: %w", m.Target, err))
		}

		if err := ctrMount.All([]ctrMount.Mount{{
			Type:    m.Type,
			Source:  m.Source,
			Target:  m.Target,
			Options: m.Options,
		}}, "/"); err != nil {
			return nil, errgrpc.ToGRPC(fmt.Errorf("failed to mount %s at %s: %w", m.Source, m.Target, err))
		}

		s.mounts = append(s.mounts, m)
	}
	return &api.MountAllResponse{}, nil
}

func (s *service) Unmount(ctx context.Context, r *api.UnmountRequest) (*api.UnmountResponse, error) {
	log.G(ctx).WithField("target", r.Target).Info("unmounting filesystem")

	s.mu.Lock()
	defer s.mu.Unlock()

	i := slices.IndexFunc(s.mounts, func(m *api.MountSpec) bool { return m.Target == r.Target })
	if i < 0 {
		return nil, errgrpc.ToGRPC(fmt.Errorf("cannot unmount %s: %w", r.Target, errdefs.ErrNotFound))
	}
	if err := ctrMount.Unmount(r.Target, 0); err != nil {
		return nil, errgrpc.ToGRPC(fmt.Errorf("failed to unmount %s: %w", r.Target, err))
	}
	s.mounts = slices.Delete(s.mounts, i, i+1)

	return &api.UnmountResponse{}, nil
}

func (s *service) UnmountAll(ctx context.Context, _ *api.UnmountAllRequest) (*api.UnmountAllResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Unmount in reverse order (deepest first).
	var errs []error
	for i := len(s.mounts) - 1; i >= 0; i-- {
		target := s.mounts[i].Target
		log.G(ctx).WithField("target", target).Info("unmounting filesystem")
		if err := ctrMount.Unmount(target, 0); err != nil {
			log.G(ctx).WithError(err).WithField("target", target).Warn("failed to unmount")
			errs = append(errs, fmt.Errorf("unmount %s: %w", target, err))
			continue
		}
		s.mounts = slices.Delete(s.mounts, i, i+1)
	}
	if len(errs) > 0 {
		return nil, errgrpc.ToGRPC(fmt.Errorf("unmount errors: %w", errors.Join(errs...)))
	}
	return &api.UnmountAllResponse{}, nil
}

func mountSpecsEqual(a, b *api.MountSpec) bool {
	return a.Type == b.Type &&
		a.Source == b.Source &&
		a.Target == b.Target &&
		slices.Equal(a.Options, b.Options)
}
