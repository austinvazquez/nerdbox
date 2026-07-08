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

package task

import (
	"context"
	"io"

	"github.com/containerd/log"
)

// stdinEOFFunc builds the stdinEOF callback invoked by CloseIO. It delivers the
// CloseIO request context to the copy goroutine on closeCh and blocks until the
// goroutine has sent the in-band EOF (drainDone) or the caller's context is
// done. Binding the drain to the CloseIO context means a peer that requests
// CloseIO without closing the FIFO write end cannot wedge CloseIO forever: the
// drain lasts only as long as the caller is willing to wait.
func stdinEOFFunc(closeCh chan<- context.Context, drainDone <-chan struct{}) func(context.Context) error {
	return func(cctx context.Context) error {
		select {
		case closeCh <- cctx:
		case <-drainDone:
			// Goroutine already exited (pipe/FIFO EOF): EOF already delivered.
			return nil
		case <-cctx.Done():
			return cctx.Err()
		}
		select {
		case <-drainDone:
			return nil
		case <-cctx.Done():
			return cctx.Err()
		}
	}
}

// copyStdinUntilClose reads from f and writes raw bytes to sc until a CloseIO
// context arrives on closeCh (CloseIO) or f delivers EOF. On either exit it
// calls sc.CloseWrite() to send OP_SHUTDOWN(SEND) in-order on the vsock stdin
// stream, guaranteeing the guest sees EOF after all data already written —
// not via an out-of-band RPC that could race in-flight bytes.
//
// Reads always run on a background goroutine feeding readCh, so no read is ever
// performed synchronously in a select. That matters on the CloseIO drain path:
// draining to EOF requires the FIFO write end to be closed, but a peer can
// issue CloseIO without ever closing it. The drain is therefore bound to the
// CloseIO request context delivered on closeCh — if the caller cancels or its
// deadline elapses, the drain stops and still delivers the in-band EOF instead
// of wedging forever. On the conforming path (writer closes on CloseIO) the
// drain reads through to EOF with no data lost.
func copyStdinUntilClose(ctx context.Context, sc interface {
	io.Writer
	CloseWrite() error
}, f io.Reader, buf []byte, closeCh <-chan context.Context) {
	type readResult struct {
		n   int
		err error
	}
	readCh := make(chan readResult, 1)
	// read spawns a single background read into buf. At most one read is ever
	// in flight: the next read is only started after the current result has
	// been consumed and its bytes written, so buf is never shared concurrently.
	read := func() {
		go func() {
			n, err := f.Read(buf)
			readCh <- readResult{n, err}
		}()
	}
	closeWrite := func() {
		if err := sc.CloseWrite(); err != nil {
			log.G(ctx).WithError(err).Warn("error sending stdin EOF via CloseWrite")
		}
	}

	read()
	for {
		select {
		case cctx := <-closeCh:
			// CloseIO fired: drain remaining FIFO data in-band, then send EOF.
			for {
				select {
				case res := <-readCh:
					if res.n > 0 {
						if _, err := sc.Write(buf[:res.n]); err != nil {
							log.G(ctx).WithError(err).Warn("error writing stdin on CloseIO")
							closeWrite()
							return
						}
					}
					if res.err != nil || res.n == 0 {
						// EOF (writer closed) or a read error: fully drained.
						closeWrite()
						return
					}
					read()
				case <-cctx.Done():
					// The CloseIO caller gave up (cancelled or deadline) before
					// the FIFO delivered EOF: stop draining and still send the
					// in-band EOF rather than wedge on a read that may never
					// complete.
					log.G(ctx).WithError(cctx.Err()).Warn("stdin drain aborted; CloseIO context done before FIFO EOF")
					closeWrite()
					return
				}
			}
		case res := <-readCh:
			if res.n > 0 {
				if _, err := sc.Write(buf[:res.n]); err != nil {
					log.G(ctx).WithError(err).Warn("error writing stdin")
					return
				}
			}
			if res.err != nil {
				// Pipe/named-pipe EOF: client closed its write end.
				closeWrite()
				return
			}
			read()
		}
	}
}
