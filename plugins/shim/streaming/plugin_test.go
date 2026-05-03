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

package streaming

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	streamapi "github.com/containerd/containerd/api/services/streaming/v1"
	"github.com/containerd/errdefs"
	"github.com/containerd/ttrpc"
	"github.com/containerd/typeurl/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/containerd/nerdbox/internal/shim/sandbox"
)

// fakeStreamServer implements streamapi.TTRPCStreaming_StreamServer in
// memory so the service can be invoked without spinning up a ttrpc
// server. The test feeds inbound messages via recvCh and reads outbound
// ones via sendCh.
type fakeStreamServer struct {
	ctx    context.Context
	recvCh chan *anypb.Any
	sendCh chan *anypb.Any

	mu     sync.Mutex
	closed bool
}

func newFakeStreamServer(ctx context.Context) *fakeStreamServer {
	return &fakeStreamServer{
		ctx:    ctx,
		recvCh: make(chan *anypb.Any, 16),
		sendCh: make(chan *anypb.Any, 16),
	}
}

// closeRecv signals end-of-stream to the handler's Recv loop, mirroring
// a client that has called CloseSend.
func (f *fakeStreamServer) closeRecv() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	f.closed = true
	close(f.recvCh)
}

func (f *fakeStreamServer) Send(m *anypb.Any) error {
	select {
	case f.sendCh <- m:
		return nil
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}

func (f *fakeStreamServer) Recv() (*anypb.Any, error) {
	select {
	case a, ok := <-f.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return a, nil
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeStreamServer) SendMsg(m interface{}) error {
	return f.Send(m.(*anypb.Any))
}

func (f *fakeStreamServer) RecvMsg(m interface{}) error {
	a, err := f.Recv()
	if err != nil {
		return err
	}
	dst, ok := m.(proto.Message)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	proto.Reset(dst)
	proto.Merge(dst, a)
	return nil
}

// fakeSandbox is a minimal sandbox.Sandbox that hands out a pre-supplied
// net.Conn from StartStream. The other methods are not exercised by the
// Stream handler.
type fakeSandbox struct {
	conn net.Conn
}

func (s *fakeSandbox) Start(context.Context, ...sandbox.Opt) error { return errdefs.ErrNotImplemented }
func (s *fakeSandbox) Stop(context.Context) error                  { return errdefs.ErrNotImplemented }
func (s *fakeSandbox) Client() (*ttrpc.Client, error)              { return nil, errdefs.ErrNotImplemented }
func (s *fakeSandbox) StartStream(context.Context, string) (net.Conn, error) {
	return s.conn, nil
}

// streamInitAny marshals StreamInit{ID: id} as an *anypb.Any so it can
// be fed to the handler through the fake server's Recv channel.
func streamInitAny(t *testing.T, id string) *anypb.Any {
	t.Helper()
	a, err := typeurl.MarshalAnyToProto(&streamapi.StreamInit{ID: id})
	if err != nil {
		t.Fatalf("marshal StreamInit: %v", err)
	}
	return a
}

// startStream wires up a service+fake harness, kicks off the Stream
// handler in a goroutine, drains the post-init ack, and returns the
// pieces a test needs to drive the bridge.
func startStream(t *testing.T, ctx context.Context, id string) (srv *fakeStreamServer, vmSide net.Conn, done <-chan error) {
	t.Helper()

	shimSide, vm := net.Pipe()
	t.Cleanup(func() {
		shimSide.Close()
		vm.Close()
	})

	srv = newFakeStreamServer(ctx)
	srv.recvCh <- streamInitAny(t, id)

	svc := &service{sb: &fakeSandbox{conn: shimSide}}

	d := make(chan error, 1)
	go func() { d <- svc.Stream(ctx, srv) }()

	// Drain the ack the service sends right after StreamInit.
	select {
	case <-srv.sendCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream init ack")
	}
	return srv, vm, d
}

// TestStreamReturnsAfterVMEOFWithoutClientClose reproduces the deadlock
// fixed by the surrounding change. In a unidirectional VM->client
// transfer the client never issues CloseSend, so bridgeTTRPCToVM stays
// blocked in srv.Recv() forever. When the VM signals end-of-stream with
// a zero-length frame the handler must still return promptly so ttrpc
// closes the server stream and the client unblocks; without the fix the
// handler waits for both bridge directions and hangs indefinitely.
func TestStreamReturnsAfterVMEOFWithoutClientClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, vmSide, done := startStream(t, ctx, "stream-eof")

	// VM finishes work without sending any data and signals EOF with a
	// zero-length frame. The fake server is intentionally left with
	// nothing more to deliver via Recv, so bridgeTTRPCToVM remains
	// blocked just like a real handler waiting on a quiet client.
	if err := binary.Write(vmSide, binary.BigEndian, uint32(0)); err != nil {
		t.Fatalf("write VM EOF marker: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stream returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream handler did not return after VM EOF; the client->server bridge is still blocked in srv.Recv() and the handler is waiting for both directions to finish")
	}
}

// TestStreamReturnsWhenVMEOFAfterClientClose covers the case where the
// client sends CloseSend first and the VM finishes shortly after. The
// handler must still wait for the VM->client direction to drain before
// returning so no in-flight server replies are lost.
func TestStreamReturnsWhenVMEOFAfterClientClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, vmSide, done := startStream(t, ctx, "stream-client-close")

	// Client closes its send side. bridgeTTRPCToVM observes io.EOF and
	// writes the zero-length frame to the VM.
	srv.closeRecv()

	// Drain the EOF marker that bridgeTTRPCToVM forwards to the VM so
	// the pipe write does not block.
	go func() {
		var n uint32
		_ = binary.Read(vmSide, binary.BigEndian, &n)
	}()

	// Handler must NOT have returned yet — the VM->client direction is
	// still open. Give it a brief moment to settle and confirm it is
	// still running.
	select {
	case err := <-done:
		t.Fatalf("Stream returned before VM EOF (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	// VM signals end-of-stream; handler returns.
	if err := binary.Write(vmSide, binary.BigEndian, uint32(0)); err != nil {
		t.Fatalf("write VM EOF marker: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stream returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream handler did not return after VM EOF")
	}
}

// TestStreamForwardsBothDirections is a sanity check that data still
// moves through the bridge correctly so the regression coverage above
// is not vacuous.
func TestStreamForwardsBothDirections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, vmSide, done := startStream(t, ctx, "stream-bidi")

	// Client -> VM: enqueue payloads through the fake server's recv
	// channel and confirm the VM peer reads them as length-prefixed
	// proto Any frames.
	payloads := [][]byte{[]byte("hello"), []byte("world")}
	for _, p := range payloads {
		srv.recvCh <- &anypb.Any{TypeUrl: "test/bytes", Value: p}
	}
	for i, want := range payloads {
		var n uint32
		if err := binary.Read(vmSide, binary.BigEndian, &n); err != nil {
			t.Fatalf("read frame %d length: %v", i, err)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(vmSide, buf); err != nil {
			t.Fatalf("read frame %d data: %v", i, err)
		}
		var got anypb.Any
		if err := proto.Unmarshal(buf, &got); err != nil {
			t.Fatalf("unmarshal frame %d: %v", i, err)
		}
		if !bytes.Equal(got.Value, want) {
			t.Fatalf("VM frame %d = %q, want %q", i, got.Value, want)
		}
	}

	// VM -> client: write framed messages and verify the client picks
	// them up via Send.
	replies := []string{"reply-1", "reply-2"}
	for _, p := range replies {
		frame := &anypb.Any{TypeUrl: "test/bytes", Value: []byte(p)}
		data, err := proto.Marshal(frame)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := binary.Write(vmSide, binary.BigEndian, uint32(len(data))); err != nil {
			t.Fatalf("write frame length: %v", err)
		}
		if _, err := vmSide.Write(data); err != nil {
			t.Fatalf("write frame data: %v", err)
		}
	}
	for _, want := range replies {
		select {
		case got := <-srv.sendCh:
			if string(got.Value) != want {
				t.Fatalf("client received %q, want %q", got.Value, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %q from server", want)
		}
	}

	// VM signals EOF; handler must return without error.
	if err := binary.Write(vmSide, binary.BigEndian, uint32(0)); err != nil {
		t.Fatalf("write VM EOF: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stream returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream handler did not return after VM EOF")
	}
}
