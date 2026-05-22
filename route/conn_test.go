package route

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type blockingHandshakeContextConn struct {
	started chan struct{}
}

func (c *blockingHandshakeContextConn) HandshakeContext(ctx context.Context) error {
	close(c.started)
	<-ctx.Done()
	return ctx.Err()
}

type upstreamHandshakeWrapper struct {
	upstream any
}

func (w upstreamHandshakeWrapper) Upstream() any {
	return w.upstream
}

type fallbackHandshakeSuccessConn struct {
	called bool
}

func (c *fallbackHandshakeSuccessConn) HandshakeSuccess() error {
	c.called = true
	return nil
}

func TestReportConnHandshakeSuccessUsesBoundedContext(t *testing.T) {
	conn := &blockingHandshakeContextConn{started: make(chan struct{})}
	start := time.Now()

	err := reportConnHandshakeSuccess(context.Background(), conn, nil, 20*time.Millisecond)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected bounded handshake to return quickly, took %s", elapsed)
	}
	select {
	case <-conn.started:
	default:
		t.Fatal("expected HandshakeContext to be called")
	}
}

func TestReportConnHandshakeSuccessFindsContextHandshakeBehindWrapper(t *testing.T) {
	conn := &blockingHandshakeContextConn{started: make(chan struct{})}
	wrappedConn := upstreamHandshakeWrapper{upstream: conn}

	err := reportConnHandshakeSuccess(context.Background(), wrappedConn, nil, 20*time.Millisecond)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	select {
	case <-conn.started:
	default:
		t.Fatal("expected wrapped HandshakeContext to be called")
	}
}

func TestReportConnHandshakeSuccessFallsBackToNetworkReporter(t *testing.T) {
	conn := &fallbackHandshakeSuccessConn{}

	err := reportConnHandshakeSuccess(context.Background(), conn, net.Conn(nil), time.Second)

	if err != nil {
		t.Fatalf("expected fallback handshake success, got %v", err)
	}
	if !conn.called {
		t.Fatal("expected HandshakeSuccess fallback to be called")
	}
}

func TestExpectedConnectionCopyCloseErrorsAreQuiet(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "http2 cancel",
			err:  errors.New("stream error: stream ID 35; CANCEL; received from peer"),
			want: true,
		},
		{
			name: "closed pipe",
			err:  errors.New("read/write on closed pipe"),
			want: true,
		},
		{
			name: "response body closed",
			err:  errors.New("response body closed"),
			want: true,
		},
		{
			name: "no error close",
			err:  errors.New("stream error: NO_ERROR"),
			want: true,
		},
		{
			name: "real failure",
			err:  errors.New("remote tls handshake failed"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isExpectedConnectionCopyCloseError(tt.err); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

type nilCachedPacketReader struct {
	cachedRead bool
	packetRead bool
}

func (r *nilCachedPacketReader) ReadCachedPacket() *N.PacketBuffer {
	if r.cachedRead {
		return nil
	}
	r.cachedRead = true
	return N.NewPacketBuffer()
}

func (r *nilCachedPacketReader) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	if r.packetRead {
		return M.Socksaddr{}, io.EOF
	}
	r.packetRead = true
	_, _ = buffer.Write([]byte("ok"))
	return M.ParseSocksaddrHostPort("example.com", 443), nil
}

type recordingPacketWriter struct {
	payload string
}

func (w *recordingPacketWriter) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	w.payload = string(buffer.Bytes())
	buffer.Release()
	return nil
}

func TestSafePacketReaderDropsNilCachedPacketAndFallsBack(t *testing.T) {
	source := &nilCachedPacketReader{}
	destination := &recordingPacketWriter{}

	_, err := bufio.CopyPacket(destination, newSafePacketReader(source))

	if err != io.EOF {
		t.Fatalf("expected EOF after fallback packet read, got %v", err)
	}
	if destination.payload != "ok" {
		t.Fatalf("expected fallback packet to be copied, got %q", destination.payload)
	}
}

type countingNilCachedPacketReader struct {
	upstream *nilCachedPacketReader
}

func (r *countingNilCachedPacketReader) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return r.upstream.ReadPacket(buffer)
}

func (r *countingNilCachedPacketReader) UnwrapPacketReader() (N.PacketReader, []N.CountFunc) {
	return r.upstream, nil
}

func TestSafePacketReaderDropsNilCachedPacketBehindCounter(t *testing.T) {
	source := &countingNilCachedPacketReader{upstream: &nilCachedPacketReader{}}
	destination := &recordingPacketWriter{}

	_, err := bufio.CopyPacket(destination, newSafePacketReader(source))

	if err != io.EOF {
		t.Fatalf("expected EOF after fallback packet read, got %v", err)
	}
	if destination.payload != "ok" {
		t.Fatalf("expected fallback packet to be copied, got %q", destination.payload)
	}
}

type fakePacketAddr string

func (a fakePacketAddr) Network() string {
	return "udp"
}

func (a fakePacketAddr) String() string {
	return string(a)
}

type failingHandshakePacketConn struct {
	closed bool
}

func (c *failingHandshakePacketConn) HandshakeSuccess() error {
	return errors.New("handshake failed")
}

func (c *failingHandshakePacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, io.EOF
}

func (c *failingHandshakePacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	buffer.Release()
	return nil
}

func (c *failingHandshakePacketConn) Close() error {
	c.closed = true
	return nil
}

func (c *failingHandshakePacketConn) LocalAddr() net.Addr {
	return fakePacketAddr("local")
}

func (c *failingHandshakePacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *failingHandshakePacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *failingHandshakePacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

type fakeNetPacketConn struct {
	closed bool
}

func (c *fakeNetPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, io.EOF
}

func (c *fakeNetPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	return len(p), nil
}

func (c *fakeNetPacketConn) Close() error {
	c.closed = true
	return nil
}

func (c *fakeNetPacketConn) LocalAddr() net.Addr {
	return fakePacketAddr("remote")
}

func (c *fakeNetPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *fakeNetPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *fakeNetPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

type fakePacketDialer struct {
	packetConn net.PacketConn
}

func (d fakePacketDialer) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("unexpected dial")
}

func (d fakePacketDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return d.packetConn, nil
}

func TestNewPacketConnectionCallsOnCloseWhenHandshakeSuccessFails(t *testing.T) {
	manager := NewConnectionManager(logger.NOP())
	conn := &failingHandshakePacketConn{}
	remote := &fakeNetPacketConn{}
	var closeErr error
	closeCalled := false

	manager.NewPacketConnection(
		context.Background(),
		fakePacketDialer{packetConn: remote},
		conn,
		adapter.InboundContext{
			Destination: M.ParseSocksaddrHostPort("example.com", 53),
		},
		func(err error) {
			closeCalled = true
			closeErr = err
		},
	)

	if !closeCalled {
		t.Fatal("expected onClose to be called")
	}
	if closeErr == nil {
		t.Fatal("expected onClose error")
	}
	if !conn.closed {
		t.Fatal("expected source packet conn to be closed")
	}
	if !remote.closed {
		t.Fatal("expected remote packet conn to be closed")
	}
}
