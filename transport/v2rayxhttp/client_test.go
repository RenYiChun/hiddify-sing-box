package xhttp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

type testClosableRoundTripper struct {
	closeCount     int
	closeIdleCount int
}

type contextBlockingRoundTripper struct {
	started chan struct{}
	done    chan struct{}
}

type contextRecordingDialerClient struct {
	contexts chan context.Context
}

func (c *contextRecordingDialerClient) IsClosed() bool {
	return false
}

func (c *contextRecordingDialerClient) OpenStream(
	ctx context.Context,
	_ string,
	_ io.Reader,
	_ bool,
) (io.ReadCloser, net.Addr, net.Addr, error) {
	c.contexts <- ctx
	return http.NoBody, nil, nil, nil
}

func (c *contextRecordingDialerClient) PostPacket(context.Context, string, io.Reader, int64) error {
	return nil
}

func (t *contextBlockingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	close(t.started)
	<-request.Context().Done()
	close(t.done)
	return nil, request.Context().Err()
}

func (t *testClosableRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (t *testClosableRoundTripper) Close() error {
	t.closeCount++
	return nil
}

func (t *testClosableRoundTripper) CloseIdleConnections() {
	t.closeIdleCount++
}

type testXmuxConn struct {
	closed     bool
	closeCount int
}

func (c *testXmuxConn) IsClosed() bool {
	return c.closed
}

func (c *testXmuxConn) Close() error {
	c.closeCount++
	c.closed = true
	return nil
}

func TestXmuxManagerClosesRetiredClient(t *testing.T) {
	var connections []*testXmuxConn
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn {
		conn := &testXmuxConn{}
		connections = append(connections, conn)
		return conn
	})

	first := manager.GetXmuxClient(context.Background())
	connections[0].closed = true
	second := manager.GetXmuxClient(context.Background())

	if first == second {
		t.Fatal("expected a replacement xmux client")
	}
	if connections[0].closeCount != 1 {
		t.Fatalf("expected retired xmux connection to be closed once, got %d", connections[0].closeCount)
	}
}

func TestXmuxManagerDefersRetiredClientCloseWhileInUse(t *testing.T) {
	var connections []*testXmuxConn
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn {
		conn := &testXmuxConn{}
		connections = append(connections, conn)
		return conn
	})

	first := manager.GetXmuxClient(context.Background())
	first.OpenUsage.Store(1)
	connections[0].closed = true
	second := manager.GetXmuxClient(context.Background())

	if first == second {
		t.Fatal("expected an unusable in-flight client to be excluded from reuse")
	}
	if connections[0].closeCount != 0 {
		t.Fatal("expected in-flight retired connection close to be deferred")
	}

	first.OpenUsage.Store(0)
	if manager.GetXmuxClient(context.Background()) != second {
		t.Fatal("expected the reusable replacement client to remain selected")
	}
	if connections[0].closeCount != 1 {
		t.Fatalf("expected retired connection to close after its final user, got %d closes", connections[0].closeCount)
	}
}

func TestDefaultDialerClientCloseClosesTransport(t *testing.T) {
	transport := &testClosableRoundTripper{}
	client := &DefaultDialerClient{
		client: &http.Client{Transport: transport},
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if transport.closeCount != 1 {
		t.Fatalf("expected transport Close to run once, got %d", transport.closeCount)
	}
	if transport.closeIdleCount != 0 {
		t.Fatalf("expected full transport close instead of idle-only close, got %d idle closes", transport.closeIdleCount)
	}
}

func TestDefaultDialerClientClosedStateIsConcurrentSafe(t *testing.T) {
	client := &DefaultDialerClient{}
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		for range 1000 {
			_ = client.IsClosed()
		}
		close(done)
	}()

	<-started
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
	if !client.IsClosed() {
		t.Fatal("expected client to remain marked closed")
	}
}

func TestClientCloseClosesXmuxManagers(t *testing.T) {
	primaryConn := &testXmuxConn{}
	secondaryConn := &testXmuxConn{}
	primary := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn {
		return primaryConn
	})
	secondary := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn {
		return secondaryConn
	})
	primary.GetXmuxClient(context.Background())
	secondary.GetXmuxClient(context.Background())

	client := &Client{
		xmuxManager:  primary,
		xmuxManager2: secondary,
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if !primaryConn.closed {
		t.Fatal("expected primary xmux connection to be closed")
	}
	if !secondaryConn.closed {
		t.Fatal("expected secondary xmux connection to be closed")
	}
}

func TestDefaultDialerClientOpenStreamHonorsContextCancellation(t *testing.T) {
	transport := &contextBlockingRoundTripper{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
	client := &DefaultDialerClient{
		options: &option.V2RayXHTTPBaseOptions{},
		client:  &http.Client{Transport: transport},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan io.ReadCloser, 1)
	go func() {
		stream, _, _, _ := client.OpenStream(ctx, "http://example.test/", nil, false)
		result <- stream
	}()

	<-transport.started
	cancel()
	stream := <-result
	if stream != nil {
		_ = stream.Close()
	}
	select {
	case <-transport.done:
	case <-time.After(time.Second):
		t.Fatal("expected OpenStream request to stop after context cancellation")
	}
}

func TestDefaultDialerClientPostPacketHonorsContextCancellation(t *testing.T) {
	transport := &contextBlockingRoundTripper{
		started: make(chan struct{}),
		done:    make(chan struct{}),
	}
	client := &DefaultDialerClient{
		options: &option.V2RayXHTTPBaseOptions{},
		client:  &http.Client{Transport: transport},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.PostPacket(ctx, "http://example.test/", nil, 0)
	}()

	<-transport.started
	cancel()
	select {
	case <-transport.done:
	case <-time.After(time.Second):
		t.Fatal("expected PostPacket request to stop after context cancellation")
	}
	if err := <-result; err == nil {
		t.Fatal("expected canceled PostPacket to return an error")
	}
}

func TestClientDialContextDetachesCallerCancellationUntilConnectionClose(t *testing.T) {
	dialerClient := &contextRecordingDialerClient{contexts: make(chan context.Context, 1)}
	requestURL := func(string) url.URL {
		return url.URL{Scheme: "http", Host: "example.test", Path: "/"}
	}
	client := &Client{
		options:        &option.V2RayXHTTPOptions{Mode: "stream-one"},
		getRequestURL:  requestURL,
		getRequestURL2: requestURL,
		getHTTPClient: func() (DialerClient, *XmuxClient) {
			return dialerClient, nil
		},
		getHTTPClient2: func() (DialerClient, *XmuxClient) {
			return dialerClient, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	connection, err := client.DialContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	requestCtx := <-dialerClient.contexts

	cancel()
	select {
	case <-requestCtx.Done():
		t.Fatal("expected established XHTTP connection to detach from the caller's dial context")
	case <-time.After(25 * time.Millisecond):
	}

	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected closing the XHTTP connection to cancel its request context")
	}
}

func TestClientDialContextRejectsAlreadyCanceledCaller(t *testing.T) {
	dialerClient := &contextRecordingDialerClient{contexts: make(chan context.Context, 1)}
	requestURL := func(string) url.URL {
		return url.URL{Scheme: "http", Host: "example.test", Path: "/"}
	}
	client := &Client{
		options:        &option.V2RayXHTTPOptions{Mode: "stream-one"},
		getRequestURL:  requestURL,
		getRequestURL2: requestURL,
		getHTTPClient: func() (DialerClient, *XmuxClient) {
			return dialerClient, nil
		},
		getHTTPClient2: func() (DialerClient, *XmuxClient) {
			return dialerClient, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connection, err := client.DialContext(ctx)
	if connection != nil {
		_ = connection.Close()
		t.Fatal("expected an already canceled dial context not to return a connection")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	requestCtx := <-dialerClient.contexts
	select {
	case <-requestCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected the rejected connection request context to be canceled")
	}
}
