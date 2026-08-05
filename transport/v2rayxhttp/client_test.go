package xhttp

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/sagernet/sing-box/option"
)

type testClosableRoundTripper struct {
	closeCount     int
	closeIdleCount int
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
