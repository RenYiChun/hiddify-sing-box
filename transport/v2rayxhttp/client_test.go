package xhttp

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/option"
)

type testXmuxConn struct {
	closed bool
}

func (c *testXmuxConn) IsClosed() bool {
	return c.closed
}

func (c *testXmuxConn) Close() error {
	c.closed = true
	return nil
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
