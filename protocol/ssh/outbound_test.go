package ssh

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type blockingDialer struct {
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (d *blockingDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.once.Do(func() {
		close(d.entered)
	})
	defer close(d.exited)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *blockingDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, context.Canceled
}

type pipeDialer struct {
	entered chan struct{}
	once    sync.Once
	access  sync.Mutex
	conns   []net.Conn
}

func (d *pipeDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	client, server := net.Pipe()
	d.access.Lock()
	d.conns = append(d.conns, client, server)
	d.access.Unlock()
	d.once.Do(func() {
		close(d.entered)
	})
	return client, nil
}

func (d *pipeDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, context.Canceled
}

func (d *pipeDialer) closeAll() {
	d.access.Lock()
	defer d.access.Unlock()
	for _, conn := range d.conns {
		conn.Close()
	}
}

func TestPostStartDoesNotBlockOnSSHPreconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialer := &blockingDialer{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	outbound := &Outbound{
		ctx:    ctx,
		dialer: dialer,
	}

	done := make(chan error, 1)
	go func() {
		done <- outbound.PostStart()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("PostStart blocked while opening an SSH connection")
	}

	select {
	case <-dialer.entered:
	case <-time.After(time.Second):
		t.Fatal("PostStart did not schedule SSH preconnect")
	}

	cancel()
	select {
	case <-dialer.exited:
	case <-time.After(time.Second):
		t.Fatal("SSH preconnect did not stop after context cancellation")
	}
}

func TestDialContextUsesCallerContextWhileOpeningSSHConnection(t *testing.T) {
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	dialer := &blockingDialer{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	outbound := &Outbound{
		ctx:    appCtx,
		dialer: dialer,
	}

	callCtx, cancelCall := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := outbound.DialContext(callCtx, "tcp", M.Socksaddr{})
		done <- err
	}()

	select {
	case <-dialer.entered:
	case <-time.After(time.Second):
		t.Fatal("SSH dialer was not called")
	}

	cancelCall()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected caller context cancellation, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		appCancel()
		<-done
		t.Fatal("DialContext ignored caller context while opening an SSH connection")
	}
}

func TestDialContextUsesCallerContextDuringSSHHandshake(t *testing.T) {
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	dialer := &pipeDialer{
		entered: make(chan struct{}),
	}
	t.Cleanup(dialer.closeAll)
	outbound := &Outbound{
		ctx:    appCtx,
		dialer: dialer,
	}

	callCtx, cancelCall := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelCall()

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := outbound.DialContext(callCtx, "tcp", M.Socksaddr{})
		done <- err
	}()

	select {
	case <-dialer.entered:
	case <-time.After(time.Second):
		t.Fatal("SSH dialer was not called")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected SSH handshake to fail when caller context expires")
		}
		if time.Since(start) > 200*time.Millisecond {
			t.Fatalf("SSH handshake returned too late: %s", time.Since(start))
		}
	case <-time.After(250 * time.Millisecond):
		dialer.closeAll()
		<-done
		t.Fatal("DialContext ignored caller context during SSH handshake")
	}
}
