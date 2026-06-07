package urltest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
)

type fixedAddressDialer struct {
	address string
}

func (d fixedAddressDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	var dialer net.Dialer
	return dialer.DialContext(ctx, network, d.address)
}

func (fixedAddressDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

func TestURLTestFallsBackToGETWhenHEADConnectionCloses(t *testing.T) {
	var (
		mu      sync.Mutex
		methods []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()

		if r.Method == http.MethodHead {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}

		if r.Method != http.MethodGet {
			t.Errorf("unexpected fallback method %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	_, err := URLTest(context.Background(), server.URL, fixedAddressDialer{address: server.Listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 2 || methods[0] != http.MethodHead || methods[1] != http.MethodGet {
		t.Fatalf("expected HEAD then GET, got %v", methods)
	}
}

func TestURLTestDoesNotFallbackWhenHEADSucceeds(t *testing.T) {
	var (
		mu      sync.Mutex
		methods []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	_, err := URLTest(context.Background(), server.URL, fixedAddressDialer{address: server.Listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 1 || methods[0] != http.MethodHead {
		t.Fatalf("expected only HEAD, got %v", methods)
	}
}

func TestURLTestRejectsServerErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	_, err := URLTest(context.Background(), server.URL, fixedAddressDialer{address: server.Listener.Addr().String()})
	if err == nil {
		t.Fatal("expected URL test to reject 5xx responses")
	}
}
