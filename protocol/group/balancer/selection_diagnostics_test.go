package balancer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/interrupt"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type diagnosticCaptureLogger struct {
	log.ContextLogger
	warnings []string
	errors   []string
}

func (l *diagnosticCaptureLogger) WarnContext(_ context.Context, args ...any) {
	l.warnings = append(l.warnings, fmt.Sprint(args...))
}

func (l *diagnosticCaptureLogger) ErrorContext(_ context.Context, args ...any) {
	l.errors = append(l.errors, fmt.Sprint(args...))
}

type diagnosticStrategy struct{ selected adapter.Outbound }

func (s diagnosticStrategy) UpdateOutboundsInfo(map[string]*adapter.URLTestHistory) bool {
	return false
}
func (s diagnosticStrategy) Select(adapter.InboundContext, string, bool) adapter.Outbound {
	return s.selected
}
func (s diagnosticStrategy) Now() string { return "" }

type diagnosticOutbound struct{ testOutbound }

func (o diagnosticOutbound) NewConnectionEx(_ context.Context, conn net.Conn, _ adapter.InboundContext, onClose N.CloseHandlerFunc) {
	if onClose != nil {
		onClose(errors.New("upstream closed"))
	}
	_ = conn.Close()
}

func TestProcessProxyLogsActualSelectedNodeOnConnectionFailure(t *testing.T) {
	logger := &diagnosticCaptureLogger{}
	selected := diagnosticOutbound{testOutbound{tag: "stable-node-01", networks: []string{N.NetworkTCP}}}
	group := &Balancer{
		Adapter:        outbound.NewAdapter("balancer", "process-stable-proxy", []string{N.NetworkTCP}, []string{selected.Tag()}),
		logger:         logger,
		strategyFn:     diagnosticStrategy{selected: selected},
		options:        option.BalancerOutboundOptions{LogSelectedOutbound: true},
		interruptGroup: interrupt.NewGroup(),
	}
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	var closeErr error
	group.NewConnectionEx(context.Background(), client, adapter.InboundContext{
		Network:     N.NetworkTCP,
		Source:      M.ParseSocksaddr("172.19.0.1:53000"),
		Destination: M.ParseSocksaddr("chatgpt.com:443"),
	}, func(err error) { closeErr = err })

	if closeErr == nil || closeErr.Error() != "upstream closed" {
		t.Fatalf("expected original close callback error, got %v", closeErr)
	}
	if len(logger.warnings) != 1 || !strings.Contains(logger.warnings[0], "selected_outbound=stable-node-01") {
		t.Fatalf("expected selected node in connection log, got %#v", logger.warnings)
	}
	if len(logger.errors) != 1 || !strings.Contains(logger.errors[0], "selected_outbound=stable-node-01") || !strings.Contains(logger.errors[0], "upstream closed") {
		t.Fatalf("expected selected node and cause in failure log, got %#v", logger.errors)
	}
}
