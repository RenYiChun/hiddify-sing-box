package monitoring

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

type blockingTestOutbound struct{}

func (blockingTestOutbound) Type() string           { return "test" }
func (blockingTestOutbound) Tag() string            { return "blocked" }
func (blockingTestOutbound) Network() []string      { return []string{N.NetworkTCP} }
func (blockingTestOutbound) Dependencies() []string { return nil }
func (blockingTestOutbound) DisplayType() string    { return "test" }
func (blockingTestOutbound) IsReady() bool          { return true }

func (blockingTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	select {}
}

func (blockingTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

func TestExecuteTaskTimesOutBlockedURLTest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	outbound := blockingTestOutbound{}
	state := &outboundState{
		outbound:  outbound,
		invalid:   true,
		groupTags: []string{},
	}
	monitor := &OutboundMonitoring{
		ctx:            ctx,
		cancel:         cancel,
		logger:         log.NewNOPFactory().NewLogger("monitoring"),
		urls:           []string{defaultURLTest},
		urlTestTimeout: 20 * time.Millisecond,
		history:        urltest.NewHistoryStorage(),
		outbounds: map[string]*outboundState{
			outbound.Tag(): state,
		},
		groups:        map[string]*groupState{},
		priorityQueue: make(chan *testTask, 1),
		normalQueue:   make(chan *testTask, 1),
	}

	done := make(chan struct{})
	go func() {
		monitor.executeTask(&testTask{outboundTag: outbound.Tag()})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("executeTask did not enforce URL test timeout")
	}

	state.mu.Lock()
	if state.history.Delay != TimeoutDelay {
		t.Fatalf("expected timed out history delay %d, got %d", TimeoutDelay, state.history.Delay)
	}
	if !state.invalid {
		t.Fatal("expected timed out outbound to remain invalid")
	}
	if !state.testing {
		t.Fatal("expected timed out URL test to remain in-flight until the underlying dial returns")
	}
	state.mu.Unlock()

	if monitor.enqueueTask(&testTask{outboundTag: outbound.Tag(), cycleID: 1}) {
		t.Fatal("expected duplicate task to be rejected while timed out URL test is still in-flight")
	}
}

var _ adapter.Outbound = blockingTestOutbound{}

type readyTestOutbound struct {
	tag string
}

func (o readyTestOutbound) Type() string           { return "test" }
func (o readyTestOutbound) Tag() string            { return o.tag }
func (o readyTestOutbound) Network() []string      { return []string{N.NetworkTCP} }
func (o readyTestOutbound) Dependencies() []string { return nil }
func (o readyTestOutbound) DisplayType() string    { return "test" }
func (o readyTestOutbound) IsReady() bool          { return true }

func (readyTestOutbound) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, errors.New("unused")
}

func (readyTestOutbound) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("unsupported")
}

func TestGroupTestNowKeepsPriorityForChildren(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := readyTestOutbound{tag: "first"}
	second := readyTestOutbound{tag: "second"}
	monitor := &OutboundMonitoring{
		ctx:            ctx,
		cancel:         cancel,
		logger:         log.NewNOPFactory().NewLogger("monitoring"),
		urls:           []string{defaultURLTest},
		urlTestTimeout: 20 * time.Millisecond,
		history:        urltest.NewHistoryStorage(),
		outbounds: map[string]*outboundState{
			first.Tag():  {outbound: first, invalid: true, groupTags: []string{"group"}},
			second.Tag(): {outbound: second, invalid: true, groupTags: []string{"group"}},
		},
		groups: map[string]*groupState{
			"group": {
				tag: "group",
				outbounds: map[string]struct{}{
					first.Tag():  {},
					second.Tag(): {},
				},
			},
		},
		priorityQueue: make(chan *testTask, 2),
		normalQueue:   make(chan *testTask, 2),
	}

	if err := monitor.testNow("group", true); err != nil {
		t.Fatal(err)
	}

	if got := len(monitor.priorityQueue); got != 2 {
		t.Fatalf("expected group children in priority queue, got %d", got)
	}
	if got := len(monitor.normalQueue); got != 0 {
		t.Fatalf("expected normal queue to remain empty, got %d", got)
	}
}

func TestInterfaceUpdatedDoesNotStartRegularCycle(t *testing.T) {
	monitor := &OutboundMonitoring{}

	monitor.InterfaceUpdated()

	if monitor.cycleRunning.Load() {
		t.Fatal("expected interface updates to avoid starting a full monitoring cycle")
	}
}

var _ adapter.Outbound = readyTestOutbound{}

func TestURLTestFailureUsesSeparateLogFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir := t.TempDir()
	mainLogPath := filepath.Join(tempDir, "box.log")
	urlTestLogPath := filepath.Join(tempDir, "url-test.log")
	mainFactory := newTestLogFactory(t, ctx, mainLogPath)

	options := option.MonitoringOptions{
		URLs:           []string{defaultURLTest},
		URLTestLogFile: urlTestLogPath,
	}

	monitor, err := NewOutboundMonitoring(ctx, mainFactory.NewLogger("monitoring"), options)
	if err != nil {
		t.Fatal(err)
	}

	outbound := readyTestOutbound{tag: "failing"}
	monitor.outbounds[outbound.Tag()] = &outboundState{
		outbound:  outbound,
		invalid:   true,
		groupTags: []string{},
	}

	_, err = monitor.tester(ctx, outbound.Tag())
	if err == nil {
		t.Fatal("expected URL test to fail")
	}

	if err := monitor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mainFactory.Close(); err != nil {
		t.Fatal(err)
	}

	mainLog := string(mustReadFile(t, mainLogPath))
	if strings.Contains(mainLog, "URL test failed") {
		t.Fatalf("expected main log not to contain URL test failure details, got %q", mainLog)
	}

	urlTestLog := string(mustReadFile(t, urlTestLogPath))
	if !strings.Contains(urlTestLog, "outbound failing URL test failed") {
		t.Fatalf("expected URL test log to contain failure details, got %q", urlTestLog)
	}
}

func TestURLTestDetailLoggerUsesSeparateLogFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir := t.TempDir()
	mainLogPath := filepath.Join(tempDir, "box.log")
	urlTestLogPath := filepath.Join(tempDir, "url-test.log")
	mainFactory := newTestLogFactory(t, ctx, mainLogPath)

	monitor, err := NewOutboundMonitoring(ctx, mainFactory.NewLogger("monitoring"), option.MonitoringOptions{
		URLTestLogFile: urlTestLogPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	monitor.urlTestDetailLogger().Warn("Failed try 0 to get IP info: test")

	if err := monitor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mainFactory.Close(); err != nil {
		t.Fatal(err)
	}

	mainLog := string(mustReadFile(t, mainLogPath))
	if strings.Contains(mainLog, "Failed try 0 to get IP info") {
		t.Fatalf("expected main log not to contain URL test detail, got %q", mainLog)
	}

	urlTestLog := string(mustReadFile(t, urlTestLogPath))
	if !strings.Contains(urlTestLog, "Failed try 0 to get IP info") {
		t.Fatalf("expected URL test log to contain URL test detail, got %q", urlTestLog)
	}
}

func newTestLogFactory(t *testing.T, ctx context.Context, path string) log.Factory {
	t.Helper()
	factory, err := log.New(log.Options{
		Context: ctx,
		Options: option.LogOptions{
			Level:        "debug",
			Output:       path,
			DisableColor: true,
		},
		BaseTime: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := factory.Start(); err != nil {
		t.Fatal(err)
	}
	return factory
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}
