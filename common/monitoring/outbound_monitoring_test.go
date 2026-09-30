package monitoring

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/urltest"
	"github.com/sagernet/sing-box/hiddify/ipinfo"
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

func TestDuplicateTaskReportsSkippedResultToBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitor := &OutboundMonitoring{
		ctx:       ctx,
		logger:    log.NewNOPFactory().NewLogger("monitoring"),
		outbounds: map[string]*outboundState{"busy": {testing: true}},
	}
	resultCh := make(chan testOutcome, 1)
	monitor.executeTask(&testTask{outboundTag: "busy", resultCh: resultCh})
	select {
	case outcome := <-resultCh:
		if !outcome.skipped || outcome.err == nil {
			t.Fatalf("expected a skipped duplicate result, got %+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate task left the batch waiting for a result")
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

func TestInitialCycleKeepsFreshSelectedNodeResult(t *testing.T) {
	now := time.Now()
	monitor := &OutboundMonitoring{
		mainInterval: 10 * time.Minute,
		outbounds: map[string]*outboundState{
			"selected":      {outbound: readyTestOutbound{tag: "selected"}, history: adapter.URLTestHistory{Time: now, Delay: 180}},
			"pending":       {outbound: readyTestOutbound{tag: "pending"}, invalid: true},
			"failed":        {outbound: readyTestOutbound{tag: "failed"}, history: adapter.URLTestHistory{Time: now, Delay: TimeoutDelay}, invalid: true},
			"§hide§ hidden": {outbound: readyTestOutbound{tag: "§hide§ hidden"}, invalid: true},
		},
		groups: map[string]*groupState{},
	}

	tags := monitor.collectCycleTargets()
	if len(tags) != 2 || !containsTag(tags, "pending") || !containsTag(tags, "failed") {
		t.Fatalf("expected only untested and failed visible nodes, got %v", tags)
	}
}

func containsTag(tags []string, wanted string) bool {
	for _, tag := range tags {
		if tag == wanted {
			return true
		}
	}
	return false
}

var _ adapter.Outbound = readyTestOutbound{}

type retryTestOutbound struct {
	readyTestOutbound
	address      string
	failAttempts int32
	attempts     atomic.Int32
}

func (o *retryTestOutbound) DialContext(ctx context.Context, network string, _ M.Socksaddr) (net.Conn, error) {
	if o.attempts.Add(1) <= o.failAttempts {
		return nil, errors.New("transient dial failure")
	}
	return (&net.Dialer{}).DialContext(ctx, network, o.address)
}

func TestFailedParallelURLTestsRetrySerially(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := strings.TrimPrefix(server.URL, "http://")
	stable := &retryTestOutbound{readyTestOutbound: readyTestOutbound{tag: "stable"}, address: address}
	transient := &retryTestOutbound{readyTestOutbound: readyTestOutbound{tag: "transient"}, address: address, failAttempts: 2}
	persistent := &retryTestOutbound{readyTestOutbound: readyTestOutbound{tag: "persistent"}, address: address, failAttempts: 100}
	monitor := &OutboundMonitoring{
		ctx:            ctx,
		logger:         log.NewNOPFactory().NewLogger("monitoring"),
		urls:           []string{server.URL},
		urlTestTimeout: time.Second,
		history:        urltest.NewHistoryStorage(),
		outbounds:      make(map[string]*outboundState),
		groups:         make(map[string]*groupState),
		priorityQueue:  make(chan *testTask, 3),
		normalQueue:    make(chan *testTask, 3),
	}
	for _, outbound := range []*retryTestOutbound{stable, transient, persistent} {
		monitor.outbounds[outbound.Tag()] = &outboundState{
			outbound: outbound,
			history:  adapter.URLTestHistory{IpInfo: &ipinfo.IpInfo{}},
		}
	}
	monitor.workerWG.Add(3)
	for range 3 {
		go monitor.workerLoop()
	}
	defer func() {
		cancel()
		monitor.workerWG.Wait()
	}()

	outcomes := monitor.runStage(10, []string{stable.Tag(), transient.Tag(), persistent.Tag()})
	if len(outcomes) != 3 {
		t.Fatalf("expected three first-pass results, got %d", len(outcomes))
	}
	for _, tag := range []string{transient.Tag(), persistent.Tag()} {
		state := monitor.outbounds[tag]
		state.mu.Lock()
		failedBeforeRetry := state.invalid || state.history.Delay == TimeoutDelay
		state.mu.Unlock()
		if failedBeforeRetry {
			t.Fatalf("first-pass failure for %s should not be published before retry", tag)
		}
	}
	monitor.retryFailedSerially(10, outcomes, false)
	results := make(map[string]testOutcome)
	for _, outcome := range outcomes {
		results[outcome.outboundTag] = outcome
	}
	if results[stable.Tag()].err != nil || results[transient.Tag()].err != nil {
		t.Fatalf("stable and transient outbounds should succeed: %+v", results)
	}
	if results[persistent.Tag()].err == nil {
		t.Fatal("persistent failure should remain failed after one retry")
	}
	if state := monitor.outbounds[persistent.Tag()]; !state.invalid || state.history.Delay != TimeoutDelay {
		t.Fatal("persistent retry failure should be published")
	}
	if stable.attempts.Load() != 1 || transient.attempts.Load() != 3 || persistent.attempts.Load() != 4 {
		t.Fatalf("unexpected dial attempts: stable=%d transient=%d persistent=%d",
			stable.attempts.Load(), transient.attempts.Load(), persistent.attempts.Load())
	}
}

func TestPriorityGroupRetriesTransientFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := strings.TrimPrefix(server.URL, "http://")
	stable := &retryTestOutbound{readyTestOutbound: readyTestOutbound{tag: "stable"}, address: address}
	transient := &retryTestOutbound{readyTestOutbound: readyTestOutbound{tag: "transient"}, address: address, failAttempts: 2}
	monitor := &OutboundMonitoring{
		ctx:            ctx,
		logger:         log.NewNOPFactory().NewLogger("monitoring"),
		urls:           []string{server.URL},
		urlTestTimeout: time.Second,
		history:        urltest.NewHistoryStorage(),
		outbounds: map[string]*outboundState{
			stable.Tag():    {outbound: stable, history: adapter.URLTestHistory{IpInfo: &ipinfo.IpInfo{}}},
			transient.Tag(): {outbound: transient, history: adapter.URLTestHistory{IpInfo: &ipinfo.IpInfo{}}},
		},
		groups: map[string]*groupState{
			"group": {outbounds: map[string]struct{}{stable.Tag(): {}, transient.Tag(): {}}},
		},
		priorityQueue: make(chan *testTask, 2),
		normalQueue:   make(chan *testTask, 2),
	}
	monitor.workerWG.Add(3)
	for range 3 {
		go monitor.workerLoop()
	}
	defer func() {
		cancel()
		monitor.workerWG.Wait()
	}()

	if err := monitor.testNow("group", true); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		state := monitor.outbounds[transient.Tag()]
		state.mu.Lock()
		passed := transient.attempts.Load() == 3 && !state.history.Time.IsZero() && state.history.Delay < TimeoutDelay && !state.invalid
		state.mu.Unlock()
		if passed {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("priority group did not recover transient failure; attempts=%d", transient.attempts.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if stable.attempts.Load() != 1 {
		t.Fatalf("successful priority test should not be retried; attempts=%d", stable.attempts.Load())
	}
}

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
