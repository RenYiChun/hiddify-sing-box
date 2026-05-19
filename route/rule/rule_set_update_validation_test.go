package rule

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/common/x/list"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

type fakeDNSRuleSetUpdateValidator struct {
	validate func(tag string, metadata adapter.RuleSetMetadata) error
}

func (v *fakeDNSRuleSetUpdateValidator) ValidateRuleSetMetadataUpdate(tag string, metadata adapter.RuleSetMetadata) error {
	if v.validate == nil {
		return nil
	}
	return v.validate(tag, metadata)
}

func TestLocalRuleSetReloadRulesRejectsInvalidUpdateBeforeCommit(t *testing.T) {
	t.Parallel()

	var callbackCount atomic.Int32
	ctx := service.ContextWith[adapter.DNSRuleSetUpdateValidator](context.Background(), &fakeDNSRuleSetUpdateValidator{
		validate: func(tag string, metadata adapter.RuleSetMetadata) error {
			require.Equal(t, "dynamic-set", tag)
			if metadata.ContainsDNSQueryTypeRule {
				return E.New("dns conflict")
			}
			return nil
		},
	})
	ruleSet := &LocalRuleSet{
		ctx:        ctx,
		tag:        "dynamic-set",
		fileFormat: C.RuleSetFormatSource,
	}
	_ = ruleSet.callbacks.PushBack(func(adapter.RuleSet) {
		callbackCount.Add(1)
	})

	err := ruleSet.reloadRules([]option.HeadlessRule{{
		Type: C.RuleTypeDefault,
		DefaultOptions: option.DefaultHeadlessRule{
			Domain: badoption.Listable[string]{"example.com"},
		},
	}})
	require.NoError(t, err)
	require.Equal(t, int32(1), callbackCount.Load())
	require.False(t, ruleSet.metadata.ContainsDNSQueryTypeRule)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "example.com"}))

	err = ruleSet.reloadRules([]option.HeadlessRule{{
		Type: C.RuleTypeDefault,
		DefaultOptions: option.DefaultHeadlessRule{
			QueryType: badoption.Listable[option.DNSQueryType]{option.DNSQueryType(1)},
		},
	}})
	require.ErrorContains(t, err, "dns conflict")
	require.Equal(t, int32(1), callbackCount.Load())
	require.False(t, ruleSet.metadata.ContainsDNSQueryTypeRule)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "example.com"}))
}

func TestRemoteRuleSetLoopUpdateExitsAfterInitialFetchFailureWithoutStartupTicker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(context.Background())
	ruleSet := &RemoteRuleSet{
		ctx:            ctx,
		cancel:         cancel,
		logger:         log.NewNOPFactory().NewLogger("router"),
		options:        option.RuleSet{Tag: "geoip-cn", RemoteOptions: option.RemoteRuleSet{URL: server.URL}},
		updateInterval: 24 * time.Hour,
		httpClient:     server.Client(),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ruleSet.loopUpdate()
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected remote rule-set update loop to exit after context cancellation")
	}
}

func TestRemoteRuleSetLoadBytesRejectsInvalidUpdateBeforeCommit(t *testing.T) {
	t.Parallel()

	var callbackCount atomic.Int32
	ctx := service.ContextWith[adapter.DNSRuleSetUpdateValidator](context.Background(), &fakeDNSRuleSetUpdateValidator{
		validate: func(tag string, metadata adapter.RuleSetMetadata) error {
			require.Equal(t, "dynamic-set", tag)
			if metadata.ContainsDNSQueryTypeRule {
				return E.New("dns conflict")
			}
			return nil
		},
	})
	ruleSet := &RemoteRuleSet{
		ctx: ctx,
		options: option.RuleSet{
			Tag:    "dynamic-set",
			Format: C.RuleSetFormatSource,
		},
		callbacks: list.List[adapter.RuleSetUpdateCallback]{},
	}
	_ = ruleSet.callbacks.PushBack(func(adapter.RuleSet) {
		callbackCount.Add(1)
	})

	err := ruleSet.loadBytes([]byte(`{"version":4,"rules":[{"domain":["example.com"]}]}`))
	require.NoError(t, err)
	require.Equal(t, int32(1), callbackCount.Load())
	require.False(t, ruleSet.metadata.ContainsDNSQueryTypeRule)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "example.com"}))

	err = ruleSet.loadBytes([]byte(`{"version":4,"rules":[{"query_type":["A"]}]}`))
	require.ErrorContains(t, err, "dns conflict")
	require.Equal(t, int32(1), callbackCount.Load())
	require.False(t, ruleSet.metadata.ContainsDNSQueryTypeRule)
	require.True(t, ruleSet.Match(&adapter.InboundContext{Domain: "example.com"}))
}
