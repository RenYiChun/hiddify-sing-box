package route

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	R "github.com/sagernet/sing-box/route/rule"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/stretchr/testify/require"
)

func TestProcessRouteMatchMessageIncludesConfiguredProcessWithoutCodexSpecialCase(t *testing.T) {
	t.Parallel()

	selectedRule, err := R.NewRule(context.Background(), nil, option.Rule{
		Type: C.RuleTypeDefault,
		DefaultOptions: option.DefaultRule{
			RawDefaultRule: option.RawDefaultRule{
				ProcessName: []string{"my-special-process.exe"},
			},
			RuleAction: option.RuleAction{
				Action: C.RuleActionTypeRoute,
				RouteOptions: option.RouteActionOptions{
					Outbound: "process-stable-proxy \u00a7hide\u00a7",
				},
			},
		},
	}, true)
	require.NoError(t, err)

	message, ok := processRouteMatchMessage(&adapter.InboundContext{
		Network:     N.NetworkTCP,
		Source:      M.ParseSocksaddr("172.19.0.1:53000"),
		Destination: M.ParseSocksaddr("chatgpt.com:443"),
		ProcessInfo: &adapter.ConnectionOwner{
			ProcessPath: `C:\Tools\my-special-process.exe`,
		},
	}, selectedRule, 7, "process-stable-proxy \u00a7hide\u00a7", "stable-node-01 \u00a7hide\u00a7")

	require.True(t, ok)
	require.Contains(t, message, "process route matched")
	require.Contains(t, message, "rule_index=7")
	require.Contains(t, message, "network=tcp")
	require.Contains(t, message, "source=172.19.0.1:53000")
	require.Contains(t, message, "destination=chatgpt.com:443")
	require.Contains(t, message, "outbound=process-stable-proxy \u00a7hide\u00a7")
	require.Contains(t, message, "selected_outbound=stable-node-01 \u00a7hide\u00a7")
	require.Contains(t, message, "process_name=my-special-process.exe")
	require.Contains(t, message, `process_path=C:\Tools\my-special-process.exe`)
	require.NotContains(t, message, "codex")
}

func TestProcessRouteMatchMessageSkipsNonProcessRule(t *testing.T) {
	t.Parallel()

	selectedRule, err := R.NewRule(context.Background(), nil, option.Rule{
		Type: C.RuleTypeDefault,
		DefaultOptions: option.DefaultRule{
			RawDefaultRule: option.RawDefaultRule{
				Domain: []string{"chatgpt.com"},
			},
			RuleAction: option.RuleAction{
				Action: C.RuleActionTypeRoute,
				RouteOptions: option.RouteActionOptions{
					Outbound: "select",
				},
			},
		},
	}, true)
	require.NoError(t, err)

	message, ok := processRouteMatchMessage(&adapter.InboundContext{
		Network:     N.NetworkTCP,
		Destination: M.ParseSocksaddr("chatgpt.com:443"),
	}, selectedRule, 3, "select", "select")

	require.False(t, ok)
	require.Empty(t, message)
}
