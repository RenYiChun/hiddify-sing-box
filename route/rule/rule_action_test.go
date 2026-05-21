package rule

import (
	"context"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
)

func TestNewRuleActionSniffKeepsOverrideDestination(t *testing.T) {
	action, err := NewRuleAction(context.Background(), logger.NOP(), option.RuleAction{
		Action: C.RuleActionTypeSniff,
		SniffOptions: option.RouteActionSniff{
			OverrideDestination: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sniffAction, ok := action.(*RuleActionSniff)
	if !ok {
		t.Fatalf("expected sniff action, got %T", action)
	}
	if !sniffAction.OverrideDestination {
		t.Fatal("expected override destination to be preserved")
	}
}
