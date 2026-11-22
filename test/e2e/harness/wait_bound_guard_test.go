package harness

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestWaitBoundsCoverPolledToolCalls rejects a wait whose bound is shorter than
// the wait bound of the tool its callback calls.
//
// A tool call made from a wait runs its own poll bound in full, so a wait
// shorter than that bound can be consumed by a single stalled call: the wait
// ends while the call is still running, and a healthy but slow Broker is
// reported as a stall. A wait must therefore last at least ToolWaitBound(tool),
// two poll bounds of the tool it polls, so one stalled call still leaves a
// second poll to observe the state the first could not. For list_agents that is
// ListAgentsWaitBound, and for send_message it is two SendMessagePollBounds.
//
// The test is the companion to TestEventuallyCallbacksUsePollBoundedCalls: that
// one rejects a callback that bypasses the wait's bound, this one rejects a
// bound that is too short for the call the callback makes.
//
// It reads every call of EventuallyPoll and every wait helper that takes its
// bound from a parameter, resolves the bound to a duration when it is a
// constant expression, and resolves the tool from a direct PollCall,
// PollCallWithin, pollDecode or sysPollDecode call, or from a same-directory
// helper that makes one. A wait whose bound or tool it cannot resolve is not
// guessed at: it is recorded as a skip with its reason, and every skip must
// appear in boundGuardExpectedSkips. A new skip fails the test until it is
// registered there, and an entry that no longer matches a skip fails too.
func TestWaitBoundsCoverPolledToolCalls(t *testing.T) {
	root, err := repositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	e2eDir := filepath.Join(root, "test", "e2e")

	var sources []boundGuardSource
	fset := token.NewFileSet()
	err = filepath.WalkDir(e2eDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		sources = append(sources, boundGuardSource{path: path, file: file})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", e2eDir, err)
	}

	filesByDir := make(map[string][]*ast.File)
	for _, src := range sources {
		dir := filepath.Dir(src.path)
		filesByDir[dir] = append(filesByDir[dir], src.file)
	}

	var violations, skips []string
	checked := 0
	for _, files := range filesByDir {
		consts := boundGuardConsts(files)
		tools := boundGuardToolFuncs(files)
		dispatches := boundGuardDispatches(files, tools)
		for _, file := range files {
			closures := closureLiterals(file)
			resolve := func(expr ast.Expr) map[string]bool {
				return boundGuardCallbackTools(expr, closures, tools)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := callName(call)
				if !ok {
					return true
				}
				if dispatch, ok := dispatches[name]; ok && dispatch.boundPos >= 0 {
					if dispatch.boundPos >= len(call.Args) {
						boundGuardSkip(&skips, fset, e2eDir, call, name, "the call passes no argument at the dispatch's bound position")
						return true
					}
					polled := boundGuardCloneTools(dispatch.tools)
					for _, at := range dispatch.callbackPositions {
						if at < len(call.Args) {
							boundGuardUnionTools(polled, resolve(call.Args[at]))
						}
					}
					if boundGuardCheck(t, fset, e2eDir, call, call.Args[dispatch.boundPos], name, polled, consts, &violations, &skips) {
						checked++
					}
					return true
				}
				if name != "EventuallyPoll" {
					return true
				}
				if len(call.Args) < 2 {
					boundGuardSkip(&skips, fset, e2eDir, call, name, "the call passes no bound argument")
					return true
				}
				if boundGuardCheck(t, fset, e2eDir, call, call.Args[1], name, resolve(lastArg(call)), consts, &violations, &skips) {
					checked++
				}
				return true
			})
		}
	}
	if checked < 50 {
		t.Fatalf("checked only %d wait call sites, want at least 50; the walk is not seeing test/e2e", checked)
	}

	sort.Strings(violations)
	for _, violation := range violations {
		t.Error(violation)
	}
	if len(violations) > 0 {
		t.Errorf("%d wait bounds are shorter than the wait bound of the tool they call; use the tool's ToolWaitBound, or a product timer plus EventTimeout", len(violations))
	}

	boundGuardCheckSkips(t, skips)
	t.Logf("checked %d wait call sites, skipped %d", checked, len(skips))
}

// boundGuardSource is one parsed file under test/e2e.
type boundGuardSource struct {
	path string
	file *ast.File
}

// boundGuardDispatch describes a function whose wait takes its bound from one of
// the function's parameters, so the check belongs at the function's call sites.
type boundGuardDispatch struct {
	// boundPos is the parameter position passed as the wait's bound, or -1.
	boundPos int
	// callbackPositions are the parameter positions forwarded as the wait's
	// callback; the tool is resolved from the callback at each call site.
	callbackPositions []int
	// tools are the tools an inline callback of the function's wait polls.
	tools map[string]bool
}

// boundGuardExternDurations resolves a duration constant a wait bound names
// from another package. The harness values are read directly; the mcpserver
// values are the product timers the e2e tests import.
var boundGuardExternDurations = map[string]time.Duration{
	"EventTimeout":            EventTimeout,
	"ToolCallWaitBound":       ToolCallWaitBound,
	"ListAgentsWaitBound":     ListAgentsWaitBound,
	"ListAgentsPollBound":     ListAgentsPollBound,
	"TopicCallWaitBound":      TopicCallWaitBound,
	"TopicCallPollBound":      TopicCallPollBound,
	"RegistrationReportBound": RegistrationReportBound,
	"HandlerRPCPollBound":     HandlerRPCPollBound,
	"SendMessagePollBound":    SendMessagePollBound,
	"ConnectBrokerPollBound":  ConnectBrokerPollBound,
	"PollInterval":            PollInterval,
	"FastReconnectMax":        FastReconnectMax,
	"FastDialTimeout":         FastDialTimeout,
	"SystemRetryDeadline":     10 * time.Second, // mcpserver.SystemRetryDeadline
	"ConnBackoffCap":          5 * time.Second,  // mcpserver.ConnBackoffCap
}

// boundGuardCheck compares call's bound with the wait bound of each tool in
// polled. It returns false, after recording a skip with the reason, when the
// tool or the bound cannot be resolved statically; a caller must not treat such
// a site as checked.
func boundGuardCheck(t *testing.T, fset *token.FileSet, e2eDir string, call *ast.CallExpr, boundExpr ast.Expr, owner string, polled map[string]bool, consts map[string]ast.Expr, violations, skips *[]string) bool {
	t.Helper()
	if len(polled) == 0 {
		boundGuardSkip(skips, fset, e2eDir, call, owner, "the callback polls no statically resolvable tool")
		return false
	}
	bound, ok := boundGuardDuration(boundExpr, consts, map[string]bool{})
	if !ok {
		boundGuardSkip(skips, fset, e2eDir, call, owner, "the bound is not a static duration")
		return false
	}
	// EventuallyPoll lifts a short bound to ToolCallWaitBound, so the check
	// compares the lifted value; a slower tool still exceeds it.
	if bound < ToolCallWaitBound {
		bound = ToolCallWaitBound
	}
	var slowest string
	var need time.Duration
	for tool := range polled {
		if want := ToolWaitBound(tool); want > need {
			need = want
			slowest = tool
		}
	}
	if need == 0 {
		boundGuardSkip(skips, fset, e2eDir, call, owner, "the callback polls no tool with a known handler bound")
		return false
	}
	if bound >= need {
		return true
	}
	tools := make([]string, 0, len(polled))
	for tool := range polled {
		tools = append(tools, tool)
	}
	sort.Strings(tools)
	*violations = append(*violations, fmt.Sprintf(
		"%s: %s waits %s for %v, shorter than the %s wait bound (%s); a single stalled call can consume the wait",
		fset.Position(call.Lparen), owner, bound, tools, slowest, need))
	return true
}

// boundGuardSkip records one wait call site the guard cannot check, as
// "file:line: owner: reason" with the file relative to test/e2e. Every
// recorded skip must be listed in boundGuardExpectedSkips.
func boundGuardSkip(skips *[]string, fset *token.FileSet, e2eDir string, call *ast.CallExpr, owner, reason string) {
	pos := fset.Position(call.Lparen)
	name := pos.Filename
	if rel, err := filepath.Rel(e2eDir, name); err == nil {
		name = rel
	}
	*skips = append(*skips, fmt.Sprintf("%s:%d: %s: %s", filepath.ToSlash(name), pos.Line, owner, reason))
}

// boundGuardExpectedSkips is the complete set of wait call sites the guard
// cannot check, each with the reason it cannot. A site that skips without an
// entry here fails TestWaitBoundsCoverPolledToolCalls, and an entry that no
// longer matches a skip fails too, so a new skip must be registered
// deliberately and a fixed one removed. A site whose bound is not static
// carries the bound a reviewer computed and its comparison with the polled
// tool's wait bound in place of the bare reason, in the form "bound is not
// static: ... checked by hand".
var boundGuardExpectedSkips = []string{
	"addresses_test.go:107: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:111: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:118: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:128: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:129: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:138: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:139: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:162: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:166: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:174: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:175: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:198: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:203: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:204: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:210: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:211: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:216: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:217: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:227: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:228: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:72: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:91: adWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:100: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:107: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:115: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:134: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:143: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:152: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:175: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:184: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:226: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:289: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:375: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:383: alWaitPeerAlias: the callback polls no statically resolvable tool",
	"aliases_test.go:457: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:660: EventuallyPoll: bound is not static: caller-provided, every caller passes ListAgentsWaitBound 48s for list_agents (48s), checked by hand",
	"aliases_test.go:79: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:80: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:93: alWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:200: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:201: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:249: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:250: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:290: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:291: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:343: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:344: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:416: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:485: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:79: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:80: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:91: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:92: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_test.go:182: alWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_test.go:183: alWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_test.go:201: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:330: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:374: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:410: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:454: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:465: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:500: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:592: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"degraded_run_test.go:230: EventuallyPoll: the callback polls no statically resolvable tool",
	"degraded_run_test.go:313: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:314: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:402: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:417: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:59: EventuallyPoll: the callback polls no statically resolvable tool",
	"discovery_address_test.go:121: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:125: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:139: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:148: dadWaitPeerAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:149: dadWaitCardAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:156: dadWaitPeerAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:160: dadWaitCardAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:191: dadWaitCardAt: the callback polls no statically resolvable tool",
	"discovery_address_test.go:225: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:229: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:65: dadWaitCardAt: the callback polls no statically resolvable tool",
	"discovery_address_test.go:84: dadWaitCardAddresses: the callback polls no statically resolvable tool",
	"discovery_expiry_test.go:101: dxWaitCard: the callback polls no statically resolvable tool",
	"discovery_expiry_test.go:165: dxWaitCard: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:143: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:184: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:238: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:449: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:513: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:554: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:1006: discWaitPublicTopic: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:1008: discWaitPublicTopicGone: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:1019: discWaitPublicTopic: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:1039: discWaitPublicTopic: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:1041: discWaitPublicTopicGone: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:1044: discWaitPublicTopic: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:1047: discWaitPublicTopicGone: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:1053: EventuallyPoll: bound is not static: caller-provided, callers pass discBound(1) 31s or discBound(2) 32s for list_topics (28s), checked by hand",
	"discovery_test.go:1069: EventuallyPoll: bound is not static: caller-provided, callers pass discBound(1) 31s or discBound(2) 32s for list_topics (28s), checked by hand",
	"discovery_test.go:180: EventuallyPoll: the callback polls no statically resolvable tool",
	"discovery_test.go:225: discWaitConnected: the callback polls no statically resolvable tool",
	"discovery_test.go:226: discWaitConnected: the callback polls no statically resolvable tool",
	"discovery_test.go:557: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:620: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:624: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:692: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:790: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:815: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:840: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:865: discWaitConnected: the callback polls no statically resolvable tool",
	"discovery_test.go:893: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"harness/wait.go:151: EventuallyPoll: bound is not static: liftWaitBound(bound, list_agents) is at least ListAgentsWaitBound 48s for list_agents (48s), checked by hand",
	"harness/wait.go:183: EventuallyPoll: bound is not static: liftWaitBound(bound, list_brokers) is at least ToolWaitBound 28s for list_brokers (28s), checked by hand",
	"harness/wait_test.go:206: EventuallyPoll: the callback polls no statically resolvable tool",
	"harness/wait_test.go:234: EventuallyPoll: the callback polls no statically resolvable tool",
	"harness/wait_test.go:565: CallUntilOK: the callback polls no statically resolvable tool",
	"harness/wait_test.go:586: CallUntilOK: the callback polls no statically resolvable tool",
	"journey_collaborate_test.go:43: collabWaitPeer: the callback polls no statically resolvable tool",
	"journey_collaborate_test.go:44: collabWaitPeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:204: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:221: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:287: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:308: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:309: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_public_broker_test.go:121: EventuallyPoll: bound is not static: caller-provided, callers pass discBound(1) 31s or EventTimeout 30s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:174: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:237: EventuallyPoll: bound is not static: max(discBound(1), ListAgentsWaitBound) = 48s for list_agents (48s), raised from 31s, checked by hand",
	"journey_public_broker_test.go:280: journeyWaitPeer: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:282: EventuallyPoll: bound is not static: max(discBound(1), ListAgentsWaitBound) = 48s for list_agents (48s), raised from 31s, checked by hand",
	"journey_public_broker_test.go:324: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:379: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:63: EventuallyPoll: the callback polls no statically resolvable tool",
	"links_test.go:120: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:121: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:157: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:190: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:251: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:252: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:341: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:342: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:371: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:392: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:397: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:462: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:463: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:496: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:497: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:506: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:507: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:92: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:93: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:98: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:99: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:129: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:130: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:198: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:199: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:218: EventuallyPoll: bound is not static: harness.ToolWaitBound(send_message) = 2*SendMessagePollBound = 68s for send_message (68s), checked by hand",
	"offline_test.go:360: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:361: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:391: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:392: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:410: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:411: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:417: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"offline_test.go:448: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"offline_test.go:54: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:55: lkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:155: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:156: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:186: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:260: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:353: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:90: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:91: pkWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:172: secWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:173: secWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:263: secWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:264: secWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:107: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:108: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:168: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:169: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:240: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:241: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:467: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:468: srWaitConnected: the callback polls no statically resolvable tool",
	"syscontainer/pair_test.go:94: PairAwait: the callback polls no statically resolvable tool",
	"topics_local_test.go:605: EventuallyPoll: bound is not static: caller-provided, callers pass TopicCallWaitBound 28s or EventTimeout 30s for list_topics (28s), checked by hand",
	"visibility_remote_test.go:208: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:209: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:351: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:352: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:86: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:87: vrWaitConnected: the callback polls no statically resolvable tool",
}

// boundGuardNotStaticReason is the reason the walk records for a wait whose
// bound it cannot evaluate. When a reviewer checks such a site by hand, the
// registration entry replaces this reason with the conclusion the reviewer
// reached, beginning with boundGuardHandCheckedPrefix. boundGuardSkipKey
// treats the two as the same call site, so the table can carry the conclusion
// while the walk keeps its base reason.
const (
	boundGuardNotStaticReason   = "the bound is not a static duration"
	boundGuardHandCheckedPrefix = "bound is not static:"
)

// boundGuardSkipKey reduces a skip or a registration entry to the identity the
// two are compared by. Every entry normally matches the walk's text exactly. A
// wait whose bound is not static matches any registration entry for the same
// call site whose reason is the hand-checked form, because the walk emits the
// bare reason while the table states the reviewed conclusion.
func boundGuardSkipKey(entry string) string {
	head, rest, ok := strings.Cut(entry, ": ") // "file:line", "owner: reason"
	if !ok {
		return entry
	}
	owner, reason, ok := strings.Cut(rest, ": ")
	if !ok {
		return entry
	}
	site := head + ": " + owner
	if reason == boundGuardNotStaticReason || strings.HasPrefix(reason, boundGuardHandCheckedPrefix) {
		return site + "::" + boundGuardNotStaticReason
	}
	return entry
}

// boundGuardCheckSkips compares the skips the walk recorded with
// boundGuardExpectedSkips in both directions.
func boundGuardCheckSkips(t *testing.T, skips []string) {
	t.Helper()
	sort.Strings(skips)
	actual := make(map[string]bool, len(skips))
	for _, skip := range skips {
		actual[boundGuardSkipKey(skip)] = true
	}
	expected := make(map[string]bool, len(boundGuardExpectedSkips))
	for _, skip := range boundGuardExpectedSkips {
		expected[boundGuardSkipKey(skip)] = true
	}
	for _, skip := range skips {
		if !expected[boundGuardSkipKey(skip)] {
			t.Errorf("unregistered wait bound skip, add it to boundGuardExpectedSkips with its reason: %s", skip)
		}
	}
	for _, skip := range boundGuardExpectedSkips {
		if !actual[boundGuardSkipKey(skip)] {
			t.Errorf("boundGuardExpectedSkips lists a skip the walk did not record, remove the stale entry: %s", skip)
		}
	}
	if len(skips) != len(boundGuardExpectedSkips) {
		t.Errorf("the walk recorded %d skipped wait call sites, boundGuardExpectedSkips lists %d", len(skips), len(boundGuardExpectedSkips))
	}
}

// boundGuardDuration evaluates expr as a duration. It understands integer
// literals, time units, arithmetic, the max and min builtins, package-level
// constants in consts and the harness and mcpserver constants above.
func boundGuardDuration(expr ast.Expr, consts map[string]ast.Expr, stack map[string]bool) (time.Duration, bool) {
	value, ok := boundGuardValue(expr, consts, stack)
	if !ok || !value.duration {
		return 0, false
	}
	return time.Duration(value.count), true
}

// boundGuardNumber is an evaluated number, either a plain count or a duration
// in nanoseconds.
type boundGuardNumber struct {
	count    int64
	duration bool
}

// boundGuardValue evaluates expr to a count or a duration.
func boundGuardValue(expr ast.Expr, consts map[string]ast.Expr, stack map[string]bool) (boundGuardNumber, bool) {
	switch e := expr.(type) {
	case *ast.ParenExpr:
		return boundGuardValue(e.X, consts, stack)
	case *ast.BasicLit:
		if e.Kind != token.INT {
			return boundGuardNumber{}, false
		}
		n, err := strconv.ParseInt(e.Value, 0, 64)
		if err != nil {
			return boundGuardNumber{}, false
		}
		return boundGuardNumber{count: n}, true
	case *ast.SelectorExpr:
		pkg, ok := e.X.(*ast.Ident)
		if !ok {
			return boundGuardNumber{}, false
		}
		if pkg.Name == "time" {
			if unit, ok := boundGuardTimeUnit(e.Sel.Name); ok {
				return boundGuardNumber{count: int64(unit), duration: true}, true
			}
			return boundGuardNumber{}, false
		}
		if pkg.Name != "harness" && pkg.Name != "mcpserver" {
			return boundGuardNumber{}, false
		}
		if d, ok := boundGuardExternDurations[e.Sel.Name]; ok {
			return boundGuardNumber{count: int64(d), duration: true}, true
		}
		return boundGuardNumber{}, false
	case *ast.Ident:
		if stack[e.Name] {
			return boundGuardNumber{}, false
		}
		if value, ok := consts[e.Name]; ok {
			stack[e.Name] = true
			got, ok := boundGuardValue(value, consts, stack)
			delete(stack, e.Name)
			return got, ok
		}
		if d, ok := boundGuardExternDurations[e.Name]; ok {
			return boundGuardNumber{count: int64(d), duration: true}, true
		}
		return boundGuardNumber{}, false
	case *ast.BinaryExpr:
		left, ok := boundGuardValue(e.X, consts, stack)
		if !ok {
			return boundGuardNumber{}, false
		}
		right, ok := boundGuardValue(e.Y, consts, stack)
		if !ok {
			return boundGuardNumber{}, false
		}
		switch e.Op {
		case token.MUL:
			switch {
			case left.duration && right.duration:
				return boundGuardNumber{}, false
			case !left.duration && !right.duration:
				return boundGuardNumber{count: left.count * right.count}, true
			case left.duration:
				return boundGuardNumber{count: left.count * right.count, duration: true}, true
			default:
				return boundGuardNumber{count: left.count * right.count, duration: true}, true
			}
		case token.QUO:
			if right.count == 0 {
				return boundGuardNumber{}, false
			}
			switch {
			case left.duration && !right.duration:
				return boundGuardNumber{count: left.count / right.count, duration: true}, true
			case !left.duration && !right.duration:
				return boundGuardNumber{count: left.count / right.count}, true
			}
			return boundGuardNumber{}, false
		case token.ADD, token.SUB:
			if left.duration != right.duration {
				return boundGuardNumber{}, false
			}
			count := left.count + right.count
			if e.Op == token.SUB {
				count = left.count - right.count
			}
			return boundGuardNumber{count: count, duration: left.duration}, true
		}
		return boundGuardNumber{}, false
	case *ast.CallExpr:
		id, ok := callFun(e.Fun).(*ast.Ident)
		if !ok || len(e.Args) != 2 || (id.Name != "max" && id.Name != "min") {
			return boundGuardNumber{}, false
		}
		left, ok := boundGuardValue(e.Args[0], consts, stack)
		if !ok {
			return boundGuardNumber{}, false
		}
		right, ok := boundGuardValue(e.Args[1], consts, stack)
		if !ok || left.duration != right.duration {
			return boundGuardNumber{}, false
		}
		if id.Name == "max" {
			if left.count >= right.count {
				return left, true
			}
			return right, true
		}
		if left.count <= right.count {
			return left, true
		}
		return right, true
	}
	return boundGuardNumber{}, false
}

// boundGuardTimeUnit returns the duration of a time package unit name.
func boundGuardTimeUnit(name string) (time.Duration, bool) {
	switch name {
	case "Nanosecond":
		return time.Nanosecond, true
	case "Microsecond":
		return time.Microsecond, true
	case "Millisecond":
		return time.Millisecond, true
	case "Second":
		return time.Second, true
	case "Minute":
		return time.Minute, true
	case "Hour":
		return time.Hour, true
	}
	return 0, false
}

// boundGuardConsts collects the package-level duration constants declared in
// files, mapped to their value expressions.
func boundGuardConsts(files []*ast.File) map[string]ast.Expr {
	consts := make(map[string]ast.Expr)
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok || len(value.Names) != len(value.Values) {
					continue
				}
				for i, name := range value.Names {
					consts[name.Name] = value.Values[i]
				}
			}
		}
	}
	return consts
}

// boundGuardToolFuncs maps each function in files to the tools it polls,
// directly or through a same-directory helper.
func boundGuardToolFuncs(files []*ast.File) map[string]map[string]bool {
	funcs := make(map[string]*ast.FuncDecl)
	direct := make(map[string]map[string]bool)
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			funcs[fn.Name.Name] = fn
			direct[fn.Name.Name] = boundGuardDirectTools(fn.Body)
		}
	}
	all := make(map[string]map[string]bool, len(direct))
	for name, tools := range direct {
		all[name] = boundGuardCloneTools(tools)
	}
	for changed := true; changed; {
		changed = false
		for name, fn := range funcs {
			before := len(all[name])
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee, ok := callName(call)
				if !ok {
					return true
				}
				boundGuardUnionTools(all[name], all[callee])
				return true
			})
			if len(all[name]) != before {
				changed = true
			}
		}
	}
	return all
}

// boundGuardDirectTools returns the tools a body polls through a poll call with
// a literal tool name.
func boundGuardDirectTools(body *ast.BlockStmt) map[string]bool {
	tools := make(map[string]bool)
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		boundGuardAddPollTool(call, tools)
		return true
	})
	return tools
}

// boundGuardAddPollTool adds the tool of a poll call with a literal tool name.
func boundGuardAddPollTool(call *ast.CallExpr, tools map[string]bool) {
	name, ok := callName(call)
	if !ok {
		return
	}
	var at int
	switch name {
	case "PollCall":
		at = 2
	case "PollCallWithin", "pollDecode", "sysPollDecode":
		at = 3
	default:
		return
	}
	if at >= len(call.Args) {
		return
	}
	lit, ok := call.Args[at].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}
	tool, err := strconv.Unquote(lit.Value)
	if err != nil || ToolPollBound(tool) == ToolCallPollBound {
		return
	}
	tools[tool] = true
}

// boundGuardCallbackTools returns the tools a wait callback polls.
func boundGuardCallbackTools(expr ast.Expr, closures map[string]*ast.FuncLit, toolFuncs map[string]map[string]bool) map[string]bool {
	switch value := expr.(type) {
	case *ast.FuncLit:
		return boundGuardBodyTools(value.Body, toolFuncs)
	case *ast.Ident:
		if lit := closures[value.Name]; lit != nil {
			return boundGuardBodyTools(lit.Body, toolFuncs)
		}
		if tools, ok := toolFuncs[value.Name]; ok {
			return boundGuardCloneTools(tools)
		}
	}
	return nil
}

// boundGuardBodyTools returns the tools a callback body polls directly or
// through a same-directory helper.
func boundGuardBodyTools(body *ast.BlockStmt, toolFuncs map[string]map[string]bool) map[string]bool {
	tools := make(map[string]bool)
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		boundGuardAddPollTool(call, tools)
		if callee, ok := callName(call); ok {
			boundGuardUnionTools(tools, toolFuncs[callee])
		}
		return true
	})
	return tools
}

// boundGuardDispatches maps each function that runs a wait whose bound is one of
// its parameters to that parameter position and the wait's tools. A function
// that forwards its bound to another dispatch is included by the fixpoint.
func boundGuardDispatches(files []*ast.File, toolFuncs map[string]map[string]bool) map[string]*boundGuardDispatch {
	type builder struct {
		boundPositions    []int
		callbackPositions []int
		tools             map[string]bool
	}
	funcs := make(map[string]*ast.FuncDecl)
	params := make(map[string]map[string]int)
	builders := make(map[string]*builder)
	wrappers := wrapperFuncs(files)
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			name := fn.Name.Name
			funcs[name] = fn
			params[name] = boundGuardParamPositions(fn.Type)
			b := &builder{
				callbackPositions: append([]int(nil), wrappers[name]...),
				tools:             make(map[string]bool),
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				wait, ok := waitCallName(call)
				if !ok || wait == "ConsistentlyPoll" {
					return true
				}
				if len(call.Args) >= 2 {
					if id, ok := call.Args[1].(*ast.Ident); ok {
						if at, ok := params[name][id.Name]; ok {
							b.boundPositions = append(b.boundPositions, at)
						}
					}
				}
				if lit, ok := lastArg(call).(*ast.FuncLit); ok {
					boundGuardUnionTools(b.tools, boundGuardBodyTools(lit.Body, toolFuncs))
				}
				return true
			})
			builders[name] = b
		}
	}
	for changed := true; changed; {
		changed = false
		for name, fn := range funcs {
			b := builders[name]
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee, ok := callName(call)
				if !ok {
					return true
				}
				other := builders[callee]
				if other == nil || len(other.boundPositions) != 1 || other.boundPositions[0] >= len(call.Args) {
					return true
				}
				id, ok := call.Args[other.boundPositions[0]].(*ast.Ident)
				if !ok {
					return true
				}
				at, ok := params[name][id.Name]
				if !ok {
					return true
				}
				before := len(b.tools) + len(b.boundPositions) + len(b.callbackPositions)
				if !boundGuardHasInt(b.boundPositions, at) {
					b.boundPositions = append(b.boundPositions, at)
				}
				boundGuardUnionTools(b.tools, other.tools)
				for _, pos := range other.callbackPositions {
					if !boundGuardHasInt(b.callbackPositions, pos) {
						b.callbackPositions = append(b.callbackPositions, pos)
					}
				}
				if len(b.tools)+len(b.boundPositions)+len(b.callbackPositions) != before {
					changed = true
				}
				return true
			})
		}
	}
	dispatches := make(map[string]*boundGuardDispatch)
	for name, b := range builders {
		boundPos := -1
		if len(b.boundPositions) == 1 {
			boundPos = b.boundPositions[0]
		}
		dispatches[name] = &boundGuardDispatch{
			boundPos:          boundPos,
			callbackPositions: b.callbackPositions,
			tools:             b.tools,
		}
	}
	return dispatches
}

// boundGuardParamPositions maps every named parameter of ft to its call
// position.
func boundGuardParamPositions(ft *ast.FuncType) map[string]int {
	positions := make(map[string]int)
	if ft == nil || ft.Params == nil {
		return positions
	}
	position := 0
	for _, field := range ft.Params.List {
		width := len(field.Names)
		if width == 0 {
			width = 1
		}
		for i, name := range field.Names {
			positions[name.Name] = position + i
		}
		position += width
	}
	return positions
}

// boundGuardCloneTools returns a copy of tools.
func boundGuardCloneTools(tools map[string]bool) map[string]bool {
	if len(tools) == 0 {
		return make(map[string]bool)
	}
	out := make(map[string]bool, len(tools))
	for tool := range tools {
		out[tool] = true
	}
	return out
}

// boundGuardUnionTools adds every tool in more to dst.
func boundGuardUnionTools(dst, more map[string]bool) {
	for tool := range more {
		dst[tool] = true
	}
}

// boundGuardHasInt reports whether values contains want.
func boundGuardHasInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
