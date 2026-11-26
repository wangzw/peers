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

type boundGuardSource struct {
	path string
	file *ast.File
}

// boundGuardDispatch describes a function whose wait bound is one of its
// parameters; boundPos is that position, or -1 when the function is not a
// dispatch.
type boundGuardDispatch struct {
	boundPos          int
	callbackPositions []int
	tools             map[string]bool
}

// boundGuardExternDurations resolves a duration constant a wait bound names from
// another package; the mcpserver entries are product timers the harness values
// do not cover.
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
// polled, recording a skip and returning false when the tool or the bound cannot
// be resolved; a caller must not count that site as checked.
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

func boundGuardSkip(skips *[]string, fset *token.FileSet, e2eDir string, call *ast.CallExpr, owner, reason string) {
	pos := fset.Position(call.Lparen)
	name := pos.Filename
	if rel, err := filepath.Rel(e2eDir, name); err == nil {
		name = rel
	}
	*skips = append(*skips, fmt.Sprintf("%s:%d: %s: %s", filepath.ToSlash(name), pos.Line, owner, reason))
}

// boundGuardExpectedSkips lists every wait call site TestWaitBoundsCoverPolledToolCalls
// cannot check, each with its reason; a skip without an entry and a stale entry
// both fail the test, and an entry whose bound is not static states the
// hand-checked conclusion in the form "bound is not static: ... checked by hand".
var boundGuardExpectedSkips = []string{
	"addresses_test.go:96: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:98: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:103: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:111: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:112: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:120: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:121: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:141: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:143: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:150: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:151: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:170: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:175: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:176: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:181: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:182: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:187: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:188: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:196: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:197: adWaitPeerAddresses: the callback polls no statically resolvable tool",
	"addresses_test.go:65: adWaitConnected: the callback polls no statically resolvable tool",
	"addresses_test.go:81: adWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:85: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:91: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:99: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:117: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:125: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:133: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:153: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:162: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:201: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:259: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:339: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:345: alWaitPeerAlias: the callback polls no statically resolvable tool",
	"aliases_test.go:408: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:587: EventuallyPoll: bound is not static: caller-provided, every caller passes ListAgentsWaitBound 48s for list_agents (48s), checked by hand",
	"aliases_test.go:70: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:71: alWaitConnected: the callback polls no statically resolvable tool",
	"aliases_test.go:79: alWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:179: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:180: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:224: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:225: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:264: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:265: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:312: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:313: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:383: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:447: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:69: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:80: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:70: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_cluster_test.go:81: pkWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_test.go:171: alWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_test.go:172: alWaitConnected: the callback polls no statically resolvable tool",
	"config_tools_test.go:190: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:316: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:358: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:392: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:434: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:445: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:478: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"config_tools_test.go:567: ctWaitConfigValue: the callback polls no statically resolvable tool",
	"degraded_run_test.go:215: EventuallyPoll: the callback polls no statically resolvable tool",
	"degraded_run_test.go:291: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:292: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:371: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:390: srWaitConnected: the callback polls no statically resolvable tool",
	"degraded_run_test.go:58: EventuallyPoll: the callback polls no statically resolvable tool",
	"discovery_address_test.go:101: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:105: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:118: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:126: dadWaitPeerAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:127: dadWaitCardAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:132: dadWaitPeerAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:135: dadWaitCardAddresses: the callback polls no statically resolvable tool",
	"discovery_address_test.go:159: dadWaitCardAt: the callback polls no statically resolvable tool",
	"discovery_address_test.go:190: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:194: adWaitConnected: the callback polls no statically resolvable tool",
	"discovery_address_test.go:57: dadWaitCardAt: the callback polls no statically resolvable tool",
	"discovery_address_test.go:68: dadWaitCardAddresses: the callback polls no statically resolvable tool",
	"discovery_expiry_test.go:84: dxWaitCard: the callback polls no statically resolvable tool",
	"discovery_expiry_test.go:145: dxWaitCard: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:132: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:173: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:227: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:427: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:490: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_hostile_test.go:531: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:918: discWaitPublicTopic: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:920: discWaitPublicTopicGone: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:931: discWaitPublicTopic: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:951: discWaitPublicTopic: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:953: discWaitPublicTopicGone: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:956: discWaitPublicTopic: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:959: discWaitPublicTopicGone: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_topics (28s), checked by hand",
	"discovery_test.go:964: EventuallyPoll: bound is not static: caller-provided, callers pass discBound(1) 31s or discBound(2) 32s for list_topics (28s), checked by hand",
	"discovery_test.go:978: EventuallyPoll: bound is not static: caller-provided, callers pass discBound(1) 31s or discBound(2) 32s for list_topics (28s), checked by hand",
	"discovery_test.go:149: EventuallyPoll: the callback polls no statically resolvable tool",
	"discovery_test.go:192: discWaitConnected: the callback polls no statically resolvable tool",
	"discovery_test.go:193: discWaitConnected: the callback polls no statically resolvable tool",
	"discovery_test.go:489: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"discovery_test.go:550: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:554: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:616: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:706: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:731: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:755: discWaitBroker: the callback polls no statically resolvable tool",
	"discovery_test.go:780: discWaitConnected: the callback polls no statically resolvable tool",
	"discovery_test.go:808: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"harness/wait.go:113: EventuallyPoll: bound is not static: liftWaitBound(bound, list_agents) is at least ListAgentsWaitBound 48s for list_agents (48s), checked by hand",
	"harness/wait.go:131: EventuallyPoll: bound is not static: liftWaitBound(bound, list_brokers) is at least ToolWaitBound 28s for list_brokers (28s), checked by hand",
	"harness/wait_test.go:204: EventuallyPoll: the callback polls no statically resolvable tool",
	"harness/wait_test.go:232: EventuallyPoll: the callback polls no statically resolvable tool",
	"harness/wait_test.go:542: CallUntilOK: the callback polls no statically resolvable tool",
	"harness/wait_test.go:562: CallUntilOK: the callback polls no statically resolvable tool",
	"journey_collaborate_test.go:37: collabWaitPeer: the callback polls no statically resolvable tool",
	"journey_collaborate_test.go:38: collabWaitPeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:183: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:199: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:259: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:277: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_pair_test.go:278: pairWaitOraclePeer: the callback polls no statically resolvable tool",
	"journey_public_broker_test.go:107: EventuallyPoll: bound is not static: caller-provided, callers pass discBound(1) 31s or EventTimeout 30s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:157: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:214: EventuallyPoll: bound is not static: max(discBound(1), ListAgentsWaitBound) = 48s for list_agents (48s), raised from 31s, checked by hand",
	"journey_public_broker_test.go:254: journeyWaitPeer: bound is not static: discBound(1) = 1*1s + EventTimeout = 31s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:256: EventuallyPoll: bound is not static: max(discBound(1), ListAgentsWaitBound) = 48s for list_agents (48s), raised from 31s, checked by hand",
	"journey_public_broker_test.go:297: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:348: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"journey_public_broker_test.go:56: EventuallyPoll: the callback polls no statically resolvable tool",
	"links_test.go:102: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:103: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:137: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:169: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:229: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:230: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:308: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:309: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:336: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:356: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:360: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:413: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:414: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:442: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:443: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:450: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:451: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:79: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:80: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:85: lkWaitConnected: the callback polls no statically resolvable tool",
	"links_test.go:86: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:115: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:116: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:175: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:176: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:192: EventuallyPoll: bound is not static: harness.ToolWaitBound(send_message) = 2*SendMessagePollBound = 68s for send_message (68s), checked by hand",
	"offline_test.go:326: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:327: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:356: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:357: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:373: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:374: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:380: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_brokers (28s), checked by hand",
	"offline_test.go:409: EventuallyPoll: bound is not static: discBound(2) = 2*1s + EventTimeout = 32s for list_topics (28s), checked by hand",
	"offline_test.go:47: lkWaitConnected: the callback polls no statically resolvable tool",
	"offline_test.go:48: lkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:132: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:133: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:156: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:226: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:310: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:79: pkWaitConnected: the callback polls no statically resolvable tool",
	"pairing_keys_test.go:80: pkWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:147: secWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:148: secWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:232: secWaitConnected: the callback polls no statically resolvable tool",
	"security_test.go:233: secWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:77: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:78: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:132: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:133: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:197: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:198: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:400: srWaitConnected: the callback polls no statically resolvable tool",
	"send_remote_test.go:401: srWaitConnected: the callback polls no statically resolvable tool",
	"syscontainer/pair_test.go:101: PairAwait: the callback polls no statically resolvable tool",
	"topics_local_test.go:544: EventuallyPoll: bound is not static: caller-provided, callers pass TopicCallWaitBound 28s or EventTimeout 30s for list_topics (28s), checked by hand",
	"visibility_remote_test.go:181: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:182: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:308: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:309: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:67: vrWaitConnected: the callback polls no statically resolvable tool",
	"visibility_remote_test.go:68: vrWaitConnected: the callback polls no statically resolvable tool",
}

// boundGuardNotStaticReason is the reason the walk records for a wait whose
// bound it cannot evaluate; a hand-checked registration entry replaces it with
// boundGuardHandCheckedPrefix and the conclusion, and boundGuardSkipKey treats
// both as the same call site.
const (
	boundGuardNotStaticReason   = "the bound is not a static duration"
	boundGuardHandCheckedPrefix = "bound is not static:"
)

// boundGuardSkipKey reduces a skip or a registration entry to the identity the
// two are compared by; a hand-checked conclusion matches the walk's bare
// non-static reason at the same site.
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

// boundGuardDuration evaluates expr as a duration, understanding integer
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
// its parameters to that position and the wait's tools; a function that forwards
// its bound to another dispatch is included by the fixpoint.
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

func boundGuardUnionTools(dst, more map[string]bool) {
	for tool := range more {
		dst[tool] = true
	}
}

func boundGuardHasInt(values []int, want int) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
