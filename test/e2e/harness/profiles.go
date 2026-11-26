package harness

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// The product timers FastTimers configures are exported so a test that derives
// a wait bound from one tracks the profile instead of repeating a literal that
// can drift.
const (
	// FastReconnectMax is link.reconnect_max in FastTimers: the longest the
	// reconnect backoff waits between attempts.
	FastReconnectMax = 400 * time.Millisecond

	// FastDialTimeout is link.dial_timeout in FastTimers. It is the production
	// default, so a successful dial keeps the headroom a production Broker has;
	// a short test value could be crossed by a healthy dial on a loaded runner.
	FastDialTimeout = 5 * time.Second

	// FastRequestTimeout is send.request_timeout in FastTimers, the sender's
	// budget for one send. It is sized far above the worst normal send latency
	// because too short a value reports a send that would have completed as
	// delivery_unknown under load; a case that asserts that result configures a
	// short request_timeout of its own.
	FastRequestTimeout = 30 * time.Second
)

// FastTimers shortens the timers a scenario does not need at production
// length: the reconnect backoff, the send link backoff and the advertised
// address check. request_timeout and dial_timeout keep generous values, and
// only a case that asserts a timeout overrides them.
var FastTimers = fmt.Sprintf(`link:
  reconnect_min: 100ms
  reconnect_max: %s
  dial_timeout: %s
send:
  link_backoff: 100ms
  request_timeout: %s
broker:
  address_check_interval: 500ms
`, FastReconnectMax, FastDialTimeout, FastRequestTimeout)

// ConfigYAML merges YAML fragments by key, with later fragments overriding
// earlier leaves, and returns the result in a deterministic key order. Empty
// fragments are ignored.
func ConfigYAML(parts ...string) string {
	merged := map[string]any{}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		var fragment map[string]any
		if err := yaml.Unmarshal([]byte(part), &fragment); err != nil {
			panic(fmt.Sprintf("harness.ConfigYAML: invalid YAML fragment: %v", err))
		}
		mergeConfig(merged, fragment)
	}
	if len(merged) == 0 {
		return ""
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(merged); err != nil {
		panic(fmt.Sprintf("harness.ConfigYAML: encoding merged configuration: %v", err))
	}
	if err := enc.Close(); err != nil {
		panic(fmt.Sprintf("harness.ConfigYAML: encoding merged configuration: %v", err))
	}
	return buf.String()
}

func mergeConfig(dst, src map[string]any) {
	for key, value := range src {
		if srcMap, ok := value.(map[string]any); ok {
			if dstMap, ok := dst[key].(map[string]any); ok {
				mergeConfig(dstMap, srcMap)
				continue
			}
		}
		dst[key] = value
	}
}
