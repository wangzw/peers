package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// KeyKind is the type of a configuration key.
type KeyKind uint8

const (
	keyString KeyKind = iota
	keyBool
	keyInt
	keyDuration
	keyStringList
	keySecret
)

type keySpec struct {
	Name     string
	Kind     KeyKind
	Default  Value
	Dynamic  bool // default is computed by defaultAlias
	Secret   bool
	Doc      string // one English line describing the key, for the generated template
	MinInt   int64
	MaxInt   int64
	HasMax   bool
	MinDur   time.Duration
	Validate func(Value) error
}

// listenAddr is the normalized form of broker.listen.
type listenAddr struct {
	set  bool // false means do not listen
	all  bool // true means every local address
	host string
	port int
}

type defaultContext struct {
	mode     Mode
	hostname string
	account  string
}

func stringValue(s string) Value { return Value{Kind: KindString, String: s} }
func boolValue(b bool) Value     { return Value{Kind: KindBool, Bool: b} }
func intValue(i int64) Value     { return Value{Kind: KindInt, Int: i} }
func durationValue(d time.Duration) Value {
	return Value{Kind: KindDuration, Duration: d}
}
func stringListValue(s []string) Value {
	return Value{Kind: KindStringList, Strings: append([]string(nil), s...)}
}
func secretValue(s string) Value {
	return Value{Kind: KindSecret, String: s, SecretIsSet: s != ""}
}

func positiveInt() func(Value) error {
	return func(v Value) error {
		if v.Int <= 0 {
			return errors.New("must be greater than 0")
		}
		return nil
	}
}

func nonNegativeInt() func(Value) error {
	return func(v Value) error {
		if v.Int < 0 {
			return errors.New("must be greater than or equal to 0")
		}
		return nil
	}
}

func positiveDuration() func(Value) error {
	return func(v Value) error {
		if v.Duration <= 0 {
			return errors.New("must be greater than 0")
		}
		return nil
	}
}

var logLevels = map[string]bool{"debug": true, "info": true, "warn": true, "error": true}

// keyTable is the complete set of configuration keys. Order is irrelevant;
// allKeyNames sorts.
var keyTable = map[string]*keySpec{
	"broker.alias": {
		Name: "broker.alias", Kind: keyString, Dynamic: true,
		Doc:      "Alias this Broker reports: 1 to 24 characters from letters, digits, '.', '-' and '_'.",
		Validate: func(v Value) error { return validAlias(v.String) },
	},
	"broker.public": {
		Name: "broker.public", Kind: keyBool, Default: boolValue(false),
		Doc: "Whether this Broker is public.",
	},
	"broker.listen": {
		Name: "broker.listen", Kind: keyString, Default: stringValue(":17443"),
		Doc: "Address and port to bind; an empty host listens on every local address, and an empty value means do not listen.",
		Validate: func(v Value) error {
			_, err := parseListen(v.String)
			return err
		},
	},
	"broker.offline": {
		Name: "broker.offline", Kind: keyBool, Default: boolValue(false),
		Doc: "Refuse messages from other hosts while keeping links; peers service offline / online.",
	},
	"broker.advertise": {
		Name: "broker.advertise", Kind: keyString, Default: stringValue(""),
		Doc: "Addresses to advertise; valid forms are empty, host, host:port and :port.",
		Validate: func(v Value) error {
			_, err := parseAdvertise(v.String)
			return err
		},
	},
	"broker.advertise_extra": {
		Name: "broker.advertise_extra", Kind: keyStringList, Default: stringListValue(nil),
		Doc: "Extra addresses to advertise; each item is host or host:port.",
		Validate: func(v Value) error {
			for _, item := range v.Strings {
				if err := validateExtraItem(item); err != nil {
					return err
				}
			}
			return nil
		},
	},
	"broker.address_check_interval": {
		Name: "broker.address_check_interval", Kind: keyDuration,
		Doc:     "Interval for re-enumerating the host's addresses.",
		Default: durationValue(time.Minute), Validate: positiveDuration(),
	},
	"broker.local_socket_group": {
		Name: "broker.local_socket_group", Kind: keyString, Default: stringValue(""),
		Doc: "Group owning the local socket; empty keeps the runtime directory's group, and user mode ignores it.",
		Validate: func(v Value) error {
			if v.String == "" {
				return nil
			}
			if strings.ContainsAny(v.String, " \t\r\n:/") {
				return errors.New("must not contain whitespace, ':', or '/'")
			}
			return nil
		},
	},
	"broker.local_socket_mode": {
		Name: "broker.local_socket_mode", Kind: keyString, Default: stringValue("0666"),
		Doc: "File mode of the local socket; user mode always uses 0600.",
		Validate: func(v Value) error {
			if err := validSocketMode(v.String); err != nil {
				return err
			}
			return nil
		},
	},
	"pairing.static_key": {
		Name: "pairing.static_key", Kind: keySecret, Secret: true, Default: secretValue(""),
		Doc: "Static pairing key of 16 to 128 characters; a non-empty value takes effect.",
		Validate: func(v Value) error {
			if v.String == "" {
				return nil
			}
			n := utf8.RuneCountInString(v.String)
			if n < 16 || n > 128 {
				return fmt.Errorf("length must be between 16 and 128 characters, got %d", n)
			}
			return nil
		},
	},
	"pairing.max_failures": {
		Name: "pairing.max_failures", Kind: keyInt, Default: intValue(5), Validate: positiveInt(),
		Doc: "Failed verifications allowed before a one-time pairing key is invalidated.",
	},
	"pairing.max_attempts_per_minute": {
		Name: "pairing.max_attempts_per_minute", Kind: keyInt, Default: intValue(10), Validate: positiveInt(),
		Doc: "Unmatched pairing requests allowed per source per minute; all sources together are limited to 10 times this, and static-key matches do not count.",
	},
	"topics.auto_join": {
		Name: "topics.auto_join", Kind: keyBool, Default: boolValue(true),
		Doc: "Whether to join the default topic automatically.",
	},
	"directory.reconcile_interval": {
		Name: "directory.reconcile_interval", Kind: keyDuration,
		Doc:     "Interval for periodic reconciliation.",
		Default: durationValue(10 * time.Minute), Validate: positiveDuration(),
	},
	"directory.max_records": {
		Name: "directory.max_records", Kind: keyInt, Default: intValue(1000), Validate: positiveInt(),
		Doc: "Maximum number of directory records in total.",
	},
	"directory.max_records_per_link": {
		Name: "directory.max_records_per_link", Kind: keyInt, Default: intValue(250), Validate: positiveInt(),
		Doc: "Maximum number of records accepted over one link.",
	},
	"directory.max_topics_per_publisher": {
		Name: "directory.max_topics_per_publisher", Kind: keyInt, Default: intValue(16), Validate: positiveInt(),
		Doc: "Maximum public topic declarations plus unknown-kind records per publisher; cards and withdrawals are not counted.",
	},
	"send.link_retries": {
		Name: "send.link_retries", Kind: keyInt, Default: intValue(5), Validate: nonNegativeInt(),
		Doc: "Retries while the link is not up.",
	},
	"send.link_backoff": {
		Name: "send.link_backoff", Kind: keyDuration,
		Doc:     "Wait before the first retry; it doubles each time, capped at 8 seconds.",
		Default: durationValue(500 * time.Millisecond), Validate: positiveDuration(),
	},
	"send.request_timeout": {
		Name: "send.request_timeout", Kind: keyDuration,
		Doc:     "Budget for one send call, i.e. the total time limit.",
		Default: durationValue(20 * time.Second), Validate: positiveDuration(),
	},
	"send.inbox_retries": {
		Name: "send.inbox_retries", Kind: keyInt, Default: intValue(2), Validate: nonNegativeInt(),
		Doc: "Retries when the inbox write definitely failed.",
	},
	"send.inbox_backoff": {
		Name: "send.inbox_backoff", Kind: keyDuration,
		Doc:     "Wait between inbox write retries.",
		Default: durationValue(200 * time.Millisecond), Validate: positiveDuration(),
	},
	"send.inbox_timeout": {
		Name: "send.inbox_timeout", Kind: keyDuration,
		Doc:     "Time limit for a single inbox write.",
		Default: durationValue(5 * time.Second), Validate: positiveDuration(),
	},
	"send.dedupe_window": {
		Name: "send.dedupe_window", Kind: keyDuration,
		Doc:     "How long deduplication table entries are kept.",
		Default: durationValue(5 * time.Minute), Validate: positiveDuration(),
	},
	"link.dial_timeout": {
		Name: "link.dial_timeout", Kind: keyDuration,
		Doc:     "Connection time limit for each address during dialing.",
		Default: durationValue(5 * time.Second), Validate: positiveDuration(),
	},
	"link.reconnect_min": {
		Name: "link.reconnect_min", Kind: keyDuration,
		Doc:     "Initial backoff for re-establishing a link.",
		Default: durationValue(time.Second), Validate: positiveDuration(),
	},
	"link.reconnect_max": {
		Name: "link.reconnect_max", Kind: keyDuration,
		Doc:     "Maximum backoff for re-establishing a link.",
		Default: durationValue(time.Minute), Validate: positiveDuration(),
	},
	"link.keepalive_interval": {
		Name: "link.keepalive_interval", Kind: keyDuration,
		Doc:     "Keepalive probe interval; the gRPC client probes at least every 10 seconds.",
		Default: durationValue(15 * time.Second), Validate: positiveDuration(),
	},
	"link.keepalive_timeout": {
		Name: "link.keepalive_timeout", Kind: keyDuration,
		Doc:     "Response time limit for a keepalive probe.",
		Default: durationValue(10 * time.Second), Validate: positiveDuration(),
	},
	"log.level": {
		Name: "log.level", Kind: keyString, Default: stringValue("info"),
		Doc: "Runtime log level.",
		Validate: func(v Value) error {
			if !logLevels[v.String] {
				return errors.New("must be one of debug, info, warn, error")
			}
			return nil
		},
	},
	"update.check": {
		Name: "update.check", Kind: keyBool, Default: boolValue(true),
		Doc: "Periodically check GitHub for a newer release.",
	},
	"update.check_interval": {
		Name: "update.check_interval", Kind: keyDuration,
		Default: durationValue(24 * time.Hour), MinDur: time.Hour,
		Doc: "Interval between GitHub release checks; values below 1h are invalid.",
	},
}

func lookupKey(name string) (*keySpec, bool) {
	spec, ok := keyTable[name]
	return spec, ok
}

func allKeyNames() []string {
	names := make([]string, 0, len(keyTable))
	for name := range keyTable {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func valueKindForSpec(spec *keySpec) ValueKind {
	switch spec.Kind {
	case keyString:
		return KindString
	case keyBool:
		return KindBool
	case keyInt:
		return KindInt
	case keyDuration:
		return KindDuration
	case keyStringList:
		return KindStringList
	case keySecret:
		return KindSecret
	}
	return KindUnset
}

func defaultValue(spec *keySpec, ctx defaultContext) Value {
	if spec.Dynamic {
		return stringValue(defaultAlias(ctx.mode, ctx.hostname, ctx.account))
	}
	return copyValue(spec.Default)
}

func validateValue(spec *keySpec, v Value) error {
	if want := valueKindForSpec(spec); v.Kind != want {
		return errors.New("type does not match the key")
	}
	if spec.Secret && spec.Name == "pairing.static_key" && v.String == "" {
		return nil // empty means unset and is always valid
	}
	if spec.Validate != nil {
		if err := spec.Validate(v); err != nil {
			return err
		}
	}
	if spec.Kind == keyInt {
		if spec.MinInt != 0 || spec.HasMax {
			if v.Int < spec.MinInt {
				return fmt.Errorf("must be at least %d", spec.MinInt)
			}
			if spec.HasMax && v.Int > spec.MaxInt {
				return fmt.Errorf("must be at most %d", spec.MaxInt)
			}
		}
	}
	if spec.Kind == keyDuration && spec.MinDur != 0 && v.Duration < spec.MinDur {
		return fmt.Errorf("must be at least %s", spec.MinDur)
	}
	return nil
}

func validSocketMode(s string) error {
	if len(s) < 3 || len(s) > 4 {
		return errors.New("must be 3 or 4 octal digits")
	}
	for _, r := range s {
		if r < '0' || r > '7' {
			return errors.New("must be 3 or 4 octal digits")
		}
	}
	n, err := strconv.ParseUint(s, 8, 32)
	if err != nil || n > 0o777 {
		return errors.New("must not exceed 0777")
	}
	return nil
}

// crossKeyGroups lists the keys that constrain one another. Set and reload
// judge a group as a whole: the group's members take their new values
// together, or every member keeps its old value. The listen/advertise group is
// handled separately because it also depends on the effective listen address
// and the default-route enumeration.
var crossKeyGroups = [][]string{
	{"link.reconnect_min", "link.reconnect_max"},
	{"link.keepalive_interval", "link.keepalive_timeout"},
}

// addressGroupKeys are the keys that together determine the effective listen
// address and the advertised address list. Set and reload judge them as one
// group.
var addressGroupKeys = []string{"broker.listen", "broker.advertise", "broker.advertise_extra"}

// isAddressGroupKey reports whether key belongs to the address group.
func isAddressGroupKey(name string) bool {
	for _, key := range addressGroupKeys {
		if key == name {
			return true
		}
	}
	return false
}

// groupForKey returns the cross-key group containing key, or nil when key has
// no group. The returned slice must not be modified.
func groupForKey(key string) []string {
	for _, group := range crossKeyGroups {
		for _, name := range group {
			if name == key {
				return group
			}
		}
	}
	return nil
}

// validateCrossKeys enforces the constraints that involve more than one key.
// They are pure value rules, so they live here rather than in a Validate
// callback registered by another package. It checks every group, which is
// harmless because a caller's candidate map always holds all keys.
func validateCrossKeys(values map[string]Value) error {
	minV, okMin := values["link.reconnect_min"]
	maxV, okMax := values["link.reconnect_max"]
	if okMin && okMax && maxV.Duration < minV.Duration {
		return wrapInvalid("link.reconnect_max", "must be greater than or equal to link.reconnect_min")
	}
	interval, okInterval := values["link.keepalive_interval"]
	timeout, okTimeout := values["link.keepalive_timeout"]
	if okInterval && okTimeout && timeout.Duration >= interval.Duration {
		return wrapInvalid("link.keepalive_timeout", "must be less than link.keepalive_interval")
	}
	return nil
}

// crossKeyError reports whether the candidate value for key violates its
// group's cross-key rule given the other current values.
func crossKeyError(values map[string]Value, key string) error {
	if groupForKey(key) == nil {
		return nil
	}
	return validateCrossKeys(values)
}

func parseListen(s string) (listenAddr, error) {
	if s == "" {
		return listenAddr{}, nil
	}
	host, portText, err := splitHostPort(s)
	if err != nil {
		return listenAddr{}, err
	}
	port, err := parsePort(portText)
	if err != nil {
		return listenAddr{}, err
	}
	all := host == "" || host == "0.0.0.0" || host == "::"
	return listenAddr{set: true, all: all, host: host, port: port}, nil
}

func parsePort(s string) (int, error) {
	if s == "" {
		return 0, errors.New("port is empty")
	}
	port, err := strconv.Atoi(s)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("port must be between 1 and 65535")
	}
	return port, nil
}

// splitHostPort accepts "host:port", ":port" and "[v6]:port".
func splitHostPort(s string) (host, port string, err error) {
	if strings.HasPrefix(s, "[") {
		end := strings.Index(s, "]")
		if end < 0 || end+1 >= len(s) || s[end+1] != ':' {
			return "", "", errors.New("must be host:port")
		}
		return s[1:end], s[end+2:], nil
	}
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return "", "", errors.New("must be host:port")
	}
	if strings.Contains(s[:i], ":") {
		return "", "", errors.New("IPv6 hosts must be written in brackets")
	}
	return s[:i], s[i+1:], nil
}

// parseNode converts one YAML node to a Value according to spec. It checks
// only the type, not the value.
func parseNode(spec *keySpec, n *yaml.Node, fromSetConfig bool) (Value, error) {
	n = resolveAliasNode(n)
	if n == nil {
		return Value{}, wrapInvalid(spec.Name, "missing value")
	}
	if spec.Kind == keyString && spec.Name == "broker.local_socket_mode" {
		if n.Kind == yaml.ScalarNode && (n.Tag == "!!str" || n.Tag == "!!int") {
			return stringValue(n.Value), nil
		}
		return Value{}, wrapInvalid(spec.Name, "must be a YAML string or integer")
	}
	switch spec.Kind {
	case keyString:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML string")
		}
		return stringValue(n.Value), nil
	case keyBool:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!bool" {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML boolean")
		}
		var b bool
		if err := n.Decode(&b); err != nil {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML boolean")
		}
		return boolValue(b), nil
	case keyInt:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!int" {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML integer")
		}
		var i int64
		if err := n.Decode(&i); err != nil {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML integer")
		}
		return intValue(i), nil
	case keyDuration:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML string holding a duration")
		}
		d, err := time.ParseDuration(n.Value)
		if err != nil {
			return Value{}, wrapInvalid(spec.Name, "must be a duration such as 10m or 500ms")
		}
		return durationValue(d), nil
	case keyStringList:
		if n.Kind != yaml.SequenceNode {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML sequence")
		}
		if fromSetConfig && n.Style&yaml.FlowStyle == 0 {
			return Value{}, wrapInvalid(spec.Name, "must be a flow sequence such as [a, b]")
		}
		out := make([]string, 0, len(n.Content))
		for _, item := range n.Content {
			item = resolveAliasNode(item)
			if item == nil || item.Kind != yaml.ScalarNode || item.Tag != "!!str" {
				return Value{}, wrapInvalid(spec.Name, "every element must be a YAML string")
			}
			out = append(out, item.Value)
		}
		return stringListValue(out), nil
	case keySecret:
		if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
			return Value{}, wrapInvalid(spec.Name, "must be a YAML string")
		}
		return secretValue(n.Value), nil
	}
	return Value{}, wrapInvalid(spec.Name, "unsupported key type")
}

func resolveAliasNode(n *yaml.Node) *yaml.Node {
	seen := 0
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
		seen++
		if seen > 64 {
			return nil
		}
	}
	return n
}

// ParseValue parses the raw SetConfig text for key into a Value. It checks the
// YAML type only; value and combination checks belong to Set.
func ParseValue(key, text string) (Value, error) {
	spec, ok := lookupKey(key)
	if !ok {
		return Value{}, fmt.Errorf("%w: %s", ErrUnknownKey, key)
	}
	if strings.TrimSpace(text) == "" {
		if spec.Kind == keyString || spec.Kind == keySecret {
			if spec.Kind == keySecret {
				return secretValue(""), nil
			}
			return stringValue(""), nil
		}
		return Value{}, wrapInvalid(key, "missing value")
	}
	dec := yaml.NewDecoder(bytes.NewReader([]byte(text)))
	var doc yaml.Node
	if err := dec.Decode(&doc); err != nil {
		if errors.Is(err, io.EOF) {
			return Value{}, wrapInvalid(key, "missing value")
		}
		return Value{}, wrapInvalid(key, "not valid YAML")
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err != nil && !errors.Is(err, io.EOF) {
		return Value{}, wrapInvalid(key, "not valid YAML")
	} else if err == nil && extra.Kind != 0 {
		return Value{}, wrapInvalid(key, "must be a single YAML document")
	}
	root := &doc
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			return Value{}, wrapInvalid(key, "missing value")
		}
		root = root.Content[0]
	}
	if root.Kind == 0 {
		return Value{}, wrapInvalid(key, "missing value")
	}
	return parseNode(spec, root, true)
}
