package config

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"time"
)

func (c *core) getLocked(key string) (Value, error) {
	spec, ok := lookupKey(key)
	if !ok {
		return Value{}, fmt.Errorf("%w: %s", ErrUnknownKey, key)
	}
	v := c.values[key]
	if spec.Secret {
		return Value{Kind: KindSecret, SecretIsSet: v.String != ""}, nil
	}
	return copyValue(v), nil
}

func (c *core) allLocked() []Entry {
	names := allKeyNames()
	out := make([]Entry, 0, len(names))
	for _, name := range names {
		spec, _ := lookupKey(name)
		v := c.values[name]
		if spec.Secret {
			v = Value{Kind: KindSecret, SecretIsSet: v.String != ""}
		} else {
			v = copyValue(v)
		}
		out = append(out, Entry{Key: name, Value: v})
	}
	return out
}

func (c *core) secretLocked(key string) string {
	if c.closed {
		return ""
	}
	spec, ok := lookupKey(key)
	if !ok || !spec.Secret {
		return ""
	}
	if _, err := c.reloadLocked(); err != nil {
		c.logger.Error("config: cannot refresh the configuration for a secret", "error", err)
	}
	return c.values[key].String
}

func (c *core) setLocked(actor Actor, key string, v Value) (Value, error) {
	if c.closed {
		return Value{}, ErrNotLoaded
	}
	spec, ok := lookupKey(key)
	if !ok {
		return Value{}, fmt.Errorf("%w: %s", ErrUnknownKey, key)
	}
	if spec.Secret {
		return Value{}, fmt.Errorf("%w: %s", ErrSecretKey, key)
	}
	if c.listenOverride && key == "broker.listen" {
		return Value{}, fmt.Errorf("%w: %s", ErrListenOverridden, key)
	}
	// A privileged port needs root and a configuration file that no other
	// account can write. This is checked before the pending external change is
	// adopted and before anything is written, so a refused caller leaves both
	// the file and the in-memory values untouched.
	if key == "broker.listen" && c.mode == ModeSystem && lowPrivilegedListenPort(v) {
		if err := c.checkLowPortAllowed(actor); err != nil {
			return Value{}, err
		}
	}
	// Adopt any external change that is still pending, by the reload rules and
	// with its audits, before validating and writing. Validation must start
	// from the values now on disk, not from a stale memory snapshot. The bytes
	// read here are the only read of the file: the write-back and the restore
	// after a failed write use them too, so a write by another process between
	// this read and the write-back cannot put content in the file that this
	// process never adopts.
	read, err := c.reloadLocked()
	if err != nil {
		// Reading the file failed for a reason outside the caller's value, so
		// this is an internal failure rather than an invalid_argument.
		return Value{}, fmt.Errorf("cannot read %s: %w", c.paths.ConfigFile, err)
	}
	if v.Kind != valueKindForSpec(spec) {
		return Value{}, wrapInvalid(key, "type does not match the key")
	}
	v = copyValue(v)
	if err := validateValue(spec, v); err != nil {
		return Value{}, wrapInvalid(key, err.Error())
	}

	candidate := copyValues(c.values)
	old := c.values[key]
	candidate[key] = v
	effective := c.effectiveListen
	if key == "broker.listen" {
		parsed, err := parseListen(v.String)
		if err != nil {
			return Value{}, wrapInvalid(key, err.Error())
		}
		effective = parsed
	}
	inGroup := key == "broker.listen" || key == "broker.advertise" || key == "broker.advertise_extra"
	var newAdvertised []AdvertisedAddr
	if inGroup {
		adv, err := c.buildAdvertised(candidate, effective, true)
		if err != nil {
			return Value{}, err
		}
		newAdvertised = adv
	}
	if err := crossKeyError(candidate, key); err != nil {
		return Value{}, err
	}
	for _, check := range c.validators[key] {
		if err := check(old, v); err != nil {
			return Value{}, wrapInvalid(key, err.Error())
		}
	}
	// Without a configured broker.alias the effective alias is derived from the
	// deployment mode, the hostname and the account, none of which a listen
	// change touches. The derived value is compared anyway so a change that ever
	// makes it differ still passes the broker.alias validators before the
	// listener is committed.
	if key == "broker.listen" && !c.explicitAlias {
		derived := stringValue(c.defaultAliasLocked())
		if !valueEqual(derived, c.values["broker.alias"]) {
			for _, check := range c.validators["broker.alias"] {
				if err := check(c.values["broker.alias"], derived); err != nil {
					return Value{}, wrapInvalid("broker.alias", err.Error())
				}
			}
		}
	}

	// The raw bytes that the reload above read are both the input to
	// parseConfigFile and the content restored if the in-place rewrite
	// truncates and then fails. Reading the file again here could pick up a
	// concurrent external write that reload did not adopt.
	original := read.data
	existed := read.exists
	doc, err := parseConfigFile(original)
	if err != nil {
		return Value{}, fmt.Errorf("%w: %s: config file cannot be parsed", ErrInvalidValue, key)
	}
	if err := doc.setKey(key, v); err != nil {
		return Value{}, err
	}
	data, err := doc.bytes()
	if err != nil {
		return Value{}, err
	}
	mode := os.FileMode(0o600)
	if c.mode == ModeSystem {
		mode = 0o644
	}
	if err := c.writeFile(c.paths.ConfigFile, data, mode); err != nil {
		c.logger.Error("config: cannot write the configuration file", "error", err)
		// The in-place rewrite truncates before writing, so a failure can leave
		// an empty or partial file. Put the previous bytes back when possible.
		if existed {
			if restoreErr := c.writeFile(c.paths.ConfigFile, original, mode); restoreErr != nil {
				c.logger.Error("config: cannot restore the configuration file after a failed write", "error", restoreErr)
			}
		}
		// Whether or not the restore worked, remember the hash of the bytes now
		// on disk so this process does not adopt the empty or damaged file as
		// an external change.
		if current, hashErr := readConfigFileLimited(c.paths.ConfigFile); hashErr == nil {
			c.lastFileHash = sha256.Sum256(current)
		} else if errors.Is(hashErr, os.ErrNotExist) {
			c.lastFileHash = sha256.Sum256(nil)
		} else {
			c.logger.Error("config: cannot read the configuration file after a failed write", "error", hashErr)
		}
		return Value{}, err
	}

	oldAlias := c.values["broker.alias"]
	oldAdvertised := copyAddresses(c.advertised)

	c.values[key] = v
	if key == "broker.listen" {
		c.effectiveListen = effective
	}
	if key == "broker.alias" {
		c.explicitAlias = true
	}
	newAlias := c.values["broker.alias"]
	if key == "broker.listen" && !c.explicitAlias {
		newAlias = stringValue(c.defaultAliasLocked())
		c.values["broker.alias"] = newAlias
	}
	if inGroup {
		c.advertised = newAdvertised
	}
	c.file = &doc
	c.lastFileHash = sha256.Sum256(data)

	if key == "broker.address_check_interval" {
		c.startAddressTickerLocked(v.Duration)
	}

	c.auditAppend(Event{
		Event:   EventConfigSet,
		Actor:   actor.Agent,
		Account: actor.Account,
		Key:     key,
		Old:     ptrValue(old),
		New:     ptrValue(v),
	})
	c.notifyKeyLocked(key, old, v)
	if !valueEqual(oldAlias, newAlias) && key != "broker.alias" {
		c.notifyKeyLocked("broker.alias", oldAlias, newAlias)
	}
	if inGroup && !sameAddresses(oldAdvertised, c.advertised) {
		c.notifyAdvertisedLocked(oldAdvertised, c.advertised)
	}
	return old, nil
}

func ptrValue(v Value) *Value {
	copy := copyValue(v)
	return &copy
}

func copyValues(in map[string]Value) map[string]Value {
	out := make(map[string]Value, len(in))
	for k, v := range in {
		out[k] = copyValue(v)
	}
	return out
}

func (c *core) notifyKeyLocked(key string, old, new Value) {
	oldCopy := copyValue(old)
	newCopy := copyValue(new)
	for _, fn := range c.subs[key] {
		fn := fn
		c.notify.enqueue(func() { fn(oldCopy, newCopy) })
	}
}

func (c *core) notifyAdvertisedLocked(old, new []AdvertisedAddr) {
	oldCopy := copyAddresses(old)
	newCopy := copyAddresses(new)
	for _, fn := range c.advSubs {
		fn := fn
		c.notify.enqueue(func() { fn(oldCopy, newCopy) })
	}
}

func (c *core) addressConfig(values map[string]Value, effective listenAddr) (advertiseSpec, []AdvertisedAddr, error) {
	adv, err := parseAdvertise(values["broker.advertise"].String)
	if err != nil {
		return advertiseSpec{}, nil, wrapInvalid("broker.advertise", err.Error())
	}
	extra, err := parseExtra(values["broker.advertise_extra"].Strings, adv, effective)
	if err != nil {
		return advertiseSpec{}, nil, wrapInvalid("broker.advertise_extra", err.Error())
	}
	return adv, extra, nil
}

func (c *core) buildAdvertised(values map[string]Value, effective listenAddr, forceRoutes bool) ([]AdvertisedAddr, error) {
	adv, extra, err := c.addressConfig(values, effective)
	if err != nil {
		return nil, err
	}
	var routes []netip.Addr
	if needsRoutes(effective, adv) {
		routes, err = c.routesForLocked(forceRoutes)
		if err != nil {
			c.logger.Error("config: cannot enumerate the default-route addresses; keeping the previous list", "error", err)
			routes = c.routes
		}
	}
	return computeAdvertised(effective, adv, extra, routes)
}

func (c *core) routesForLocked(force bool) ([]netip.Addr, error) {
	if c.routesValid && !force {
		return c.routes, nil
	}
	addrs, err := c.routesSrc.DefaultRouteAddresses()
	if err != nil {
		return c.routes, err
	}
	c.routes = addrs
	c.routesValid = true
	return addrs, nil
}

// tickerExitReporter is implemented by tickers that want to learn when the
// goroutine driving them has returned. The address-ticker tests use it to
// assert that replacing the ticker leaves no old goroutine behind.
type tickerExitReporter interface {
	tickerExited()
}

func (c *core) startAddressTickerLocked(d time.Duration) {
	if c.noBackground {
		return
	}
	if c.addrTicker != nil {
		c.addrTicker.Stop()
		if c.addrTickerDone != nil {
			close(c.addrTickerDone)
		}
	}
	done := make(chan struct{})
	t := c.newTicker(d)
	c.addrTicker = t
	c.addrTickerDone = done
	go func() {
		if reporter, ok := t.(tickerExitReporter); ok {
			defer reporter.tickerExited()
		}
		for {
			select {
			case <-t.C():
				c.reevaluateAdvertised()
			case <-done:
				return
			}
		}
	}()
}

func (c *core) reevaluateAdvertised() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	adv, _, err := c.addressConfig(c.values, c.effectiveListen)
	if err != nil {
		c.logger.Error("config: cannot parse the address configuration", "error", err)
		return
	}
	if !needsRoutes(c.effectiveListen, adv) {
		return
	}
	if _, err := c.routesForLocked(true); err != nil {
		c.logger.Error("config: cannot re-enumerate the default-route addresses; keeping the previous list", "error", err)
		return
	}
	newList, err := c.buildAdvertised(c.values, c.effectiveListen, false)
	if err != nil {
		c.logger.Error("config: cannot recompute the advertised addresses", "error", err)
		return
	}
	if sameAddresses(c.advertised, newList) {
		return
	}
	old := copyAddresses(c.advertised)
	c.advertised = newList
	c.notifyAdvertisedLocked(old, newList)
}

// reloadFromDisk is called by the watcher and by Secret. It compares the file
// hash before doing any work.
func (c *core) reloadFromDisk() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if _, err := c.reloadLocked(); err != nil {
		c.logger.Error("config: reload failed", "error", err)
	}
}

// configBytes is one read of the configuration file. It carries the exact
// bytes of that read and whether the file existed, so callers that both adopt
// the file and write it back can use the same read for both.
type configBytes struct {
	data   []byte
	exists bool
}

func (c *core) readConfigData() (configBytes, error) {
	data, err := c.readFile(c.paths.ConfigFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return configBytes{}, nil
		}
		return configBytes{}, err
	}
	return configBytes{data: data, exists: true}, nil
}

func (c *core) reloadLocked() (configBytes, error) {
	read, err := c.readConfigData()
	if err != nil {
		return configBytes{}, err
	}
	data := read.data
	hash := sha256.Sum256(data)
	if hash == c.lastFileHash {
		return read, nil
	}
	c.lastFileHash = hash

	doc, err := parseConfigFile(data)
	if err != nil {
		c.logger.Error("config: configuration file cannot be parsed; keeping the current values", "error", err)
		return read, nil
	}
	if doc.empty {
		// An empty file during a reload is the truncation window of an in-place
		// rewrite, not a request to reset every value. Only Load treats an
		// empty file as an empty configuration.
		c.logger.Error("config: configuration file is empty or missing; keeping the current values")
		return read, nil
	}
	c.file = &doc

	// Snapshot the pre-reload values. Adoption writes into c.values, so a
	// reference to the live map would make every later comparison see the new
	// value as old and skip the effective listen address and ticker updates.
	values := copyValues(c.values)
	group := []string{"broker.listen", "broker.advertise", "broker.advertise_extra"}
	isGroup := func(name string) bool {
		return name == "broker.listen" || name == "broker.advertise" || name == "broker.advertise_extra"
	}

	if c.listenOverride {
		c.noteIgnoredListen(doc)
	}

	candidate := copyValues(values)
	groupValid := true
	for _, name := range group {
		if c.listenOverride && name == "broker.listen" {
			candidate[name] = values[name]
			continue
		}
		if bad, ok := doc.invalid[name]; ok {
			c.logger.Error("config: keeping the current value of an invalid key", "key", name, "error", bad)
			groupValid = false
			continue
		}
		spec, _ := lookupKey(name)
		if !doc.present[name] {
			candidate[name] = defaultValue(spec, defaultContext{})
		} else {
			candidate[name] = copyValue(doc.values[name])
		}
		if err := validateValue(spec, candidate[name]); err != nil {
			c.logger.Error("config: keeping the current value of an invalid key", "key", name, "error", err)
			groupValid = false
		}
	}

	effective := c.effectiveListen
	if !c.listenOverride && groupValid {
		parsed, err := parseListen(candidate["broker.listen"].String)
		if err != nil {
			c.logger.Error("config: keeping the current listen address group", "key", "broker.listen", "error", err)
			groupValid = false
		} else {
			effective = parsed
		}
	}
	if groupValid {
		if _, err := c.buildAdvertised(candidate, effective, false); err != nil {
			c.logger.Error("config: keeping the current listen and advertised address group", "error", err)
			groupValid = false
		}
	}
	if groupValid {
		for _, name := range group {
			if valueEqual(candidate[name], values[name]) {
				continue
			}
			for _, check := range c.validators[name] {
				if err := check(values[name], candidate[name]); err != nil {
					c.logger.Error("config: a validator rejected a reloaded value", "key", name, "error", err)
					groupValid = false
					break
				}
			}
			if !groupValid {
				break
			}
		}
	}
	// A privileged listen port is adopted only while the configuration file is
	// writable only by root. Otherwise the whole listen group keeps its old
	// values, the rejection is logged, and config_rejected is audited.
	if groupValid && c.rejectReloadedLowPort(values["broker.listen"], candidate["broker.listen"]) {
		groupValid = false
	}
	if !groupValid {
		for _, name := range group {
			candidate[name] = values[name]
		}
		effective = c.effectiveListen
	}

	type adoption struct {
		key   string
		old   Value
		new   Value
		audit bool
	}
	var adoptions []adoption

	// Cross-key groups are judged as a whole. A member's new value is adopted
	// only if every member is individually valid and the group's combined rule
	// holds; otherwise every member keeps its old value. Checking each member
	// against the old partner, as the generic loop below would, makes the
	// result depend on the lexicographic order of the keys.
	for _, members := range crossKeyGroups {
		groupCandidate := copyValues(candidate)
		groupOK := true
		for _, name := range members {
			spec, _ := lookupKey(name)
			if bad, ok := doc.invalid[name]; ok {
				c.logger.Error("config: keeping a cross-key group; a member is invalid", "key", name, "error", bad)
				groupOK = false
				break
			}
			var next Value
			if doc.present[name] {
				next = copyValue(doc.values[name])
			} else {
				next = defaultValue(spec, defaultContext{})
			}
			if err := validateValue(spec, next); err != nil {
				c.logger.Error("config: keeping a cross-key group; a member is invalid", "key", name, "error", err)
				groupOK = false
				break
			}
			groupCandidate[name] = next
		}
		if groupOK {
			if err := validateCrossKeys(groupCandidate); err != nil {
				c.logger.Error("config: keeping a cross-key group; the combination is invalid", "group", members[0], "error", err)
				groupOK = false
			}
		}
		if groupOK {
			for _, name := range members {
				if valueEqual(groupCandidate[name], values[name]) {
					continue
				}
				for _, check := range c.validators[name] {
					if err := check(values[name], groupCandidate[name]); err != nil {
						c.logger.Error("config: keeping a cross-key group; a validator rejected a member", "key", name, "error", err)
						groupOK = false
						break
					}
				}
				if !groupOK {
					break
				}
			}
		}
		if !groupOK {
			for _, name := range members {
				candidate[name] = values[name]
			}
			continue
		}
		for _, name := range members {
			candidate[name] = groupCandidate[name]
			if !valueEqual(groupCandidate[name], values[name]) {
				adoptions = append(adoptions, adoption{key: name, old: values[name], new: groupCandidate[name], audit: true})
			}
		}
	}

	for _, name := range allKeyNames() {
		if isGroup(name) || name == "broker.alias" || groupForKey(name) != nil {
			continue
		}
		if bad, ok := doc.invalid[name]; ok {
			c.logger.Error("config: keeping the current value of an invalid key", "key", name, "error", bad)
			candidate[name] = values[name]
			continue
		}
		var next Value
		spec, _ := lookupKey(name)
		if doc.present[name] {
			next = copyValue(doc.values[name])
		} else {
			next = defaultValue(spec, defaultContext{})
		}
		if err := validateValue(spec, next); err != nil {
			c.logger.Error("config: keeping the current value of an invalid key", "key", name, "error", err)
			continue
		}
		if valueEqual(next, values[name]) {
			continue
		}
		probe := copyValues(candidate)
		probe[name] = next
		if err := crossKeyError(probe, name); err != nil {
			c.logger.Error("config: keeping the current value; it conflicts with another key", "key", name, "error", err)
			continue
		}
		rejected := false
		for _, check := range c.validators[name] {
			if err := check(values[name], next); err != nil {
				c.logger.Error("config: a validator rejected a reloaded value", "key", name, "error", err)
				rejected = true
				break
			}
		}
		if rejected {
			continue
		}
		candidate[name] = next
		adoptions = append(adoptions, adoption{key: name, old: values[name], new: next, audit: true})
	}

	wasExplicit := c.explicitAlias
	var aliasOld, aliasNew Value
	if bad, ok := doc.invalid["broker.alias"]; ok {
		c.logger.Error("config: keeping the current alias; the file value is invalid", "error", bad)
	} else if !doc.present["broker.alias"] {
		newAlias := stringValue(c.defaultAliasLocked())
		accepted := true
		if !valueEqual(newAlias, values["broker.alias"]) {
			// The file removed the explicit alias, so the default alias takes
			// over. Run the broker.alias checks on it before adopting it; a
			// rejected derived alias keeps the previous value.
			for _, check := range c.validators["broker.alias"] {
				if err := check(values["broker.alias"], newAlias); err != nil {
					c.logger.Error("config: keeping the current alias; a validator rejected the derived alias", "key", "broker.alias", "error", err)
					accepted = false
					break
				}
			}
			if accepted {
				aliasOld = values["broker.alias"]
				aliasNew = newAlias
				candidate["broker.alias"] = newAlias
				adoptions = append(adoptions, adoption{key: "broker.alias", old: aliasOld, new: aliasNew, audit: wasExplicit})
			} else {
				// Keeping the old alias keeps the listen address group at its old
				// values as well, so the alias decision and the listener move
				// together.
				for _, name := range group {
					candidate[name] = values[name]
				}
				effective = c.effectiveListen
				groupValid = false
			}
		}
		// A rejected derivation leaves the previous value in effect, so keep
		// the explicit flag as it was; only an adopted or unchanged
		// derivation makes the alias derived again.
		if accepted {
			c.explicitAlias = false
		}
	} else {
		newAlias := copyValue(doc.values["broker.alias"])
		spec, _ := lookupKey("broker.alias")
		if err := validateValue(spec, newAlias); err != nil {
			c.logger.Error("config: keeping the current alias; the file value is invalid", "key", "broker.alias", "error", err)
		} else {
			// The file configures broker.alias explicitly. Run the checks
			// registered for broker.alias on a changed value before adopting
			// it; a rejection keeps the previous alias.
			accepted := true
			if !valueEqual(newAlias, values["broker.alias"]) {
				for _, check := range c.validators["broker.alias"] {
					if err := check(values["broker.alias"], newAlias); err != nil {
						c.logger.Error("config: keeping the current alias; a validator rejected the reloaded alias", "key", "broker.alias", "error", err)
						accepted = false
						break
					}
				}
			}
			if accepted {
				if !valueEqual(newAlias, values["broker.alias"]) {
					aliasOld = values["broker.alias"]
					aliasNew = newAlias
					candidate["broker.alias"] = newAlias
					adoptions = append(adoptions, adoption{key: "broker.alias", old: aliasOld, new: aliasNew, audit: true})
				}
				c.explicitAlias = true
			} else {
				// Keeping the old alias means the listen address group must
				// keep its old values too, as on the derived-alias path.
				for _, name := range group {
					candidate[name] = values[name]
				}
				effective = c.effectiveListen
				groupValid = false
			}
		}
	}

	oldAdvertised := copyAddresses(c.advertised)
	for _, name := range group {
		if valueEqual(candidate[name], values[name]) {
			continue
		}
		adoptions = append(adoptions, adoption{key: name, old: values[name], new: candidate[name], audit: true})
	}
	for _, a := range adoptions {
		c.values[a.key] = a.new
	}
	if !c.listenOverride && !valueEqual(candidate["broker.listen"], values["broker.listen"]) {
		c.effectiveListen = effective
	}
	if groupValid {
		newList, err := c.buildAdvertised(c.values, c.effectiveListen, false)
		if err == nil {
			c.advertised = newList
		}
	}
	for _, a := range adoptions {
		if a.audit {
			c.auditAppend(Event{
				Event: EventConfigFileChanged,
				Key:   a.key,
				Old:   ptrValue(a.old),
				New:   ptrValue(a.new),
			})
		}
		c.notifyKeyLocked(a.key, a.old, a.new)
	}
	if !sameAddresses(oldAdvertised, c.advertised) {
		c.notifyAdvertisedLocked(oldAdvertised, c.advertised)
	}
	if !valueEqual(c.values["broker.address_check_interval"], values["broker.address_check_interval"]) {
		c.startAddressTickerLocked(c.values["broker.address_check_interval"].Duration)
	}
	return read, nil
}

func (c *core) noteIgnoredListen(doc configDocument) {
	raw := ""
	if n := doc.nodeFor("broker.listen"); n != nil {
		raw = n.Value
	}
	if raw == c.lastIgnoredListen {
		return
	}
	c.lastIgnoredListen = raw
	c.logger.Error("config: ignoring broker.listen because the command line fixed the listen address")
}
