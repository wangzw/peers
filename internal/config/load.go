package config

import (
	"errors"
	"fmt"
	"net/netip"
)

// load runs before the core is published, so it takes no c.mu; the helpers that
// swap a running ticker do.
func (c *core) load(mode Mode, listenOverride string) error {
	c.mode = mode

	paths, err := c.resolvePaths()
	if err != nil {
		return err
	}
	c.paths = paths

	if err := c.checkRuntimeForLoad(); err != nil {
		return err
	}

	doc, err := readConfigFile(paths.ConfigFile)
	if err != nil {
		return err
	}
	c.file = &doc
	if doc.exists {
		c.lastFileHash = doc.contentHash
	}

	if err := c.adoptValues(&doc, listenOverride); err != nil {
		return err
	}
	if err := c.validateLoadedValues(); err != nil {
		return err
	}
	if err := c.prepareAdvertised(); err != nil {
		return err
	}

	// The data check runs after every configuration check, so a
	// DataUnavailableError always means the configuration itself was usable.
	if !c.skipDataCheck {
		if err := c.prepareDataDirs(paths); err != nil {
			return c.dataUnavailable(paths, err)
		}
	}

	audit, err := openAudit(paths.AuditLog, c.now, c.logger)
	if err != nil {
		return c.dataUnavailable(paths, err)
	}
	c.audit = audit
	if c.notify == nil {
		c.notify = newNotifier(c.logger)
	}
	// Background workers start last, and only when the core was not built for a
	// test that drives them by hand.
	if !c.noBackground {
		c.startBackground()
	}
	return nil
}

func (c *core) dataUnavailable(paths PathSet, err error) error {
	return &DataUnavailableError{
		Paths:            paths,
		LocalSocketGroup: c.values["broker.local_socket_group"].String,
		LocalSocketMode:  c.values["broker.local_socket_mode"].String,
		Err:              err,
	}
}

// checkRuntimeForLoad creates and checks the runtime directory; tests replace
// checkRuntime so they never touch the real one.
func (c *core) checkRuntimeForLoad() error {
	if c.checkRuntime != nil {
		_, err := c.checkRuntime()
		return err
	}
	_, err := ensureRuntimeDir(c.euid())
	return err
}

func (c *core) adoptValues(doc *configDocument, listenOverride string) error {
	override := listenOverride != ""
	for _, name := range allKeyNames() {
		spec, ok := lookupKey(name)
		if !ok {
			continue
		}
		if override && name == "broker.listen" {
			continue
		}
		if err, bad := doc.invalid[name]; bad {
			return err
		}
		if doc.present[name] {
			c.values[name] = copyValue(doc.values[name])
			if name == "broker.alias" {
				c.explicitAlias = true
			}
			continue
		}
		if spec.Dynamic {
			continue
		}
		c.values[name] = defaultValue(spec, defaultContext{})
	}

	if override {
		addr, err := parseListen(listenOverride)
		if err != nil {
			return wrapInvalid("broker.listen", err.Error())
		}
		c.effectiveListen = addr
		c.listenOverride = true
		c.values["broker.listen"] = stringValue(listenOverride)
		if n := doc.nodeFor("broker.listen"); n != nil {
			c.lastIgnoredListen = n.Value
		}
	} else {
		addr, err := parseListen(c.values["broker.listen"].String)
		if err != nil {
			return wrapInvalid("broker.listen", err.Error())
		}
		c.effectiveListen = addr
	}

	if !c.explicitAlias {
		spec, _ := lookupKey("broker.alias")
		c.values["broker.alias"] = defaultValue(spec, defaultContext{
			mode:     c.mode,
			hostname: c.hostnameString(),
			account:  c.accountString(),
		})
	}
	return nil
}

func (c *core) validateLoadedValues() error {
	for _, name := range allKeyNames() {
		spec, ok := lookupKey(name)
		if !ok {
			continue
		}
		v, ok := c.values[name]
		if !ok {
			return wrapInvalid(name, "missing value")
		}
		if err := validateValue(spec, v); err != nil {
			return wrapInvalid(name, err.Error())
		}
	}
	return validateCrossKeys(c.values)
}

func (c *core) prepareAdvertised() error {
	adv, err := parseAdvertise(c.values["broker.advertise"].String)
	if err != nil {
		return wrapConfigError("broker.advertise", err)
	}
	if !c.effectiveListen.set && len(c.values["broker.advertise_extra"].Strings) > 0 {
		return fmt.Errorf("%w: broker.advertise_extra is set but broker.listen is empty", ErrInvalidCombination)
	}
	extra, err := parseExtra(c.values["broker.advertise_extra"].Strings, adv, c.effectiveListen)
	if err != nil {
		return wrapConfigError("broker.advertise_extra", err)
	}
	var routes []netip.Addr
	routesValid := false
	if needsRoutes(c.effectiveListen, adv) {
		enumerated, err := c.routesSrc.DefaultRouteAddresses()
		if err != nil {
			c.logger.Error("config: cannot enumerate the default route addresses; starting with none", "error", err)
		} else {
			routes = enumerated
			routesValid = true
		}
	}
	advertised, err := computeAdvertised(c.effectiveListen, adv, extra, routes)
	if err != nil {
		return err
	}
	c.routes = routes
	c.routesValid = routesValid
	c.advertised = advertised
	return nil
}

func wrapConfigError(key string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrInvalidValue) || errors.Is(err, ErrInvalidCombination) || errors.Is(err, ErrTooManyAddresses) {
		return err
	}
	return wrapInvalid(key, err.Error())
}

// The caller must hold c.mu or be initializing the core.
func (c *core) effectiveAlias() string {
	if c.explicitAlias {
		return c.values["broker.alias"].String
	}
	return c.defaultAliasLocked()
}

func (c *core) startBackground() {
	if w, err := startWatch(c); err != nil {
		c.logger.Error("config: cannot start the config file watcher", "error", err)
	} else if w != nil {
		c.watch = w
	}
	c.startAddressTickerLocked(c.values["broker.address_check_interval"].Duration)
}
