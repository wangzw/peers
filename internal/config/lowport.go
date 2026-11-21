package config

import "fmt"

// lowPrivilegedListenPort reports whether v is a broker.listen value naming a
// TCP port below 1024. An empty value (do not listen) and a value that does not
// parse are not privileged; the ordinary validation reports the parse error.
func lowPrivilegedListenPort(v Value) bool {
	if v.Kind != KindString {
		return false
	}
	addr, err := parseListen(v.String)
	if err != nil || !addr.set {
		return false
	}
	return addr.port < 1024
}

// lowPortSetRefusal explains a Set that asks for a privileged port while the
// file is writable by another account. It states what the file must be for the
// request to be allowed and the two remedies, without telling the operator to
// widen the file for every account.
func lowPortSetRefusal(configFile, value string) string {
	return fmt.Sprintf("cannot set broker.listen to %s while %s is writable by an account other than root; run `chmod g-w %s` (and remove any ACL that grants write), or use a port at or above 1024", value, configFile, configFile)
}

// lowPortStartupRemedy explains a refused start or hot reload. It names the
// value found in the file, why the file is not accepted, and the two remedies.
// The operator checks that the value is the intended one before narrowing the
// permissions, because an account that can write the file may have written it.
func lowPortStartupRemedy(configFile, value string) string {
	return fmt.Sprintf("broker.listen in %s is %s, a port below 1024, and the file is writable by an account other than root, which may have written this value; check that it is the one you want, then run `chmod g-w %s` (and remove any ACL that grants write) or use a port at or above 1024", configFile, value, configFile)
}

// checkLowPortAllowed enforces the rule for a Set: a system Broker accepts a
// privileged port only from a known root caller, and only while the
// configuration file cannot be written by another account. A caller whose
// identity is unknown is treated as unprivileged. A non-root caller is refused
// before the file is touched, so a group administrator cannot use the Broker to
// gain the privileged bind that the kernel reserves for root.
func (c *core) checkLowPortAllowed(actor Actor, v Value) error {
	if !actor.UIDKnown || actor.UID != 0 {
		return fmt.Errorf("%w: changing broker.listen to a port below 1024 requires root", ErrLowPortDenied)
	}
	ok, err := c.configFileRootOnly(c.paths.ConfigFile)
	if err != nil {
		return fmt.Errorf("%w: cannot verify that %s is writable only by root: %v", ErrLowPortDenied, c.paths.ConfigFile, err)
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrLowPortDenied, lowPortSetRefusal(c.paths.ConfigFile, v.String))
	}
	return nil
}

// rejectReloadedLowPort reports whether a reloaded broker.listen value must be
// kept at its old value because it names a privileged port while the
// configuration file can be written by another account. Such a change is not
// adopted, is logged at warn level, and is written to the audit log as
// config_rejected.
func (c *core) rejectReloadedLowPort(old, next Value) bool {
	if c.mode != ModeSystem || c.listenOverride || valueEqual(old, next) || !lowPrivilegedListenPort(next) {
		return false
	}
	ok, err := c.configFileRootOnly(c.paths.ConfigFile)
	if err != nil {
		c.logger.Error("config: cannot verify that the configuration file is writable only by root; keeping the current listen address", "file", c.paths.ConfigFile, "error", err)
		c.auditAppend(Event{
			Event:  EventConfigRejected,
			Key:    "broker.listen",
			Old:    ptrValue(old),
			New:    ptrValue(next),
			Detail: "the configuration file permission could not be verified",
		})
		return true
	}
	if ok {
		return false
	}
	c.logger.Warn("config: keeping the current listen address; "+lowPortStartupRemedy(c.paths.ConfigFile, next.String), "file", c.paths.ConfigFile)
	c.auditAppend(Event{
		Event:  EventConfigRejected,
		Key:    "broker.listen",
		Old:    ptrValue(old),
		New:    ptrValue(next),
		Detail: "the configuration file is writable by an account other than root",
	})
	return true
}

// CheckLowPort applies the startup rule for a privileged listen port against
// the loaded configuration. A system Broker accepts a port below 1024 only
// while the configuration file can be written by root alone; a command-line
// --listen override and a non-system mode are exempt. Broker calls it in the
// listen-binding step, so a refusal is treated as a listen failure and a
// degraded run can keep serving locally.
func CheckLowPort() error {
	c := currentCore()
	if c == nil {
		return ErrNotLoaded
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkLowPortAtStartup()
}

// checkLowPortAtStartup refuses to start a system Broker on a privileged port
// while the configuration file can be written by another account. The message
// names both remedies so the operator can either restrict the file or choose a
// port at or above 1024.
func (c *core) checkLowPortAtStartup() error {
	if c.mode != ModeSystem || c.listenOverride {
		return nil
	}
	if !lowPrivilegedListenPort(c.values["broker.listen"]) {
		return nil
	}
	ok, err := c.configFileRootOnly(c.paths.ConfigFile)
	if err != nil {
		return fmt.Errorf("%w: cannot verify that %s is writable only by root: %v", ErrLowPortDenied, c.paths.ConfigFile, err)
	}
	if !ok {
		return fmt.Errorf("%w: %s", ErrLowPortDenied, lowPortStartupRemedy(c.paths.ConfigFile, c.values["broker.listen"].String))
	}
	return nil
}
