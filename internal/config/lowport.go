package config

import "fmt"

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

func lowPortSetRefusal(configFile, value string) string {
	return fmt.Sprintf("cannot set broker.listen to %s while %s is writable by an account other than root; run `chmod g-w %s` (and remove any ACL that grants write), or use a port at or above 1024", value, configFile, configFile)
}

func lowPortStartupRemedy(configFile, value string) string {
	return fmt.Sprintf("broker.listen in %s is %s, a port below 1024, and the file is writable by an account other than root, which may have written this value; check that it is the one you want, then run `chmod g-w %s` (and remove any ACL that grants write) or use a port at or above 1024", configFile, value, configFile)
}

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

// CheckLowPort reports an error when the loaded configuration's broker.listen
// names a port below 1024 while the configuration file is not writable by root
// alone: its owner is not root, it is a symbolic link, the group or others can
// write it, it carries an ACL on Darwin, or its permissions cannot be verified.
// A refused port returns an error wrapping ErrLowPortDenied; a startup --listen
// override and user mode are exempt, and before Load it returns ErrNotLoaded.
func CheckLowPort() error {
	c := currentCore()
	if c == nil {
		return ErrNotLoaded
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.checkLowPortAtStartup()
}

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
