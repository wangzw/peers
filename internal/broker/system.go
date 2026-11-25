package broker

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strconv"
	"syscall"

	"github.com/wangzw/peers/internal/accountdb"
	"github.com/wangzw/peers/internal/config"
)

// Event names for the system local socket permission changes.
const (
	evLocalSocketPermsApplied = "local_socket_perms_applied"
	evLocalSocketPermsFailed  = "local_socket_perms_failed"
)

var lookupGroupFromDB = accountdb.LookupGroup

// applyLocalSocketPerms sets the group and mode of the system Broker's local
// socket; an empty group means the group of runtimeDir.
func applyLocalSocketPerms(socketPath, runtimeDir, group, mode string) error {
	var gid int
	if group == "" {
		fi, err := os.Stat(runtimeDir)
		if err != nil {
			return fmt.Errorf("broker.local_socket_group: stat runtime directory %s: %w", runtimeDir, err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("broker.local_socket_group: runtime directory %s has no owner information", runtimeDir)
		}
		gid = int(st.Gid)
	} else {
		groupGID, err := lookupGroupFromDB(group)
		if err != nil {
			if errors.Is(err, accountdb.ErrNotFound) {
				return fmt.Errorf("broker.local_socket_group: group %q does not exist", group)
			}
			return fmt.Errorf("broker.local_socket_group: group %q: %w", group, err)
		}
		gid = int(groupGID)
	}
	if err := os.Lchown(socketPath, -1, gid); err != nil {
		return fmt.Errorf("broker.local_socket_group: set the socket group to %d: %w", gid, err)
	}

	m, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return fmt.Errorf("broker.local_socket_mode: %q is not an octal file mode", mode)
	}
	if err := os.Chmod(socketPath, fs.FileMode(m)); err != nil {
		return fmt.Errorf("broker.local_socket_mode: set the socket mode to %s: %w", mode, err)
	}
	return nil
}

func validateSocketGroup(_, new config.Value) error {
	if new.String == "" {
		return nil
	}
	if _, err := lookupGroupFromDB(new.String); err != nil {
		if errors.Is(err, accountdb.ErrNotFound) {
			return fmt.Errorf("group %q does not exist", new.String)
		}
		return fmt.Errorf("group %q: %w", new.String, err)
	}
	return nil
}

func checkSocketGroupAtStartup(cfg ConfigSource) error {
	v, err := cfg.Get("broker.local_socket_group")
	if err != nil {
		return fmt.Errorf("%w: broker.local_socket_group: %v", ErrInvalidConfig, err)
	}
	if err := validateSocketGroup(config.Value{}, v); err != nil {
		return fmt.Errorf("%w: broker.local_socket_group: %v", ErrInvalidConfig, err)
	}
	return nil
}

func watchSocketPerms(cfg ConfigSource, apply func() error, logger *slog.Logger) {
	reapply := func() {
		if err := apply(); err != nil {
			logger.Error(evLocalSocketPermsFailed, "error", err.Error())
			return
		}
		logger.Info(evLocalSocketPermsApplied)
	}
	cfg.Subscribe("broker.local_socket_group", func(_, _ config.Value) { reapply() })
	cfg.Subscribe("broker.local_socket_mode", func(_, _ config.Value) { reapply() })
}
