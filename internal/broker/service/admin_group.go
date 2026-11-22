package service

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// DefaultAdminGroup is the group that administers a system Broker unless
// the installation names another one. Its members can read and write the
// system configuration file without root.
const DefaultAdminGroup = "peers"

// adminGroupRecordPath records the name of a group the installation created.
// Uninstall reads it before removing the data directory: a group without a
// record was not created by peers and is never removed.
const adminGroupRecordPath = "/var/lib/peers/admin-group"

// adminGroupNameRE matches a group name both Linux and macOS accept: a letter
// or underscore first, then letters, digits, underscores and hyphens. A name
// that starts with a hyphen is rejected so it cannot be mistaken for an option.
var adminGroupNameRE = regexp.MustCompile(`^[a-z_][a-z0-9_-]*$`)

// ValidAdminGroupName reports whether name is a usable administration group
// name.
func ValidAdminGroupName(name string) bool {
	return adminGroupNameRE.MatchString(name)
}

// adminGroupName returns the group the installation delegates administration
// to.
func adminGroupName(opts Options) string {
	if opts.AdminGroup != "" {
		return opts.AdminGroup
	}
	return DefaultAdminGroup
}

// ensureAdminGroup returns the group id of the administration group, creating
// the group when it does not exist. An existing group is used as it is; its
// members are never changed. A system installation runs this before it writes
// any file, so a failure leaves no configuration behind.
//
// A group whose id is 0 is refused: that is root's group, and every account it
// names would hold the configuration file's permissions. An existing group that
// already lists members is refused too when this run would give it a new
// configuration file and the administrator never named it: those members would
// otherwise be handed management of the system Broker without the
// administrator's knowledge. The member list counts accounts whose primary
// group is the group as well as supplementary members, and on macOS nested
// groups; it is not read at all when the administrator named the group or an
// existing configuration file is kept, so a broken lookup cannot block a
// reinstall.
func ensureAdminGroup(opts Options, res *Result) (int, error) {
	name := adminGroupName(opts)
	if !ValidAdminGroupName(name) {
		return 0, fmt.Errorf("cannot use administration group %q: the name is invalid", name)
	}
	gid, exists, err := lookupGroup(opts, name)
	if err != nil {
		return 0, err
	}
	if exists {
		if gid == 0 {
			return 0, fmt.Errorf("cannot use group %q as the administration group: its group id is 0", name)
		}
		// The member list exists only to decide whether to refuse an
		// unexplained group. Naming the group with --admin-group, or keeping an
		// existing configuration file, settles that decision, so neither
		// lookup runs: a broken account database must not block a reinstall.
		var members []string
		if opts.AdminGroup == "" && !configFileExists(opts) {
			members, err = groupMembers(opts, name)
			if err != nil {
				return 0, err
			}
			// getent lists only supplementary members, and the directory
			// service's GroupMembership does the same, so an account whose
			// primary group is name is missing from both. It holds the
			// configuration file's permissions just the same.
			primary, err := primaryGroupAccounts(opts, gid)
			if err != nil {
				return 0, err
			}
			members = sortedMemberNames(append(members, primary...))
			res.AdminGroupMembersQueried = true
			if err := checkReusedAdminGroup(opts, name, members); err != nil {
				return 0, err
			}
		}
		res.AdminGroupMembers = members
		return gid, nil
	}
	if err := createAdminGroup(opts, res, name); err != nil {
		return 0, err
	}
	gid, exists, err = lookupGroup(opts, name)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, fmt.Errorf("cannot create group %q: it is still missing afterwards", name)
	}
	res.AdminGroupCreated = true
	return gid, nil
}

// checkReusedAdminGroup refuses an existing group the administrator did not
// name when every one of these holds: it already lists members, this run will
// generate the configuration file, and no earlier peers installation recorded
// creating the group. Naming the group with --admin-group is the
// administrator's acceptance of its members, and a kept configuration file
// already grants them the same access, so neither is refused.
func checkReusedAdminGroup(opts Options, name string, members []string) error {
	if len(members) == 0 || opts.AdminGroup != "" || configFileExists(opts) || recordedAdminGroup(opts) == name {
		return nil
	}
	who := "members " + joinAnd(members) + " would be able to manage the system Broker; pass --admin-group " + name + " to accept them"
	if len(members) == 1 {
		who = "member " + members[0] + " would be able to manage the system Broker; pass --admin-group " + name + " to accept them"
	}
	return fmt.Errorf("cannot use the existing group %q as the administration group: its %s, or pass --admin-group with another name", name, who)
}

// configFileExists reports whether this installation's configuration file is
// already there, which means this run keeps it instead of generating one.
func configFileExists(opts Options) bool {
	_, err := os.Lstat(rooted(opts, configFilePath(opts)))
	return err == nil
}

// recordedAdminGroup returns the group name an earlier peers installation
// recorded creating, or an empty string when there is no record.
func recordedAdminGroup(opts Options) string {
	data, err := os.ReadFile(rooted(opts, adminGroupRecordPath))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// groupMembers returns the members the account database lists for the group,
// sorted by name. Options.GroupMembers replaces the lookup in a test; otherwise
// the platform's group database is queried through the command runner.
//
// On Linux the fourth field of getent holds the supplementary members. On
// macOS the GroupMembership attribute holds them and NestedGroups holds nested
// groups, whose accounts reach the configuration file through this group and
// are therefore counted too. Accounts whose primary group is name are absent
// from both queries; ensureAdminGroup adds them separately.
func groupMembers(opts Options, name string) ([]string, error) {
	if opts.GroupMembers != nil {
		members, err := opts.GroupMembers(name)
		if err != nil {
			return nil, err
		}
		return sortedMemberNames(members), nil
	}
	switch opts.GOOS {
	case "linux":
		return groupMembersLinux(opts, name)
	case "darwin":
		return groupMembersDarwin(opts, name)
	default:
		return nil, fmt.Errorf("cannot read the members of group %q on platform %q", name, opts.GOOS)
	}
}

// sortedMemberNames trims, drops empty and duplicate entries and sorts the
// names, so the result does not depend on the account database's order and two
// runs with the same members read the same.
func sortedMemberNames(members []string) []string {
	seen := make(map[string]bool, len(members))
	out := make([]string, 0, len(members))
	for _, member := range members {
		member = strings.TrimSpace(member)
		if member == "" || seen[member] {
			continue
		}
		seen[member] = true
		out = append(out, member)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}

// groupMembersLinux reads the member list from the fourth field of getent.
func groupMembersLinux(opts Options, name string) ([]string, error) {
	out, err := opts.Run("getent", "group", name)
	if err != nil {
		return nil, commandError(fmt.Sprintf("cannot read the members of group %q", name), out, err)
	}
	fields := strings.Split(strings.TrimSpace(string(out)), ":")
	if len(fields) < 4 {
		return nil, nil
	}
	return sortedMemberNames(strings.Split(fields[3], ",")), nil
}

// groupMembersDarwin reads the GroupMembership and NestedGroups attributes from
// the directory service record. dscl exits nonzero with "No such key" for an
// attribute that is absent, which is an empty list rather than an error, so a
// group with members but no NestedGroups key is read rather than refused. Any
// other failure, including eDSRecordNotFound, is returned: the installation then
// refuses instead of treating an unreadable record as an empty group.
//
// NestedGroups holds each nested group's GUID. Each GUID is resolved to the
// group's name for display; a GUID the directory service cannot resolve is kept
// as it is, so its account list still counts as members and the installation
// still refuses.
func groupMembersDarwin(opts Options, name string) ([]string, error) {
	out, err := opts.Run("dscl", ".", "-read", "/Groups/"+name, "GroupMembership", "NestedGroups")
	members, nested := parseDarwinGroupRecord(string(out))
	if err != nil && !onlyMissingKeys(string(out)) {
		return nil, commandError(fmt.Sprintf("cannot read the members of group %q", name), out, err)
	}
	members = append(members, resolveNestedGroupNames(opts, nested)...)
	return sortedMemberNames(members), nil
}

// parseDarwinGroupRecord splits a dscl group record into the accounts that
// GroupMembership lists and the GUIDs NestedGroups lists. dscl puts the first
// value on the attribute's line and prints the remaining values on indented
// continuation lines, so both forms are read.
func parseDarwinGroupRecord(out string) (members, nested []string) {
	var current string
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			switch current {
			case "GroupMembership":
				members = append(members, strings.Fields(line)...)
			case "NestedGroups":
				nested = append(nested, strings.Fields(line)...)
			}
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			current = ""
			continue
		}
		current = strings.TrimSpace(key)
		switch current {
		case "GroupMembership":
			members = append(members, strings.Fields(value)...)
		case "NestedGroups":
			nested = append(nested, strings.Fields(value)...)
		default:
			current = ""
		}
	}
	return members, nested
}

// onlyMissingKeys reports whether a dscl error output is explained entirely by
// attributes that are absent: every line is either a value of an attribute the
// read asked for or a "No such key" note, and at least one such note is there.
// dscl exits nonzero for an absent attribute, and such a record lists no
// members for it rather than being unreadable. An error with no missing-key
// note, such as eDSRecordNotFound, is not tolerated.
func onlyMissingKeys(out string) bool {
	sawMissing := false
	inKnown := false
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			// A continuation value belongs to the key above it.
			if !inKnown {
				return false
			}
			continue
		}
		switch {
		case strings.HasPrefix(line, "No such key:"):
			sawMissing = true
			inKnown = false
		case strings.HasPrefix(line, "GroupMembership:"), strings.HasPrefix(line, "NestedGroups:"):
			inKnown = true
		default:
			return false
		}
	}
	return sawMissing
}

// resolveNestedGroupNames turns each nested group GUID into its group name. A
// GUID that cannot be resolved is returned unchanged: the group still grants
// access to the configuration file, so its members must still count.
func resolveNestedGroupNames(opts Options, guids []string) []string {
	if len(guids) == 0 {
		return nil
	}
	names := make([]string, 0, len(guids))
	for _, guid := range guids {
		name := lookupGroupByGUID(opts, guid)
		if name == "" {
			name = guid
		}
		names = append(names, name)
	}
	return names
}

// lookupGroupByGUID returns the record name of the group whose GeneratedUID is
// guid, or an empty string when the directory service cannot resolve it.
func lookupGroupByGUID(opts Options, guid string) string {
	out, err := opts.Run("dscl", ".", "-search", "/Groups", "GeneratedUID", guid, "RecordName")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 {
			return fields[0]
		}
	}
	return ""
}

// recordAdminGroup writes the name of the group the installation created. The
// record sits in the data directory, which uninstall reads before deleting it.
func recordAdminGroup(opts Options, name string) error {
	if err := os.MkdirAll(rooted(opts, systemDataDir), 0o700); err != nil {
		return fmt.Errorf("cannot create directory %s: %w", systemDataDir, err)
	}
	return writeFileAtomic(rooted(opts, adminGroupRecordPath), name+"\n", 0o600)
}

// lookupGroup reports the group id of name and whether the group exists.
// Options.GroupGID replaces the lookup in a test; otherwise the platform's
// group database is queried through the command runner.
func lookupGroup(opts Options, name string) (int, bool, error) {
	if opts.GroupGID != nil {
		return opts.GroupGID(name)
	}
	switch opts.GOOS {
	case "linux":
		return lookupGroupLinux(opts, name)
	case "darwin":
		return lookupGroupDarwin(opts, name)
	default:
		return 0, false, fmt.Errorf("cannot look up the group on platform %q", opts.GOOS)
	}
}

// lookupGroupLinux reads the group id from getent. getent exits 2 when the
// group is absent.
func lookupGroupLinux(opts Options, name string) (int, bool, error) {
	out, err := opts.Run("getent", "group", name)
	if err != nil {
		if exitStatus(err) == 2 {
			return 0, false, nil
		}
		return 0, false, commandError(fmt.Sprintf("cannot look up group %q", name), out, err)
	}
	fields := strings.Split(strings.TrimSpace(string(out)), ":")
	if len(fields) < 3 {
		return 0, false, fmt.Errorf("cannot read group %s: unexpected getent output %q", name, strings.TrimSpace(string(out)))
	}
	gid, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0, false, fmt.Errorf("cannot read group %s: group id %q is not a number", name, fields[2])
	}
	return gid, true, nil
}

// lookupGroupDarwin reads the group id from the directory service record.
// Only a record dscl reports as missing (eDSRecordNotFound) is an absent
// group. Every other nonzero exit, such as a directory service outage or a
// permission error, is a query failure: reading it as an absent group would
// make the installation create a group that may already exist and record it as
// created by this run.
func lookupGroupDarwin(opts Options, name string) (int, bool, error) {
	out, err := opts.Run("dscl", ".", "-read", "/Groups/"+name)
	if recordNotFound(out) {
		return 0, false, nil
	}
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg != "" {
			return 0, false, fmt.Errorf("cannot look up group %q: %w: %s", name, err, msg)
		}
		return 0, false, fmt.Errorf("cannot look up group %q: %w", name, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(key) != "PrimaryGroupID" {
			continue
		}
		gid, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return 0, false, fmt.Errorf("cannot read group %s: group id %q is not a number", name, strings.TrimSpace(value))
		}
		return gid, true, nil
	}
	return 0, false, fmt.Errorf("cannot read group %s: no PrimaryGroupID in output %q", name, strings.TrimSpace(string(out)))
}

// recordNotFound reports whether dscl said the record does not exist. dscl
// prints "<dscl_cmd> DS Error: -14136 (eDSRecordNotFound)" for a missing
// record, on stderr, which Run includes in its combined output. The note is
// what identifies the missing record, so it is read whether or not dscl also
// exits nonzero.
func recordNotFound(out []byte) bool {
	return strings.Contains(string(out), "eDSRecordNotFound")
}

// createAdminGroup creates a system group for administration.
func createAdminGroup(opts Options, res *Result, name string) error {
	var argv []string
	switch opts.GOOS {
	case "linux":
		argv = []string{"groupadd", "--system", name}
	case "darwin":
		argv = []string{"dseditgroup", "-o", "create", "-r", "peers administrators", name}
	default:
		return fmt.Errorf("cannot create the group on platform %q", opts.GOOS)
	}
	cmdline := strings.Join(argv, " ")
	res.Commands = append(res.Commands, cmdline)
	out, err := opts.Run(argv[0], argv[1:]...)
	if err != nil {
		return commandError("cannot run "+cmdline, out, err)
	}
	return nil
}
