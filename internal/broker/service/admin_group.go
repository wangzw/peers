package service

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// DefaultAdminGroup is the name of the group that administers a system Broker
// unless the installation names another one.
const DefaultAdminGroup = "peers"

const adminGroupRecordPath = "/var/lib/peers/admin-group"

var adminGroupNameRE = regexp.MustCompile(`^[a-z_][a-z0-9_-]*$`)

// ValidAdminGroupName reports whether name is a usable administration group
// name.
func ValidAdminGroupName(name string) bool {
	return adminGroupNameRE.MatchString(name)
}

func adminGroupName(opts Options) string {
	if opts.AdminGroup != "" {
		return opts.AdminGroup
	}
	return DefaultAdminGroup
}

// ensureAdminGroup returns the gid of the administration group, creating the
// group when it does not exist. It refuses group id 0, and an existing group
// that lists members when this run would generate a new configuration file, the
// administrator never named the group, and a previous install did not record
// that this deployment created it. A system installation runs this before it
// writes any file, so a failure leaves no configuration behind.
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
		// Naming the group or keeping an existing configuration file settles
		// the decision, so neither lookup runs and a broken account database
		// cannot block a reinstall.
		var members []string
		if opts.AdminGroup == "" && !configFileExists(opts) {
			members, err = groupMembers(opts, name)
			if err != nil {
				return 0, err
			}
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

func configFileExists(opts Options) bool {
	_, err := os.Lstat(rooted(opts, configFilePath(opts)))
	return err == nil
}

func recordedAdminGroup(opts Options) string {
	data, err := os.ReadFile(rooted(opts, adminGroupRecordPath))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

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

// dscl exits nonzero with "No such key" for an absent attribute, which is an
// empty list rather than a failure; any other error is returned so an
// unreadable record is not treated as an empty group.
func groupMembersDarwin(opts Options, name string) ([]string, error) {
	out, err := opts.Run("dscl", ".", "-read", "/Groups/"+name, "GroupMembership", "NestedGroups")
	members, nested := parseDarwinGroupRecord(string(out))
	if err != nil && !onlyMissingKeys(string(out)) {
		return nil, commandError(fmt.Sprintf("cannot read the members of group %q", name), out, err)
	}
	members = append(members, resolveNestedGroupNames(opts, nested)...)
	return sortedMemberNames(members), nil
}

// dscl prints the first value on the attribute's line and the rest on indented
// continuation lines.
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

// dscl exits nonzero for an absent attribute, so an error output made only of
// "No such key" notes means the record lists no members.
func onlyMissingKeys(out string) bool {
	sawMissing := false
	inKnown := false
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
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

func recordAdminGroup(opts Options, name string) error {
	if err := os.MkdirAll(rooted(opts, systemDataDir), 0o700); err != nil {
		return fmt.Errorf("cannot create directory %s: %w", systemDataDir, err)
	}
	return writeFileAtomic(rooted(opts, adminGroupRecordPath), name+"\n", 0o600)
}

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

// getent exits 2 when the group is absent.
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

// Only eDSRecordNotFound is an absent group; any other failure must not be read
// as one, or the installation creates a group that may already exist and
// records it as created by this run.
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

// dscl reports a missing record with the eDSRecordNotFound note on stderr,
// which Run combines into its output.
func recordNotFound(out []byte) bool {
	return strings.Contains(string(out), "eDSRecordNotFound")
}

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
