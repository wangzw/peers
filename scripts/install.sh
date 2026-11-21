#!/bin/sh
# Install peers from an extracted release archive.
#
# The archive keeps this script next to the peers binary. The script
# installs that binary, starts the Broker as a user or system
# service, and registers the MCP server for the account that runs it. It never
# downloads anything, so it works on a host without network access.
#
# Usage:
#   sh ./install.sh [--system | --user] [--prefix <dir>] [--no-service]
#                   [--no-mcp] [--admin-group <name>] [--join-admin-group]
#                   [--no-admin-group]
#
#   --system        install for the whole host. The script runs itself through
#                   sudo when it is not root. The binary goes to /usr/local/bin
#                   unless --prefix says otherwise, and the service runs at
#                   boot.
#   --user          install for the account that runs the script. The account
#                   must not be root.
#   --prefix <dir>  install the binary in <dir> instead of the default:
#                   $HOME/.local/bin for a user install and
#                   /usr/local/bin for a system install.
#   --no-service    do not install the Broker service.
#   --no-mcp        do not register the MCP server with Claude Code.
#   --admin-group <name>
#                   manage the system Broker through <name> instead of
#                   the default peers group; passed to
#                   "service install --system".
#   --join-admin-group
#                   add the account that ran sudo to the admin group without
#                   asking.
#   --no-admin-group
#                   do not ask to add the account that ran sudo to the admin
#                   group.
#   -h, --help      print this help and exit.
#
# Without --system or --user the script chooses the level itself. Root installs
# for every account without asking. An account without a controlling terminal
# gets a user install without waiting for input. An account with a
# controlling terminal is asked whether to install for every account, and is
# asked whether to upgrade the existing system installation when the host
# already has one; answering no to the upgrade leaves the host unchanged.
#
# A system install asks, on the controlling terminal, whether to add the
# account that ran sudo to the admin group, so that account can manage the
# Broker without sudo after logging in again. The answer defaults to no. The
# prompt is skipped when the answer cannot be read, and when the account is
# already in the group.
#
# Uninstall with "peers uninstall". This script does not uninstall
# anything.

set -eu

system=0
system_set=0
user_set=0
prefix=
prefix_set=0
install_service=1
install_mcp=1
admin_group=peers
admin_group_set=0
join_admin_group=0
no_admin_group=0

# fail reports the step that failed and exits. Every step that can fail calls
# it explicitly, so the message names the part of the installation that stopped
# rather than leaving the caller with a bare shell exit status.
fail() {
	printf 'install.sh: %s\n' "$1" >&2
	exit 1
}

# The completion writer stages generated content in a root-owned temporary file
# under /tmp before copying it into the target directory. Remove that file when
# the script exits or is interrupted, so an abort does not leave it behind.
# _write_stage names the current file and is empty when there is none.
_write_stage=
remove_stage() {
	if [ -n "$_write_stage" ]; then
		# Clear the name before removing the file. A signal that arrives while
		# rm runs then finds no name here, and the trap at exit never removes a
		# file another process created at the same path after the removal.
		_stage_path=$_write_stage
		_write_stage=
		rm -f "$_stage_path" || :
	fi
}
trap remove_stage EXIT
trap 'remove_stage; trap - EXIT; exit 129' HUP
trap 'remove_stage; trap - EXIT; exit 130' INT
trap 'remove_stage; trap - EXIT; exit 143' TERM

# ask_tty writes the prompt to the controlling terminal and reads one line from
# it, never from stdin: when the script is piped into sh, stdin is the script
# itself. It prints the answer without its newline and returns 0 when a line was
# read, so an empty answer is distinct from a terminal that could not be read.
ask_tty() {
	printf '%s' "$1" 2>/dev/null >/dev/tty || return 1
	_reply=
	if ! _reply=$( { read -r _line && printf '%s' "$_line"; } 2>/dev/null </dev/tty ); then
		printf '\n' 2>/dev/null >/dev/tty || true
		return 1
	fi
	printf '\n' 2>/dev/null >/dev/tty || true
	printf '%s' "$_reply"
	return 0
}

# version_parts prints the three numeric fields of a release version of the
# form MAJOR.MINOR.PATCH, separated by spaces. It returns 1 for any other form,
# such as the development build "dev" or a pre-release, whose order this script
# does not try to establish.
version_parts() {
	case $1 in
		''|*[!0-9.]*) return 1 ;;
		.*|*.|*..*) return 1 ;;
	esac
	_rest=${1#*.}
	[ "$_rest" = "$1" ] && return 1
	_major=${1%%.*}
	_minor=${_rest%%.*}
	_patch=${_rest#*.}
	[ "$_patch" = "$_rest" ] && return 1
	case $_minor in
		''|*.*) return 1 ;;
	esac
	case $_patch in
		''|*.*) return 1 ;;
	esac
	printf '%s %s %s\n' "$_major" "$_minor" "$_patch"
}

# strip_leading_zeros removes the leading zeros from one numeric field, so the
# integer comparisons below do not depend on how the shell reads a value such
# as 08.
strip_leading_zeros() {
	_value=$1
	while :; do
		case $_value in
			0?*) _value=${_value#0} ;;
			*) break ;;
		esac
	done
	printf '%s\n' "$_value"
}

# compare_versions prints "newer" when $1 is a later release than $2, "older"
# when it is earlier, and "same" when they are equal. Two identical strings name
# the same version even without the release form, so replacing a development
# build or a pre-release with itself is a reinstall. Other fields are compared
# as numbers, so 0.10.0 is newer than 0.9.0. A version that does not have the
# release form prints "unknown"; the caller then reports a replacement rather
# than claim an order it cannot establish.
compare_versions() {
	if [ "$1" = "$2" ]; then
		printf 'same\n'
		return 0
	fi
	if ! _left=$(version_parts "$1") || ! _right=$(version_parts "$2"); then
		printf 'unknown\n'
		return 0
	fi
	_left_major=${_left%% *}
	_left=${_left#* }
	_left_minor=${_left%% *}
	_left_patch=${_left#* }
	_right_major=${_right%% *}
	_right=${_right#* }
	_right_minor=${_right%% *}
	_right_patch=${_right#* }
	_left_major=$(strip_leading_zeros "$_left_major")
	_left_minor=$(strip_leading_zeros "$_left_minor")
	_left_patch=$(strip_leading_zeros "$_left_patch")
	_right_major=$(strip_leading_zeros "$_right_major")
	_right_minor=$(strip_leading_zeros "$_right_minor")
	_right_patch=$(strip_leading_zeros "$_right_patch")

	_order=same
	if [ "$_left_major" -ne "$_right_major" ]; then
		if [ "$_left_major" -gt "$_right_major" ]; then _order=newer; else _order=older; fi
	elif [ "$_left_minor" -ne "$_right_minor" ]; then
		if [ "$_left_minor" -gt "$_right_minor" ]; then _order=newer; else _order=older; fi
	elif [ "$_left_patch" -ne "$_right_patch" ]; then
		if [ "$_left_patch" -gt "$_right_patch" ]; then _order=newer; else _order=older; fi
	fi
	printf '%s\n' "$_order"
}

usage() {
	cat <<'EOF'
usage: sh ./install.sh [--system | --user] [--prefix <dir>] [--no-service]
                       [--no-mcp] [--admin-group <name>] [--join-admin-group]
                       [--no-admin-group]

Install peers from the extracted release archive.

  --system        install for the whole host; the script runs itself through
                  sudo when it is not root
  --user          install for the account that runs the script; the account
                  must not be root
  --prefix <dir>  install the binary in <dir> instead of $HOME/.local/bin
                  for a user install or /usr/local/bin for a
                  system install
  --no-service    do not install the Broker service
  --no-mcp        do not register the MCP server with Claude Code
  --admin-group <name>
                  manage the system Broker through <name> instead of
                  the default peers group; passed to
                  "service install --system"
  --join-admin-group
                  add the account that ran sudo to the admin group without
                  asking
  --no-admin-group
                  do not ask to add the account that ran sudo to the admin
                  group
  -h, --help      print this help and exit

Uninstall with "peers uninstall". This script does not uninstall
anything.
EOF
}

# The archive root is the directory that holds this script. When $0 carries no
# slash, the caller reached the script through the current directory, as in
# "sh install.sh", so resolve it there rather than searching PATH for another
# file of the same name.
case $0 in
	*/*) _script_path=$0 ;;
	*) _script_path=./$0 ;;
esac
case $_script_path in
	*/*)
		if ! script_dir=$(CDPATH='' cd -P "${_script_path%/*}" && pwd); then
			fail "cannot resolve the directory that contains this script"
		fi
		;;
	*)
		if ! script_dir=$(CDPATH='' cd -P . && pwd); then
			fail "cannot resolve the current directory"
		fi
		;;
esac
archive_binary=$script_dir/peers

while [ "$#" -gt 0 ]; do
	case $1 in
		--system)
			system=1
			system_set=1
			;;
		--user)
			user_set=1
			;;
		--prefix)
			if [ "$#" -lt 2 ]; then
				printf 'install.sh: --prefix needs a directory\n' >&2
				usage >&2
				exit 2
			fi
			prefix=$2
			prefix_set=1
			shift
			;;
		--prefix=*)
			prefix=${1#--prefix=}
			prefix_set=1
			;;
		--no-service)
			install_service=0
			;;
		--no-mcp)
			install_mcp=0
			;;
		--admin-group)
			if [ "$#" -lt 2 ]; then
				printf 'install.sh: --admin-group needs a group name\n' >&2
				usage >&2
				exit 2
			fi
			admin_group=$2
			admin_group_set=1
			shift
			;;
		--admin-group=*)
			admin_group=${1#--admin-group=}
			admin_group_set=1
			;;
		--join-admin-group)
			join_admin_group=1
			;;
		--no-admin-group)
			no_admin_group=1
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			printf 'install.sh: unknown argument: %s\n' "$1" >&2
			usage >&2
			exit 2
			;;
	esac
	shift
done

if [ "$system_set" -eq 1 ] && [ "$user_set" -eq 1 ]; then
	printf 'install.sh: --system and --user cannot be used together\n' >&2
	usage >&2
	exit 2
fi

if [ "$prefix_set" -eq 1 ] && [ -z "$prefix" ]; then
	printf 'install.sh: --prefix needs a non-empty directory\n' >&2
	usage >&2
	exit 2
fi

if [ "$admin_group_set" -eq 1 ] && [ -z "$admin_group" ]; then
	printf 'install.sh: --admin-group needs a non-empty group name\n' >&2
	usage >&2
	exit 2
fi

# The service install applies the same rule. A leading hyphen is rejected here
# so the name cannot be mistaken for an option, and the character set matches
# what Linux and macOS accept.
if ! printf '%s\n' "$admin_group" | LC_ALL=C grep -Eq '^[a-z_][a-z0-9_-]*$'; then
	printf 'install.sh: invalid --admin-group name: %s\n' "$admin_group" >&2
	usage >&2
	exit 2
fi

if [ "$join_admin_group" -eq 1 ] && [ "$no_admin_group" -eq 1 ]; then
	printf 'install.sh: --join-admin-group and --no-admin-group cannot be used together\n' >&2
	usage >&2
	exit 2
fi

if ! _current_uid=$(id -u); then
	fail "cannot determine the current user id"
fi

if ! _os=$(uname -s); then
	fail "cannot determine the operating system"
fi

# Choose the install level. An explicit option wins. Root installs for every
# account without asking. An account with a controlling terminal is asked
# whether to install for every account, and whether to upgrade the
# system installation when the host already has one. An account without a
# terminal gets a user install without waiting for input.
if [ "$system_set" -eq 1 ]; then
	system=1
elif [ "$user_set" -eq 1 ]; then
	system=0
elif [ "$_current_uid" -eq 0 ]; then
	system=1
elif true 2>/dev/null </dev/tty; then
	case $_os in
		Darwin) _definition=/Library/LaunchDaemons/io.github.wangzw.peers.plist ;;
		*) _definition=/etc/systemd/system/peers.service ;;
	esac
	_existing_system=0
	# stat takes a bare path on both platforms and succeeds only when it exists.
	if stat "$_definition" >/dev/null 2>&1; then
		_existing_system=1
	fi
	if [ "$_existing_system" -eq 1 ]; then
		_prompt='Upgrade the system installation? [Y/n] '
		_default=1
	else
		_prompt='Install for every account on this host? [y/N] '
		_default=0
	fi
	_choice=0
	if _answer=$(ask_tty "$_prompt"); then
		case $_answer in
			[yY]|[yY][eE][sS]) _choice=1 ;;
			'') _choice=$_default ;;
		esac
	fi
	if [ "$_choice" -eq 1 ]; then
		system=1
	else
		system=0
		if [ "$_existing_system" -eq 1 ]; then
			printf 'install.sh: the system installation was left unchanged.\n'
			exit 0
		fi
	fi
else
	system=0
fi

# A root account that asked for a user install is told to drop --user
# rather than silently installing for every account instead.
if [ "$system" -eq 0 ] && [ "$_current_uid" -eq 0 ]; then
	fail "a user install must not run as root; drop --user to install for every account"
fi

# The admin group belongs to a system install. Reject the options on a
# user install rather than silently doing nothing. The level is known
# only now: an account without --system may have chosen it at the prompt.
if [ "$system" -eq 0 ]; then
	if [ "$admin_group_set" -eq 1 ]; then
		printf 'install.sh: --admin-group is only valid with --system\n' >&2
		usage >&2
		exit 2
	fi
	if [ "$join_admin_group" -eq 1 ] || [ "$no_admin_group" -eq 1 ]; then
		printf 'install.sh: the admin group options are only valid with --system\n' >&2
		usage >&2
		exit 2
	fi
fi

if [ ! -f "$archive_binary" ]; then
	fail "no peers binary next to $0; extract the whole release archive and run sh ./install.sh from it"
fi

# A system install needs root. Recheck the same script under sudo with the
# options the caller gave, so a system choice made at the prompt and an
# explicit --system take the same path.
if [ "$system" -eq 1 ] && [ "$_current_uid" -ne 0 ]; then
	set -- --system
	if [ "$prefix_set" -eq 1 ]; then
		set -- "$@" --prefix "$prefix"
	fi
	if [ "$install_service" -eq 0 ]; then
		set -- "$@" --no-service
	fi
	if [ "$install_mcp" -eq 0 ]; then
		set -- "$@" --no-mcp
	fi
	if [ "$admin_group_set" -eq 1 ]; then
		set -- "$@" --admin-group "$admin_group"
	fi
	if [ "$join_admin_group" -eq 1 ]; then
		set -- "$@" --join-admin-group
	fi
	if [ "$no_admin_group" -eq 1 ]; then
		set -- "$@" --no-admin-group
	fi
	if ! command -v sudo >/dev/null 2>&1; then
		fail "a system install needs sudo, which was not found; install sudo, or pass --user for a user install"
	fi
	_sudo_status=0
	sudo sh "$script_dir/install.sh" "$@" || _sudo_status=$?
	case $_sudo_status in
		0|3) exit "$_sudo_status" ;;
	esac
	printf 'install.sh: the system installation did not complete: sudo exited with status %d.\n' "$_sudo_status" >&2
	# A sudo that cannot run at all cannot be fixed by rerunning the same
	# command; a user install does not need sudo. -n keeps the check from
	# asking for a password a second time on a terminal that is still open.
	if sudo -n -v >/dev/null 2>&1; then
		printf 'Fix the error above and run the script again.\n' >&2
	else
		printf 'sudo is not usable; pass --user for a user install.\n' >&2
	fi
	exit "$_sudo_status"
fi

if [ "$system" -eq 1 ]; then
	if [ -z "$prefix" ]; then
		prefix=/usr/local/bin
	fi
	case $prefix in
		/*) ;;
		*) fail "a --prefix for the system installation must be an absolute path" ;;
	esac
else
	if [ -z "$prefix" ]; then
		if [ -z "${HOME:-}" ]; then
			fail "HOME is not set; pass --prefix with the directory to install into"
		fi
		prefix=$HOME/.local/bin
	fi
	if ! mkdir -p "$prefix"; then
		fail "cannot create the install directory $prefix"
	fi
fi

# A system Broker runs as root, so the binary and every directory above
# it must be owned by root and not writable by group or others. Otherwise an
# ordinary account could replace the binary and take over the service.
if [ "$system" -eq 1 ]; then
	if [ ! -d "$prefix" ]; then
		fail "the system install directory $prefix does not exist; create it first (for example with 'sudo mkdir -p $prefix') or pass --prefix"
	fi
	if ! prefix=$(CDPATH='' cd -P "$prefix" && pwd); then
		fail "cannot resolve the install directory $prefix"
	fi

	# check_system_dir fails unless one directory is root-owned and cannot be
	# written by group or others. stat takes different flags on Linux and
	# macOS, so try the GNU form first and fall back to the BSD form.
	check_system_dir() {
		_dir=$1
		if ! _uid=$(stat -L -c '%u' "$_dir" 2>/dev/null); then
			if ! _uid=$(stat -L -f '%u' "$_dir" 2>/dev/null); then
				fail "cannot read the owner of $_dir; pass --prefix with a directory whose owner can be checked"
			fi
		fi
		if ! _mode=$(stat -L -c '%a' "$_dir" 2>/dev/null); then
			if ! _mode=$(stat -L -f '%Lp' "$_dir" 2>/dev/null); then
				fail "cannot read the mode of $_dir; pass --prefix with a directory whose mode can be checked"
			fi
		fi
		if [ "$_uid" != "0" ]; then
			fail "system install: $_dir is not owned by root; pass --prefix with a root-owned directory such as /usr/local/bin"
		fi
		case $_mode in
			[0-7][0-7][0-7]|[0-7][0-7][0-7][0-7]) ;;
			*) fail "system install: unexpected mode '$_mode' for $_dir" ;;
		esac
		# The last three octal digits are the permission bits. A leading zero
		# makes the shell read them as octal, so 022 selects group write and
		# other write.
		_last=${_mode#"${_mode%???}"}
		if [ $((0$_last & 022)) -ne 0 ]; then
			fail "system install: $_dir is writable by group or others; pass --prefix with a directory such as /usr/local/bin"
		fi
		# Every directory above the binary must also be searchable by others.
		# Otherwise the service, which may run with a reduced set of
		# supplementary groups, cannot traverse the path to the binary.
		if [ $((0$_last & 001)) -eq 0 ]; then
			fail "system install: $_dir is not searchable by others; pass --prefix with a directory that others can enter, such as /usr/local/bin"
		fi
	}

	_check=$prefix
	while :; do
		check_system_dir "$_check"
		[ "$_check" = "/" ] && break
		_check=$(dirname "$_check")
	done
fi

# The macOS release binaries are not signed or notarized. A browser download
# carries the quarantine attribute, and macOS blocks the binary until it is
# removed. The attribute is absent after a command-line download, and xattr
# then fails; that is not an error here.
if [ "$_os" = "Darwin" ]; then
	xattr -d com.apple.quarantine "$archive_binary" 2>/dev/null || true
fi

# Record the version of a binary already at the target path, so the summary can
# report an upgrade. Only the peers binary answers with the expected
# prefix; anything else is replaced without claiming an upgrade.
_previous_version=
if [ -f "$prefix/peers" ]; then
	if _previous_output=$("$prefix/peers" --version 2>/dev/null); then
		case $_previous_output in
			"peers "*) _previous_version=${_previous_output#peers } ;;
		esac
	fi
fi

# 1. Install the binary.
if [ "$system" -eq 1 ]; then
	if ! install -o root -g 0 -m 0755 "$archive_binary" "$prefix/peers"; then
		fail "cannot install the binary to $prefix"
	fi
else
	if ! install -m 0755 "$archive_binary" "$prefix/peers"; then
		fail "cannot install the binary to $prefix"
	fi
fi

# 2. Check that the installed binary actually runs and identifies itself. A
# failure here usually means the archive was built for another platform.
if ! _version_output=$("$prefix/peers" --version 2>&1); then
	fail "cannot run $prefix/peers --version: the binary may have been built for another platform"
fi
case $_version_output in
	"peers "*) ;;
	*) fail "unexpected output from $prefix/peers --version: $_version_output" ;;
esac

# Report the version that was replaced before the service prints its summary;
# the service does not know what was installed before. The new version comes
# from the binary just installed, and the comparison decides whether this run
# upgraded, reinstalled or downgraded; a version whose order cannot be
# established is reported as a replacement.
_installed_version=${_version_output#peers }
if [ -n "$_previous_version" ]; then
	case $(compare_versions "$_installed_version" "$_previous_version") in
		older) printf 'Downgraded from %s.\n' "$_previous_version" ;;
		same) printf 'Reinstalled peers %s.\n' "$_installed_version" ;;
		newer) printf 'Upgraded from %s.\n' "$_previous_version" ;;
		*) printf 'Replaced peers %s.\n' "$_previous_version" ;;
	esac
fi

# 3. Install the service. service install is repeatable: an existing service is
# restarted with the new binary, so the script does not check first. Exit status
# 3 means the service is installed and running, but the Broker runs degraded;
# service install has already printed the summary and the reason, so the script
# finishes the remaining steps and exits with 3 at the end.
_service_status=0
_service_degraded=0
_service_level="user"
if [ "$install_service" -eq 1 ]; then
	if [ "$system" -eq 1 ]; then
		_service_level="system"
		if [ "$admin_group_set" -eq 1 ]; then
			"$prefix/peers" service install --system --admin-group "$admin_group" || _service_status=$?
		else
			"$prefix/peers" service install --system || _service_status=$?
		fi
	else
		"$prefix/peers" service install || _service_status=$?
	fi
	case $_service_status in
		0) ;;
		3) _service_degraded=1 ;;
		*) fail "cannot install the $_service_level service" ;;
	esac
fi

# 4. Install the shell completions, and for the system installation the man pages. The
# completion scripts come from the installed binary, so they always match it.
# This runs after the service so that a completion or man page that cannot be
# written does not fail an otherwise complete installation: each failure is a
# warning and the script keeps its exit status. A user install skips the
# zsh completion, because a user file only becomes active after the
# account adds it to fpath; the caller is told how. The fish completion is
# installed only when fish itself is installed. New files and directories are
# group- and world-readable regardless of root's umask. DESTDIR prefixes the
# system share directory, for staged installs and tests; production
# leaves it unset.
umask 022
_share=${DESTDIR:-}/usr/local/share
_completion_bash_dir=
_completion_fish_dir=
if [ "$system" -eq 1 ]; then
	_completion_bash_dir=$_share/bash-completion/completions
	_completion_zsh_dir=$_share/zsh/site-functions
	_completion_fish_dir=$_share/fish/vendor_completions.d
	_man_dir=$_share/man/man1
elif [ -n "${HOME:-}" ]; then
	_completion_bash_dir=$HOME/.local/share/bash-completion/completions
	_completion_fish_dir=$HOME/.config/fish/completions
else
	printf 'install.sh: warning: HOME is not set; the shell completions were not installed\n' >&2
fi

# The accounts that may own a directory the system installation writes into:
# the account running the script, which is root in production, and, when sudo
# started it, the account that ran sudo. Homebrew on Intel macOS owns the
# directories under /usr/local/share as the account that installed it, and
# that account could already become root through sudo.
if [ "$system" -eq 1 ]; then
	_allowed_uids=" $_current_uid "
	_owner_name=root
	_sudo_uid=
	if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
		if _sudo_uid=$(id -u "$SUDO_USER" 2>/dev/null); then
			_allowed_uids="$_allowed_uids $_sudo_uid "
			_owner_name="root or $SUDO_USER"
		fi
	fi
fi

# as_sudo_user runs a command as the account that ran sudo.
as_sudo_user() {
	sudo -u "$SUDO_USER" "$@"
}

# check_install_dir checks one directory the system installation writes into.
# It returns 0 when the directory exists, is not a symlink, is owned by an
# allowed account and is not writable by group or others; otherwise it returns 1
# and sets _dir_reason, which the caller turns into a warning. On success it
# sets _dir_needs_user to 1 when the account that ran sudo owns the directory,
# so the caller must write into it as that account. It reads the owner and mode
# with the same GNU and BSD stat forms as check_system_dir; a directory writable
# by group or others is refused outright, which is stricter than writing into it
# as the account.
check_install_dir() {
	_check_dir=$1
	if [ -L "$_check_dir" ]; then
		_dir_reason="$_check_dir is a symbolic link"
		return 1
	fi
	if [ -e "$_check_dir" ] && [ ! -d "$_check_dir" ]; then
		_dir_reason="$_check_dir is not a directory"
		return 1
	fi
	if ! _dir_uid=$(stat -L -c '%u' "$_check_dir" 2>/dev/null); then
		if ! _dir_uid=$(stat -L -f '%u' "$_check_dir" 2>/dev/null); then
			_dir_reason="cannot read the owner of $_check_dir"
			return 1
		fi
	fi
	if ! _dir_mode=$(stat -L -c '%a' "$_check_dir" 2>/dev/null); then
		if ! _dir_mode=$(stat -L -f '%Lp' "$_check_dir" 2>/dev/null); then
			_dir_reason="cannot read the mode of $_check_dir"
			return 1
		fi
	fi
	case " $_allowed_uids " in
		*" $_dir_uid "*) ;;
		*)
			_dir_reason="$_check_dir is not owned by $_owner_name"
			return 1
			;;
	esac
	case $_dir_mode in
		[0-7][0-7][0-7]|[0-7][0-7][0-7][0-7]) ;;
		*)
			_dir_reason="unexpected mode '$_dir_mode' for $_check_dir"
			return 1
			;;
	esac
	# A directory owned by the account that ran sudo can have an entry swapped
	# by another process of that account between this check and a later write.
	# Record the decision here, from the owner this check already read, so the
	# caller never has to stat the directory a second time; a second stat would
	# follow a link the account put in place after this check.
	_dir_needs_user=0
	if [ -n "${_sudo_uid:-}" ] && [ "$_dir_uid" = "$_sudo_uid" ]; then
		_dir_needs_user=1
	fi
	# The last three octal digits are the permission bits; 022 selects group
	# write and other write, as in check_system_dir.
	_dir_last=${_dir_mode#"${_dir_mode%???}"}
	if [ $((0$_dir_last & 022)) -ne 0 ]; then
		_dir_reason="$_check_dir is writable by group or others"
		return 1
	fi
	return 0
}

# ensure_install_dir checks every path component from ${DESTDIR:-}/usr/local
# down to $1 and creates the missing ones only after the component above them
# has passed. A symlink anywhere in the path is refused before anything is
# created through it. On success it sets _ensure_write_as_user to 1 when any
# component is owned by the account that ran sudo; the caller must then perform
# every later write into $1 (the temporary file, its content, its mode and the
# rename) as that account. It is 0 only when every component is owned by root
# and not writable by group or others, where no other account can replace any
# component after the check, so root may write directly.
#
# A directory created below one owned by the account that ran sudo is created
# by that account with sudo -u; the decision comes from the accumulated walk,
# never from a second stat. That account can replace any component below the one
# it owns, so a root-owned immediate parent does not make root's mkdir safe.
ensure_install_dir() {
	_ensure_write_as_user=0
	_ensure_install_dir_walk "$1"
}

_ensure_install_dir_walk() {
	_ensure_stop=${DESTDIR:-}/usr/local
	if [ "$1" = "$_ensure_stop" ] || [ "$1" = "/" ]; then
		if [ ! -e "$1" ]; then
			# In production /usr/local exists; a staged installation under
			# DESTDIR may not have it yet.
			if [ "$1" = "/" ] || ! mkdir -p "$1"; then
				_dir_reason="$1 does not exist"
				return 1
			fi
		fi
		check_install_dir "$1" || return 1
		if [ "$_dir_needs_user" -eq 1 ]; then
			_ensure_write_as_user=1
		fi
		return 0
	fi
	_ensure_install_dir_walk "$(dirname "$1")" || return 1
	if [ -L "$1" ] || [ -e "$1" ]; then
		check_install_dir "$1" || return 1
		if [ "$_dir_needs_user" -eq 1 ]; then
			_ensure_write_as_user=1
		fi
		return 0
	fi
	# _ensure_write_as_user covers every component above this one. When any of
	# them belongs to the account that ran sudo, that account could swap a
	# lower component for a link after its check, so the account creates the
	# directory; root's mkdir would follow such a link.
	if [ "$_ensure_write_as_user" -eq 1 ]; then
		as_sudo_user mkdir -m 0755 "$1" || {
			_dir_reason="cannot create $1"
			return 1
		}
	elif ! mkdir -m 0755 "$1"; then
		_dir_reason="cannot create $1"
		return 1
	fi
	check_install_dir "$1" || return 1
	if [ "$_dir_needs_user" -eq 1 ]; then
		_ensure_write_as_user=1
	fi
	return 0
}

# install_completion writes one completion script through a temporary file in
# the same directory. The rename replaces a symlink at the target instead of
# following it, so a link cannot redirect the write. A failed run removes the
# temporary file and leaves the target unchanged. It returns 0 on success, 1
# when the file cannot be written, and 2 when a directory, not a symlink, sits
# at the target, which would swallow the rename; _completion_reason explains
# the skip in the last case.
#
# A directory the caller must write as the account that ran sudo ($4 is 1) is
# written by that account through sudo -u: it creates the temporary file, copies
# the generated content into it, sets the mode and renames it into place. The
# content is generated once into a root-owned temporary file under /tmp, whose
# sticky bit keeps the account from replacing it, and root never creates, writes
# or renames a file in the account's directory. $4 is decided once by
# ensure_install_dir from the owners it already read; this function never stats
# the directory again, because a second stat would follow a link the account
# swapped in after the check. Otherwise root writes there directly.
install_completion() {
	_write_shell=$1
	_write_dir=$2
	_write_name=$3
	_write_as_user=$4
	_write_target=$_write_dir/$_write_name
	_completion_reason=
	if [ -d "$_write_target" ] && [ ! -L "$_write_target" ]; then
		_completion_reason="$_write_target is a directory"
		return 2
	fi
	if [ "$_write_as_user" -eq 1 ]; then
		# /tmp is sticky, so the account cannot replace this root-owned file
		# after mktemp creates it.
		_write_stage=$(mktemp /tmp/peers-completion-XXXXXX) || return 1
		if ! "$prefix/peers" completion "$_write_shell" > "$_write_stage"; then
			remove_stage
			return 1
		fi
		if ! chmod 0644 "$_write_stage"; then
			remove_stage
			return 1
		fi
		_write_tmp=$(as_sudo_user mktemp "$_write_dir/.peers-XXXXXX") || {
			remove_stage
			return 1
		}
		if ! as_sudo_user cp "$_write_stage" "$_write_tmp"; then
			remove_stage
			as_sudo_user rm -f "$_write_tmp"
			return 1
		fi
		remove_stage
		if ! as_sudo_user chmod 0644 "$_write_tmp"; then
			as_sudo_user rm -f "$_write_tmp"
			return 1
		fi
		# mv follows a symlink that points at a directory and moves the file
		# inside it. Remove such a link first so the rename replaces the link
		# itself; rm removes the link, never what it points at.
		if [ -L "$_write_target" ] && [ -d "$_write_target" ]; then
			if ! as_sudo_user rm -f "$_write_target"; then
				as_sudo_user rm -f "$_write_tmp"
				return 1
			fi
		fi
		if ! as_sudo_user mv -f "$_write_tmp" "$_write_target"; then
			as_sudo_user rm -f "$_write_tmp"
			return 1
		fi
		return 0
	fi
	_write_tmp=$(mktemp "$_write_dir/.peers-XXXXXX") || return 1
	if ! "$prefix/peers" completion "$_write_shell" > "$_write_tmp"; then
		rm -f "$_write_tmp"
		return 1
	fi
	if ! chmod 0644 "$_write_tmp"; then
		rm -f "$_write_tmp"
		return 1
	fi
	# mv follows a symlink that points at a directory and moves the file
	# inside it. Remove such a link first so the rename replaces the link
	# itself; rm removes the link, never what it points at.
	if [ -L "$_write_target" ] && [ -d "$_write_target" ]; then
		if ! rm -f "$_write_target"; then
			rm -f "$_write_tmp"
			return 1
		fi
	fi
	if ! mv -f "$_write_tmp" "$_write_target"; then
		rm -f "$_write_tmp"
		return 1
	fi
	return 0
}

# warn_completion_skipped reports a completion the installation left alone,
# naming the reason and the command that installs it by hand.
warn_completion_skipped() {
	_warn_shell=$1
	_warn_target=$2
	_warn_reason=$3
	printf 'install.sh: warning: skipped the %s completion: %s; install it with: %s completion %s > %s\n' \
		"$_warn_shell" "$_warn_reason" "$prefix/peers" "$_warn_shell" "$_warn_target" >&2
}

# place_completion installs one completion. $4 is 1 for a system installation,
# which checks and creates the directory tree first, and 0 for a user
# installation, which does not. For a system installation the write identity is
# the one ensure_install_dir decided while it walked the path; it is passed to
# install_completion unchanged. Every failure is a warning: the service is
# already installed and the script keeps its exit status.
place_completion() {
	_place_shell=$1
	_place_dir=$2
	_place_name=$3
	_place_check=$4
	_place_target=$_place_dir/$_place_name
	_place_as_user=0
	if [ "$_place_check" -eq 1 ]; then
		if ! ensure_install_dir "$_place_dir"; then
			warn_completion_skipped "$_place_shell" "$_place_target" "$_dir_reason"
			return 0
		fi
		_place_as_user=$_ensure_write_as_user
	elif ! mkdir -p "$_place_dir"; then
		printf 'install.sh: warning: could not write the %s completion to %s\n' "$_place_shell" "$_place_target" >&2
		return 0
	fi
	_place_rc=0
	install_completion "$_place_shell" "$_place_dir" "$_place_name" "$_place_as_user" || _place_rc=$?
	case $_place_rc in
		0) ;;
		2) warn_completion_skipped "$_place_shell" "$_place_target" "$_completion_reason" ;;
		*) printf 'install.sh: warning: could not write the %s completion to %s\n' "$_place_shell" "$_place_target" >&2 ;;
	esac
	return 0
}

if [ -n "$_completion_bash_dir" ]; then
	place_completion bash "$_completion_bash_dir" peers "$system"
fi
if [ -n "$_completion_fish_dir" ] && command -v fish >/dev/null 2>&1; then
	place_completion fish "$_completion_fish_dir" peers.fish "$system"
fi

if [ "$system" -eq 1 ]; then
	place_completion zsh "$_completion_zsh_dir" _peers 1
	if ! ensure_install_dir "$_man_dir"; then
		for _page in "$script_dir"/man/*.1; do
			[ -e "$_page" ] || continue
			printf 'install.sh: warning: skipped the man page %s: %s; install it with: install -m 0644 %s %s/\n' \
				"${_page##*/}" "$_dir_reason" "$_page" "$_man_dir" >&2
		done
	else
		# The write identity comes from the walk above; it is never a second
		# stat of the destination, which a swapped ancestor could redirect to a
		# root-owned directory.
		_man_as_user=$_ensure_write_as_user
		for _page in "$script_dir"/man/*.1; do
			[ -e "$_page" ] || continue
			if [ "$_man_as_user" -eq 1 ]; then
				# The account that owns the directory writes the page itself, so
				# root never creates a file there that a swapped link could
				# redirect.
				if ! as_sudo_user install -m 0644 "$_page" "$_man_dir/"; then
					printf 'install.sh: warning: could not install the man page %s\n' \
						"${_page##*/}" >&2
				fi
			elif ! install -o root -g 0 -m 0644 "$_page" "$_man_dir/"; then
				printf 'install.sh: warning: could not install the man page %s\n' \
					"${_page##*/}" >&2
			fi
		done
	fi
else
	printf 'install.sh: the zsh completion was not installed; enable it with: %s completion zsh > ~/.zfunc/_peers (create ~/.zfunc and add it to fpath).\n' "$prefix/peers"
fi

# 5. The system service creates the admin group. Put the account that ran
# sudo into it, so that account can manage the Broker without sudo after
# logging in again. The answer comes from the controlling terminal, never from
# stdin: when the script is piped into sh, stdin is the script itself. A
# missing terminal, an empty answer and any answer other than y or yes all
# leave the group alone and print the command that would join it. The account
# is added at most once.
_admin_group_user=
if [ "$system" -eq 1 ] && [ "$install_service" -eq 1 ] && [ "$no_admin_group" -eq 0 ]; then
	_sudo_user=${SUDO_USER:-}
	if [ -n "$_sudo_user" ] && [ "$_sudo_user" != "root" ]; then
		if ! _user_groups=$(id -Gn "$_sudo_user" 2>/dev/null); then
			fail "cannot list the groups of $_sudo_user"
		fi
		case " $_user_groups " in
			*" $admin_group "*) _in_admin_group=1 ;;
			*) _in_admin_group=0 ;;
		esac
		if [ "$_in_admin_group" -eq 0 ]; then
			_join=0
			if [ "$join_admin_group" -eq 1 ]; then
				_join=1
			# true, not the special builtin ":", so a missing terminal fails
			# this test instead of exiting the whole shell.
			elif true 2>/dev/null </dev/tty; then
				if printf 'Add %s to the %s group so %s can manage the Broker without sudo? [y/N] ' \
					"$_sudo_user" "$admin_group" "$_sudo_user" 2>/dev/null >/dev/tty; then
					if _answer=$( { read -r _reply && printf '%s' "$_reply"; } 2>/dev/null </dev/tty ); then
						case $_answer in
							[yY]|[yY][eE][sS]) _join=1 ;;
						esac
					fi
					printf '\n' 2>/dev/null >/dev/tty || true
				fi
			fi
			if [ "$_join" -eq 1 ]; then
				if [ "$_os" = "Darwin" ]; then
					if ! dseditgroup -o edit -a "$_sudo_user" -t user "$admin_group"; then
						fail "cannot add $_sudo_user to the $admin_group group"
					fi
				else
					if ! usermod -aG "$admin_group" "$_sudo_user"; then
						fail "cannot add $_sudo_user to the $admin_group group"
					fi
				fi
				_admin_group_user=$_sudo_user
			elif [ "$_os" = "Darwin" ]; then
				printf 'install.sh: to add %s to the %s group, run: sudo dseditgroup -o edit -a %s -t user %s\n' \
					"$_sudo_user" "$admin_group" "$_sudo_user" "$admin_group"
			else
				printf 'install.sh: to add %s to the %s group, run: sudo usermod -aG %s %s\n' \
					"$_sudo_user" "$admin_group" "$admin_group" "$_sudo_user"
			fi
		fi
	fi
fi

# 6. Register the MCP server with Claude Code. A system install registers
# it for the account that ran sudo; running the script directly as root leaves
# no account to register for, so it prints the command instead.
_mcp_state="skipped (--no-mcp)"
if [ "$install_mcp" -eq 1 ]; then
	_target_user=
	_find_claude=0
	if [ "$system" -eq 1 ]; then
		_target_user=${SUDO_USER:-}
		if [ -z "$_target_user" ] || [ "$_target_user" = "root" ]; then
			_mcp_state="skipped (no account to register for)"
			printf 'install.sh: register the MCP server for your account manually:\n'
			printf '  claude mcp add --scope user peer -- %s mcp\n' "$prefix/peers"
		else
			_find_claude=1
		fi
	else
		if ! _target_user=$(id -un); then
			fail "cannot determine the current user"
		fi
		_find_claude=1
	fi

	if [ "$_find_claude" -eq 1 ]; then
		if [ "$system" -eq 1 ]; then
			# Resolve claude through the target account's own login shell. The
			# account may configure PATH in a zsh or bash profile that a plain
			# sh does not read. getent answers on Linux and dscl on macOS; when
			# neither knows the account, fall back to sh.
			_target_shell=
			if command -v getent >/dev/null 2>&1; then
				_target_shell=$(getent passwd "$_target_user" 2>/dev/null | cut -d: -f7)
			elif command -v dscl >/dev/null 2>&1; then
				_target_shell=$(dscl . -read "/Users/$_target_user" UserShell 2>/dev/null | sed -n 's/^UserShell: //p')
			fi
			[ -n "$_target_shell" ] || _target_shell=/bin/sh
			_claude_path=$(sudo -u "$_target_user" -H "$_target_shell" -lc 'command -v claude' 2>/dev/null) || _claude_path=
		else
			_claude_path=$(command -v claude 2>/dev/null) || _claude_path=
		fi

		if [ -z "$_claude_path" ]; then
			_mcp_state="skipped (claude not found)"
			printf 'install.sh: claude was not found; register the MCP server manually:\n'
			printf '  claude mcp add --scope user peer -- %s mcp\n' "$prefix/peers"
		else
			if [ "$system" -eq 1 ]; then
				# The login shell resolved claude to an absolute path, so run
				# that path directly as the target account.
				_get_output=$(sudo -u "$_target_user" -H "$_claude_path" mcp get peer 2>&1) && _get_status=0 || _get_status=$?
			else
				_get_output=$("$_claude_path" mcp get peer 2>&1) && _get_status=0 || _get_status=$?
			fi

			if [ "$_get_status" -eq 0 ]; then
				_existing=$(printf '%s\n' "$_get_output" | sed -n 's/^[[:space:]]*Command:[[:space:]]*//p' | head -n 1)
				if [ -n "$_existing" ] && [ "$_existing" != "$prefix/peers" ]; then
					_mcp_state="already registered with a different command; left unchanged"
					printf 'install.sh: the peer MCP server is already registered with %s; leaving it unchanged.\n' "$_existing"
					printf 'install.sh: to point it at this install, run "claude mcp remove -s user peer", then rerun this script.\n'
				else
					_mcp_state="already registered"
					printf 'install.sh: the peer MCP server is already registered; leaving it unchanged.\n'
				fi
			else
				if [ "$system" -eq 1 ]; then
					if ! sudo -u "$_target_user" -H "$_claude_path" mcp add --scope user peer -- "$prefix/peers" mcp; then
						fail "cannot register the MCP server for $_target_user"
					fi
				else
					if ! "$_claude_path" mcp add --scope user peer -- "$prefix/peers" mcp; then
						fail "cannot register the MCP server"
					fi
				fi
				_mcp_state="registered"
			fi
		fi
	fi
fi

# 7. A user prefix is often outside PATH. Say so instead of leaving the
# caller with "peers: command not found" in the next terminal.
if [ "$system" -eq 0 ]; then
	case ":${PATH:-}:" in
		*":$prefix:"*) ;;
		*) printf 'install.sh: %s is not on PATH; add it to PATH to run peers by name.\n' "$prefix" ;;
	esac
fi

# 8. Summarize what the script alone knows. service install printed the binary,
# version, configuration, logs and service commands. Without a service, the
# script reports the binary it placed.
if [ "$install_service" -eq 0 ]; then
	printf '\npeers %s installed (no service)\n' "${_version_output#peers }"
	printf '  %-12s%s\n' 'binary:' "$prefix/peers"
fi
printf '  %-12s%s\n' 'MCP:' "$_mcp_state"
if [ -n "$_admin_group_user" ]; then
	printf "\n%s was added to the %s group; log in again or run \`newgrp %s\` to manage the Broker without sudo.\n" \
		"$_admin_group_user" "$admin_group" "$admin_group"
fi
printf '\nRestart any Claude Code session that is already running so it loads the peer MCP server.\n'

# A degraded service is installed and running, so the installation succeeded;
# the exit status tells the caller that the Broker needs attention.
if [ "$_service_degraded" -eq 1 ]; then
	exit 3
fi
