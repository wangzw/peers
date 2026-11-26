#!/bin/sh
# Install peers with one command from a published GitHub release.
#
# Usage:
#   curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
#   curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh -s -- --version <version>
#
# The packaged install.sh chooses the installation level. It asks on the
# controlling terminal whether to install for every account, runs itself
# through sudo for that choice, and installs for every account without asking
# when it runs as root.
# Pass --system or --user after "--" to skip the question.
#
# The script selects the release for this host's operating system and
# architecture, downloads the archive and checksums.txt, verifies the archive
# against its SHA-256 checksum, extracts it, and runs the install.sh the archive
# contains. The installation rules, including the installation level and the
# sudo run, belong to that packaged install.sh. This script forwards every
# argument it does not use itself unchanged.
#
# The whole body is the main function, and the call to it is the last line of
# this file. If the download is cut short before that line, the shell defines
# the functions but runs none of them, so a half-downloaded script cannot
# start an installation.
#
# Options:
#   --version <version>
#                  install a published version number, for example 0.6.1,
#                  instead of the latest release. It must come before every
#                  other argument, and its value is always validated. A version
#                  with no installable release package fails with
#                  "version <version> not found".
#
# PEERS_TEST_RELEASE_BASE replaces the release URL prefix. It exists for
# the end-to-end tests only, and it may point at a plain HTTP server. After it
# is exported, every run in that same shell uses it until it is unset, so a
# user who sets it by hand changes only his own runs. sudo's default env_reset
# drops the variable, so a sudo run goes to the real release URL.

set -eu

default_release_base=https://github.com/wangzw/peers/releases

# The test hook may point at a local HTTP server, so the scheme of the base
# decides which transport restriction is applied below.
release_base=${PEERS_TEST_RELEASE_BASE:-$default_release_base}
release_base=${release_base%/}

fail() {
	printf 'remote-install.sh: %s\n' "$1" >&2
	exit 1
}

# validate_version accepts MAJOR.MINOR.PATCH with digits only. The version ends
# up in a download path and a file name, so rejecting everything else keeps a
# malformed value from changing either.
validate_version() {
	case $1 in
		''|*[!0-9.]*)
			fail "invalid version '$1': expected MAJOR.MINOR.PATCH, for example 0.6.1"
			;;
	esac
	_rest=$1
	_major=${_rest%%.*}
	case $_rest in
		*.*) _rest=${_rest#*.} ;;
		*)
			fail "invalid version '$1': expected MAJOR.MINOR.PATCH, for example 0.6.1"
			;;
	esac
	_minor=${_rest%%.*}
	case $_rest in
		*.*) _patch=${_rest#*.} ;;
		*)
			fail "invalid version '$1': expected MAJOR.MINOR.PATCH, for example 0.6.1"
			;;
	esac
	case $_major in
		''|*[!0-9]*) fail "invalid version '$1': expected MAJOR.MINOR.PATCH, for example 0.6.1" ;;
	esac
	case $_minor in
		''|*[!0-9]*) fail "invalid version '$1': expected MAJOR.MINOR.PATCH, for example 0.6.1" ;;
	esac
	case $_patch in
		''|*[!0-9]*) fail "invalid version '$1': expected MAJOR.MINOR.PATCH, for example 0.6.1" ;;
	esac
}

# check_wget_locations reads a wget header dump from stdin and fails when any
# Location header is an absolute URL whose scheme is not HTTPS. A relative
# Location keeps the current scheme, so it is accepted.
check_wget_locations() {
	_bad=$(sed -n 's/^[[:space:]]*[Ll]ocation:[[:space:]]*//p' \
		| sed -n '/^[A-Za-z][A-Za-z0-9+.-]*:/{ /^[Hh][Tt][Tt][Pp][Ss]:\/\//!p; }')
	if [ -n "$_bad" ]; then
		fail "refusing a wget redirect to a non-HTTPS URL: $_bad"
	fi
}

download() {
	_url=$1
	_dest=$2
	if [ "$downloader" = curl ]; then
		if [ "$https_only" -eq 1 ]; then
			curl --proto '=https' --tlsv1.2 -fsSL -o "$_dest" "$_url"
		else
			curl -fsSL -o "$_dest" "$_url"
		fi
		return
	fi
	# wget's --https-only limits recursion, not the redirects of a single
	# download, so a download from an HTTPS URL could still end at plain HTTP.
	# The header dump of this same request is inspected instead, and an
	# absolute Location that is not HTTPS aborts before the body is used; that
	# is fail-closed. --secure-protocol=TLSv1_2 rules out older TLS versions on
	# the HTTPS hops.
	if [ "$https_only" -eq 1 ]; then
		_headers="${_dest}.headers"
		if ! wget --https-only --secure-protocol=TLSv1_2 --server-response \
			-O "$_dest" "$_url" 2>"$_headers"; then
			return 1
		fi
		check_wget_locations < "$_headers"
	else
		wget -q -O "$_dest" "$_url"
	fi
}

# effective_url prints the URL that <url> finally resolves to after redirects.
# curl reports it directly. wget does not, so its --server-response output is
# read for the Location header of the last redirect; that header goes to
# stderr, which is captured here.
effective_url() {
	_url=$1
	if [ "$downloader" = curl ]; then
		if [ "$https_only" -eq 1 ]; then
			curl --proto '=https' --tlsv1.2 -fsSL -o /dev/null -w '%{url_effective}' "$_url"
		else
			curl -fsSL -o /dev/null -w '%{url_effective}' "$_url"
		fi
		return
	fi

	if [ "$https_only" -eq 1 ]; then
		_headers=$(wget --https-only --secure-protocol=TLSv1_2 --server-response \
			--spider --max-redirect=10 "$_url" 2>&1) || return 1
		check_wget_locations <<__WGET_HEADERS_END__
$_headers
__WGET_HEADERS_END__
	else
		_headers=$(wget --server-response --spider --max-redirect=10 "$_url" 2>&1) || return 1
	fi
	printf '%s\n' "$_headers" | sed -n 's/^[[:space:]]*[Ll]ocation:[[:space:]]*//p' | tail -n 1
}

main() {
	version=
	version_requested=0
	if [ "$#" -gt 0 ]; then
		case $1 in
			--version)
				version_requested=1
				if [ "$#" -lt 2 ]; then
					fail "--version needs a published version number, for example 0.6.1"
				fi
				version=$2
				shift 2
				;;
			--version=*)
				version_requested=1
				version=${1#--version=}
				shift
				;;
		esac
	fi
	# Every other argument goes to the packaged install.sh, so a --version
	# found later is a usage error rather than something the installer would
	# reject with a less helpful message.
	for _arg in "$@"; do
		case $_arg in
			--version|--version=*)
				fail "--version must come before every other argument"
				;;
		esac
	done
	# --version always validates its value. An empty value (--version "" or
	# --version=) is rejected here instead of being treated as "no version
	# requested", which would silently install the latest release.
	if [ "$version_requested" -eq 1 ]; then
		validate_version "$version"
	fi

	# 1. Pick the downloader, which the latest-release lookup needs. curl is
	# preferred; wget is the fallback.
	if command -v curl >/dev/null 2>&1; then
		downloader=curl
	elif command -v wget >/dev/null 2>&1; then
		downloader=wget
	else
		fail "neither curl nor wget is installed; install one of them and run this script again"
	fi

	case $release_base in
		https://*) https_only=1 ;;
		http://*) https_only=0 ;;
		*) fail "the release base must start with http:// or https://: $release_base" ;;
	esac

	# 2. Resolve the version unless --version fixed it. releases/latest answers
	# with a redirect to the tag, so the version is the last path component;
	# this avoids the GitHub API.
	if [ "$version_requested" -eq 0 ]; then
		if ! _effective=$(effective_url "$release_base/latest"); then
			fail "cannot resolve the latest release from $release_base/latest"
		fi
		_effective=${_effective%/}
		_tag=${_effective##*/}
		case $_tag in
			v*) version=${_tag#v} ;;
			*) fail "cannot read a release version from $release_base/latest; it resolved to '$_effective'" ;;
		esac
		validate_version "$version"
	fi

	# 3. Select the release for this platform.
	_os_name=$(uname -s) || fail "cannot determine the operating system"
	case $_os_name in
		Darwin) os=darwin ;;
		Linux) os=linux ;;
		*) fail "unsupported operating system '$_os_name': this script supports macOS and Linux" ;;
	esac

	_machine=$(uname -m) || fail "cannot determine the machine architecture"
	case $_machine in
		x86_64|amd64) arch=amd64 ;;
		arm64|aarch64) arch=arm64 ;;
		*) fail "unsupported architecture '$_machine': this script supports x86_64 and arm64" ;;
	esac

	archive="peers_${version}_${os}_${arch}.tar.gz"
	release_url=$release_base/download/v$version

	# 4. Download into a temporary directory that exists only for this run. The
	# EXIT trap covers an ordinary failure and the explicit signal traps make an
	# interrupt leave through the same path. The template is explicit because
	# BSD mktemp, unlike GNU mktemp, ignores TMPDIR when it creates a directory
	# from no template; passing one keeps the directory in TMPDIR on every
	# supported platform.
	tmp=$(mktemp -d "${TMPDIR:-/tmp}/peers-install.XXXXXXXXXX") \
		|| fail "cannot create a temporary directory"
	trap 'rm -rf "$tmp"' EXIT
	trap 'exit 129' HUP
	trap 'exit 130' INT
	trap 'exit 143' TERM

	if ! download "$release_url/$archive" "$tmp/$archive"; then
		fail "version $version not found"
	fi
	if ! download "$release_url/checksums.txt" "$tmp/checksums.txt"; then
		fail "version $version not found"
	fi

	# 5. Verify the archive. Exactly one line of checksums.txt may name it; zero
	# lines mean the release does not cover this platform, and more than one is
	# an ambiguity this script will not guess through.
	if command -v sha256sum >/dev/null 2>&1; then
		_actual=$(sha256sum "$tmp/$archive") || fail "cannot compute the SHA-256 checksum of $archive"
	elif command -v shasum >/dev/null 2>&1; then
		_actual=$(shasum -a 256 "$tmp/$archive") || fail "cannot compute the SHA-256 checksum of $archive"
	else
		fail "neither sha256sum nor shasum is installed; cannot verify the download"
	fi
	_actual=${_actual%% *}

	# The awk program strips the binary-mode marker some checksum tools put
	# before the file name, so both "hash  name" and "hash *name" are
	# understood.
	# shellcheck disable=SC2016 # The $2 and $1 are awk fields, not shell parameters.
	_expected=$(awk -v name="$archive" '
		{
			file = $2
			sub(/^\*/, "", file)
			if (file == name) {
				count++
				hash = $1
			}
		}
		END {
			if (count != 1 || hash == "")
				exit 1
			print hash
		}
	' "$tmp/checksums.txt") || fail "checksums.txt must contain exactly one line for $archive"

	_actual=$(printf '%s' "$_actual" | tr '[:upper:]' '[:lower:]')
	_expected=$(printf '%s' "$_expected" | tr '[:upper:]' '[:lower:]')
	if [ "$_actual" != "$_expected" ]; then
		fail "checksum mismatch for $archive: checksums.txt lists $_expected, but the download hashes to $_actual"
	fi

	# 6. Extract and hand over to the packaged install.sh. Its exit status is
	# the status this script returns: with set -e an exiting installer ends this
	# shell with the same value, and the EXIT trap still removes the temporary
	# directory.
	if ! tar -xzf "$tmp/$archive" -C "$tmp"; then
		fail "cannot extract $archive"
	fi
	if [ ! -f "$tmp/install.sh" ]; then
		fail "version $version not found"
	fi
	sh "$tmp/install.sh" "$@"
}

main "$@"
