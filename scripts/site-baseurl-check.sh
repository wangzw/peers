#!/usr/bin/env bash
# Fail when docs/site does not build for the address GitHub Pages serves.
#
# The argument is the base_url output of actions/configure-pages. It carries
# the http scheme while Pages does not enforce HTTPS, so the scheme and a
# trailing slash are ignored and only the host and path are compared. An
# address that names no host compares equal to another empty address, so each
# side must name one.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf 'usage: %s <pages-base-url>\n' "$(basename "$0")" >&2
  exit 2
fi

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
site="$root/docs/site"

if ! config="$(hugo config --source "$site" 2>&1)"; then
  printf 'site-baseurl-check: cannot read the site configuration:\n%s\n' "$config" >&2
  exit 1
fi
base="$(printf '%s\n' "$config" | sed -n "s/^baseurl = ['\"]\(.*\)['\"]$/\1/p")"
if [[ -z "$base" ]]; then
  printf 'site-baseurl-check: %s has no baseURL\n' "$site" >&2
  exit 1
fi

# split_address parses an address into split_host and split_path. It strips
# the scheme, any query or fragment and trailing slashes, and lowercases the
# host.
split_address() {
  local rest="$1"
  rest="${rest#*://}"
  rest="${rest%%#*}"
  rest="${rest%%\?*}"
  split_host="${rest%%/*}"
  split_host="${split_host,,}"
  if [[ "$rest" == */* ]]; then
    split_path="/${rest#*/}"
    while [[ "$split_path" == */ ]]; do split_path="${split_path%/}"; done
  else
    split_path=""
  fi
}

split_address "$base"
want_host="$split_host"
want="$split_host$split_path"

split_address "$1"
have_host="$split_host"
have="$split_host$split_path"

if [[ -z "$want_host" || -z "$have_host" ]]; then
  printf 'site-baseurl-check: baseURL %q and Pages address %q must each name a host\n' "$base" "$1" >&2
  exit 1
fi

if [[ "$want" == "$have" ]]; then
  printf 'site-baseurl-check: %s matches the Pages address %s\n' "$base" "$1"
  exit 0
fi

printf 'site-baseurl-check: docs/site builds for %s but GitHub Pages serves %s\n' "$base" "$1" >&2
printf 'site-baseurl-check: compared %s against %s\n' "$want" "$have" >&2
exit 1
