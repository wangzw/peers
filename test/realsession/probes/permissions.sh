#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# Each session gets its own --settings allowlist, so the shared R/cfg/settings.json is never modified.

dialog_pattern='Do you want to proceed\?|Do you want to allow .*to use|Yes, and don.t ask again|No, and tell Claude'

PERM_DIALOGS=0

perm_write_settings() {
	local file="$1" tool="$2"
	cat >"$file" <<EOF
{
  "permissions": {
    "allow": ["$tool"]
  }
}
EOF
}

# Readiness is the shared predicate, so the prompt that the permission dialog will cover is never mistaken for it.
perm_wait_ready() {
	local window="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if rs_socket_of "$window" >/dev/null 2>&1; then
			if rs_wait_ready "$window" "$((deadline - SECONDS))"; then
				rs_obs_mode "$window"
				return 0
			fi
			return 1
		fi
		sleep 0.2
	done
	rs_snap "$window" "ready-$window-timeout.txt" || true
	return 1
}

perm_send() {
	local window="$1" text="$2" needle="${3:-}"
	rs_send_prompt "$window" "$text" "$needle" 60
}

# A dialog is recognised on screen, so each denial is followed by a pause that lets it leave the pane before the next poll.
perm_run_turn() {
	local window="$1" label="$2"
	local deadline=$((SECONDS + 180))
	local started=0 sent_at=$SECONDS now pane
	while ((SECONDS < deadline)); do
		now=$SECONDS
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		if [[ "$(rs_grep_status "$pane" -qE -- "$dialog_pattern")" == 0 ]]; then
			PERM_DIALOGS=$((PERM_DIALOGS + 1))
			printf '%s\n' "$pane" >"$OUT/$window-dialog-$PERM_DIALOGS.txt"
			# Escape cancels the permission dialog.
			rs_keys "$window" Escape
			started=1
			sleep 3
			continue
		fi
		if [[ "$(rs_grep_status "$pane" -qF 'esc to interrupt')" == 0 ]]; then
			started=1
			sleep 0.3
			continue
		fi
		if rs_ready_criterion "$pane" >/dev/null; then
			if ((started == 1 || now - sent_at >= 30)); then
				break
			fi
		fi
		sleep 0.3
	done
	rs_snap "$window" "$label"
}

perm_close() {
	local window="$1"
	# /exit is a slash command, not a model prompt, so it bypasses the helper.
	rs_keys_literal "$window" "/exit"
	rs_keys "$window" Enter
	sleep 2
	tmux -L "$TMUX_SERVER" kill-window -t "$window" 2>/dev/null || true
}

rs_tmux_start

# Record the shared settings hash before and after so the evidence shows it was never edited.
sha256sum "$R/cfg/settings.json" >"$OUT/settings-before.sha256"

perm_write_settings "$OUT/perm-a.settings.json" mcp__peer__get_config
rs_window perm-a claude --model haiku -n perm-a --permission-mode default --settings "$OUT/perm-a.settings.json"
perm_wait_ready perm-a 30
rs_snap perm-a perm-a-start.txt
perm_send perm-a 'Call mcp__peer__get_config for key broker.alias and show the result. Then call mcp__peer__set_config with key broker.alias and value hostB2.'
perm_run_turn perm-a perm-a-final.txt
rs_obs "perm-a finished with $PERM_DIALOGS dialog(s); allowlist was mcp__peer__get_config"
perm_close perm-a

before=$PERM_DIALOGS
perm_write_settings "$OUT/perm-b.settings.json" mcp__peer__create_pairing_key
rs_window perm-b claude --model haiku -n perm-b --permission-mode default --settings "$OUT/perm-b.settings.json"
perm_wait_ready perm-b 30
rs_snap perm-b perm-b-start.txt
perm_send perm-b 'Call mcp__peer__create_pairing_key and show the result. Then call mcp__peer__set_config with key broker.alias and value hostB2.'
perm_run_turn perm-b perm-b-final.txt
rs_obs "perm-b finished with $((PERM_DIALOGS - before)) dialog(s); allowlist was mcp__peer__create_pairing_key"

sha256sum "$R/cfg/settings.json" >"$OUT/settings-after.sha256"
if cmp -s "$OUT/settings-before.sha256" "$OUT/settings-after.sha256"; then
	rs_obs "shared settings.json unchanged: hashes match"
else
	rs_obs "shared settings.json CHANGED: compare settings-before.sha256 and settings-after.sha256"
fi

# The Broker data directory is under the account's real home, not $R.
account_home="$(getent passwd "$(id -un)" | cut -d: -f6)"
if [[ -z "$account_home" ]]; then
	account_home="$HOME"
fi
config_file="$account_home/.peers/config.yaml"
{
	printf 'config_file=%s\n' "$config_file"
	if [[ -f "$config_file" ]]; then
		grep -n -E '^[[:space:]]*alias[[:space:]]*:' "$config_file" || true
	else
		printf 'missing\n'
	fi
} >"$OUT/config-alias.txt"
if [[ -f "$config_file" ]] && ! grep -qF 'hostB2' "$config_file"; then
	rs_obs "alias unchanged: $config_file contains no hostB2"
else
	rs_obs "alias check failed: inspect config-alias.txt"
fi

if ! "$R/run" rsprobe status >"$OUT/status.txt" 2>"$OUT/status.err"; then
	rs_obs "rsprobe status failed; see status.err"
fi
perm_close perm-b
rs_obs "permissions probe finished with $PERM_DIALOGS dialog(s) denied in total"

# A denied dialog counts only if the prompt that raised it started a model turn.
rs_require_model_turn 'Call mcp__peer__get_config for key broker.alias' 1
