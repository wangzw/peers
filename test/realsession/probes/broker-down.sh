#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"


PORT=27443
RUNTIME_DIR="/tmp/peers-$(id -u)"
BROKER_SOCKET="$RUNTIME_DIR/broker.sock"
BROKER_LOCK="$RUNTIME_DIR/broker.lock"
PROMPT='Call mcp__peer__list_agents and show the raw result.'
ERROR_MARKER='no Broker is available'
SUCCESS_MARKER='down@hostB'
TERMINAL_ERROR_MARKER='is unavailable'

occupier_pid=""
cleanup() {
	if [[ -n "$occupier_pid" ]] && kill -0 "$occupier_pid" 2>/dev/null; then
		kill "$occupier_pid" 2>/dev/null || true
		wait "$occupier_pid" 2>/dev/null || true
	fi
}
rs_on_exit cleanup

broker_pids() {
	local dir pid exe arg1 arg2
	for dir in /proc/[0-9]*; do
		pid="${dir#/proc/}"
		exe="$(readlink "$dir/exe" 2>/dev/null || true)"
		if [[ "$exe" != "$R/bin/peers" ]]; then
			continue
		fi
		arg1="$(tr '\0' '\n' <"$dir/cmdline" 2>/dev/null | sed -n '2p' || true)"
		arg2="$(tr '\0' '\n' <"$dir/cmdline" 2>/dev/null | sed -n '3p' || true)"
		if [[ "$arg1" == "service" && "$arg2" == "run" ]]; then
			printf '%s\n' "$pid"
		fi
	done
}

# An empty list means the lock is free: the Broker releases its flock when it exits.
lock_holders() {
	local dir pid fd target
	for dir in /proc/[0-9]*; do
		pid="${dir#/proc/}"
		for fd in "$dir"/fd/*; do
			target="$(readlink "$fd" 2>/dev/null || true)"
			if [[ "$target" == "$BROKER_LOCK" ]]; then
				printf '%s\n' "$pid"
				break
			fi
		done
	done
}

wait_for() {
	local seconds="$1" check="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if "$check"; then
			return 0
		fi
		sleep 0.2
	done
	"$check"
}

# A pid that stays the same across recovery is the evidence that the session never reconnected the service.
mcp_pids() {
	local dir pid exe arg1
	for dir in /proc/[0-9]*; do
		pid="${dir#/proc/}"
		exe="$(readlink "$dir/exe" 2>/dev/null || true)"
		if [[ "$exe" != "$R/bin/peers" ]]; then
			continue
		fi
		arg1="$(tr '\0' '\n' <"$dir/cmdline" 2>/dev/null | sed -n '2p' || true)"
		if [[ "$arg1" == "mcp" ]]; then
			printf '%s\n' "$pid"
		fi
	done
}

# An unreadable counter prints 0 so arithmetic still works; rs_require_model_turn still fails the run.
usage_total() {
	local out total
	out="$("$R/run" rsprobe env usage --root "$R" 2>/dev/null || true)"
	total="$(printf '%s\n' "$out" | sed -n 's/^total //p' | tail -n 1)"
	if [[ ! "$total" =~ ^[0-9]+$ ]]; then
		total=0
	fi
	printf '%s\n' "$total"
}

no_broker_running() {
	[[ -z "$(broker_pids)" ]]
}

no_lock_holder() {
	[[ -z "$(lock_holders)" ]]
}

wait_for_record() {
	local marker="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) file
	while ((SECONDS < deadline)); do
		while IFS= read -r -d '' file; do
			if grep -qF -- "$marker" "$file"; then
				return 0
			fi
		done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
		sleep 0.5
	done
	return 1
}

existing="$(broker_pids)"
if [[ -n "$existing" ]]; then
	rs_obs "stopping the running Broker: $(printf '%s' "$existing" | tr '\n' ' ')"
	while IFS= read -r pid; do
		if [[ -n "$pid" ]]; then
			kill -TERM "$pid" 2>/dev/null || true
		fi
	done <<<"$existing"
	if ! wait_for 20 no_broker_running; then
		rs_obs "a Broker process did not exit after SIGTERM"
		exit 1
	fi
else
	rs_obs "no Broker was running before the probe"
fi
# The account lock must be free before the listen address is held.
if ! wait_for 10 no_lock_holder; then
	rs_obs "the account lock is still held after the Broker stopped"
	exit 1
fi
rs_obs "account lock released"

"$R/run" python3 -c '
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("127.0.0.1", 27443))
sock.listen(8)
print("occupying", flush=True)
while True:
    conn, _ = sock.accept()
    conn.close()
' >"$OUT/occupier.log" 2>&1 &
occupier_pid=$!
deadline=$((SECONDS + 10))
until grep -q '^occupying$' "$OUT/occupier.log" 2>/dev/null; do
	if ((SECONDS >= deadline)) || ! kill -0 "$occupier_pid" 2>/dev/null; then
		rs_obs "the port occupier failed to bind 127.0.0.1:$PORT"
		exit 1
	fi
	sleep 0.2
done
rs_obs "port $PORT is held by pid $occupier_pid"

# The config names 127.0.0.1:27443, so holding that port makes the start fail.
terminal_rc=0
"$R/run" peers service run --user >"$OUT/terminal-start.stdout" 2>"$OUT/terminal-start.stderr" || terminal_rc=$?
printf '%s\n' "$terminal_rc" >"$OUT/terminal-start.exit"
if ((terminal_rc != 4)); then
	rs_obs "the terminal Broker start exited $terminal_rc, want 4"
	exit 1
fi
if ! grep -qF -- "$TERMINAL_ERROR_MARKER" "$OUT/terminal-start.stderr"; then
	rs_obs "the terminal Broker start did not say the listen address is unavailable"
	exit 1
fi
rs_obs "the terminal Broker start failed with exit 4 and named the unavailable address"

rs_tmux_start
rs_window down claude --model haiku -n down --permission-mode default --allowedTools mcp__peer__list_agents

socket=""
deadline=$((SECONDS + 40))
while ((SECONDS < deadline)); do
	if socket="$(rs_socket_of down)"; then
		break
	fi
	sleep 0.2
done
if [[ -z "$socket" ]]; then
	rs_obs "session down did not register an inbox socket"
	rs_snap down timeout-down-start.txt
	exit 1
fi
rs_obs "session down uses inbox socket $socket"
if ! rs_wait_ready down 60; then
	rs_obs "session down did not reach its input prompt within 60 seconds"
	rs_snap down timeout-down-prompt.txt
	exit 1
fi
rs_obs_mode down

if [[ -n "$(broker_pids)" ]]; then
	rs_obs "a Broker process is running after the session started: $(broker_pids | tr '\n' ' ')"
	exit 1
fi
rs_obs "no Broker process is running after the session started"

# Claude Code 2.1.311's /mcp list shows an available server as "✔ peer   N tools", not "connected".
sleep 2
rs_keys down '/mcp'
rs_keys down Enter
if rs_wait_pane_literal down '✔ peer' 20; then
	rs_obs "the /mcp view lists peer as available (2.1.311's list form, ✔ peer) before a Broker exists (see mcp-before.txt)"
else
	rs_obs "the /mcp view did not list ✔ peer within 20s (see mcp-before.txt)"
	rs_snap down mcp-before.txt
	exit 1
fi
rs_snap down mcp-before.txt
rs_keys down Escape
sleep 1

# Record the service pid set before recovery so the after-recovery set can show the same process stayed connected.
mcp_pids | sort >"$OUT/mcp-pids-before.txt"
mcp_before="$(tr '\n' ' ' <"$OUT/mcp-pids-before.txt")"
if [[ -z "${mcp_before// /}" ]]; then
	rs_obs "no MCP service process is running before recovery"
	rs_snap down timeout-down-no-mcp.txt
	exit 1
fi
rs_obs "MCP service process before recovery: $mcp_before"

rs_send_prompt down "$PROMPT" 'Call mcp__peer__list_agents' 60
rs_obs "submitted the first list_agents prompt while no Broker was running"
if ! wait_for_record "$ERROR_MARKER" 120; then
	rs_obs "the first list_agents result did not report that no Broker is available"
	rs_snap down timeout-down-first.txt
	exit 1
fi
rs_excerpt "$ERROR_MARKER" list-agents-error.jsonl
for want in 'peers service install' 'peers service run'; do
	if ! grep -qF -- "$want" "$OUT/list-agents-error.jsonl"; then
		rs_obs "the first list_agents result does not name $want"
		rs_snap down timeout-down-first-startup-advice.txt
		exit 1
	fi
done
rs_obs "the first list_agents result reports that no Broker is available and names both startup methods"

# Freeze the usage baseline after the first turn so the second prompt must add its own model call.
if ! rs_wait_turn_idle down 120; then
	rs_obs "the first list_agents turn did not end within 120s; the usage baseline may be early"
fi
rs_require_model_turn "$PROMPT" 1
usage_after_first="$(usage_total)"
rs_obs "usage total after the first prompt: $usage_after_first"

kill "$occupier_pid" 2>/dev/null || true
wait "$occupier_pid" 2>/dev/null || true
occupier_pid=""
rs_obs "released port $PORT; starting the Broker by hand"

if ! rs_start_broker; then
	rs_obs "the hand-started Broker did not become ready"
	rs_snap down timeout-down-broker.txt
	exit 1
fi
rs_obs "the hand-started Broker is ready at $BROKER_SOCKET"

registered=0
deadline=$((SECONDS + 30))
while ((SECONDS < deadline)); do
	if "$R/run" rsprobe status >"$OUT/status-after-recovery.txt" 2>&1 &&
		grep -qF -- "$SUCCESS_MARKER" "$OUT/status-after-recovery.txt"; then
		registered=1
		break
	fi
	sleep 0.5
done
if ((registered != 1)); then
	rs_obs "the Broker did not list session down within 30s of it starting"
	rs_snap down timeout-down-register.txt
	exit 1
fi
rs_obs "the Broker lists session down after it started"

broker_pids >"$OUT/broker-pids.txt" 2>&1 || true
while IFS= read -r pid; do
	if [[ -n "$pid" ]]; then
		rs_obs "Broker running after recovery: pid $pid"
	fi
done <"$OUT/broker-pids.txt"

rs_send_prompt down "$PROMPT" 'Call mcp__peer__list_agents' 60
rs_obs "submitted the same list_agents prompt after recovery"
if ! wait_for_record "$SUCCESS_MARKER" 120; then
	rs_obs "the second list_agents result did not list session down"
	rs_snap down timeout-down-second.txt
	exit 1
fi
rs_excerpt "$SUCCESS_MARKER" list-agents-recovered.jsonl
rs_obs "the second list_agents result lists session down"

# Wait the turn out first so the slash command is not typed into a running turn.
if ! rs_wait_turn_idle down 120; then
	rs_obs "the second list_agents turn did not end within 120s before the /mcp view"
fi
rs_keys down '/mcp'
rs_keys down Enter
if rs_wait_pane_literal down '✔ peer' 20; then
	rs_obs "the /mcp view still lists peer as available (2.1.311's list form, ✔ peer) after recovery (see mcp-after.txt)"
else
	rs_obs "the /mcp view did not list ✔ peer after recovery (see mcp-after.txt)"
	rs_snap down mcp-after.txt
	exit 1
fi
rs_snap down mcp-after.txt
rs_keys down Escape

# A different pid would mean the service exited and Claude Code started it again.
mcp_pids | sort >"$OUT/mcp-pids-after.txt"
mcp_after="$(tr '\n' ' ' <"$OUT/mcp-pids-after.txt")"
rs_obs "MCP service process after recovery: $mcp_after"
if cmp -s "$OUT/mcp-pids-before.txt" "$OUT/mcp-pids-after.txt"; then
	rs_obs "the MCP service process pid set is unchanged across recovery"
else
	rs_obs "the MCP service process pid set changed across recovery; compare mcp-pids-before.txt and mcp-pids-after.txt"
	exit 1
fi

# A run whose prompt never submitted can still show the MCP view and a stale Broker socket; the second call must add a model turn.
rs_require_model_turn "$PROMPT" $((usage_after_first + 1))
