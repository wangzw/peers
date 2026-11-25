#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi


# Claude Code 2.1.311 rejects stream-json input and output unless --verbose is given.
stream_args=(-p --model haiku --input-format stream-json --output-format stream-json --verbose)

wait_registered() {
	local name="$1" seconds="$2" deadline
	deadline=$((SECONDS + seconds))
	until rs_socket_of "$name" >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			return 1
		fi
		sleep 0.2
	done
}

wait_status() {
	local address="$1" seconds="$2" deadline
	deadline=$((SECONDS + seconds))
	until "$R/run" rsprobe status 2>/dev/null | grep -qF -- "$address"; do
		if ((SECONDS >= deadline)); then
			return 1
		fi
		sleep 0.5
	done
}

# A headless session has no bottom bar; its permission mode comes from the stream-json init record, which may not be flushed at first.
stream_mode() {
	local file="$1" deadline=$((SECONDS + 10)) mode=""
	while ((SECONDS < deadline)); do
		mode="$(jq -r 'select(.type == "system" and .subtype == "init") | .permissionMode // empty' "$file" 2>/dev/null | head -n 1 || true)"
		if [[ -n "$mode" ]]; then
			printf '%s\n' "$mode"
			return 0
		fi
		sleep 0.2
	done
	printf '%s\n' none
}

usage_total() {
	local out total
	out="$("$R/run" rsprobe env usage --root "$R" 2>/dev/null || true)"
	total="$(printf '%s\n' "$out" | sed -n 's/^total //p' | tail -n 1)"
	printf '%s\n' "${total:-0}"
}

# Callers close the session's stdin before stop_headless; it waits for the process, then kills it if it lingers.
stop_headless() {
	local pid="$1" deadline
	deadline=$((SECONDS + 30))
	while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
		sleep 0.2
	done
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
	fi
	wait "$pid" 2>/dev/null || true
}

rs_agent hl-rx
rs_obs "hl-rx started; it receives the message sent from the headless session"

# Both headless sessions start in $WORK, whose project config declares topics: [rs-probe]; from the repository root a session derives its topic from the git remote instead.

hl_in="$R/ctl/hl.in"
rm -f "$hl_in"
mkfifo "$hl_in"
exec 3<>"$hl_in"
(
	cd "$WORK" && exec "$R/run" claude "${stream_args[@]}" -n hl \
		--permission-mode default \
		--allowedTools mcp__peer__send_message \
		--debug-file "$R/debug/hl.debug"
) <"$hl_in" >"$OUT/hl.stream.jsonl" 2>"$OUT/hl.stderr.txt" &
hl_pid=$!

if ! wait_registered hl 15; then
	rs_obs "hl did not register within 15 seconds"
	exec 3>&-
	stop_headless "$hl_pid"
	exit 1
fi
if ! wait_status hl@hostB 15; then
	rs_obs "hl@hostB did not appear in rsprobe status within 15 seconds"
	exec 3>&-
	stop_headless "$hl_pid"
	exit 1
fi
"$R/run" rsprobe status >"$OUT/status-hl.txt" 2>&1 || true

list_reply="$(rs_call hl-rx list_agents '{}')"
printf '%s\n' "$list_reply" >"$OUT/hl-list-agents.txt"
if [[ "$(rs_grep_status "$list_reply" -qF -- 'hl@hostB')" != 0 ]]; then
	rs_obs "list_agents did not show hl@hostB"
	exec 3>&-
	stop_headless "$hl_pid"
	exit 1
fi
rs_obs "list_agents showed hl@hostB"

prompt='Send the message "from-headless" to hl-rx@hostB using mcp__peer__send_message, then reply done.'
escaped_prompt="$(printf '%s' "$prompt" | jq -Rrs @json)"
user_line="{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":$escaped_prompt}}"
printf '%s\n' "$user_line" >&3
rs_obs "wrote the user message for hl"

from_name=""
deadline=$((SECONDS + 120))
while ((SECONDS < deadline)); do
	if [[ -s "$OUT/hl-rx.recv.jsonl" ]] && grep -qF -- 'from-headless' "$OUT/hl-rx.recv.jsonl"; then
		from_name="$(jq -r 'select(.content | contains("from-headless")) | .from_name' "$OUT/hl-rx.recv.jsonl" | head -n 1)"
		break
	fi
	sleep 1
done

exec 3>&-
stop_headless "$hl_pid"
hl_mode="$(stream_mode "$OUT/hl.stream.jsonl")"
rs_obs "hl: permission mode requested: default; stream-json init permissionMode=${hl_mode}; allowed tools: mcp__peer__send_message"

if [[ -z "$from_name" ]]; then
	rs_excerpt from-headless hl-record.jsonl
	rs_obs "hl-rx did not receive from-headless within 120 seconds"
	exit 1
fi
rs_obs "hl-rx received from-headless with from_name=${from_name}"

# The one exception to pinning --permission-mode default: skipping permission confirmation is the condition under test, so this mode must not be overridden.
bypass_in="$R/ctl/hl-bypass.in"
rm -f "$bypass_in"
mkfifo "$bypass_in"
exec 4<>"$bypass_in"
(
	cd "$WORK" && exec "$R/run" claude "${stream_args[@]}" -n hl-bypass \
		--dangerously-skip-permissions \
		--debug-file "$R/debug/hl-bypass.debug"
) <"$bypass_in" >"$OUT/hl-bypass.stream.jsonl" 2>"$OUT/hl-bypass.stderr.txt" &
bypass_pid=$!

if ! wait_status hl-bypass@hostB 30; then
	rs_obs "hl-bypass did not appear in rsprobe status within 30 seconds"
	exec 4>&-
	stop_headless "$bypass_pid"
	exit 1
fi
bypass_mode="$(stream_mode "$OUT/hl-bypass.stream.jsonl")"
rs_obs "hl-bypass: permission mode requested: bypassPermissions (--dangerously-skip-permissions), because skipping permission confirmation is the condition under test (REQ-agent-messaging-036); stream-json init permissionMode=${bypass_mode}"
"$R/run" rsprobe status >"$OUT/status-before-send.txt" 2>&1 || true

baseline_turns="$(usage_total)"
sent_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
send_reply="$(rs_call hl-rx send_message '{"to":"hl-bypass@hostB","message":"to-headless-bypass"}')"
printf '%s\n' "$send_reply" >"$OUT/hl-bypass.send.json"
rs_obs "hl-bypass send_message at ${sent_at} returned: ${send_reply}"

processed=no
turn_seen=no
: >"$OUT/hl-bypass.poll.txt"
for ((i = 1; i <= 24; i++)); do
	sleep 5
	rs_excerpt to-headless-bypass hl-bypass-record.jsonl
	marker=no
	if grep -qF -- 'to-headless-bypass' "$OUT/hl-bypass.stream.jsonl" 2>/dev/null; then
		marker=yes
	fi
	if [[ -s "$OUT/hl-bypass-record.jsonl" ]]; then
		marker=yes
	fi
	turns="$(usage_total)"
	delta=$((turns - baseline_turns))
	if ((delta > 0)); then
		turn_seen=yes
	fi
	printf '%s poll=%d marker=%s turns_after_send=%d\n' \
		"$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$i" "$marker" "$delta" >>"$OUT/hl-bypass.poll.txt"
	if [[ "$marker" == yes ]]; then
		processed=yes
		break
	fi
done

rs_excerpt to-headless-bypass hl-bypass-record.jsonl
: >"$OUT/hl-bypass.debug.txt"
grep -hiE -- 'hold|held|expire|discard|no-mode-asserted' "$R/debug/hl-bypass.debug" \
	>>"$OUT/hl-bypass.debug.txt" 2>/dev/null || true

exec 4>&-
stop_headless "$bypass_pid"

rs_obs "hl-bypass processed the message: ${processed}; new assistant turns after send: ${turn_seen}"
rs_obs "headless probe finished"

# There is no tmux composer, so the probe's own prompt must appear as a user line with a model call counted.
rs_require_model_turn 'Send the message "from-headless"' 1
