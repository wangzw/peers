#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi


# v2.1.311's /mcp argument hint names "/mcp disable <name>" and "/mcp enable <name>", so the probe uses that form instead of arrow-key navigation.
mkdir -p "$OUT"
rs_tmux_start


mt_status_json() {
	"$R/run" rsprobe status --json
}

mt_wait_start() {
	local count="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) found
	while ((SECONDS < deadline)); do
		found=0
		if [[ -f "$OUT/sig.jsonl" ]]; then
			found="$(grep -c '"event":"start"' "$OUT/sig.jsonl" || true)"
		fi
		if ((found >= count)); then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# The re-enabled stub truncates the log when it opens it, so a start event from a different pid means the new stub began.
mt_wait_new_stub() {
	local old_pid="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) pid
	while ((SECONDS < deadline)); do
		if [[ -f "$OUT/sig.jsonl" ]]; then
			pid="$(jq -r -s '[.[] | select(.event == "start")][0].pid // 0' \
				"$OUT/sig.jsonl" 2>/dev/null || true)"
			if [[ -n "$pid" && "$pid" != "0" && "$pid" != "$old_pid" ]]; then
				return 0
			fi
		fi
		sleep 0.2
	done
	return 1
}

mt_wait_address() {
	local prefix="$1" present="$2" seconds="$3"
	local deadline=$((SECONDS + seconds)) json got
	while ((SECONDS < deadline)); do
		if json="$(mt_status_json)"; then
			got=no
			if printf '%s' "$json" | jq -e --arg p "$prefix" \
				'any(.agents[]?; ((.address // "") | startswith($p)))' >/dev/null; then
				got=yes
			fi
			if [[ "$got" == "$present" ]]; then
				return 0
			fi
		fi
		sleep 0.1
	done
	return 1
}

mt_wait_topic() {
	local prefix="$1" topic="$2" seconds="$3"
	local deadline=$((SECONDS + seconds)) json
	while ((SECONDS < deadline)); do
		if json="$(mt_status_json)"; then
			if printf '%s' "$json" | jq -e --arg p "$prefix" --arg t "$topic" \
				'any(.agents[]?; ((.address // "") | startswith($p)) and (((.topics // []) | index($t)) != null))' >/dev/null; then
				return 0
			fi
		fi
		sleep 0.2
	done
	return 1
}

mt_wait_socket() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if rs_socket_of "$name" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# Typed keys reach the slash command line only at the empty input prompt, and the shared readiness predicate refuses to call a dialog screen ready.
mt_wait_prompt() {
	rs_wait_ready "$1" "$2"
}


jq -n --arg bin "$R/bin/rsprobe" --arg log "$OUT/sig.jsonl" \
	'{mcpServers: {stub: {command: $bin, args: ["sigstub", "--log", $log]}}}' \
	>"$OUT/stub.json"

rs_window tog-stub claude --model haiku -n tog-stub --permission-mode default --mcp-config "$OUT/stub.json"

if ! mt_wait_start 1 30; then
	rs_snap tog-stub pane-stub-timeout.txt
	rs_obs "step 1: the sigstub start event did not appear within 30 s"
	exit 1
fi
rs_obs "step 1: tog-stub started its sigstub MCP server"

stub_pid="$(jq -r -s '[.[] | select(.event == "start")][0].pid // 0' "$OUT/sig.jsonl")"
stub_socket="$(jq -r -s '[.[] | select(.event == "start")][0].messaging_socket // ""' "$OUT/sig.jsonl")"
rs_obs "step 1: first stub pid=$stub_pid messaging_socket=${stub_socket:-<empty>}"

# Attach strace as root: the tmux server daemonizes, so this shell is not an ancestor of the stub and Ubuntu refuses unprivileged ptrace.
sudo -n strace -f -tt -e trace=none -p "$stub_pid" -o "$OUT/strace.txt" &
strace_pid=$!

for _ in {1..10}; do
	if [[ -e "$OUT/strace.txt" ]]; then
		break
	fi
	if ! ps -p "$strace_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.2
done
sleep 0.5
if ps -p "$strace_pid" >/dev/null 2>&1; then
	rs_obs "step 1: strace attached to pid $stub_pid"
else
	rs_obs "step 1: strace exited before attaching; only sig.jsonl timing is available"
fi

if ! mt_wait_prompt tog-stub 30; then
	rs_obs "step 1: the input prompt did not appear; sending /mcp anyway"
fi
rs_obs_mode tog-stub
rs_snap tog-stub pane-stub-before.txt

rs_keys tog-stub '/mcp disable stub' Enter
sleep 2
rs_snap tog-stub pane-stub-disabled.txt

# The re-enabled stub opens the same log path with O_TRUNC, so keep the first process's events before that can happen.
if [[ -f "$OUT/sig.jsonl" ]]; then
	cp "$OUT/sig.jsonl" "$OUT/sig-first.jsonl"
fi

# After SIGINT and SIGTERM the stub is expected to be SIGKILLed, which ends strace; the wait is bounded anyway.
for _ in {1..50}; do
	if ! ps -p "$strace_pid" >/dev/null 2>&1; then
		break
	fi
	sleep 0.2
done
if ps -p "$strace_pid" >/dev/null 2>&1; then
	rs_obs "step 1: strace was still attached after 10 s; stopping it"
	sudo -n pkill -P "$strace_pid" 2>/dev/null || true
	sudo -n kill "$strace_pid" 2>/dev/null || true
fi
if [[ -f "$OUT/strace.txt" ]]; then
	sudo -n chmod 0644 "$OUT/strace.txt" 2>/dev/null || true
fi

if ! mt_wait_prompt tog-stub 10; then
	rs_keys tog-stub Escape
	if ! mt_wait_prompt tog-stub 10; then
		rs_obs "step 1: could not return to the input prompt after disabling stub"
	fi
fi

rs_keys tog-stub '/mcp enable stub' Enter
if mt_wait_new_stub "$stub_pid" 30; then
	new_pid="$(jq -r -s '[.[] | select(.event == "start")][0].pid' "$OUT/sig.jsonl")"
	new_socket="$(jq -r -s '[.[] | select(.event == "start")][0].messaging_socket // ""' "$OUT/sig.jsonl")"
	rs_obs "step 1: enabling stub started pid $new_pid messaging_socket=${new_socket:-<empty>}"
	cp "$OUT/sig.jsonl" "$OUT/sig-second.jsonl"
else
	rs_obs "step 1: enabling stub did not start a new sigstub process within 30 s"
fi
rs_snap tog-stub pane-stub-enabled.txt

if [[ -f "$OUT/sig-first.jsonl" ]]; then
	cp "$OUT/sig-first.jsonl" "$OUT/sig.jsonl"
fi

if [[ -f "$OUT/sig-first.jsonl" ]]; then
	{
		echo "events in order:"
		jq -r 'if .event == "signal" then "signal " + .sig + " t_mono_ns=" + (.t_mono_ns | tostring) else .event + " t_mono_ns=" + (.t_mono_ns | tostring) end' "$OUT/sig-first.jsonl"
		echo "signal intervals:"
		jq -r -s '[.[] | select(.event == "signal") | [.sig, .t_mono_ns]] | .[] | @tsv' "$OUT/sig-first.jsonl" |
			awk -F '\t' 'NR == 1 { print $1 " (first)"; prev = $2; next } { printf "%s %d ms after the previous signal\n", $1, ($2 - prev) / 1000000; prev = $2 }'
	} >"$OUT/signals.txt"
	signal_order="$(jq -r 'if .event == "signal" then .sig else .event end' "$OUT/sig-first.jsonl" | tr '\n' ' ')"
	rs_obs "step 1: sigstub events: $signal_order"
fi

if [[ -f "$OUT/strace.txt" ]]; then
	grep -E 'SIG(INT|TERM|HUP|KILL)' "$OUT/strace.txt" >"$OUT/strace-signals.txt" || true
	for sig in SIGINT SIGTERM SIGKILL; do
		if grep -q "$sig" "$OUT/strace.txt"; then
			rs_obs "step 1: strace recorded $sig"
		else
			rs_obs "step 1: strace did not record $sig"
		fi
	done
fi


rs_window tog claude --model haiku -n tog --permission-mode default --allowedTools mcp__peer__join_topic

if ! mt_wait_socket tog 30; then
	rs_snap tog pane-tog-timeout.txt
	rs_obs "step 2: tog was not registered within 30 s"
	exit 1
fi
rs_obs "step 2: tog registered"

if ! mt_wait_prompt tog 30; then
	rs_obs "step 2: the input prompt did not appear; sending the join prompt anyway"
fi
rs_obs_mode tog

rs_send_prompt tog 'Call mcp__peer__join_topic with name tog-manual, then reply done.' 'Call mcp__peer__join_topic' 60
rs_obs "step 2: asked tog to join tog-manual"

if mt_wait_topic 'tog@' tog-manual 120; then
	rs_obs "step 2: tog is in rs-probe and tog-manual"
else
	rs_obs "step 2: tog did not join tog-manual within 120 s"
fi
sleep 5
if ! mt_wait_prompt tog 60; then
	rs_obs "step 2: could not confirm the join turn ended; sending /mcp anyway"
fi
rs_snap tog pane-tog-joined.txt

"$R/run" rsprobe status >"$OUT/status-before.txt"
"$R/run" rsprobe status --json >"$OUT/status-before.json"

agent_before="$(jq -r --arg p 'tog@' \
	'.agents[]? | select((.address // "") | startswith($p)) | (.agentId // .agent_id // "")' \
	"$OUT/status-before.json" | sed -n '1p')"
address_before="$(jq -r --arg p 'tog@' \
	'.agents[]? | select((.address // "") | startswith($p)) | .address' \
	"$OUT/status-before.json" | sed -n '1p')"
topics_before="$(jq -r --arg p 'tog@' \
	'[.agents[]? | select((.address // "") | startswith($p)) | (.topics // [])] | first // [] | join(",")' \
	"$OUT/status-before.json")"
rs_obs "step 2: before disabling, address=$address_before agent_id=$agent_before topics=$topics_before"

if [[ -z "$agent_before" ]]; then
	rs_obs "step 2: tog is not in the Agent table; stopping"
	exit 1
fi

disable_start_ns="$(date +%s%N)"
rs_keys tog '/mcp disable peer' Enter
disable_ms="not observed"
if mt_wait_address 'tog@' no 10; then
	disable_ms=$((($(date +%s%N) - disable_start_ns) / 1000000))
	rs_obs "step 2: tog left the Agent table ${disable_ms} ms after disabling peer"
else
	rs_obs "step 2: tog was still in the Agent table 10 s after disabling peer"
fi
rs_snap tog pane-peer-disabled.txt
"$R/run" rsprobe status >"$OUT/status-disabled.txt"
"$R/run" rsprobe status --json >"$OUT/status-disabled.json" || true

if ! mt_wait_prompt tog 10; then
	rs_keys tog Escape
	if ! mt_wait_prompt tog 10; then
		rs_obs "step 2: could not return to the input prompt after disabling peer"
	fi
fi

enable_start_ns="$(date +%s%N)"
rs_keys tog '/mcp enable peer' Enter
enable_ms="not observed"
if mt_wait_address 'tog@' yes 30; then
	enable_ms=$((($(date +%s%N) - enable_start_ns) / 1000000))
	rs_obs "step 2: tog was registered again ${enable_ms} ms after enabling peer"
else
	rs_obs "step 2: tog did not register again within 30 s of enabling peer"
fi
sleep 1
rs_snap tog pane-peer-enabled.txt
"$R/run" rsprobe status >"$OUT/status-after.txt"
"$R/run" rsprobe status --json >"$OUT/status-after.json"

agent_after="$(jq -r --arg p 'tog@' \
	'.agents[]? | select((.address // "") | startswith($p)) | (.agentId // .agent_id // "")' \
	"$OUT/status-after.json" | sed -n '1p')"
address_after="$(jq -r --arg p 'tog@' \
	'.agents[]? | select((.address // "") | startswith($p)) | .address' \
	"$OUT/status-after.json" | sed -n '1p')"
topics_after="$(jq -r --arg p 'tog@' \
	'[.agents[]? | select((.address // "") | startswith($p)) | (.topics // [])] | first // [] | join(",")' \
	"$OUT/status-after.json")"
rs_obs "step 2: after re-enabling, address=$address_after agent_id=$agent_after topics=$topics_after"

if [[ "$agent_after" == "$agent_before" ]]; then
	rs_obs "step 2: the agent id is unchanged"
else
	rs_obs "step 2: the agent id changed from $agent_before to $agent_after"
fi
if [[ "$address_after" == "$address_before" ]]; then
	rs_obs "step 2: the address is unchanged"
else
	rs_obs "step 2: the address changed from $address_before to $address_after"
fi
if [[ "$topics_after" == "rs-probe" ]]; then
	rs_obs "step 2: the topics are back to rs-probe only"
else
	rs_obs "step 2: the topics after re-enabling are $topics_after"
fi

{
	printf 'measurement\tmilliseconds\n'
	printf 'peer disabled to deregistered\t%s\n' "$disable_ms"
	printf 'peer enabled to registered\t%s\n' "$enable_ms"
} >"$OUT/timings.tsv"

# The join prompt is the probe's model prompt; without this check a run that never submitted it could report a topic from a stale table.
rs_require_model_turn 'Call mcp__peer__join_topic with name tog-manual' 1
