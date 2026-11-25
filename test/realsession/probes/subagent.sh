#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# The probe checks that a subagent acts as its parent session's Agent, and
# whether an inline MCP server declared in a subagent definition starts a second
# process that inherits the parent's inbox socket path. It writes only $OUT,
# $R/ctl, $R/debug, $WORK and the agent definition at $R/cfg/agents/peer-inline.md.

session_pid() {
	local name="$1" file
	for file in "$R"/cfg/sessions/*.json; do
		[[ -e "$file" ]] || continue
		if jq -e --arg name "$name" 'select(.name == $name)' "$file" >/dev/null 2>&1; then
			jq -r '.pid' "$file"
			return 0
		fi
	done
	return 1
}

wait_session() {
	local name="$1" seconds="$2" pid
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if pid="$(session_pid "$name")"; then
			printf '%s\n' "$pid"
			return 0
		fi
		sleep 0.2
	done
	return 1
}

wait_new_session() {
	local name="$1" old="$2" seconds="$3" pid
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if pid="$(session_pid "$name")" && [[ "$pid" != "$old" ]]; then
			printf '%s\n' "$pid"
			return 0
		fi
		sleep 0.2
	done
	return 1
}

wait_session_gone() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if ! session_pid "$name" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

proc_ppid() {
	local pid="$1" stat rest ppid
	stat="$(<"/proc/$pid/stat")" || return 1
	rest="${stat##*) }"
	read -r _ ppid _ <<<"$rest"
	[[ -n "$ppid" ]] || return 1
	printf '%s\n' "$ppid"
}

# is_descendant reports whether pid descends from ancestor within 12 levels, the
# depth Claude Code walks when looking for the peer process.
is_descendant() {
	local pid="$1" ancestor="$2" depth=0
	while [[ -n "$pid" && "$pid" != "0" ]] && ((depth <= 12)); do
		if [[ "$pid" == "$ancestor" ]]; then
			return 0
		fi
		pid="$(proc_ppid "$pid")" || return 1
		depth=$((depth + 1))
	done
	return 1
}

mcp_pids_under() {
	local ancestor="$1" dir pid cmd
	for dir in /proc/[0-9]*; do
		pid="${dir#/proc/}"
		[[ -r "$dir/cmdline" ]] || continue
		cmd="$(tr '\0' ' ' <"$dir/cmdline" 2>/dev/null || true)"
		case "$cmd" in
		*"$R/bin/peers mcp"*) ;;
		*) continue ;;
		esac
		if is_descendant "$pid" "$ancestor"; then
			printf '%s\n' "$pid"
		fi
	done
}

wait_mcp_count() {
	local pid="$1" want="$2" seconds="$3" count
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		count="$(mcp_pids_under "$pid" | wc -l)"
		if ((count >= want)); then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

wait_status_entry() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if "$R/run" rsprobe status 2>/dev/null | grep -qF -- " $name@hostB "; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

recv_has() {
	local marker="$1" file="$OUT/sub-rx.recv.jsonl"
	[[ -s "$file" ]] || return 1
	jq -e --arg marker "$marker" 'select(.content | contains($marker))' "$file" >/dev/null 2>&1
}

from_name_for() {
	local marker="$1" file="$OUT/sub-rx.recv.jsonl"
	[[ -s "$file" ]] || return 0
	jq -r --arg marker "$marker" 'select(.content | contains($marker)) | .from_name' "$file" 2>/dev/null |
		sed -n '1p' || true
}

status_max() {
	local file="$1"
	if [[ ! -s "$file" ]]; then
		printf '0\n'
		return 0
	fi
	awk -F'\t' 'NR > 1 && $2 + 0 > max { max = $2 + 0 } END { print max + 0 }' "$file"
}

status_all_one() {
	local file="$1"
	[[ -s "$file" ]] || return 1
	awk -F'\t' 'NR > 1 { n++; if ($2 + 0 != 1) bad = 1 } END { exit (n > 0 && !bad) ? 0 : 1 }' "$file"
}

launch_submain() {
	rs_window sub-main claude --model haiku -n sub-main \
		--permission-mode default \
		--allowedTools "mcp__peer__send_message,Task,Agent" \
		--debug-file "$R/debug/sub-main.debug"
}

# wait_prompt waits for the input composer; rs_wait_ready dismisses a first-run
# dialog first, so a bare prompt glyph behind a dialog does not count.
wait_prompt() {
	if ! rs_wait_ready sub-main 90; then
		rs_obs "sub-main never reached an input prompt"
		return 1
	fi
	rs_obs_mode sub-main
	rs_snap sub-main startup.txt
	return 0
}

rs_tmux_start
rs_agent sub-rx
wait_status_entry sub-rx 30 || {
	rs_obs "sub-rx did not appear in rsprobe status"
	exit 1
}

# A subagent in the shared process must send through the main session's Agent,
# and the main session must stay a single Agent while it runs.
launch_submain
main_pid="$(wait_session sub-main 60)" || {
	rs_obs "sub-main never wrote a session record"
	exit 1
}
wait_prompt || exit 1
wait_status_entry sub-main 30 || {
	rs_obs "sub-main did not appear in rsprobe status"
	exit 1
}

printf 'time\tcount\n' >"$OUT/status-counts.tsv"
: >"$OUT/status.txt"

rs_send_prompt sub-main 'Launch a subagent with your agent/task tool. The subagent must call mcp__peer__send_message with to "sub-rx@hostB" and message "from-subagent", then finish. Do not send the message yourself.' 'Launch a subagent with your agent/task tool' 60

step1_seen=0
deadline=$((SECONDS + 300))
while ((SECONDS < deadline)); do
	status_out="$("$R/run" rsprobe status 2>&1 || true)"
	now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	{
		printf '=== %s ===\n' "$now"
		printf '%s\n' "$status_out"
	} >>"$OUT/status.txt"
	count="$(printf '%s\n' "$status_out" | grep -cF -- 'sub-main@hostB' || true)"
	printf '%s\t%s\n' "$now" "$count" >>"$OUT/status-counts.tsv"
	if recv_has from-subagent; then
		if ((step1_seen == 0)); then
			step1_seen=$SECONDS
		fi
		if ((SECONDS >= step1_seen + 2)); then
			break
		fi
	fi
	sleep 0.5
done
if ((step1_seen == 0)); then
	rs_obs "step 1: no from-subagent frame within the timeout"
fi
rs_excerpt from-subagent step1-records.jsonl
step1_from="$(from_name_for from-subagent)"

# An inline MCP server in a subagent definition starts a second process whose
# CLAUDE_CODE_MESSAGING_SOCKET must equal the main session's socket.
mkdir -p "$R/cfg/agents"
cat >"$R/cfg/agents/peer-inline.md" <<EOF
---
name: peer-inline
description: Sends one message through an inline peer server
tools: mcp__peer__send_message
mcpServers:
  - peer:
      command: $R/bin/peers
      args: [mcp]
---
Use the inline peer server to send exactly one message, then finish.
EOF

old_pid="$main_pid"
tmux -L "$TMUX_SERVER" kill-window -t sub-main
wait_session_gone sub-main 20 || true
launch_submain
main_pid="$(wait_new_session sub-main "$old_pid" 60)" || {
	rs_obs "sub-main did not restart with a new session record"
	exit 1
}
wait_prompt || exit 1
wait_status_entry sub-main 30 || {
	rs_obs "restarted sub-main did not appear in rsprobe status"
	exit 1
}

wait_mcp_count "$main_pid" 1 60 || rs_obs "step 2: no main peers mcp process appeared"
mcp_pids_under "$main_pid" | sort -n >"$OUT/step2-baseline-mcp-pids.txt"
baseline_file="$OUT/step2-baseline-mcp-pids.txt"
main_socket="$(rs_socket_of sub-main || true)"
printf '%s\n' "$main_socket" >"$OUT/main-socket.txt"
first_main_pid="$(sed -n '1p' "$baseline_file")"
if [[ -n "$first_main_pid" ]]; then
	rs_proc_vars "$first_main_pid" CLAUDE_CODE_MESSAGING_SOCKET >"$OUT/main-mcp-env.txt" || true
fi

printf 'time\tcount\n' >"$OUT/status-counts-step2.tsv"
: >"$OUT/status-step2.txt"

rs_send_prompt sub-main 'Use the peer-inline subagent to send "from-inline" to sub-rx@hostB.' 'Use the peer-inline subagent' 60

new_pid=""
inline_msg=0
deadline=$((SECONDS + 300))
while ((SECONDS < deadline)); do
	status_out="$("$R/run" rsprobe status 2>&1 || true)"
	now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	{
		printf '=== %s ===\n' "$now"
		printf '%s\n' "$status_out"
	} >>"$OUT/status-step2.txt"
	count="$(printf '%s\n' "$status_out" | grep -cF -- 'sub-main@hostB' || true)"
	printf '%s\t%s\n' "$now" "$count" >>"$OUT/status-counts-step2.tsv"

	if [[ -z "$new_pid" ]]; then
		current="$(mcp_pids_under "$main_pid" | sort -n)"
		new_pid="$(printf '%s\n' "$current" | grep -vxF -f "$baseline_file" | sed -n '1p' || true)"
		if [[ -n "$new_pid" ]]; then
			{
				printf 'pid=%s\n' "$new_pid"
				rs_proc_vars "$new_pid" CLAUDE_CODE_MESSAGING_SOCKET CLAUDE_CODE_CHILD_SESSION || true
			} >"$OUT/inline-mcp-env.txt"
			if tr '\0' ' ' <"/proc/$new_pid/cmdline" >"$OUT/inline-mcp-cmdline.txt" 2>/dev/null; then
				printf '\n' >>"$OUT/inline-mcp-cmdline.txt"
			fi
		fi
	fi

	if recv_has from-inline; then
		inline_msg=1
	fi
	if [[ -n "$new_pid" ]] && ((inline_msg == 1)); then
		break
	fi
	sleep 0.5
done
if [[ -z "$new_pid" ]]; then
	rs_obs "step 2: no second peers mcp process appeared under sub-main"
fi
if ((inline_msg == 0)); then
	rs_obs "step 2: no from-inline frame within the timeout"
fi
rs_excerpt from-inline step2-records.jsonl
step2_from="$(from_name_for from-inline)"
inline_socket=""
if [[ -s "$OUT/inline-mcp-env.txt" ]]; then
	inline_socket="$(sed -n 's/^CLAUDE_CODE_MESSAGING_SOCKET=//p' "$OUT/inline-mcp-env.txt")"
fi

step1_max="$(status_max "$OUT/status-counts.tsv")"
step2_max="$(status_max "$OUT/status-counts-step2.tsv")"
verdict=PASS
[[ "$step1_from" == "sub-main@hostB" ]] || verdict=FAIL
[[ "$step2_from" == "sub-main@hostB" ]] || verdict=FAIL
status_all_one "$OUT/status-counts.tsv" || verdict=FAIL
status_all_one "$OUT/status-counts-step2.tsv" || verdict=FAIL
[[ -n "$new_pid" ]] || verdict=FAIL
[[ -n "$main_socket" && "$inline_socket" == "$main_socket" ]] || verdict=FAIL

{
	printf 'step1 from_name: %s (want sub-main@hostB)\n' "${step1_from:-missing}"
	printf 'step1 status max: %s (want 1)\n' "$step1_max"
	printf 'step2 from_name: %s (want sub-main@hostB)\n' "${step2_from:-missing}"
	printf 'step2 status max: %s (want 1)\n' "$step2_max"
	printf 'main socket: %s\n' "${main_socket:-missing}"
	printf 'second mcp pid: %s\n' "${new_pid:-missing}"
	printf 'second mcp socket: %s\n' "${inline_socket:-missing}"
	printf 'REQ-agent-messaging-035: %s\n' "$verdict"
} >"$OUT/verdict.txt"
rs_obs "REQ-agent-messaging-035: $verdict (subagent probe)"

# The first session is killed before the second session starts but its transcript
# remains, so the first prompt is a stable marker for "the probe reached the
# model at least once".
rs_require_model_turn 'Launch a subagent with your agent/task tool' 1
