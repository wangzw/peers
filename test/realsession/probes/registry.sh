#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# On macOS /tmp is a symlink to /private/tmp and Claude Code keys workspace trust by the resolved path, so the probe root must be under /private/tmp or every session stops at the trust dialog.
#
# rsprobe status reads only the Broker's in-memory table, so it shows exactly what the notification path applied.
#
# The probe reads the registry through two independent witnesses: rsprobe watch scans the directory itself, and rsprobe status reports the Broker's table.

PROMPT='When a cross-session message arrives, reply with the single word ok and do nothing else. Do not call any tool unless the user explicitly asks.'

REG_OS="$(uname -s)"

# The budget for a rename or an exit to appear in the Broker's list; the measured elapsed value also covers the command and the 0.2-second poll granularity.
REG_REFLECT_LIMIT_MS=1000

if [[ "$REG_OS" == Darwin ]]; then
	rs_obs "macOS: /tmp is a symlink to /private/tmp; the probe root is $R, and Claude Code keys workspace trust by the resolved path"
fi

printf 'op\tsubject\tattempt\telapsed_ms\tdetail\n' >"$OUT/timings.tsv"

# A transient failure prints nothing; the polling callers retry until their deadline.
reg_status() {
	"$R/run" rsprobe status 2>>"$OUT/status.err" || true
}

# Addresses are "name@alias" or "name [ref]@alias"; comparing the name exactly keeps reg-a from matching reg-a2 and ab from matching a hex disambiguator.
reg_line_of() {
	awk -v want="$1" '
		/^peer / { next }
		{
			addr = $2
			sub(/@.*/, "", addr)
			sub(/ \[[0-9a-f]+\]$/, "", addr)
			if (addr == want) { print; exit }
		}'
}

reg_line_present() {
	local name="$1" table line
	table="$(reg_status)"
	line="$(printf '%s\n' "$table" | reg_line_of "$name")"
	[[ -n "$line" ]]
}

reg_wait_present() {
	local name="$1" seconds="$2" deadline
	deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if reg_line_present "$name"; then
			return 0
		fi
		sleep 0.2
	done
	reg_line_present "$name"
}

# GNU date prints %3N; BSD date, which macOS ships, does not, so Darwin reads the clock from python3.
reg_now_ms() {
	if [[ "$REG_OS" == Darwin ]]; then
		python3 -c 'import time; print(int(time.time() * 1000))'
	else
		date +%s%3N
	fi
}

reg_timing() {
	printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >>"$OUT/timings.tsv"
}

reg_measure_present() {
	local name="$1" op="$2" attempt="$3" seconds="$4" start="$5" want_prev="${6:-}"
	local deadline table line now elapsed prev
	deadline=$((SECONDS + seconds))
	while :; do
		table="$(reg_status)"
		line="$(printf '%s\n' "$table" | reg_line_of "$name")"
		if [[ -n "$line" ]]; then
			now="$(reg_now_ms)"
			elapsed=$((now - start))
			prev="$(printf '%s\n' "$line" | sed -n 's/.* prev=\(.*\)$/\1/p')"
			printf '%s\n' "$table" >"$OUT/status-$op-$attempt.txt"
			reg_timing "$op" "$name" "$attempt" "$elapsed" "prev=$prev"
			if [[ -n "$want_prev" && "$prev" != "$want_prev" ]]; then
				rs_obs "criterion failed: $op $name attempt $attempt was listed with prev=\"$prev\", want \"$want_prev\""
				return 2
			fi
			if ((elapsed > REG_REFLECT_LIMIT_MS)); then
				rs_obs "criterion failed: $op $name attempt $attempt took ${elapsed}ms, over the ${REG_REFLECT_LIMIT_MS}ms limit"
				return 3
			fi
			rs_obs "$op $name attempt $attempt reflected in ${elapsed}ms (prev=$prev)"
			return 0
		fi
		if ((SECONDS >= deadline)); then
			printf '%s\n' "$table" >"$OUT/status-$op-$attempt.txt"
			reg_timing "$op" "$name" "$attempt" "timeout" "not listed within ${seconds}s"
			rs_obs "criterion failed: $op $name attempt $attempt was not listed within ${seconds}s"
			return 1
		fi
		sleep 0.2
	done
}

# A table that does not parse, for example while the Broker restarts, is not taken as removal.
reg_measure_gone() {
	local name="$1" op="$2" attempt="$3" seconds="$4" start="$5"
	local deadline table line now elapsed
	deadline=$((SECONDS + seconds))
	while :; do
		table="$(reg_status)"
		if [[ "$table" == *status=* ]]; then
			line="$(printf '%s\n' "$table" | reg_line_of "$name")"
			if [[ -z "$line" ]]; then
				now="$(reg_now_ms)"
				elapsed=$((now - start))
				printf '%s\n' "$table" >"$OUT/status-$op-$attempt.txt"
				reg_timing "$op" "$name" "$attempt" "$elapsed" "removed"
				if ((elapsed > REG_REFLECT_LIMIT_MS)); then
					rs_obs "criterion failed: $op $name attempt $attempt took ${elapsed}ms, over the ${REG_REFLECT_LIMIT_MS}ms limit"
					return 2
				fi
				rs_obs "$op $name attempt $attempt reflected in ${elapsed}ms (removed)"
				return 0
			fi
		fi
		if ((SECONDS >= deadline)); then
			printf '%s\n' "$table" >"$OUT/status-$op-$attempt.txt"
			reg_timing "$op" "$name" "$attempt" "timeout" "still listed after ${seconds}s"
			rs_obs "criterion failed: $op $name attempt $attempt was still listed after ${seconds}s"
			return 1
		fi
		sleep 0.2
	done
}

reg_require_present() {
	local rc=0
	reg_measure_present "$@" || rc=$?
	((rc == 0)) || exit 1
}

reg_require_gone() {
	local rc=0
	reg_measure_gone "$@" || rc=$?
	((rc == 0)) || exit 1
}

# rs_call cannot record a refusal: under set -e it stops before writing a failing call's answer, and the rename check needs that answer as evidence.
reg_call_error() {
	local name="$1" tool="$2" args="$3" reply rc=0
	reply="$("$R/run" rsprobe agent call --ctl "$R/ctl/$name.sock" "$tool" "$args" 2>>"$OUT/agent-call.err")" || rc=$?
	printf '%s\n' "$reply" >>"$OUT/calls.jsonl"
	printf '%s\t%s\t%s\n' "$tool" "$rc" "$reply" >>"$OUT/call-results.tsv"
	printf '%s\n' "$reply"
}

reg_broker_pid() {
	if [[ "$REG_OS" == Darwin ]]; then
		local pid
		pid="$(pgrep -f -- "$R/bin/peers service run" 2>/dev/null | sed -n '1p')" || true
		[[ -n "$pid" ]] || return 1
		printf '%s\n' "$pid"
		return 0
	fi
	local proc exe second third
	for proc in /proc/[0-9]*; do
		exe="$(readlink "$proc/exe" 2>/dev/null)" || continue
		[[ "$exe" == "$R/bin/peers" ]] || continue
		[[ -r "$proc/cmdline" ]] || continue
		second="$(tr '\0' '\n' <"$proc/cmdline" | sed -n '2p')"
		third="$(tr '\0' '\n' <"$proc/cmdline" | sed -n '3p')"
		if [[ "$second" == service && "$third" == run ]]; then
			printf '%s\n' "${proc#/proc/}"
			return 0
		fi
	done
	return 1
}

"$R/run" rsprobe watch --dir "$R/cfg/sessions" --out "$OUT/watch.jsonl" \
	>"$OUT/watch-summary.txt" 2>"$OUT/watch.err" &
WATCH_PID=$!
rs_obs "watching $R/cfg/sessions (watch pid $WATCH_PID)"

rs_tmux_start
rs_window reg-a claude --model haiku -n reg-a --permission-mode default --append-system-prompt "$PROMPT" --debug-file "$R/debug/reg-a.debug"
rs_window reg-b claude --model haiku -n reg-b --permission-mode default --append-system-prompt "$PROMPT" --allowedTools "Bash(sleep:*)" --debug-file "$R/debug/reg-b.debug"
rs_window reg-c claude --model haiku -n reg-c --permission-mode default --append-system-prompt "$PROMPT" --debug-file "$R/debug/reg-c.debug"
for name in reg-a reg-b reg-c; do
	if ! reg_wait_present "$name" 30; then
		rs_snap "$name" "timeout-$name.txt" || true
		rs_obs "session $name did not register within 30s"
		exit 1
	fi
done
# Registration happens before the composer accepts keys, so the rename and prompt below must not be typed into a starting session.
for name in reg-a reg-b reg-c; do
	if ! rs_wait_ready "$name" 30; then
		rs_snap "$name" "ready-$name-timeout.txt" || true
		rs_obs "session $name did not reach its input prompt within 30s"
		exit 1
	fi
	rs_obs_mode "$name"
done
reg_status >"$OUT/status-initial.txt"
rs_obs "reg-a, reg-b and reg-c registered"

start="$(reg_now_ms)"
rs_keys reg-a '/rename reg-a2' Enter
reg_require_present reg-a2 rename 1 5 "$start" reg-a

rs_agent reg-obs
list_reply="$(rs_call reg-obs list_agents '{}')"
if [[ "$list_reply" == *reg-a2* ]]; then
	rs_obs "list_agents lists reg-a2 after the rename"
else
	rs_obs "list_agents does not list reg-a2 after the rename"
fi
stale_reply="$(reg_call_error reg-obs send_message '{"to":"reg-a@hostB","message":"x"}')"
if [[ "$stale_reply" == *'"ok":false'* && "$stale_reply" == *reg-a2* ]]; then
	rs_obs "send_message to reg-a@hostB refused and named reg-a2"
else
	rs_obs "criterion failed: send_message to reg-a@hostB answered: $stale_reply"
	exit 1
fi

start="$(reg_now_ms)"
rs_send_prompt reg-b 'Run the shell command "sleep 20" with the Bash tool, then reply done.' 'Run the shell command'
busy_seen=0
idle_seen=0
deadline=$((SECONDS + 60))
while ((SECONDS < deadline)); do
	table="$(reg_status)"
	line="$(printf '%s\n' "$table" | reg_line_of reg-b)"
	label="$(printf '%s\n' "$line" | sed -n 's/.* status=\([^ ]*\) .*/\1/p')"
	if [[ "$label" == busy ]] && ((busy_seen == 0)); then
		busy_seen=1
		now="$(reg_now_ms)"
		printf '%s\n' "$table" >"$OUT/status-busy-reg-b.txt"
		reg_timing status reg-b 1 "$((now - start))" "idle->busy"
	elif [[ "$label" == idle ]] && ((busy_seen == 1)); then
		idle_seen=1
		now="$(reg_now_ms)"
		printf '%s\n' "$table" >"$OUT/status-idle-reg-b.txt"
		reg_timing status reg-b 1 "$((now - start))" "busy->idle"
		break
	fi
	sleep 0.5
done
if ((busy_seen == 0)); then
	rs_obs "reg-b never reported busy within 60s of the Bash prompt"
elif ((idle_seen == 0)); then
	rs_obs "reg-b stayed busy past the 60s deadline"
fi

rs_window invalid claude --model haiku -n 'a@b' --permission-mode default --append-system-prompt "$PROMPT" --allowedTools mcp__peer__list_agents --debug-file "$R/debug/invalid.debug"
if ! rs_wait_ready invalid 45; then
	rs_obs "the a@b session did not reach its input prompt within 45s"
fi
rs_obs_mode invalid
reg_status >"$OUT/status-invalid.txt"
if reg_line_present ab; then
	rs_obs "the a@b session is listed as ab before /rename"
else
	rs_obs "the a@b session is not listed before /rename"
fi
rs_send_prompt invalid 'Call the mcp__peer__list_agents tool and show me its raw result.' 'Call the mcp__peer__list_agents'
if ! rs_wait_pane invalid 'code: ' 60; then
	rs_obs "no tool error text appeared in the a@b pane within 60s"
fi
rs_snap invalid invalid-call.txt
rs_excerpt 'mcp__peer__list_agents' invalid-call-record.jsonl
if ! rs_pane_idle invalid 30; then
	rs_obs "the a@b pane was not idle within 30s before /rename"
fi
start="$(reg_now_ms)"
rs_keys invalid '/rename ab' Enter
# The a@b session is not listed before the rename, so there is no previous name to require; the limit still applies.
reg_require_present ab rename-invalid 1 5 "$start"

start="$(reg_now_ms)"
rs_keys reg-c '/exit' Enter
reg_require_gone reg-c exit 1 5 "$start"

for attempt in 2 3 4 5 6 7 8 9 10; do
	if ((attempt % 2 == 0)); then
		new_name=reg-a
		want_prev=reg-a2
	else
		new_name=reg-a2
		want_prev=reg-a
	fi
	start="$(reg_now_ms)"
	rs_keys reg-a "/rename $new_name" Enter
	reg_require_present "$new_name" rename "$attempt" 5 "$start" "$want_prev"

	exit_name="reg-exit-$attempt"
	rs_window "$exit_name" claude --model haiku -n "$exit_name" --permission-mode default --append-system-prompt "$PROMPT" --debug-file "$R/debug/$exit_name.debug"
	if ! reg_wait_present "$exit_name" 30; then
		rs_snap "$exit_name" "timeout-$exit_name.txt" || true
		rs_obs "session $exit_name did not register within 30s"
		exit 1
	fi
	if ! rs_wait_ready "$exit_name" 30; then
		rs_obs "session $exit_name did not reach its input prompt within 30s"
		exit 1
	fi
	rs_obs_mode "$exit_name"
	start="$(reg_now_ms)"
	rs_keys "$exit_name" '/exit' Enter
	reg_require_gone "$exit_name" exit "$attempt" 5 "$start"
done

if [[ "$REG_OS" == Darwin ]]; then
	{
		printf 'inotify_max_user_instances=skipped (Linux only)\n'
		printf 'inotify_instances=skipped (macOS: the Broker watches with kqueue)\n'
		printf 'strace=skipped (Linux only; this probe does not use it)\n'
		if broker_pid="$(reg_broker_pid)"; then
			printf 'broker_pid=%s\n' "$broker_pid"
			if command -v lsof >/dev/null 2>&1; then
				fd_count="$(lsof -p "$broker_pid" 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')"
				printf 'broker_open_fds=%s (recorded only; not a criterion)\n' "$fd_count"
			else
				printf 'broker_open_fds=skipped (lsof not found)\n'
			fi
		else
			printf 'broker_pid=not found\n'
		fi
	} >"$OUT/inotify.txt"
else
	max_instances=unreadable
	if [[ -r /proc/sys/fs/inotify/max_user_instances ]]; then
		max_instances="$(cat /proc/sys/fs/inotify/max_user_instances)"
	fi
	{
		printf 'max_user_instances=%s\n' "$max_instances"
		if broker_pid="$(reg_broker_pid)"; then
			instances=0
			for fdinfo in "/proc/$broker_pid"/fdinfo/*; do
				[[ -e "$fdinfo" ]] || continue
				if grep -q '^inotify ' "$fdinfo"; then
					instances=$((instances + 1))
				fi
			done
			printf 'broker_pid=%s\nbroker_inotify_instances=%s\n' "$broker_pid" "$instances"
		else
			printf 'broker_pid=not found\nbroker_inotify_instances=unknown\n'
		fi
	} >"$OUT/inotify.txt"
fi

reg_status >"$OUT/status-final.txt"
kill -INT "$WATCH_PID" 2>/dev/null || true
watch_rc=0
wait "$WATCH_PID" || watch_rc=$?
watch_summary="$(sed -n '1p' "$OUT/watch-summary.txt")"
printf 'watch_rc=%s\n%s\n' "$watch_rc" "$watch_summary" >"$OUT/watch-result.txt"
misses="$(printf '%s\n' "$watch_summary" | sed -n 's/.*misses=\([0-9][0-9]*\).*/\1/p')"
if [[ ! "$misses" =~ ^[0-9]+$ ]]; then
	rs_obs "criterion failed: the watch summary has no misses count: $watch_summary"
	exit 1
fi
if ((misses != 0)); then
	rs_obs "criterion failed: the watch reported misses=$misses, want 0 ($watch_summary)"
	exit 1
fi
if ((watch_rc != 0)); then
	rs_obs "criterion failed: the watch exited with rc=$watch_rc, want 0 ($watch_summary)"
	exit 1
fi
rs_obs "watch exited with rc=$watch_rc: $watch_summary"

# A run that never submitted the status prompt, or recorded no model call, would be a false pass.
rs_require_model_turn 'Run the shell command' 1
