#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

# Probe: inbox binding timing and the child-session variables.
#
# It answers the Roadmap's Items to verify section: does an MCP server process
# ever start without the inbox socket path in its process variables, and does a
# child session inherit the parent's CLAUDE_CODE_MESSAGING_SOCKET? The first two
# steps start sessions without input and reach no model. The third step asks
# the parent session to run fixed commands with its Bash tool and to start a
# child session from that shell, under bypass permissions so no approval
# dialog blocks the command. Those turns are model calls, and the runner
# counts them in usage.txt.
#
# The step does not use shell mode ("!"). 2.1.311 sends every ! command and
# its output to the model, and the follow-up turn inspects the result with its
# own Bash calls, which raise approval dialogs under the default permission
# mode and leave the probe typing into a dialog.
#
# Claude Code 2.1.311 rejects stream-json input and output unless --verbose is
# given, so every headless command below carries it.

BROKER_SOCK="/tmp/peers-$(id -u)/broker.sock"
TSV="$OUT/binding.tsv"
headless_args=(-p --model haiku --input-format stream-json --output-format stream-json --verbose)

# remove_fifo removes the child session's FIFO from the evidence directory, so
# an upload never sees a special file. The library's cleanup chain still
# captures the windows of a failed run after this cleanup.
remove_fifo() {
	rm -f "$OUT/child.in"
}
rs_on_exit remove_fifo

# wait_record_socket prints the first non-empty inbox socket path for a name.
wait_record_socket() {
	local name="$1" seconds="$2" deadline socket
	deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if socket="$(rs_socket_of "$name" 2>/dev/null)" && [[ -n "$socket" ]]; then
			printf '%s\n' "$socket"
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# record_present reports whether the session registry has a record for a name.
record_present() {
	local name="$1" file
	for file in "$R"/cfg/sessions/*.json; do
		[[ -e "$file" ]] || continue
		if jq -e --arg name "$name" '.name == $name' "$file" >/dev/null 2>&1; then
			return 0
		fi
	done
	return 1
}

# proc_is_peers_mcp reports whether a process runs "peers mcp".
proc_is_peers_mcp() {
	local pid="$1" cmd0
	local -a argv=()
	[[ -r "/proc/$pid/cmdline" ]] || return 1
	mapfile -d '' -t argv <"/proc/$pid/cmdline" 2>/dev/null || return 1
	((${#argv[@]} >= 2)) || return 1
	cmd0="${argv[0]}"
	[[ "$cmd0" == "$R/bin/peers" || "${cmd0##*/}" == "peers" ]] || return 1
	[[ "${argv[1]}" == "mcp" ]]
}

# find_mcp_pid searches the process tree under $1 for the MCP server.
find_mcp_pid() {
	local root="$1" child
	if proc_is_peers_mcp "$root"; then
		printf '%s\n' "$root"
		return 0
	fi
	while IFS= read -r child; do
		[[ -n "$child" ]] || continue
		if find_mcp_pid "$child"; then
			return 0
		fi
	done < <(pgrep -P "$root" 2>/dev/null || true)
	return 1
}

# wait_mcp_pid retries find_mcp_pid until the server process appears.
wait_mcp_pid() {
	local root="$1" seconds="$2" deadline pid
	deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if pid="$(find_mcp_pid "$root")"; then
			printf '%s\n' "$pid"
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# pane_pid_of prints the PID of the process in a tmux window.
pane_pid_of() {
	local window="$1"
	tmux -L "$TMUX_SERVER" list-panes -t "$window" -F '#{pane_pid}' 2>/dev/null | head -n 1
}

# close_window waits for an exited window and kills a stuck one.
close_window() {
	local window="$1" deadline
	deadline=$((SECONDS + 10))
	while ((SECONDS < deadline)); do
		if ! tmux -L "$TMUX_SERVER" list-panes -t "$window" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	tmux -L "$TMUX_SERVER" kill-window -t "$window" >/dev/null 2>&1 || true
}

# status_addresses prints one Agent address per line. An unreachable Broker
# prints nothing.
status_addresses() {
	"$R/run" rsprobe status --json 2>/dev/null | jq -r '.agents[]?.address // empty' 2>/dev/null || true
}

# status_lists reports whether rsprobe status names a session.
status_lists() {
	local name="$1" address
	while IFS= read -r address; do
		[[ -n "$address" ]] || continue
		case "$address" in
			"$name"|"$name@"*|"$name ["*) return 0 ;;
		esac
	done < <(status_addresses)
	return 1
}

# count_addresses counts the non-empty lines of an address list on stdin.
count_addresses() {
	awk 'NF { n++ } END { print n + 0 }'
}

# read_proc_var classifies one variable through rs_proc_vars: absent, empty,
# value, or unreadable when the process is gone.
read_proc_var() {
	local pid="$1" name="$2" line
	PV_STATE=unreadable
	PV_VALUE=""
	if ! kill -0 "$pid" 2>/dev/null; then
		return 0
	fi
	if ! line="$(rs_proc_vars "$pid" "$name" 2>/dev/null)"; then
		return 0
	fi
	case "$line" in
		"$name <absent>") PV_STATE=absent ;;
		"$name=") PV_STATE=empty ;;
		"$name="*) PV_STATE=value; PV_VALUE="${line#"$name="}" ;;
	esac
}

# check_session records one started session in binding.tsv.
check_session() {
	local mode="$1" iter="$2" name="$3" root="$4"
	local socket="" mcp="" class="" state="" value="" status=no socket_present=no
	if socket="$(wait_record_socket "$name" 15)"; then
		socket_present=yes
	fi
	if [[ -n "$root" ]]; then
		mcp="$(wait_mcp_pid "$root" 10 || true)"
	fi
	if [[ -z "$mcp" ]]; then
		state=no-mcp-process
		class=no-mcp-process
	else
		read_proc_var "$mcp" CLAUDE_CODE_MESSAGING_SOCKET
		state=$PV_STATE
		value=$PV_VALUE
		case "$state" in
			absent) class=no-path-absent ;;
			empty) class=no-path-empty ;;
			value)
				if [[ "$socket_present" == yes && "$value" == "$socket" ]]; then
					class=match
				else
					class=mismatch
				fi
				;;
			*) class=unreadable ;;
		esac
	fi
	if status_lists "$name"; then
		status=yes
	fi
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$mode" "$iter" "$name" "${socket:--}" "$socket_present" \
		"${mcp:--}" "$state" "${value:--}" "$class" "$status" >>"$TSV"
}

# count_mode_class counts TSV rows of one mode and one classification.
count_mode_class() {
	awk -F'\t' -v mode="$1" -v class="$2" \
		'NR > 1 && $1 == mode && $9 == class { n++ } END { print n + 0 }' "$TSV"
}

# send_prompt_command submits a prompt through the shared rs_send_prompt helper
# and waits for the file the prompt asks the model to write. The helper types the
# text literally, waits for the composer to echo it, pauses, and sends Enter in a
# second write; 2.1.311 folds a text-and-Enter burst into one paste and never
# submits it. The prompt is typed once; when the marker never appears the probe
# fails and keeps the pane instead of typing the prompt again.
send_prompt_command() {
	local window="$1" prompt="$2" needle="$3" marker="$4" seconds="$5"
	if ! rs_send_prompt "$window" "$prompt" "$needle" 60; then
		echo "binding: the $window session did not accept its prompt (see $OUT/observations.txt)" >&2
		return 1
	fi
	if ! rs_wait_file "$marker" "$seconds"; then
		rs_snap "$window" "prompt-output-$window.txt"
		echo "binding: the $window session did not write $marker within ${seconds}s (see $OUT/prompt-output-$window.txt)" >&2
		return 1
	fi
}

# stop_headless closes stdin before this call, then waits for the process and
# kills it if it lingers.
stop_headless() {
	local pid="$1" deadline
	deadline=$((SECONDS + 15))
	while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do
		sleep 0.2
	done
	if kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
	fi
	wait "$pid" 2>/dev/null || true
}

# --- setup -----------------------------------------------------------------

# Start the Broker explicitly so its stderr goes to the evidence directory
# instead of /dev/null. Registration is judged from rsprobe status and the
# child session socket only: this log records successful registrations alone,
# so it is kept as raw evidence and is not read by the checks.
"$R/run" peers service run --user 2>"$OUT/broker.log" &
if ! rs_wait_file "$BROKER_SOCK" 15; then
	echo "binding: the Broker socket $BROKER_SOCK did not appear (see $OUT/broker.log)" >&2
	exit 1
fi
rs_obs "Broker ready at $BROKER_SOCK"

printf 'mode\titer\tname\trecord_socket\tsocket_present\tmcp_pid\tenv_state\tenv_value\tclass\tstatus_listed\n' >"$TSV"

rs_tmux_start

# --- step 1: 20 interactive starts -----------------------------------------

for i in {1..20}; do
	name="bind-$i"
	if ((i > 10)); then
		rs_window "load-$i" stress-ng --cpu 4 --timeout 60s
		sleep 0.5
	fi
	rs_window "$name" claude --model haiku -n "$name" --permission-mode default
	root="$(pane_pid_of "$name" || true)"
	check_session interactive "$i" "$name" "$root"
	rs_obs_mode "$name"
	rs_keys "$name" '/exit' Enter
	close_window "$name"
done

interactive_absent="$(count_mode_class interactive no-path-absent)"
interactive_empty="$(count_mode_class interactive no-path-empty)"
interactive_match="$(count_mode_class interactive match)"
interactive_mismatch="$(count_mode_class interactive mismatch)"
interactive_none="$(count_mode_class interactive no-mcp-process)"
interactive_unreadable="$(count_mode_class interactive unreadable)"
rs_obs "interactive: with-path=$interactive_match no-path=$((interactive_absent + interactive_empty)) mismatch=$interactive_mismatch no-mcp-process=$interactive_none unreadable=$interactive_unreadable (absent=$interactive_absent empty=$interactive_empty)"

# --- step 2: 10 headless starts --------------------------------------------

for i in {1..10}; do
	name="bindh-$i"
	fifo="$WORK/$name.in"
	rm -f "$fifo"
	mkfifo "$fifo"
	exec 3<>"$fifo"
	"$R/run" claude "${headless_args[@]}" -n "$name" <"$fifo" >"$OUT/$name.out" 2>&1 &
	pid=$!
	check_session headless "$i" "$name" "$pid"
	exec 3>&-
	stop_headless "$pid"
	rm -f "$fifo"
done

headless_absent="$(count_mode_class headless no-path-absent)"
headless_empty="$(count_mode_class headless no-path-empty)"
headless_match="$(count_mode_class headless match)"
headless_mismatch="$(count_mode_class headless mismatch)"
headless_none="$(count_mode_class headless no-mcp-process)"
headless_unreadable="$(count_mode_class headless unreadable)"
rs_obs "headless: with-path=$headless_match no-path=$((headless_absent + headless_empty)) mismatch=$headless_mismatch no-mcp-process=$headless_none unreadable=$headless_unreadable (absent=$headless_absent empty=$headless_empty)"

# --- step 3: a child session started from the parent's shell ----------------

parent_name=parent
# Bypass permissions, because the fixed commands below run through the Bash
# tool and a default-mode approval dialog would stop the session.
rs_window "$parent_name" claude --model haiku -n "$parent_name" --permission-mode bypassPermissions
if ! parent_socket="$(wait_record_socket "$parent_name" 20)"; then
	echo "binding: the parent session did not publish an inbox socket path" >&2
	exit 1
fi
# The prompts below must land in the composer, so wait for it before typing.
if ! rs_wait_ready "$parent_name" 45; then
	echo "binding: the parent session did not reach its input prompt" >&2
	exit 1
fi
rs_obs_mode "$parent_name"
# The shell-mode attempt showed that 2.1.311 renders its prompt prefix as "!"
# followed by U+00A0 and a space, so a literal match on "! " misses it. The
# fact is kept even though this step now uses the Bash tool: it is the version
# behaviour that made the earlier attempt fail to submit.
rs_obs "2.1.311 shell-mode composer renders the prefix as '!' U+00A0 ' '; an ASCII-space match on the prefix does not match"
parent_deadline=$((SECONDS + 10))
while ! status_lists "$parent_name"; do
	if ((SECONDS >= parent_deadline)); then
		break
	fi
	sleep 0.2
done
sleep 2
rs_snap "$parent_name" parent-pane.txt
agents_before="$(status_addresses)"

env_file="$OUT/parent-shell-env.txt"
rm -f "$env_file"
env_command="for v in CLAUDE_CODE_MESSAGING_SOCKET CLAUDE_CODE_CHILD_SESSION; do echo \"\$v=\${!v-}\"; done > $env_file"
env_prompt="First task: use the Bash tool to run exactly this command, with no changes and no other commands: $env_command"
if ! send_prompt_command "$parent_name" "$env_prompt" "First task" "$env_file" 60; then
	echo "binding: the parent session did not write its variable excerpt" >&2
	exit 1
fi

rm -f "$OUT/child.in" "$OUT/child.pid" "$OUT/child.out"
# The trailing sleep keeps the Bash tool call alive while the probe reads the
# child's process environment, so the shell that started the child is still
# running. The FIFO writer is started in the same call and inherits the same
# lifetime.
child_command="cd $WORK && rm -f $OUT/child.in $OUT/child.pid $OUT/child.out && mkfifo $OUT/child.in && (sleep 600 > $OUT/child.in &) && (claude -p --model haiku --input-format stream-json --output-format stream-json --verbose -n child-of-parent < $OUT/child.in > $OUT/child.out 2>&1 & echo \$! > $OUT/child.pid) && sleep 90"
# The prompt is long enough that the composer shows only its last lines, so
# the readiness needle comes from the end of the command, which stays visible.
child_prompt="Second task: use the Bash tool to run exactly this command, with no changes and no other commands: $child_command"
if ! send_prompt_command "$parent_name" "$child_prompt" "sleep 90" "$OUT/child.pid" 90; then
	echo "binding: the parent session did not start the child session" >&2
	exit 1
fi

child_root="$(cat "$OUT/child.pid" 2>/dev/null || true)"
child_mcp=""
if [[ -n "$child_root" ]]; then
	child_mcp="$(wait_mcp_pid "$child_root" 20 || true)"
fi

child_state=no-mcp-process
child_value=""
child_marker_state=no-mcp-process
child_marker_value=""
if [[ -n "$child_mcp" ]]; then
	read_proc_var "$child_mcp" CLAUDE_CODE_MESSAGING_SOCKET
	child_state=$PV_STATE
	child_value=$PV_VALUE
	read_proc_var "$child_mcp" CLAUDE_CODE_CHILD_SESSION
	child_marker_state=$PV_STATE
	child_marker_value=$PV_VALUE
fi
{
	printf 'mcp_pid=%s\n' "${child_mcp:--}"
	printf 'CLAUDE_CODE_MESSAGING_SOCKET state=%s value=%s\n' "$child_state" "${child_value:--}"
	printf 'CLAUDE_CODE_CHILD_SESSION state=%s value=%s\n' "$child_marker_state" "${child_marker_value:--}"
} >"$OUT/child-env.txt"

child_record=no
child_record_socket=""
child_deadline=$((SECONDS + 10))
while ((SECONDS < child_deadline)); do
	if record_present child-of-parent; then
		child_record=yes
		break
	fi
	sleep 0.2
done
if ! child_record_socket="$(rs_socket_of child-of-parent 2>/dev/null)"; then
	child_record_socket=""
fi

child_verdict=no-mcp-process
case "$child_state" in
	absent) child_verdict=absent ;;
	empty) child_verdict=empty ;;
	value)
		if [[ "$child_value" == "$parent_socket" ]]; then
			child_verdict=inherits-parent
		elif [[ -n "$child_record_socket" && "$child_value" == "$child_record_socket" ]]; then
			child_verdict=own-socket
		else
			child_verdict=other
		fi
		;;
	unreadable) child_verdict=unreadable ;;
esac

agents_after="$(status_addresses)"
new_agents=""
while IFS= read -r address; do
	[[ -n "$address" ]] || continue
	if ! printf '%s\n' "$agents_before" | grep -Fxq -- "$address"; then
		new_agents="${new_agents}${address}"$'\n'
	fi
done <<<"$agents_after"

agents_before_count="$(printf '%s\n' "$agents_before" | count_addresses)"
agents_after_count="$(printf '%s\n' "$agents_after" | count_addresses)"
new_agents_count="$(printf '%s\n' "$new_agents" | count_addresses)"
new_agents_list="$(printf '%s' "$new_agents" | tr '\n' ',' | sed 's/,$//')"

parent_listed=no
if status_lists "$parent_name"; then
	parent_listed=yes
fi
child_listed=no
if status_lists child-of-parent; then
	child_listed=yes
fi

{
	printf 'parent_socket=%s\n' "$parent_socket"
	printf 'child_root_pid=%s\n' "${child_root:--}"
	printf 'child_record=%s\n' "$child_record"
	printf 'child_record_socket=%s\n' "${child_record_socket:--}"
	printf 'child_env_state=%s\n' "$child_state"
	printf 'child_env_value=%s\n' "${child_value:--}"
	printf 'child_marker_state=%s\n' "$child_marker_state"
	printf 'inherit_verdict=%s\n' "$child_verdict"
	printf 'agents_before=%s\n' "$agents_before_count"
	printf 'agents_after=%s\n' "$agents_after_count"
	printf 'new_agents=%s\n' "$new_agents_count"
	printf 'new_agent_addresses=%s\n' "$new_agents_list"
	printf 'parent_listed=%s\n' "$parent_listed"
	printf 'child_listed=%s\n' "$child_listed"
} >"$OUT/subsession.txt"

rs_obs "child session: record=$child_record socket=${child_record_socket:--} mcp=${child_mcp:--} env_state=$child_state env=${child_value:--} verdict=$child_verdict agents=$agents_before_count->$agents_after_count new=$new_agents_count parent_listed=$parent_listed child_listed=$child_listed"

# --- teardown ---------------------------------------------------------------

if [[ -n "$child_root" ]] && kill -0 "$child_root" 2>/dev/null; then
	kill "$child_root" 2>/dev/null || true
fi
while IFS= read -r spid; do
	kill "$spid" 2>/dev/null || true
done < <(pgrep -f '^sleep 600$' 2>/dev/null || true)
rs_keys "$parent_name" '/exit' Enter
close_window "$parent_name"

rs_obs "binding probe finished"

# The parent session's first task is a model prompt; a run that never submitted
# it, or that recorded no model call at all, is a false pass.
rs_require_model_turn 'First task: use the Bash tool' 1
