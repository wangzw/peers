#!/usr/bin/env bash
# lib.sh is sourced by every real-session probe script and provides the tmux,
# simulated Agent, peer host container and evidence helpers.

set -euo pipefail

: "${R:?lib.sh needs R}"
: "${PROBE:?lib.sh needs PROBE}"

OUT="$R/out/$PROBE"
WORK="$R/work/$PROBE"
TMUX_SERVER=peers-rs

# Resolve the compose file from this script's location, not the caller's working
# directory, so every probe finds it from any working directory.
RS_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RS_COMPOSE="$RS_REPO_ROOT/test/realsession/compose.yaml"

# Compose interpolates RS_ROOT for every command, not only up, so export it once
# and the exec calls carry it too.
export RS_ROOT="$R"

RS_BROKER_PID=""
RS_BROKER_HOOKED=0
RS_BROKER_LOG=""

# RS_CONTAINER_WAIT bounds how long a probe waits for a started process, on the
# host or inside the peer host container, to create its control socket; it
# covers a cold container start and an external process start, so it is not the
# Go tests' 3-second rule.
RS_CONTAINER_WAIT=10

# RS_HOLD_CALL_WAIT bounds the ref-clear probe's background send_message under
# the hold policy; whether the call returns at once is the observation, so the
# bound only prevents waiting forever.
RS_HOLD_CALL_WAIT="${RS_HOLD_CALL_WAIT:-10}"

# rs_on_exit registers a cleanup hook, run in registration order; a non-zero
# hook makes the exit status non-zero. Probes register cleanups here rather than
# installing their own EXIT trap, which would replace the library's.
RS_EXIT_HOOKS=()
rs_on_exit() {
	RS_EXIT_HOOKS+=("$1")
}

# rs_run_exit_hooks is the only EXIT trap; it runs the registered cleanups and,
# on failure, captures every window into $OUT.
rs_run_exit_hooks() {
	local status=$? hook
	if ((${#RS_EXIT_HOOKS[@]} > 0)); then
		for hook in "${RS_EXIT_HOOKS[@]}"; do
			"$hook" || status=1
		done
	fi
	if ((status != 0)) && [[ -d "$OUT" ]]; then
		local window
		while IFS= read -r window; do
			[[ -n "$window" ]] || continue
			tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" >"$OUT/fail-$window.txt" 2>/dev/null || true
		done < <(tmux -L "$TMUX_SERVER" list-windows -F '#{window_name}' 2>/dev/null || true)
	fi
	exit "$status"
}
trap rs_run_exit_hooks EXIT

rs_tmux_start() {
	"$R/run" tmux -L "$TMUX_SERVER" -f /dev/null new-session -d -s base -c "$WORK"
}

rs_window() {
	local window="$1"
	shift
	tmux -L "$TMUX_SERVER" new-window -d -n "$window" -c "$WORK" "$R/run" "$@"
}

rs_keys() {
	local window="$1"
	shift
	tmux -L "$TMUX_SERVER" send-keys -t "$window" "$@"
}

# rs_keys_literal sends text literally, without tmux key-name lookup, so spaces
# and shell punctuation are typed as written.
rs_keys_literal() {
	local window="$1" text="$2"
	tmux -L "$TMUX_SERVER" send-keys -t "$window" -l -- "$text"
}

rs_snap() {
	local window="$1" file="$2"
	tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" >"$OUT/$file"
}

# rs_grep_status prints grep's exit status over the pane text: 0 matched, 1 no
# match, and 2 or more when the search could not run. The pane goes through
# process substitution so the status is grep's own: under pipefail a plain
# "printf ... | grep -q" reports the writer's SIGPIPE when the pane has content
# after the match.
rs_grep_status() {
	local pane="$1" st=0
	shift
	grep "$@" < <(printf '%s\n' "$pane") || st=$?
	printf '%s\n' "$st"
}

rs_wait_pane() {
	local window="$1" pattern="$2" seconds="$3"
	local deadline=$((SECONDS + seconds))
	local pane=""
	while ((SECONDS < deadline)); do
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		if [[ "$(rs_grep_status "$pane" -qE -- "$pattern")" == 0 ]]; then
			return 0
		fi
		sleep 0.2
	done
	pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
	printf '%s\n' "$pane" >"$OUT/timeout-$window.txt"
	return 1
}

rs_wait_pane_literal() {
	local window="$1" text="$2" seconds="$3"
	local deadline=$((SECONDS + seconds)) pane=""
	while ((SECONDS < deadline)); do
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		if [[ "$(rs_grep_status "$pane" -qF -- "$text")" == 0 ]]; then
			return 0
		fi
		sleep 0.1
	done
	pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
	printf '%s\n' "$pane" >"$OUT/timeout-$window.txt"
	return 1
}

# rs_wait_turn_idle waits until no turn has been running for one second. It uses
# the "esc to interrupt" hint instead of a readiness criterion because Claude
# Code 2.1.311 rotates the idle footer hints, so a criterion can be absent while
# the session is idle.
rs_wait_turn_idle() {
	local window="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) pane="" quiet=0 st=0
	while ((SECONDS < deadline)); do
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		st="$(rs_grep_status "$pane" -qF -- 'esc to interrupt')"
		case "$st" in
		0) quiet=0 ;;
		1)
			quiet=$((quiet + 1))
			if ((quiet >= 5)); then
				return 0
			fi
			;;
		# A search that could not run is no evidence of a quiet turn, so the
		# run of quiet polls restarts.
		*) quiet=0 ;;
		esac
		sleep 0.2
	done
	return 1
}

# rs_submit_line submits text to a session's composer: it waits for the previous
# turn to end, types literally, waits until the composer echoes NEEDLE, and sends
# Enter as a second tmux write. The separate write matters because Claude Code
# 2.1.311 folds a text-and-Enter burst into one paste, leaving a newline in the
# composer.
rs_submit_line() {
	local window="$1" text="$2" needle="$3" seconds="${4:-60}"
	if ! rs_wait_turn_idle "$window" "$seconds"; then
		rs_snap "$window" "prompt-busy-$window.txt"
		rs_obs "$window: previous turn still running after ${seconds}s, so the text was not typed"
		return 1
	fi
	rs_keys_literal "$window" "$text"
	if ! rs_wait_pane_literal "$window" "$needle" 10; then
		rs_obs "$window: the composer did not echo the typed text"
		return 1
	fi
	# Pause so the last keystroke and Enter arrive in separate reads and the TUI
	# does not fold them into one paste.
	sleep 0.3
	rs_keys "$window" Enter
}

# rs_send_prompt submits one prompt through rs_submit_line. NEEDLE defaults to
# the first 24 characters of PROMPT; a prompt whose beginning scrolls off the
# composer needs a NEEDLE taken from the part that stays visible.
rs_send_prompt() {
	local window="$1" prompt="$2" needle="${3:-}" seconds="${4:-60}"
	if [[ -z "$needle" ]]; then
		needle="${prompt:0:24}"
	fi
	rs_submit_line "$window" "$prompt" "$needle" "$seconds"
}

# rs_send_shell submits one bash-mode command through rs_submit_line; pass it
# without the leading "!", and NEEDLE defaults to the first 24 characters of the
# "!" line.
rs_send_shell() {
	local window="$1" command="$2" needle="${3:-}" seconds="${4:-60}"
	if [[ -z "$needle" ]]; then
		needle="! ${command:0:22}"
	fi
	rs_submit_line "$window" "! $command" "$needle" "$seconds"
}

rs_wait_file() {
	local path="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if [[ -e "$path" ]]; then
			return 0
		fi
		sleep 0.2
	done
	if [[ -e "$path" ]]; then
		return 0
	fi
	return 1
}

rs_socket_of() {
	local name="$1" file
	for file in "$R"/cfg/sessions/*.json; do
		if [[ ! -e "$file" ]]; then
			continue
		fi
		if jq -e --arg name "$name" 'select(.name == $name) | .messagingSocketPath' "$file" >/dev/null 2>&1; then
			jq -r --arg name "$name" 'select(.name == $name) | .messagingSocketPath' "$file"
			return 0
		fi
	done
	return 1
}

rs_agent() {
	local name="$1" dir="${2:-$WORK}"
	"$R/run" rsprobe agent run \
		--config-dir "$R/cfg" \
		--name "$name" \
		--cwd "$dir" \
		--ctl "$R/ctl/$name.sock" \
		--recv-log "$OUT/$name.recv.jsonl" \
		--peers "$R/bin/peers" \
		>>"$OUT/agents.log" 2>&1 &
	rs_wait_file "$R/ctl/$name.sock" "$RS_CONTAINER_WAIT"
}

rs_call() {
	local name="$1" tool="$2" args="${3-}" reply
	if [[ -z "$args" ]]; then
		args='{}'
	fi
	reply="$("$R/run" rsprobe agent call --ctl "$R/ctl/$name.sock" "$tool" "$args")"
	printf '%s\n' "$reply" >>"$OUT/calls.jsonl"
	printf '%s\n' "$reply"
}

rs_excerpt() {
	local marker="$1" file="$2" src
	: >"$OUT/$file"
	: >"$OUT/$file.debug"
	while IFS= read -r -d '' src; do
		grep -F -- "$marker" "$src" >>"$OUT/$file" || true
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
	if [[ -d "$R/debug" ]]; then
		grep -RhF -- "$marker" "$R/debug" >>"$OUT/$file.debug" 2>/dev/null || true
	fi
}

# rs_reply_excerpt writes the raw assistant records whose text contains marker;
# only assistant records are read, because a prompt that asks for a string is
# echoed in a user record and matching the whole transcript would take the
# prompt for the answer.
rs_reply_excerpt() {
	local marker="$1" file="$2" src
	: >"$OUT/$file"
	while IFS= read -r -d '' src; do
		jq -Rrc --arg marker "$marker" '
			fromjson?
			| select(type == "object")
			| select(.type == "assistant")
			| . as $record
			| ((.message.content? // empty)
				| if type == "string" then .
				  elif type == "array" then [.[] | select(.type == "text") | .text] | join("\n")
				  else "" end) as $text
			| select($text | contains($marker))
			| $record
		' "$src" >>"$OUT/$file" 2>/dev/null || true
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
}

rs_assistant_text_present() {
	local src
	while IFS= read -r -d '' src; do
		if jq -Re '
			fromjson?
			| select(type == "object")
			| select(.type == "assistant")
			| ((.message.content? // empty)
				| if type == "string" then .
				  elif type == "array" then [.[] | select(.type == "text") | .text] | join("\n")
				  else "" end)
			| select(length > 0)
		' "$src" >/dev/null 2>&1; then
			return 0
		fi
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
	return 1
}

# rs_wait_reply polls the session transcripts until MIN (default 1) assistant
# records contain marker and writes them to $OUT/$file. The pane is not
# consulted, because it can show the prompt that echoes the requested string; it
# returns 0 on a match, 1 when assistant text was read but the marker never
# appeared, and 2 when no assistant text could be read at all.
rs_wait_reply() {
	local marker="$1" file="$2" seconds="$3" min="${4:-1}"
	local deadline=$((SECONDS + seconds)) count=0
	while ((SECONDS < deadline)); do
		rs_reply_excerpt "$marker" "$file"
		count="$(wc -l <"$OUT/$file" | tr -d ' ')"
		if ((count >= min)); then
			return 0
		fi
		sleep 0.2
	done
	rs_reply_excerpt "$marker" "$file"
	count="$(wc -l <"$OUT/$file" | tr -d ' ')"
	if ((count >= min)); then
		return 0
	fi
	if rs_assistant_text_present; then
		return 1
	fi
	return 2
}

# rs_proc_vars prints selected variables of a process: a missing variable prints
# as "<NAME> absent" and an empty one as "<NAME>=". Linux only, since it reads
# /proc/<pid>/environ.
rs_proc_vars() {
	local pid="$1" var line environ
	shift
	for var in "$@"; do
		if [[ ! "$var" =~ ^[A-Z0-9_]+$ ]]; then
			echo "rs_proc_vars: refusing to read variable name $var" >&2
			return 1
		fi
		if [[ "$var" == "CLAUDE_CODE_OAUTH_TOKEN" ]]; then
			echo "rs_proc_vars: refusing to read CLAUDE_CODE_OAUTH_TOKEN" >&2
			return 1
		fi
	done
	environ="$(tr '\0' '\n' <"/proc/$pid/environ")"
	for var in "$@"; do
		line="$(printf '%s\n' "$environ" | grep -E -m1 -- "^${var}=" || true)"
		if [[ "$line" == "$var="* ]]; then
			printf '%s=%s\n' "$var" "${line#"$var="}"
		else
			printf '%s absent\n' "$var"
		fi
	done
}

# socket_owner_pid prints the pid of the process listening on the Unix socket at
# path. On macOS lsof marks a connected client's name with a leading "->", and
# only the exact bound name matches, so a client that merely holds the socket is
# not mistaken for the listener.
socket_owner_pid() {
	local path="$1"
	if [[ "$(uname -s)" == Darwin ]]; then
		lsof -U -Fpn 2>/dev/null | awk -v s="$path" '
			/^p/ { pid = substr($0, 2) }
			$0 == "n" s { print pid; exit }'
		return
	fi
	ss -xlpn 2>/dev/null | awk -v s="$path" '
		index($0, s) {
			if (match($0, /pid=[0-9]+/)) {
				print substr($0, RSTART + 4, RLENGTH - 4)
				exit
			}
		}'
}

# rs_start_broker starts the account's user Broker and waits for the socket it
# serves. Every probe that uses a Broker calls it before starting a session, and
# the Broker is stopped when the probe exits.
rs_start_broker() {
	local log="${RS_BROKER_LOG:-$OUT/broker.log}"
	local sock deadline owner
	sock="/tmp/peers-$(id -u)/broker.sock"
	mkdir -p "$OUT"
	"$R/run" peers service run --user >>"$log" 2>&1 &
	RS_BROKER_PID=$!
	if ((RS_BROKER_HOOKED == 0)); then
		rs_on_exit rs_stop_broker
		RS_BROKER_HOOKED=1
	fi
	deadline=$((SECONDS + 15))
	while ((SECONDS < deadline)); do
		if [[ -S "$sock" ]]; then
			if ! kill -0 "$RS_BROKER_PID" 2>/dev/null; then
				echo "rs_start_broker: the socket $sock is served by another process; the started Broker exited (see $log)" >&2
				return 1
			fi
			owner="$(socket_owner_pid "$sock" || true)"
			if [[ "$owner" == "$RS_BROKER_PID" ]]; then
				rs_obs "Broker ready at $sock (pid $RS_BROKER_PID)"
				return 0
			fi
			# The socket is stale or served by another process; wait for the
			# started Broker to bind it.
			sleep 0.2
			continue
		fi
		if ! kill -0 "$RS_BROKER_PID" 2>/dev/null; then
			wait "$RS_BROKER_PID" 2>/dev/null || true
			RS_BROKER_PID=""
			echo "rs_start_broker: the Broker exited before its socket appeared (see $log)" >&2
			return 1
		fi
		sleep 0.2
	done
	echo "rs_start_broker: the Broker socket $sock did not appear within 15s (see $log)" >&2
	return 1
}

rs_stop_broker() {
	if [[ -n "${RS_BROKER_PID:-}" ]]; then
		kill "$RS_BROKER_PID" 2>/dev/null || true
		wait "$RS_BROKER_PID" 2>/dev/null || true
		RS_BROKER_PID=""
	fi
}

rs_hosta_up() {
	docker compose -f "$RS_COMPOSE" up -d
	HOSTA_IP="$(docker compose -f "$RS_COMPOSE" exec -T hosta /hosta-init.sh)"
}

rs_hosta_agent() {
	local name="$1" deadline
	docker compose -f "$RS_COMPOSE" exec -d -T hosta \
		/usr/local/bin/rsprobe agent run \
		--config-dir /rs/cfg \
		--name "$name" \
		--cwd /rs/work \
		--ctl "/rs/$name.ctl" \
		--recv-log "/rs/$name.recv.jsonl" \
		--peers /usr/local/bin/peers
	deadline=$((SECONDS + RS_CONTAINER_WAIT))
	while ((SECONDS < deadline)); do
		if docker compose -f "$RS_COMPOSE" exec -T hosta test -S "/rs/$name.ctl"; then
			return 0
		fi
		sleep 0.2
	done
	docker compose -f "$RS_COMPOSE" exec -T hosta test -S "/rs/$name.ctl"
}

rs_hosta_call() {
	local name="$1" tool="$2" args="${3-}" reply
	if [[ -z "$args" ]]; then
		args='{}'
	fi
	reply="$(docker compose -f "$RS_COMPOSE" exec -T hosta \
		/usr/local/bin/rsprobe agent call --ctl "/rs/$name.ctl" "$tool" "$args")"
	printf '%s\n' "$reply"
}

# rs_artifact_label percent-encodes every byte outside [A-Za-z0-9._-], because an
# upload-artifact name rejects characters such as "<" and Windows unpacking
# rejects a backslash; distinct names stay distinct.
rs_artifact_label() {
	local s="$1" out="" ch enc i
	local LC_ALL=C
	for ((i = 0; i < ${#s}; i++)); do
		ch="${s:i:1}"
		case "$ch" in
			[A-Za-z0-9._-]) out+="$ch" ;;
			*)
				printf -v enc '%%%02X' "'$ch"
				out+="$enc"
				;;
		esac
	done
	printf '%s\n' "$out"
}

# rs_hosta_recv copies a container-side receive log out, naming the local file
# through rs_artifact_label so the Agent name cannot break the upload.
rs_hosta_recv() {
	local name="$1"
	docker compose -f "$RS_COMPOSE" exec -T hosta cat "/rs/$name.recv.jsonl" \
		>"$OUT/hosta-$(rs_artifact_label "$name").recv.jsonl"
}

# rs_pair_text prints a tool reply's .text for diagnostics with pairing keys
# redacted, so a failure never writes a whole key to the probe log.
rs_pair_text() {
	printf '%s' "$1" | sed -E 's/("key"[[:space:]]*:[[:space:]]*")[^"]*(")/\1<redacted>\2/g'
}

# rs_pair_hosta pairs the runner Broker with hosta and checks its alias. Both
# tools return a JSON object in .text; anything else fails the probe rather than
# using whatever text arrived.
rs_pair_hosta() {
	local key reply text alias
	rs_hosta_agent pairer
	reply="$(rs_hosta_call pairer create_pairing_key '{}')"
	if ! text="$(printf '%s' "$reply" | jq -er '.text')"; then
		echo "rs_pair_hosta: create_pairing_key reply has no text field" >&2
		return 1
	fi
	if ! key="$(printf '%s' "$text" | jq -er '.key // empty')" || [[ -z "$key" ]]; then
		echo "rs_pair_hosta: create_pairing_key .text is not a key object; got: $(rs_pair_text "$text")" >&2
		return 1
	fi
	rs_agent pairer-b
	reply="$(rs_call pairer-b connect_broker "{\"address\":\"$HOSTA_IP:17443\",\"key\":\"$key\"}")"
	if ! text="$(printf '%s\n' "$reply" | jq -er '.text')"; then
		echo "rs_pair_hosta: connect_broker reply has no text field" >&2
		return 1
	fi
	if ! alias="$(printf '%s' "$text" | jq -er '.alias // empty')" || [[ -z "$alias" ]]; then
		echo "rs_pair_hosta: connect_broker .text is not an alias object; got: $(rs_pair_text "$text")" >&2
		return 1
	fi
	if [[ "$alias" != "hostA" ]]; then
		echo "rs_pair_hosta: expected alias hostA, got: $alias" >&2
		return 1
	fi
}

rs_obs() {
	printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"$OUT/observations.txt"
}

# rs_require_model_turn fails a probe when its prompts never reached the model:
# the environment's model-call counter must be at least MIN (default 1) and some
# transcript must hold a user record containing MARKER. Probes call it last, so a
# run whose prompt never submitted cannot pass on a pane capture alone.
rs_require_model_turn() {
	local marker="$1" min="${2:-1}"
	local usage total found=no src
	if [[ -z "$marker" || ! "$min" =~ ^[0-9]+$ ]]; then
		rs_obs "model-turn check: bad arguments marker=\"$marker\" min=\"$min\""
		return 1
	fi
	usage="$("$R/run" rsprobe env usage --root "$R" 2>/dev/null || true)"
	total="$(printf '%s\n' "$usage" | sed -n 's/^total //p' | tail -n 1)"
	if [[ ! "$total" =~ ^[0-9]+$ ]]; then
		total=unreadable
	fi
	while IFS= read -r -d '' src; do
		if jq -Rre --arg marker "$marker" '
			fromjson?
			| select(type == "object" and .type == "user")
			| (.message.content // empty)
			| if type == "string" then .
			  elif type == "array" then [.[] | (.text // "")] | join("\n")
			  else "" end
			| select(contains($marker))
		' "$src" >/dev/null 2>&1; then
			found=yes
			break
		fi
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
	rs_obs "model-turn check: marker=\"$marker\" user_record=$found usage_total=$total (want >= $min)"
	if [[ "$found" != yes ]]; then
		rs_obs "model-turn check failed: no user record contains \"$marker\""
		return 1
	fi
	if [[ "$total" == unreadable ]] || ((total < min)); then
		rs_obs "model-turn check failed: usage total $total is below $min"
		return 1
	fi
	return 0
}

# RS_READY_DIALOG_RE matches the startup dialogs and selection menus that must
# not be on screen when a session is called ready.
RS_READY_DIALOG_RE='Quick safety check|Yes, I trust this folder|Do you trust the files|Yes, I trust|Yes, I accept|Enter to confirm'

# rs_ready_criterion prints the readiness criterion the pane satisfies and
# returns 0, or 1 when the pane shows no composer or still shows a dialog. The
# three criteria are "? for shortcuts", "(shift+tab to cycle)" and a Try "
# placeholder; a bare "❯" is not one, since it is visible behind a dialog too.
rs_ready_criterion() {
	local pane="$1" st=0
	st="$(rs_grep_status "$pane" -qiE -- "$RS_READY_DIALOG_RE")"
	case "$st" in
	0) return 1 ;;
	1) ;;
	# A search that could not run cannot rule out a dialog, so the pane is not
	# ready.
	*) return 1 ;;
	esac
	if [[ "$(rs_grep_status "$pane" -qF -- '? for shortcuts')" == 0 ]]; then
		printf '%s\n' '? for shortcuts'
		return 0
	fi
	if [[ "$(rs_grep_status "$pane" -qF -- "(shift+tab to cycle)")" == 0 ]]; then
		printf '%s\n' "(shift+tab to cycle)"
		return 0
	fi
	if [[ "$(rs_grep_status "$pane" -qF -- 'Try "')" == 0 ]]; then
		printf '%s\n' 'Try " placeholder'
		return 0
	fi
	return 1
}

# RS_TRUST_DIALOG_RE matches the workspace trust dialog.
RS_TRUST_DIALOG_RE='Quick safety check|Yes, I trust this folder|Do you trust the files|Yes, I trust'

# RS_TRUST_DIALOG_RESEND_BUDGET bounds, in seconds, how long
# rs_dismiss_trust_dialog keeps resending when the caller passes no bound. Keep
# it under half of the smallest readiness budget a probe passes, so a timeout
# there still leaves time for one retry.
RS_TRUST_DIALOG_RESEND_BUDGET=10

# rs_accept_trust_dialog sends the keys that answer the workspace trust dialog.
# It returns 0 after pressing, 2 without pressing when the dialog is on screen
# but neither option carries the cursor, and 3 when a search over the pane could
# not run. The dialog focuses "No, exit" when the probe root resolves through a
# symlink, as /tmp does on macOS (/private/tmp), so Enter alone would end the
# session; the cursor moves to the trust entry only when the marker sits on the
# refusal.
rs_accept_trust_dialog() {
	local window="$1" pane="$2" st=0
	st="$(rs_grep_status "$pane" -qF -- '❯ No, exit')"
	case "$st" in
	0)
		rs_keys "$window" Down Enter
		return 0
		;;
	1) ;;
	*) return 3 ;;
	esac
	st="$(rs_grep_status "$pane" -qF -- '❯ Yes, I trust this folder')"
	case "$st" in
	0)
		rs_keys "$window" Enter
		return 0
		;;
	1) return 2 ;;
	*) return 3 ;;
	esac
}

# rs_dismiss_trust_dialog sends the trust keys until the dialog leaves the pane
# and prints how many key bursts that took. It returns 0 once the dialog is gone,
# 1 when it is still there at the deadline, and 2 when a recognized dialog
# carries the cursor on neither known option.
#
# Capture the pane again before every send: Claude Code may discard a burst that
# arrives while it switches the terminal to raw mode, and a stale capture could
# send the next burst to the following screen.
#
# Check the deadline only after a burst has been sent: SECONDS counts wall-clock
# time and can step behind the deadline before the first iteration, so one
# attempt is always made.
#
# A failed capture or a search that could not run says nothing about the dialog,
# so neither ends the wait; only a search that ran and found no match reports the
# dialog gone.
rs_dismiss_trust_dialog() {
	local window="$1" seconds="${2:-$RS_TRUST_DIALOG_RESEND_BUDGET}" deadline pane="" sends=0 rc=0 st=0
	deadline=$((SECONDS + seconds))
	while :; do
		if ! pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null)"; then
			if ((SECONDS >= deadline)); then
				printf '%s\n' "$sends"
				return 1
			fi
			sleep 0.5
			continue
		fi
		st="$(rs_grep_status "$pane" -qiE -- "$RS_TRUST_DIALOG_RE")"
		if ((st >= 2)); then
			if ((SECONDS >= deadline)); then
				printf '%s\n' "$sends"
				return 1
			fi
			sleep 0.5
			continue
		fi
		if ((st == 1)); then
			printf '%s\n' "$sends"
			return 0
		fi
		rc=0
		rs_accept_trust_dialog "$window" "$pane" || rc=$?
		if ((rc == 3)); then
			if ((SECONDS >= deadline)); then
				printf '%s\n' "$sends"
				return 1
			fi
			sleep 0.5
			continue
		fi
		if ((rc == 2)); then
			printf '%s\n' "$sends"
			return 2
		fi
		sends=$((sends + 1))
		if ((SECONDS >= deadline)); then
			break
		fi
		sleep 0.5
	done
	printf '%s\n' "$sends"
	return 1
}

# rs_wait_ready polls a window until the TUI reaches its input composer and no
# dialog is on screen. The bypass-permissions warning opens focused on "No,
# exit", so it is accepted with Down then Enter, and the matching criterion is
# written to observations.txt.
rs_wait_ready() {
	local window="$1" seconds="${2:-60}"
	local deadline=$((SECONDS + seconds)) pane="" criterion="" dialogs=0 dialog_bursts=0
	while ((SECONDS < deadline)); do
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		if [[ "$(rs_grep_status "$pane" -qF -- 'Yes, I accept')" == 0 ]]; then
			dialogs=$((dialogs + 1))
			printf '%s\n' "$pane" >"$OUT/dialog-$window-$dialogs.txt"
			rs_keys "$window" Down Enter
			sleep 1
			continue
		fi
		if [[ "$(rs_grep_status "$pane" -qiE -- "$RS_TRUST_DIALOG_RE")" == 0 ]]; then
			dialogs=$((dialogs + 1))
			printf '%s\n' "$pane" >"$OUT/dialog-$window-$dialogs.txt"
			rs_obs "$window: workspace trust dialog on screen; macOS resolves /tmp to /private/tmp and Claude Code keys trust by the resolved path, so an entry written for the literal path can miss"
			local bursts=0 rc=0
			bursts="$(rs_dismiss_trust_dialog "$window")" || rc=$?
			if ((rc == 2)); then
				rs_obs "$window: the trust dialog is on screen but the cursor is on neither known option; not pressing keys"
				return 1
			elif ((rc == 1)); then
				rs_obs "$window: the trust dialog was still on screen after $bursts key bursts; retrying"
			else
				dialog_bursts=$((dialog_bursts + bursts))
			fi
			continue
		fi
		if criterion="$(rs_ready_criterion "$pane")"; then
			if ((dialog_bursts == 1)); then
				rs_obs "$window: the trust dialog was dismissed with one key burst"
			elif ((dialog_bursts > 1)); then
				rs_obs "$window: the trust dialog took $dialog_bursts key bursts in all; the TUI discarded a burst sent while it was starting"
			fi
			rs_obs "$window: ready; matched criterion: $criterion"
			return 0
		fi
		sleep 0.2
	done
	printf '%s\n' "$pane" >"$OUT/timeout-$window.txt"
	rs_obs "$window: not ready within ${seconds}s (see timeout-$window.txt)"
	return 1
}

# rs_mode_text prints the permission-mode line from a window's bottom bar.
# Claude Code 2.1.311 writes a line ending in "(shift+tab to cycle)"; older
# versions wrote "? for shortcuts" with no mode, and an empty result means no
# mode line was printed.
rs_mode_text() {
	local window="$1" pane line=""
	pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
	line="$(printf '%s\n' "$pane" | grep -F -- "(shift+tab to cycle)" | tail -n 1 || true)"
	if [[ -z "$line" ]]; then
		line="$(printf '%s\n' "$pane" | grep -F -- '? for shortcuts' | tail -n 1 || true)"
	fi
	printf '%s\n' "$line" | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//'
}

rs_obs_mode() {
	local window="$1" line
	line="$(rs_mode_text "$window")"
	if [[ -z "$line" ]]; then
		rs_obs "$window: bottom bar shows no permission-mode text"
	else
		rs_obs "$window: bottom bar mode text: $line"
	fi
}

# rs_pane_idle waits until the window is back at its composer and no turn is
# running; the "esc to interrupt" hint marks a running turn, which the readiness
# criteria alone do not exclude.
rs_pane_idle() {
	local window="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) pane="" st=0
	while ((SECONDS < deadline)); do
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		st="$(rs_grep_status "$pane" -qF -- 'esc to interrupt')"
		# Only a search that ran and found the hint absent, with a ready
		# composer, means idle; a match or a search that could not run leaves the
		# turn possibly running.
		if ((st == 1)) && rs_ready_criterion "$pane" >/dev/null; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}
