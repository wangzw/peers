#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR

# rs_initiate_tool_rows prints one NAME<TAB>TO line per distinct tool_use block
# in the assistant records of FILE. Only assistant records are read: the
# api-request-blob records replay the same calls.
rs_initiate_tool_rows() {
	local file="$1"
	jq -Rr '
		fromjson?
		| select(type == "object" and .type == "assistant")
		| (.message.content // empty)
		| if type == "array" then .[] else empty end
		| select(type == "object" and .type == "tool_use")
		| [.id, .name, (((.input? // {}) | if type == "object" then (.to // "-") else "-" end) | tostring)]
		| @tsv
	' "$file" 2>/dev/null |
		awk -F '\t' '!seen[$1]++ { print $2 "\t" $3 }'
}

# rs_initiate_marker_texts prints the text of each received-message record in
# FILE that holds MARKER. A received message is a user record; the queue and
# api-request-blob records hold the same text again.
rs_initiate_marker_texts() {
	local file="$1" marker="$2"
	jq -Rr --arg marker "$marker" '
		fromjson?
		| select(type == "object" and .type == "user")
		| ((.message.content? // empty)
			| if type == "string" then .
			  elif type == "array" then [.[] | select(type == "object") | (.text // "")] | join("\n")
			  else "" end)
		| select(contains($marker))
	' "$file" 2>/dev/null || true
}

# rs_initiate_extract_dir replays the extraction over a saved transcript
# directory, so a downloaded artifact is re-read without a live session. Sender
# files are sender-WINDOW-FORM[-RUN].jsonl and the target file is
# TARGET.records.jsonl.
rs_initiate_extract_dir() {
	local dir="$1" target="$2"
	local file base rest run window form marker tools texts from
	printf 'session\tform\trun\ttools\ttarget_has_marker\tfrom_name\n'
	for file in "$dir"/sender-*.jsonl; do
		[[ -e "$file" ]] || continue
		base="$(basename "$file" .jsonl)"
		base="${base#sender-}"
		rest="$base"
		run="-"
		if [[ "$rest" =~ ^(.+)-([0-9]+)$ ]]; then
			rest="${BASH_REMATCH[1]}"
			run="${BASH_REMATCH[2]}"
		fi
		form="${rest##*-}"
		window="${rest%-*}"
		marker="rs-initiate-$window-$form"
		if [[ "$run" != "-" ]]; then
			marker="$marker-$run"
		fi
		tools="$(rs_initiate_tool_rows "$file" | awk -F '\t' 'BEGIN { sep = "" } { printf "%s%s->%s", sep, $1, $2; sep = ";" }')"
		if [[ -z "$tools" ]]; then
			tools="none"
		fi
		texts="$(rs_initiate_marker_texts "$dir/$target.records.jsonl" "$marker")"
		if [[ -z "$texts" ]]; then
			printf '%s\t%s\t%s\t%s\tno\t-\n' "$window" "$form" "$run" "$tools"
			continue
		fi
		from="$(printf '%s' "$texts" | jq -Rrs 'capture("from-name=\"(?<from>[^\"]*)\"").from' 2>/dev/null || true)"
		printf '%s\t%s\t%s\t%s\tyes\t%s\n' "$window" "$form" "$run" "$tools" "${from:--}"
	done
}

if [[ "${1:-}" == "--extract" ]]; then
	if [[ -z "${2:-}" || -z "${3:-}" ]]; then
		echo "usage: $0 --extract <transcript-dir> <target-session>" >&2
		exit 2
	fi
	rs_initiate_extract_dir "$2" "$3"
	exit 0
fi

# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# initiate.sh asks which tool a real session calls to start a conversation with
# a session of the same account on the same host that is both registered with
# peers and listed by Claude Code's built-in session list. The probe records
# the tool calls, the target's transcript and the sender name the target sees,
# and does not judge the choice.

readonly TARGET=init-target
readonly TARGET_ADDRESS="$TARGET@hostB"
readonly TARGET_PROMPT='When a cross-session message arrives, do not reply and do not call any tool.'
readonly SENDER_TOOLS='SendMessage,ListAgents,mcp__peer__send_message,mcp__peer__list_agents'
readonly REGISTER_WAIT=45
readonly READY_WAIT=60
readonly TURN_WAIT=180
readonly CLEAR_WAIT=30
readonly MARKER_WAIT=60

# initiate_state prints the status of one agent in the runner Broker's table;
# an absent agent prints nothing.
initiate_state() {
	local name="$1"
	local line=""
	line="$("$R/run" rsprobe status 2>/dev/null | grep -F " $name@hostB " | head -n1 || true)"
	printf '%s\n' "$line" | sed -n 's/.*status=\([a-z]*\).*/\1/p'
}

# initiate_wait_registered waits until the session has a registry record with an
# inbox socket and appears in the Broker's agent table, then prints the socket.
initiate_wait_registered() {
	local name="$1"
	local deadline=$((SECONDS + REGISTER_WAIT))
	local socket=""
	while ((SECONDS < deadline)); do
		socket="$(rs_socket_of "$name" 2>/dev/null || true)"
		if [[ -n "$socket" ]] && [[ -n "$(initiate_state "$name")" ]]; then
			printf '%s' "$socket"
			return 0
		fi
		sleep 0.3
	done
	return 1
}

# initiate_session_id_of prints the Claude Code session id stored in the session
# record; it scopes transcript excerpts to one session.
initiate_session_id_of() {
	local name="$1"
	local file=""
	local id=""
	for file in "$R"/cfg/sessions/*.json; do
		if [[ ! -e "$file" ]]; then
			continue
		fi
		id="$(jq -r --arg name "$name" 'select(.name == $name) | .sessionId // empty' "$file" 2>/dev/null || true)"
		if [[ -n "$id" ]]; then
			printf '%s' "$id"
			return 0
		fi
	done
	return 1
}

# initiate_dump_registry keeps the raw session registry in $OUT, so the record
# every session registered with is evidence rather than a summary.
initiate_dump_registry() {
	local file=""
	mkdir -p "$OUT/registry"
	for file in "$R"/cfg/sessions/*.json; do
		if [[ ! -e "$file" ]]; then
			continue
		fi
		cp "$file" "$OUT/registry/$(basename "$file")"
	done
}

# initiate_clear runs /clear and waits for the session record to carry the new
# session id, which Claude Code writes to the registry at once.
initiate_clear() {
	local window="$1"
	local old_sid="$2"
	local deadline=$((SECONDS + CLEAR_WAIT))
	local sid=""
	if ! rs_wait_turn_idle "$window" 20; then
		rs_obs "$window: the previous turn was still running before /clear"
		return 1
	fi
	rs_keys "$window" '/clear' Enter
	while ((SECONDS < deadline)); do
		sid="$(initiate_session_id_of "$window" || true)"
		if [[ -n "$sid" && "$sid" != "$old_sid" ]]; then
			rs_obs "$window: /clear started a new conversation with session $sid"
			printf '%s' "$sid"
			return 0
		fi
		sleep 0.3
	done
	return 1
}

# initiate_wait_turn waits until the new records after FROM hold the marker and
# an assistant record follows it, then until the pane shows no running turn.
initiate_wait_turn() {
	local window="$1"
	local sid="$2"
	local marker="$3"
	local from="$4"
	local seconds="$5"
	local deadline=$((SECONDS + seconds))
	local records="$OUT/$window.records.jsonl"
	local newfile="$WORK/$window-new.jsonl"
	local marker_line=""
	while ((SECONDS < deadline)); do
		rs_excerpt "$sid" "$window.records.jsonl"
		tail -n "+$((from + 1))" "$records" >"$newfile" 2>/dev/null || true
		marker_line="$(awk -v m="$marker" 'index($0, m) { print NR; exit }' "$newfile")"
		if [[ -n "$marker_line" ]] &&
			tail -n "+$marker_line" "$newfile" | jq -Rre 'fromjson? | select(type == "object" and .type == "assistant")' >/dev/null 2>&1; then
			if rs_wait_turn_idle "$window" 20; then
				return 0
			fi
		fi
		sleep 0.5
	done
	return 1
}

# initiate_target_texts refreshes the target's transcript and prints the text of
# every received message that holds the marker.
initiate_target_texts() {
	local marker="$1"
	rs_excerpt "$target_sid" "$TARGET.records.jsonl"
	rs_initiate_marker_texts "$OUT/${TARGET}.records.jsonl" "$marker"
}

# initiate_wait_target waits until the target's transcript holds the marker and
# prints the matching text; a message that never arrives returns 1.
initiate_wait_target() {
	local marker="$1"
	local seconds="$2"
	local deadline=$((SECONDS + seconds))
	local text=""
	while ((SECONDS < deadline)); do
		text="$(initiate_target_texts "$marker")"
		if [[ -n "$text" ]]; then
			printf '%s\n' "$text"
			return 0
		fi
		sleep 0.5
	done
	return 1
}

# initiate_turn types one prompt, records the sender's tool calls in call order,
# and records whether and how the target's transcript shows the message.
initiate_turn() {
	local window="$1"
	local model="$2"
	local form="$3"
	local run="$4"
	local sid="$5"
	local marker="$6"
	local prompt="$7"
	local tag="$form-$run"
	local records="$OUT/$window.records.jsonl"
	local delta="$OUT/sender-$window-$tag.jsonl"
	local prev=0
	local tool_rows=""
	local tools="none"
	local target_text=""
	local has_marker="no"
	local from_name="-"

	rs_wait_turn_idle "$TARGET" 30 || true
	rs_excerpt "$sid" "$window.records.jsonl"
	prev="$(wc -l <"$records" | tr -d ' ')"

	if ! rs_send_prompt "$window" "$prompt" 'Send the message' 90; then
		rs_snap "$window" "fail-$window-$tag.txt" || true
		rs_obs "$window form $form run $run: the composer did not echo the prompt"
		return 1
	fi

	if ! initiate_wait_turn "$window" "$sid" "$marker" "$prev" "$TURN_WAIT"; then
		rs_snap "$window" "fail-$window-$tag.txt" || true
		rs_obs "$window form $form run $run: no assistant record after the prompt within ${TURN_WAIT}s"
		return 1
	fi

	rs_excerpt "$sid" "$window.records.jsonl"
	tail -n "+$((prev + 1))" "$records" >"$delta"

	tool_rows="$(rs_initiate_tool_rows "$delta")"
	if [[ -n "$tool_rows" ]]; then
		tools="$(printf '%s\n' "$tool_rows" | awk -F'\t' 'BEGIN { sep = "" } { printf "%s%s->%s", sep, $1, $2; sep = ";" }')"
	fi

	if target_text="$(initiate_wait_target "$marker" "$MARKER_WAIT")"; then
		has_marker="yes"
		from_name="$(printf '%s' "$target_text" | jq -Rrs 'capture("from-name=\"(?<from>[^\"]*)\"").from' 2>/dev/null || true)"
		if [[ -z "$from_name" ]]; then
			from_name="-"
		fi
		printf '%s\n' "$target_text" >"$OUT/target-$window-$tag.txt"
	fi

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$window" "$model" "$form" "$run" "$tools" "$has_marker" "$from_name" >>"$OUT/turns.tsv"
	rs_snap "$window" "pane-$window-$tag.txt"
	rs_snap "$TARGET" "pane-target-$window-$tag.txt"
	rs_obs "$window form $form run $run: tools=${tools} target_marker=${has_marker} from=${from_name}"
	return 0
}

rs_tmux_start
rs_window "$TARGET" claude --model haiku -n "$TARGET" --permission-mode default --append-system-prompt "$TARGET_PROMPT"
for spec in "init-sonnet sonnet" "init-haiku haiku"; do
	name="${spec%% *}"
	model="${spec#* }"
	rs_window "$name" claude --model "$model" -n "$name" --permission-mode default --allowedTools "$SENDER_TOOLS"
done

for name in "$TARGET" init-sonnet init-haiku; do
	if ! socket="$(initiate_wait_registered "$name")"; then
		rs_snap "$name" "fail-$name-register.txt" || true
		rs_obs "$name did not register within ${REGISTER_WAIT}s"
		echo "initiate.sh: $name did not register" >&2
		exit 1
	fi
	rs_obs "$name registered, inbox socket $socket"
	if ! rs_wait_ready "$name" "$READY_WAIT"; then
		rs_snap "$name" "fail-$name-prompt.txt" || true
		rs_obs "$name did not reach its input prompt within ${READY_WAIT}s"
		echo "initiate.sh: $name did not reach its input prompt" >&2
		exit 1
	fi
	rs_obs_mode "$name"
done

# Record the agent table and the session registry before the first prompt, so a
# missing registration is evidence rather than a later unexplained absence.
"$R/run" rsprobe status >"$OUT/status-registered.txt"
initiate_dump_registry
printf 'session\tsocket\tsession_id\n' >"$OUT/sessions.tsv"
for name in "$TARGET" init-sonnet init-haiku; do
	if ! grep -qF " $name@hostB " "$OUT/status-registered.txt"; then
		rs_obs "$name is absent from the agent table snapshot"
		echo "initiate.sh: $name is absent from the agent table" >&2
		exit 1
	fi
	printf '%s\t%s\t%s\n' \
		"$name" "$(rs_socket_of "$name" || true)" "$(initiate_session_id_of "$name" || true)" >>"$OUT/sessions.tsv"
done

if ! target_sid="$(initiate_session_id_of "$TARGET")"; then
	rs_obs "$TARGET has no sessionId in its session record"
	echo "initiate.sh: $TARGET has no sessionId; cannot scope transcript excerpts" >&2
	exit 1
fi
rs_obs "$TARGET uses session record $target_sid"

printf 'session\tmodel\tform\trun\ttools\ttarget_has_marker\tfrom_name\n' >"$OUT/turns.tsv"

for spec in "init-sonnet sonnet" "init-haiku haiku"; do
	name="${spec%% *}"
	model="${spec#* }"
	if ! sid="$(initiate_session_id_of "$name")"; then
		rs_obs "$name has no sessionId in its session record"
		echo "initiate.sh: $name has no sessionId; cannot scope transcript excerpts" >&2
		exit 1
	fi
	rs_obs "$name uses session record $sid"
	fresh=0
	for form in name addr; do
		for run in 1 2 3; do
			if ((fresh == 1)); then
				if ! sid="$(initiate_clear "$name" "$sid")"; then
					rs_snap "$name" "fail-$name-clear.txt" || true
					rs_obs "$name: /clear did not confirm a fresh conversation within ${CLEAR_WAIT}s"
					echo "initiate.sh: $name did not start a fresh conversation" >&2
					exit 1
				fi
				if ! rs_wait_ready "$name" 30; then
					rs_snap "$name" "fail-$name-clear-prompt.txt" || true
					rs_obs "$name did not reach its input prompt after /clear"
					echo "initiate.sh: $name did not reach its input prompt after /clear" >&2
					exit 1
				fi
			fi
			fresh=1
			marker="rs-initiate-$name-$form-$run"
			if [[ "$form" == "name" ]]; then
				prompt="Send the message \"$marker\" to $TARGET."
			else
				prompt="Send the message \"$marker\" to $TARGET_ADDRESS."
			fi
			if ! initiate_turn "$name" "$model" "$form" "$run" "$sid" "$marker" "$prompt"; then
				rs_obs "$name stopped at form $form run $run"
				echo "initiate.sh: $name did not complete form $form run $run" >&2
				exit 1
			fi
		done
	done
done

"$R/run" rsprobe status >"$OUT/status-final.txt"
for name in "$TARGET" init-sonnet init-haiku; do
	rs_snap "$name" "pane-$name-final.txt"
done
rs_obs "initiate probe finished"

rs_require_model_turn 'rs-initiate-init-haiku-addr-3' 12
