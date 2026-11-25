#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# reply.sh asks whether a real session answers an Agent address with
# mcp__peer__send_message. A simulated Agent on the peer host sends the same
# three messages to a sonnet and a haiku session; the script records evidence
# and does not judge.

readonly SENDER=api-worker
readonly SENDER_ADDRESS="api-worker@hostA"
readonly REGISTER_WAIT=30
readonly SENDER_WAIT=30
readonly TURN_WAIT=120
readonly IDLE_WAIT=15

messages=(
	"Hi, can you tell me which directory you are working in? Please reply."
	"Quick question: what is 2+2? Answer me directly."
	"Ping. Reply with pong."
)

rs_reply_state() {
	local name="$1"
	local line=""
	line="$("$R/run" rsprobe status 2>/dev/null | grep -F " $name@hostB " | head -n1 || true)"
	printf '%s\n' "$line" | sed -n 's/.*status=\([a-z]*\).*/\1/p'
}

rs_reply_wait_registered() {
	local name="$1"
	local deadline=$((SECONDS + REGISTER_WAIT))
	local socket=""
	while ((SECONDS < deadline)); do
		socket="$(rs_socket_of "$name" 2>/dev/null || true)"
		if [[ -n "$socket" ]] && [[ -n "$(rs_reply_state "$name")" ]]; then
			printf '%s' "$socket"
			return 0
		fi
		sleep 0.3
	done
	return 1
}

# rs_reply_session_id prints the Claude Code session id stored in the session
# record; it is the marker that scopes a transcript excerpt to one session.
rs_reply_session_id() {
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

# rs_reply_dump_registry keeps the raw session registry in $OUT when a record
# carries no sessionId; the missing field is itself the observation, so the
# registry is stored as written.
rs_reply_dump_registry() {
	local name="$1"
	local file=""
	local dest="$OUT/registry-$name"
	mkdir -p "$dest"
	for file in "$R"/cfg/sessions/*.json; do
		if [[ ! -e "$file" ]]; then
			continue
		fi
		cp "$file" "$dest/$(basename "$file")"
	done
}

rs_reply_recv_count() {
	docker compose -f test/realsession/compose.yaml exec -T hosta \
		wc -l "/rs/$SENDER.recv.jsonl" | awk '{print $1}'
}

rs_reply_recv_since() {
	local from="$1"
	docker compose -f test/realsession/compose.yaml exec -T hosta \
		tail -n "+$from" "/rs/$SENDER.recv.jsonl"
}

# rs_reply_wait_turn waits until the sender received a reply or until the turn
# the message started has visibly ended; a session that never becomes busy and
# never answers times out.
rs_reply_wait_turn() {
	local name="$1"
	local before="$2"
	local deadline=$((SECONDS + TURN_WAIT))
	local state=""
	local now=0
	local saw_busy=0
	while ((SECONDS < deadline)); do
		state="$(rs_reply_state "$name")"
		now="$(rs_reply_recv_count)"
		if ((now > before)); then
			return 0
		fi
		case "$state" in
		busy | waiting | shell) saw_busy=1 ;;
		esac
		if ((saw_busy == 1)) && [[ "$state" == "idle" ]]; then
			return 0
		fi
		sleep 0.3
	done
	return 1
}

# rs_reply_wait_idle waits until the session has been idle for one second, so
# the next message does not race the end of the previous turn.
rs_reply_wait_idle() {
	local name="$1"
	local deadline=$((SECONDS + IDLE_WAIT))
	local state=""
	local idle=0
	while ((SECONDS < deadline)); do
		state="$(rs_reply_state "$name")"
		if [[ "$state" == "idle" ]]; then
			idle=$((idle + 1))
			if ((idle >= 5)); then
				return 0
			fi
		else
			idle=0
		fi
		sleep 0.2
	done
	return 1
}

rs_reply_wait_sender() {
	local deadline=$((SECONDS + SENDER_WAIT))
	local reply=""
	while ((SECONDS < deadline)); do
		reply="$("$R/run" rsprobe agent call --ctl "$R/ctl/pairer-b.sock" list_agents '{}' 2>/dev/null || true)"
		if printf '%s' "$reply" | jq -e --arg addr "$SENDER_ADDRESS" \
			'(.text | fromjson? | .agents // []) | any(.address == $addr)' >/dev/null 2>&1; then
			printf '%s\n' "$reply" >"$OUT/sender-visible.txt"
			return 0
		fi
		sleep 0.5
	done
	return 1
}

# rs_reply_turn sends one message from the peer Agent, waits for the turn, and
# records that turn's new transcript records, tool calls and replies.
rs_reply_turn() {
	local name="$1"
	local sid="$2"
	local turn="$3"
	local records="$OUT/$name.records.jsonl"
	local delta="$OUT/$name-turn$turn.jsonl"
	local recvfile="$OUT/$name-turn$turn.recv.jsonl"
	local before=0
	local prev=0
	local after=0
	local args=""
	local reply=""
	local tool_rows=""
	local tools="none"
	local from_names="-"
	local received=0
	local tool=""
	local to=""
	local match="no"

	rs_excerpt "$sid" "$name.records.jsonl"
	prev="$(wc -l <"$records")"
	before="$(rs_reply_recv_count)"

	args="$(jq -nc --arg to "$name@hostB" --arg message "${messages[turn - 1]}" '{to: $to, message: $message}')"
	if ! reply="$(rs_hosta_call "$SENDER" send_message "$args")"; then
		rs_snap "$name" "fail-$name-turn$turn.txt" || true
		rs_obs "$name turn $turn: sending to $name@hostB failed"
		return 1
	fi
	printf '%s\n' "$reply" >>"$OUT/calls.jsonl"

	if ! rs_reply_wait_turn "$name" "$before"; then
		rs_snap "$name" "fail-$name-turn$turn.txt" || true
		rs_obs "$name turn $turn: no reply and no visible turn end within ${TURN_WAIT}s"
		return 1
	fi
	rs_reply_wait_idle "$name" || true

	rs_excerpt "$sid" "$name.records.jsonl"
	tail -n "+$((prev + 1))" "$records" >"$delta"

	after="$(rs_reply_recv_count)"
	received=$((after - before))
	if ((received > 0)); then
		rs_reply_recv_since "$((before + 1))" >"$recvfile"
		from_names="$(jq -r '.from_name' "$recvfile" | paste -sd, -)"
	fi

	tool_rows="$(jq -r '(.message.content // empty) | if type == "array" then .[] else empty end | select(.type == "tool_use") | [.name, ((.input.to // "") | tostring)] | @tsv' "$delta" 2>/dev/null || true)"
	if [[ -n "$tool_rows" ]]; then
		tools="$(printf '%s\n' "$tool_rows" | cut -f1 | paste -sd, -)"
		while IFS=$'\t' read -r tool to; do
			match="no"
			if [[ "$to" == "$SENDER_ADDRESS" ]]; then
				match="yes"
			fi
			printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
				"$name" "$turn" "$tool" "$to" "$match" "$received" "$from_names" >>"$OUT/turns.tsv"
		done <<<"$tool_rows"
	else
		printf '%s\t%s\tnone\t-\tno\t%s\t%s\n' \
			"$name" "$turn" "$received" "$from_names" >>"$OUT/turns.tsv"
	fi

	rs_obs "$name turn $turn: tools=${tools} replies=${received} from=${from_names}"
	return 0
}

rs_hosta_up
rs_pair_hosta
rs_hosta_agent "$SENDER"

if ! rs_reply_wait_sender; then
	rs_obs "$SENDER_ADDRESS never became visible to the runner Broker"
	echo "reply.sh: $SENDER_ADDRESS is not visible; is the link up?" >&2
	exit 1
fi
rs_obs "sender $SENDER_ADDRESS is visible through pairer-b"

rs_tmux_start
rs_window reply-s claude --model sonnet -n reply-s --permission-mode default --allowedTools mcp__peer__send_message
rs_window reply-h claude --model haiku -n reply-h --permission-mode default --allowedTools mcp__peer__send_message

printf 'session\tturn\ttool\tto\tto_matches\treplies\tfrom\n' >"$OUT/turns.tsv"

for name in reply-s reply-h; do
	if ! socket="$(rs_reply_wait_registered "$name")"; then
		rs_snap "$name" "fail-$name-register.txt" || true
		rs_obs "$name did not register within ${REGISTER_WAIT}s"
		echo "reply.sh: $name did not register" >&2
		exit 1
	fi
	rs_obs "$name registered, inbox socket $socket"
	if ! rs_wait_ready "$name" 30; then
		rs_snap "$name" "fail-$name-prompt.txt" || true
		rs_obs "$name did not reach its input prompt within 30s"
		echo "reply.sh: $name did not reach its input prompt" >&2
		exit 1
	fi
	rs_obs_mode "$name"
done

"$R/run" rsprobe status >"$OUT/status-registered.txt"

for name in reply-s reply-h; do
	if ! sid="$(rs_reply_session_id "$name")"; then
		rs_reply_dump_registry "$name"
		rs_obs "$name has no sessionId in its session record; raw registry stored in registry-$name/"
		echo "reply.sh: $name has no sessionId; cannot scope transcript excerpts" >&2
		exit 1
	fi
	rs_obs "$name uses session record $sid"
	for turn in 1 2 3; do
		if ! rs_reply_turn "$name" "$sid" "$turn"; then
			rs_obs "$name stopped after turn $turn"
			break
		fi
	done
done

rs_hosta_recv "$SENDER"
"$R/run" rsprobe status >"$OUT/status-final.txt"
rs_snap reply-s pane-reply-s.txt
rs_snap reply-h pane-reply-h.txt
rs_obs "reply probe finished"

# The probe types no prompt, since the peer host's sender starts each turn, so
# the second message's text must appear as a user record and the counter must
# show at least one model call.
rs_require_model_turn 'Quick question: what is 2+2?' 1
