#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# inbound.sh answers the Roadmap's Items to verify section. The
# receiver's permission mode is the variable under test: an ordinary receiver
# delivers an undeclared sender's message directly, while a receiver started with
# --dangerously-skip-permissions holds it for per-message approval. The probe
# therefore runs both a default receiver and a bypass receiver, records each
# bottom-bar mode text, and compares their outcomes. The bypass session is the
# subject of this probe, not a way to skip approvals; the permissions probe
# still never starts one.
# It also gathers evidence for REQ-agent-messaging-015, 016, 021 and 024.

PROMPT='When a cross-session message arrives, reply with the single word ok and do nothing else. Do not call any tool unless the user explicitly asks.'

# The handshake-time dialog of the pinned Claude Code version. The first option
# is "Deny — drop it and tell the sender it was declined" and opens focused, so
# Enter denies; the second is "Deliver this message to Claude", reached with
# Down, so Down then Enter approves. Only the fragments needed to recognise the
# dialog and its two actions are matched here.
HANDSHAKE_TITLE='Held message from another session'
HANDSHAKE_APPROVE='Deliver this message to Claude'
HANDSHAKE_DENY='Deny'

settings_backup="$OUT/settings.before.json"
settings_touched=0
broker_pid=''
default_socket=''
default_session_id=''
bypass_socket=''
bypass_session_id=''
send_return_ms=''
capture_ms=''
dialog_at_return=no
default_dialog=no
default_peer_turn=no
default_origin=''
bypass_child_delivered=no
bypass_child_origin=''
depth13_outcome=''
depth11_outcome=''
hold_model_received=no
hold_sender_reply=no
hold_sender_delivery_result=no
hold_dialog=no
hold_notice_at_sender_done=no
hold_notice_after_enter=no
hold_sender_done_ms=''
hold_sender_done_iso=''
release_policy_accepts=no
release_model_received=no
refuse_send_ok=no
refuse_message_visible=no
idle_wake_turn=no

# now_ms and now_iso stamp the two ends of the REQ-agent-messaging-015 capture.
now_ms() { date +%s%3N; }
now_iso() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

# settings_restore restores the shared settings file and fails the run when its
# sha256 differs from the backup. It is registered with rs_on_exit, so the
# library runs it in order with the other cleanups and still captures the
# windows of a failed run.
settings_restore() {
	local ok=0 before after
	if ((settings_touched == 1)); then
		if ! cp "$settings_backup" "$R/cfg/settings.json"; then
			rs_obs "failed to restore settings.json"
			ok=1
		fi
		before="$(awk '{print $1}' "$OUT/settings.before.sha256")"
		after="$(sha256sum "$R/cfg/settings.json" | awk '{print $1}')"
		printf '%s\n' "$after" >"$OUT/settings.after.sha256"
		if [[ "$before" != "$after" ]]; then
			rs_obs "settings.json sha256 differs after restore: before=$before after=$after"
			ok=1
		else
			rs_obs "settings.json restored with sha256 $after"
		fi
	fi
	return "$ok"
}
rs_on_exit settings_restore

# wait_socket waits until the named session has registered its inbox socket.
wait_socket() {
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

# wait_socket_gone waits until the named session leaves the registry.
wait_socket_gone() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if ! rs_socket_of "$name" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# session_id_of prints the sessionId in the registry record named $1.
session_id_of() {
	local name="$1" file
	for file in "$R"/cfg/sessions/*.json; do
		[[ -e "$file" ]] || continue
		if jq -e --arg name "$name" '.name == $name' "$file" >/dev/null 2>&1; then
			jq -r '.sessionId // ""' "$file"
			return 0
		fi
	done
	return 1
}

# excerpt_session copies the records of one session through rs_excerpt. The
# marker is the registry's sessionId, so records of other sessions, such as the
# sender's own turn, cannot be mistaken for the target's.
excerpt_session() {
	local name="$1" file="$2" sid
	sid="$(session_id_of "$name" 2>/dev/null || true)"
	if [[ -z "$sid" ]]; then
		: >"$OUT/$file"
		: >"$OUT/$file.debug"
		return 1
	fi
	rs_excerpt "$sid" "$file"
}

# count_matches prints the number of fixed-string matches in a file.
count_matches() {
	local file="$1" pattern="$2" count
	count="$(grep -cF -- "$pattern" "$file" 2>/dev/null || true)"
	printf '%s\n' "${count:-0}"
}

# wait_session_marker polls one session's records for a fixed marker.
wait_session_marker() {
	local name="$1" marker="$2" file="$3" seconds="$4"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if excerpt_session "$name" "$file"; then
			if grep -qF -- "$marker" "$OUT/$file"; then
				return 0
			fi
		fi
		sleep 0.5
	done
	return 1
}

# has_peer_turn reports whether a record file holds the marker as a delivered
# peer turn. The hold notice and the queue entry both carry the message text, so
# a plain grep would mistake a held message for a delivered one; only a user
# record whose origin or turn origin is "peer" is a delivered turn.
has_peer_turn() {
	local file="$1" marker="$2"
	jq -Rre --arg marker "$marker" '
		fromjson?
		| select(type == "object" and .type == "user")
		| select((.turnOrigin // "") == "peer" or (.origin.kind? // "") == "peer")
		| (.message.content // empty)
		| if type == "string" then .
		  elif type == "array" then [.[] | (.text // "")] | join("\n")
		  else "" end
		| select(contains($marker))
	' "$file" >/dev/null 2>&1
}

# wait_sid_peer_turn polls one session's records for a delivered peer turn that
# contains marker. The caller passes the sessionId cached at startup, so a
# registry entry that changes or disappears later cannot make the poll miss a
# record that is already in the transcript.
wait_sid_peer_turn() {
	local sid="$1" marker="$2" file="$3" seconds="$4"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		rs_excerpt "$sid" "$file"
		if has_peer_turn "$OUT/$file" "$marker"; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}

# peer_turn_origin prints the sender fields a delivered peer turn recorded:
# from, verified pid and the claimed name. An own-child writer and a writer
# outside the session's process tree differ here, which is the evidence for how
# the receiver classified the writer.
peer_turn_origin() {
	local file="$1" marker="$2"
	jq -Rr --arg marker "$marker" '
		fromjson?
		| select(type == "object" and .type == "user")
		| select((.turnOrigin // "") == "peer" or (.origin.kind? // "") == "peer")
		| (.message.content // empty) as $content
		| (if ($content | type) == "string" then $content
		   elif ($content | type) == "array" then [$content[] | (.text // "")] | join("\n")
		   else "" end) as $text
		| select($text | contains($marker))
		| "from=\(.origin.from // "") pid=\(.origin.verifiedPeerPid // "") name=\(.origin.name // "")"
	' "$file" 2>/dev/null | tail -n 1
}

# wait_session_reply polls one session's records until an assistant message's
# trimmed text equals the expected string. The library's rs_wait_reply reads
# every session, so a short expected word such as "ok" could match another
# session's text or a substring like "broker"; this helper reads only the
# target session's transcript.
wait_session_reply() {
	local name="$1" expected="$2" file="$3" seconds="$4"
	local sid deadline src
	sid="$(session_id_of "$name" 2>/dev/null || true)"
	: >"$OUT/$file"
	[[ -n "$sid" ]] || return 1
	deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		: >"$OUT/$file"
		while IFS= read -r -d '' src; do
			if ! grep -qF -- "$sid" "$src" 2>/dev/null; then
				continue
			fi
			jq -Rrc --arg expected "$expected" '
				fromjson?
				| select(type == "object" and .type == "assistant")
				| (.message.content? // empty) as $content
				| (if ($content | type) == "string" then $content
				   elif ($content | type) == "array" then
					[$content[] | select(.type == "text") | .text] | join("\n")
				   else "" end) as $text
				| select(($text | gsub("^\\s+|\\s+$"; "")) == $expected)
			' "$src" >>"$OUT/$file" 2>/dev/null || true
		done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
		if [[ -s "$OUT/$file" ]]; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}

# usage_total prints the environment's model-call counter, or nothing when it
# cannot be read.
usage_total() {
	"$R/run" rsprobe env usage --root "$R" 2>/dev/null |
		sed -n 's/^total //p' | tail -n 1
}

# find_broker_pid prints the pid of the user Broker rs_start_broker
# started. It runs as "<exe> service run"; the executable and the first two
# arguments are matched instead of a command-line substring, so another
# Broker's arguments cannot make the choice ambiguous.
find_broker_pid() {
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

# write_broker_chain records the Broker and each ancestor up to pid 1.
write_broker_chain() {
	local pid="$1" out="$OUT/broker-chain.txt" chain_pid depth=0
	chain_pid="$pid"
	: >"$out"
	while [[ -n "$chain_pid" ]] && ((chain_pid > 1)) && ((depth < 32)); do
		ps -o pid=,ppid=,sid=,uid=,comm=,args= -p "$chain_pid" >>"$out" 2>/dev/null || true
		chain_pid="$(ps -o ppid= -p "$chain_pid" 2>/dev/null | tr -d ' ')"
		depth=$((depth + 1))
	done
	ps -o pid=,ppid=,sid=,uid=,comm=,args= -p 1 >>"$out" 2>/dev/null || true
}

# depth_frame_command writes a helper into $WORK and prints a fixed-size
# command that runs it. The helper recurses by invoking itself, so each level's
# argv stays the same size and the writer ends up DEPTH shell processes below
# the session. A "sh -c 'sh -c ...'" chain grows without bound because every
# level re-escapes the previous level's backslashes: the 13-level command is
# over 128 KiB, past the kernel's per-argument limit. The marker is assembled
# inside the helper, so the typed command never contains "depth-N".
depth_frame_command() {
	local depth="$1" number="$2" socket="$3" helper="$WORK/depth-frame.sh"
	cat >"$helper" <<EOF
#!/bin/sh
depth=\$1
if [ "\$depth" -gt 1 ]; then
	"\$0" "\$((depth - 1))" "\$2" "\$3"
	exit \$?
fi
exec "$R/bin/rsprobe" frame --socket "\$2" --from-name depth@x --body "depth-\$3"
EOF
	chmod +x "$helper"
	printf 'sh %q %s %q %q' "$helper" "$depth" "$socket" "$number"
}

# wait_hold_or_record reports whether a depth probe was held by the handshake
# dialog or accepted into the conversation. It prints one word and always
# returns 0 so the caller can record the outcome without tripping set -e.
wait_hold_or_record() {
	local window="$1" sid="$2" marker="$3" file="$4" seconds="$5"
	local deadline=$((SECONDS + seconds)) pane
	while ((SECONDS < deadline)); do
		pane="$(tmux -L "$TMUX_SERVER" capture-pane -p -t "$window" 2>/dev/null || true)"
		if [[ "$(rs_grep_status "$pane" -qF -- "$HANDSHAKE_TITLE")" == 0 ]]; then
			printf 'held\n'
			return 0
		fi
		rs_excerpt "$sid" "$file"
		if has_peer_turn "$OUT/$file" "$marker"; then
			printf 'accepted\n'
			return 0
		fi
		sleep 1
	done
	printf 'timeout\n'
}

# write_inbound_policy replaces the shared settings file from the untouched
# backup, so each policy value is applied to the original content.
write_inbound_policy() {
	local value="$1"
	if ! jq --arg value "$value" '.crossSessionInbound = $value' "$settings_backup" >"$WORK/settings.$value.json"; then
		rs_obs "jq failed to build settings with crossSessionInbound=$value"
		return 1
	fi
	cp "$WORK/settings.$value.json" "$R/cfg/settings.json"
	rs_obs "settings.json crossSessionInbound=$value"
}

rs_tmux_start

# --- step 1: Broker process relation (REQ-agent-messaging-024) --------------
#
# The Broker is started by rs_start_broker before any session exists, so it is
# not a descendant of a session. The environment and parent chain recorded here
# show that relation, which is what makes the receiver's inbound control apply
# to messages the Broker writes.

if ! "$R/run" rsprobe env check --root "$R" >"$OUT/env-check.txt" 2>&1; then
	rs_obs "rsprobe env check failed; another Broker may hold the account lock"
	exit 1
fi

rs_window inb-first claude --model haiku -n inb-first \
	--permission-mode default \
	--append-system-prompt "$PROMPT" \
	--allowedTools mcp__peer__list_agents \
	--debug-file "$R/debug/inb-first.debug"
if ! rs_wait_ready inb-first 45; then
	rs_obs "inb-first did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode inb-first
if ! wait_socket inb-first 30; then
	rs_obs "inb-first did not register an inbox socket within 30 seconds"
	exit 1
fi

rs_send_prompt inb-first 'Call mcp__peer__list_agents and show the raw result.' 'Call mcp__peer__list_agents' 60
if ! rs_wait_pane inb-first 'inb-first@hostB' 90; then
	rs_obs "the list_agents call in inb-first did not show an Agent row within 90 seconds"
fi
rs_snap inb-first step1-list-agents.txt
rs_excerpt 'inb-first@hostB' step1-list-agents-record.jsonl

broker_pid="$(find_broker_pid || true)"
if [[ -z "$broker_pid" ]]; then
	rs_obs "no peers service run process was found after inb-first started"
	exit 1
fi
ps -o pid=,ppid=,sid=,uid=,comm=,args= -p "$broker_pid" >"$OUT/broker-ps.txt" 2>/dev/null || true
write_broker_chain "$broker_pid"
rs_proc_vars "$broker_pid" CLAUDE_CODE_MESSAGING_SOCKET >"$OUT/broker-env.txt"
if grep -qi 'claude' "$OUT/broker-chain.txt"; then
	rs_obs "step 1: a claude process appears on the Broker's parent chain"
	exit 1
fi
rs_obs "step 1: the Broker's parent chain contains no claude process"
case "$(<"$OUT/broker-env.txt")" in
'CLAUDE_CODE_MESSAGING_SOCKET=')
	rs_obs "step 1: the Broker environment has an empty CLAUDE_CODE_MESSAGING_SOCKET"
	;;
'CLAUDE_CODE_MESSAGING_SOCKET absent')
	rs_obs "step 1: the Broker environment has no CLAUDE_CODE_MESSAGING_SOCKET"
	;;
*)
	rs_obs "step 1: the Broker environment carries a CLAUDE_CODE_MESSAGING_SOCKET"
	exit 1
	;;
esac

rs_agent inb-sender

# --- step 2: permission-mode classes and writer ancestry (roadmap 84-86) ---

# The receiver's permission mode is the variable under test. A receiver that
# prompts for permissions delivers an undeclared sender's message directly; a
# receiver started with bypassPermissions holds it for per-message approval
# (REF-claude-code-cross-session-messaging, ADR-adr-inbox-delivery-without-
# auth-token). Both receivers get the same frame kinds so the outcomes can be
# compared, and each bottom-bar mode text is recorded. The permissions probe
# still never starts a bypass session; here the mode is the subject.

# 2-default: an ordinary receiver delivers directly.
rs_window inb-default claude --model haiku -n inb-default \
	--permission-mode default \
	--append-system-prompt "$PROMPT" \
	--debug-file "$R/debug/inb-default.debug"
if ! rs_wait_ready inb-default 45; then
	rs_obs "inb-default did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode inb-default
if ! wait_socket inb-default 30; then
	rs_obs "inb-default did not register an inbox socket within 30 seconds"
	exit 1
fi
default_socket="$(rs_socket_of inb-default)"
default_session_id="$(session_id_of inb-default 2>/dev/null || true)"
if [[ -z "$default_session_id" ]]; then
	rs_obs "inb-default has no sessionId in the registry"
	exit 1
fi

# 2a-default: a frame written by a descendant is trusted and starts a turn.
rs_pane_idle inb-default 30 || true
# The body is assembled by the shell so the typed ! command is not itself a
# record that carries "from-descendant-default".
rs_send_shell inb-default "$R/bin/rsprobe frame --socket $default_socket --from-name child@x --body \"\$(printf 'from-%s' descendant-default)\"" '--from-name child@x' 60
if wait_sid_peer_turn "$default_session_id" from-descendant-default step2a-default-descendant-record.jsonl 60; then
	rs_obs "2-default a: a frame from a descendant reached the conversation"
else
	rs_obs "2-default a: from-descendant-default did not reach the conversation within 60 seconds"
fi
rs_snap inb-default step2a-default-descendant.txt

# 2b-default: a Broker-written frame is delivered directly, with no dialog.
rs_pane_idle inb-default 30 || true
if rs_call inb-sender send_message '{"to":"inb-default@hostB","message":"from-broker-default"}' >"$OUT/send-2b-default.txt"; then
	rs_obs "2-default b: the simulated sender returned success"
else
	rs_obs "2-default b: inb-sender send_message from-broker-default failed"
fi
rs_snap inb-default default-direct-2b.txt
if grep -qF -- "$HANDSHAKE_TITLE" "$OUT/default-direct-2b.txt" ||
	grep -qF -- "$HANDSHAKE_APPROVE" "$OUT/default-direct-2b.txt"; then
	default_dialog=yes
fi
if wait_sid_peer_turn "$default_session_id" from-broker-default step2b-default-record.jsonl 60; then
	default_peer_turn=yes
	rs_obs "2-default b: the Broker frame was delivered directly"
else
	rs_obs "2-default b: from-broker-default did not reach the conversation within 60 seconds"
fi
default_origin="$(peer_turn_origin "$OUT/step2b-default-record.jsonl" 'from-broker-default' || true)"
{
	printf 'dialog_shown=%s\n' "$default_dialog"
	printf 'peer_turn=%s\n' "$default_peer_turn"
	printf 'origin=%s\n' "$default_origin"
} >"$OUT/step2b-default.txt"

# 2-bypass: a receiver started with bypassPermissions holds an undeclared
# sender's frame and opens the approval dialog. A frame from its own child is
# still delivered, because process evidence marks the writer as trusted.
rs_window inb-bypass claude --model haiku -n inb-bypass \
	--dangerously-skip-permissions \
	--append-system-prompt "$PROMPT" \
	--debug-file "$R/debug/inb-bypass.debug"
if ! rs_wait_ready inb-bypass 45; then
	rs_obs "inb-bypass did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode inb-bypass
if ! wait_socket inb-bypass 30; then
	rs_obs "inb-bypass did not register an inbox socket within 30 seconds"
	exit 1
fi
bypass_socket="$(rs_socket_of inb-bypass)"
bypass_session_id="$(session_id_of inb-bypass 2>/dev/null || true)"
if [[ -z "$bypass_session_id" ]]; then
	rs_obs "inb-bypass has no sessionId in the registry"
	exit 1
fi

# 2a-bypass: the own-child frame is delivered even in bypass mode.
rs_pane_idle inb-bypass 30 || true
rs_send_shell inb-bypass "$R/bin/rsprobe frame --socket $bypass_socket --from-name child@x --body \"\$(printf 'from-%s' descendant-bypass)\"" '--from-name child@x' 60
if wait_sid_peer_turn "$bypass_session_id" from-descendant-bypass step2a-bypass-descendant-record.jsonl 60; then
	bypass_child_delivered=yes
	rs_obs "2-bypass a: a frame from a descendant reached the conversation"
else
	rs_obs "2-bypass a: from-descendant-bypass did not reach the conversation within 60 seconds"
fi
bypass_child_origin="$(peer_turn_origin "$OUT/step2a-bypass-descendant-record.jsonl" 'from-descendant-bypass' || true)"
rs_snap inb-bypass step2a-bypass-descendant.txt

# 2b-bypass (REQ-agent-messaging-015): the Broker frame is held by the
# handshake dialog. The capture runs immediately after the send call returns,
# and both timestamps are recorded.
rs_pane_idle inb-bypass 30 || true
if rs_call inb-sender send_message '{"to":"inb-bypass@hostB","message":"from-broker"}' >"$OUT/send-2b.txt"; then
	send_return_ms="$(now_ms)"
	send_return_iso="$(now_iso)"
else
	rs_obs "2b: inb-sender send_message from-broker failed"
	send_return_ms="$(now_ms)"
	send_return_iso="$(now_iso)"
fi
rs_snap inb-bypass handshake-held-2b.txt
capture_ms="$(now_ms)"
capture_iso="$(now_iso)"
if grep -qF -- "$HANDSHAKE_TITLE" "$OUT/handshake-held-2b.txt" &&
	grep -qF -- "$HANDSHAKE_DENY" "$OUT/handshake-held-2b.txt" &&
	grep -qF -- 'from-broker' "$OUT/handshake-held-2b.txt"; then
	dialog_at_return=yes
fi
{
	printf 'send_returned_at=%s\n' "$send_return_iso"
	printf 'send_return_ms=%s\n' "$send_return_ms"
	printf 'captured_at=%s\n' "$capture_iso"
	printf 'capture_ms=%s\n' "$capture_ms"
	printf 'delta_ms=%s\n' "$((capture_ms - send_return_ms))"
	printf 'dialog_shows_message_at_return=%s\n' "$dialog_at_return"
} >"$OUT/req-015-timestamps.txt"

# The held message must not start a turn during 30 seconds of waiting.
excerpt_session inb-bypass step2b-before-approval.jsonl || true
before_assistant="$(count_matches "$OUT/step2b-before-approval.jsonl" '"type":"assistant"')"
turn_before_approval=no
for _ in 1 2 3 4 5 6; do
	sleep 5
	excerpt_session inb-bypass step2b-while-held.jsonl || true
	if has_peer_turn "$OUT/step2b-while-held.jsonl" 'from-broker'; then
		turn_before_approval=yes
	fi
done
rs_snap inb-bypass step2b-while-held.txt

# Approve with Down then Enter; the dialog opens on the deny option.
rs_keys inb-bypass Down Enter
if wait_sid_peer_turn "$bypass_session_id" from-broker step2b-after-approval.jsonl 60; then
	rs_obs "2b: the held message reached the conversation after approval"
else
	rs_obs "2b: from-broker did not reach the conversation within 60 seconds after approval"
fi
rs_pane_idle inb-bypass 60 || true
rs_snap inb-bypass handshake-approved-2b.txt
after_assistant="$(count_matches "$OUT/step2b-after-approval.jsonl" '"type":"assistant"')"
{
	printf 'dialog_at_send_return=%s\n' "$dialog_at_return"
	printf 'turn_before_approval=%s\n' "$turn_before_approval"
	printf 'assistant_records_before_approval=%s\n' "$before_assistant"
	printf 'assistant_records_after_approval=%s\n' "$after_assistant"
} >"$OUT/step2b-handshake.txt"

# 2d: keep the dialog text, including any reason or waiting period it shows.
grep -E 'Held message|Deny|Deliver this message|expire|wait|second|minute' \
	"$OUT/handshake-held-2b.txt" >"$OUT/held-dialog-2b.txt" 2>/dev/null || true
rs_obs "2d: the handshake dialog text is in handshake-held-2b.txt"

# 2c: the receiver's ancestry limit in the bypass case. The 13-level writer is
# expected to be held and the 11-level writer to be accepted.
rs_pane_idle inb-bypass 30 || true
rs_send_shell inb-bypass "$(depth_frame_command 13 13 "$bypass_socket")" 'depth-frame.sh' 60
depth13_outcome="$(wait_hold_or_record inb-bypass "$bypass_session_id" depth-13 step2c-depth-13.jsonl 120)"
rs_snap inb-bypass "depth-13-$depth13_outcome.txt"
if [[ "$depth13_outcome" == 'held' ]]; then
	# The dialog opens on the deny option, so Enter denies it.
	rs_keys inb-bypass Enter
	sleep 2
	rs_snap inb-bypass depth-13-denied.txt
	excerpt_session inb-bypass step2c-depth-13-denied.jsonl || true
	if has_peer_turn "$OUT/step2c-depth-13-denied.jsonl" 'depth-13'; then
		rs_obs "2c: depth-13 was denied but still reached the conversation"
	else
		rs_obs "2c: depth-13 was held and denied, and did not reach the conversation"
	fi
fi
if [[ "$depth13_outcome" == 'timeout' ]]; then
	# Leave no dialog in the way of the next command if the screen was not
	# recognised in time.
	rs_keys inb-bypass Escape
	sleep 1
fi

rs_pane_idle inb-bypass 30 || true
rs_send_shell inb-bypass "$(depth_frame_command 11 11 "$bypass_socket")" 'depth-frame.sh' 60
depth11_outcome="$(wait_hold_or_record inb-bypass "$bypass_session_id" depth-11 step2c-depth-11.jsonl 120)"
rs_snap inb-bypass "depth-11-$depth11_outcome.txt"
if [[ "$depth11_outcome" == 'held' ]]; then
	rs_keys inb-bypass Enter
	sleep 2
	rs_snap inb-bypass depth-11-denied.txt
elif [[ "$depth11_outcome" == 'timeout' ]]; then
	rs_keys inb-bypass Escape
	sleep 1
fi
rs_pane_idle inb-bypass 30 || true
{
	printf 'depth_13=%s\n' "$depth13_outcome"
	printf 'depth_11=%s\n' "$depth11_outcome"
} >"$OUT/step2c-depth.txt"
rs_obs "2c: depth-13 outcome $depth13_outcome, depth-11 outcome $depth11_outcome"

# 2e: an idle session starts a turn on its own (REQ-agent-messaging-021). This
# runs before any inbound policy is written, so the receiver is under the
# default class rule rather than a policy left over from an earlier step.
rs_pane_idle inb-first 60 || true
excerpt_session inb-first step4-before.jsonl || true
before_assistant="$(count_matches "$OUT/step4-before.jsonl" '"type":"assistant"')"
idle_send_ms="$(now_ms)"
if ! rs_call inb-sender send_message '{"to":"inb-first@hostB","message":"idle-wake"}' >"$OUT/send-4.txt"; then
	rs_obs "2e: inb-sender send_message idle-wake failed"
fi
deadline=$((SECONDS + 20))
while ((SECONDS < deadline)); do
	excerpt_session inb-first step4-idle-wake.jsonl || true
	if has_peer_turn "$OUT/step4-idle-wake.jsonl" 'idle-wake'; then
		after_assistant="$(count_matches "$OUT/step4-idle-wake.jsonl" '"type":"assistant"')"
		if ((after_assistant > before_assistant)); then
			idle_wake_turn=yes
			idle_turn_ms=$(( $(now_ms) - idle_send_ms ))
			break
		fi
	fi
	sleep 0.5
done
rs_snap inb-first step4-idle-wake.txt
{
	printf 'turn_within_20s=%s\n' "$idle_wake_turn"
	printf 'turn_ms=%s\n' "${idle_turn_ms:-}"
} >"$OUT/step4-idle-wake-result.txt"
rs_obs "2e: idle-wake started a turn $idle_wake_turn"

# --- step 3: inbound policy (REQ-015, 016, 024) -----------------------------

# The shared settings file may be rewritten by this probe only, and only with
# a backup plus the two sha256 values. The EXIT hook restores it and fails the
# run on a mismatch. The idle-wake check (2e) already ran under the untouched
# file, so no policy written here is relied on after this section.
sha_before="$(sha256sum "$R/cfg/settings.json" | awk '{print $1}')"
cp "$R/cfg/settings.json" "$settings_backup"
printf '%s\n' "$sha_before" >"$OUT/settings.before.sha256"
settings_touched=1
rs_obs "backed up settings.json with sha256 $sha_before"

# 3a: explicit hold. One message is sent by a real sender session, so the
# sender's own records can show later whether a notification arrived.
write_inbound_policy hold
rs_window inb-hold claude --model haiku -n inb-hold \
	--permission-mode default \
	--append-system-prompt "$PROMPT" \
	--debug-file "$R/debug/inb-hold.debug"
if ! rs_wait_ready inb-hold 45; then
	rs_obs "inb-hold did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode inb-hold
if ! wait_socket inb-hold 30; then
	rs_obs "inb-hold did not register an inbox socket within 30 seconds"
	exit 1
fi
rs_window inb-src claude --model haiku -n inb-src \
	--permission-mode default \
	--allowedTools mcp__peer__send_message \
	--debug-file "$R/debug/inb-src.debug"
if ! rs_wait_ready inb-src 45; then
	rs_obs "inb-src did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode inb-src
if ! wait_socket inb-src 30; then
	rs_obs "inb-src did not register an inbox socket within 30 seconds"
	exit 1
fi

rs_pane_idle inb-src 30 || true
usage_before_sender="$(usage_total)"
hold_send_prompt_ms="$(now_ms)"
hold_send_prompt_iso="$(now_iso)"
rs_send_prompt inb-src 'Using mcp__peer__send_message, send a message to inb-hold@hostB with the text hold-1. Then stop and reply only "sent".' 'Using mcp__peer__send_message' 60
if wait_session_reply inb-src 'sent' step3a-sender-reply.jsonl 180; then
	hold_sender_reply=yes
	rs_obs "3a: inb-src finished its send turn and replied sent"
else
	rs_obs "3a: inb-src did not reply sent within 180 seconds"
fi
hold_sender_done_ms="$(now_ms)"
hold_sender_done_iso="$(now_iso)"
excerpt_session inb-src step3a-sender-turn.jsonl || true
if grep -qF -- 'delivered to the recipient' "$OUT/step3a-sender-turn.jsonl"; then
	hold_sender_delivery_result=yes
fi
rs_snap inb-src step3a-sender.txt

# Every prompt typed by this probe goes through rs_send_prompt, so a run whose
# prompt never reached the model cannot pass. The marker is the second prompt's
# text, and the counter must have grown past its pre-prompt reading.
if [[ "$usage_before_sender" =~ ^[0-9]+$ ]]; then
	sender_min_usage=$((usage_before_sender + 1))
else
	sender_min_usage=1
fi
rs_require_model_turn 'Using mcp__peer__send_message' "$sender_min_usage"

# The explicit hold is an observation, not a criterion (the ruling after the
# first ref-clear run): the sender returns immediately, the receiver shows a
# hold notice, and no per-message approval appears. Per-message approval and
# denial are measured on the handshake dialog in 2b and 2c.
if ! rs_wait_pane inb-hold 'Held|[Cc]ross-session|pending|waiting' 30; then
	rs_obs "3a: no held warning appeared in inb-hold within 30 seconds"
fi
rs_snap inb-hold step3a-hold-warning.txt
if grep -qF -- 'Held' "$OUT/step3a-hold-warning.txt"; then
	hold_notice_at_sender_done=yes
fi
if grep -qF -- "$HANDSHAKE_TITLE" "$OUT/step3a-hold-warning.txt" ||
	grep -qF -- "$HANDSHAKE_APPROVE" "$OUT/step3a-hold-warning.txt"; then
	hold_dialog=yes
fi
# Enter in the composer must not deliver the held message: 2.1.311 offers no
# per-message approval for an explicit hold.
rs_keys inb-hold Enter
sleep 5
rs_snap inb-hold step3a-after-enter.txt
if grep -qF -- 'Held' "$OUT/step3a-after-enter.txt"; then
	hold_notice_after_enter=yes
fi
sleep 5
excerpt_session inb-hold step3a-hold-record.jsonl || true
if has_peer_turn "$OUT/step3a-hold-record.jsonl" 'hold-1'; then
	hold_model_received=yes
	rs_obs "3a: the held message reached the conversation while the policy was hold"
else
	rs_obs "3a: the held message did not reach the conversation while the policy was hold"
fi
{
	printf 'sender_prompt_at=%s\n' "$hold_send_prompt_iso"
	printf 'sender_prompt_ms=%s\n' "$hold_send_prompt_ms"
	printf 'sender_done_at=%s\n' "$hold_sender_done_iso"
	printf 'sender_done_ms=%s\n' "$hold_sender_done_ms"
	printf 'sender_replied_sent=%s\n' "$hold_sender_reply"
	printf 'sender_delivery_result=%s\n' "$hold_sender_delivery_result"
	printf 'hold_notice_at_sender_done=%s\n' "$hold_notice_at_sender_done"
	printf 'per_message_dialog=%s\n' "$hold_dialog"
	printf 'hold_notice_after_enter=%s\n' "$hold_notice_after_enter"
	printf 'message_in_records=%s\n' "$hold_model_received"
} >"$OUT/step3a-hold-observation.txt"
rs_obs "3a observation: sender replied $hold_sender_reply with delivery result $hold_sender_delivery_result; hold notice at sender done $hold_notice_at_sender_done, per-message dialog $hold_dialog, notice after Enter $hold_notice_after_enter, message in records $hold_model_received"

# 3b: switching to accept releases the held message in one shot. The release
# is shown both by the model turn and by the "policy-accepts" log line.
write_inbound_policy accept
accept_ms="$(now_ms)"
accept_iso="$(now_iso)"
release_seen_ms=''
deadline=$((SECONDS + 60))
while ((SECONDS < deadline)); do
	rs_excerpt 'policy-accepts' release-log.txt
	if grep -qF -- 'policy-accepts' "$OUT/release-log.txt" 2>/dev/null ||
		grep -qF -- 'policy-accepts' "$OUT/release-log.txt.debug" 2>/dev/null; then
		release_policy_accepts=yes
	fi
	excerpt_session inb-hold release-record.jsonl || true
	if has_peer_turn "$OUT/release-record.jsonl" 'hold-1'; then
		release_model_received=yes
		if [[ -z "$release_seen_ms" ]]; then
			release_seen_ms="$(now_ms)"
		fi
	fi
	if [[ "$release_policy_accepts" == 'yes' && "$release_model_received" == 'yes' ]]; then
		break
	fi
	sleep 1
done
rs_snap inb-hold step3b-released.txt
reply_ok=no
if wait_session_reply inb-hold 'ok' step3b-reply-record.jsonl 30; then
	reply_ok=yes
fi
{
	printf 'policy_changed_at=%s\n' "$accept_iso"
	printf 'policy_changed_ms=%s\n' "$accept_ms"
	printf 'policy_accepts_line=%s\n' "$release_policy_accepts"
	printf 'model_received=%s\n' "$release_model_received"
	printf 'released_seen_ms=%s\n' "$release_seen_ms"
	printf 'reply_ok=%s\n' "$reply_ok"
} >"$OUT/step3b-release.txt"
rs_obs "3b: policy-accepts line $release_policy_accepts, model received $release_model_received, transcript reply ok $reply_ok"

# 3c: for three minutes the sender's session must see no follow-up records.
rs_pane_idle inb-src 60 || true
excerpt_session inb-src step3c-sender-before.jsonl || true
before_lines="$(wc -l <"$OUT/step3c-sender-before.jsonl" | tr -d ' ')"
notified=no
notified_at=''
deadline=$((SECONDS + 180))
while ((SECONDS < deadline)); do
	excerpt_session inb-src step3c-sender-after.jsonl || true
	after_lines="$(wc -l <"$OUT/step3c-sender-after.jsonl" | tr -d ' ')"
	if ((after_lines != before_lines)); then
		notified=yes
		notified_at="$(now_iso)"
		break
	fi
	sleep 5
done
excerpt_session inb-src step3c-sender-after.jsonl || true
after_lines="$(wc -l <"$OUT/step3c-sender-after.jsonl" | tr -d ' ')"
rs_snap inb-src step3c-sender-after.txt
{
	printf 'records_before=%s\n' "$before_lines"
	printf 'records_after=%s\n' "$after_lines"
	printf 'sender_notified=%s\n' "$notified"
	printf 'sender_notified_at=%s\n' "$notified_at"
} >"$OUT/step3c-no-notification.txt"
rs_obs "3c: sender records before $before_lines after $after_lines, notified $notified"

# 3d: refuse drops the message without showing it to the conversation.
rs_keys inb-hold '/exit' Enter
wait_socket_gone inb-hold 15 || true
write_inbound_policy refuse
rs_window inb-refuse claude --model haiku -n inb-refuse \
	--permission-mode default \
	--append-system-prompt "$PROMPT" \
	--debug-file "$R/debug/inb-refuse.debug"
if ! rs_wait_ready inb-refuse 45; then
	rs_obs "inb-refuse did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode inb-refuse
if ! wait_socket inb-refuse 30; then
	rs_obs "inb-refuse did not register an inbox socket within 30 seconds"
	exit 1
fi
rs_pane_idle inb-refuse 30 || true
if rs_call inb-sender send_message '{"to":"inb-refuse@hostB","message":"refused-1"}' >"$OUT/send-3d.txt"; then
	refuse_send_ok=yes
else
	rs_obs "3d: inb-sender send_message refused-1 failed"
fi
sleep 10
excerpt_session inb-refuse step3d-refuse-record.jsonl || true
if has_peer_turn "$OUT/step3d-refuse-record.jsonl" 'refused-1'; then
	refuse_message_visible=yes
fi
rs_snap inb-refuse step3d-refuse.txt
{
	printf 'send_returned_success=%s\n' "$refuse_send_ok"
	printf 'message_in_session=%s\n' "$refuse_message_visible"
} >"$OUT/step3d-refuse-result.txt"
rs_obs "3d: send success $refuse_send_ok, message in session $refuse_message_visible"

# The EXIT hook restores the shared settings and checks its sha256.

{
	printf 'broker_pid=%s\n' "$broker_pid"
	printf 'default_session_id=%s\n' "$default_session_id"
	printf 'bypass_session_id=%s\n' "$bypass_session_id"
	printf 'dialog_at_send_return=%s\n' "$dialog_at_return"
	printf 'send_return_ms=%s\n' "$send_return_ms"
	printf 'capture_ms=%s\n' "$capture_ms"
	printf 'default_dialog_shown=%s\n' "$default_dialog"
	printf 'default_peer_turn=%s\n' "$default_peer_turn"
	printf 'default_origin=%s\n' "$default_origin"
	printf 'bypass_child_delivered=%s\n' "$bypass_child_delivered"
	printf 'bypass_child_origin=%s\n' "$bypass_child_origin"
	printf 'depth_13=%s\n' "$depth13_outcome"
	printf 'depth_11=%s\n' "$depth11_outcome"
	printf 'hold_model_received=%s\n' "$hold_model_received"
	printf 'hold_sender_replied_sent=%s\n' "$hold_sender_reply"
	printf 'hold_sender_delivery_result=%s\n' "$hold_sender_delivery_result"
	printf 'hold_per_message_dialog=%s\n' "$hold_dialog"
	printf 'hold_notice_at_sender_done=%s\n' "$hold_notice_at_sender_done"
	printf 'hold_notice_after_enter=%s\n' "$hold_notice_after_enter"
	printf 'release_policy_accepts=%s\n' "$release_policy_accepts"
	printf 'release_model_received=%s\n' "$release_model_received"
	printf 'refuse_send_ok=%s\n' "$refuse_send_ok"
	printf 'refuse_message_visible=%s\n' "$refuse_message_visible"
	printf 'idle_wake_turn=%s\n' "$idle_wake_turn"
} >"$OUT/summary.txt"

rs_obs "probe finished"

# The list_agents prompt is the first model prompt the probe types; a run that
# never submitted it, or that recorded no model call at all, is a false pass.
rs_require_model_turn 'Call mcp__peer__list_agents and show the raw result.' 1
