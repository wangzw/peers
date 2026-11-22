#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

if ! rs_start_broker; then
	exit 1
fi

# ref-clear answers the Roadmap's Items to verify section and exercises
# REQ-agent-messaging-006 and REQ-agent-messaging-033: the disambiguation ref
# must match the one Claude Code's own session list shows, and it must survive
# /rename and /clear; a message held before /clear must reach the conversation
# created by /clear.

PROMPT='When a cross-session message arrives, reply with the single word ok and do nothing else. Do not call any tool unless the user explicitly asks.'
OBSERVER_PROMPT='Use your built-in tool for listing the other Claude Code sessions on this machine and print its raw output verbatim. Do not do anything else.'

x0=''
x1=''
x2=''
y1=''
y2=''
x1_address=''
before_socket=''
after_socket=''
assistant_turns=0
RS_HOLD_CALL_WAIT="${RS_HOLD_CALL_WAIT:-10}"

# wait_socket polls the session registry until the named session has a socket.
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

# wait_socket_gone polls until the named session is no longer registered.
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

# wait_status_contains polls the Broker's Agent table for a literal fragment.
wait_status_contains() {
	local fragment="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		"$R/run" rsprobe status >"$WORK/status-wait.txt"
		if grep -qF -- "$fragment" "$WORK/status-wait.txt"; then
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# wait_excerpt polls rs_excerpt until it copies at least one matching record.
wait_excerpt() {
	local marker="$1" file="$2" seconds="$3"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		rs_excerpt "$marker" "$file"
		if [[ -s "$OUT/$file" ]]; then
			return 0
		fi
		sleep 0.5
	done
	rs_excerpt "$marker" "$file"
	[[ -s "$OUT/$file" ]]
}

# pid_running reports whether a background job is still running.
pid_running() {
	kill -0 "$1" 2>/dev/null
}

# wait_pid_deadline PID DEADLINE polls until PID exits, and returns 0 if it has
# exited by DEADLINE (a $SECONDS value).
wait_pid_deadline() {
	local pid="$1" deadline="$2"
	while ((SECONDS < deadline)); do
		if ! pid_running "$pid"; then
			return 0
		fi
		sleep 0.2
	done
	! pid_running "$pid"
}

# session_sockets prints the inbox socket path of every session record.
session_sockets() {
	local file
	for file in "$R"/cfg/sessions/*.json; do
		if [[ ! -e "$file" ]]; then
			continue
		fi
		jq -r 'select((.messagingSocketPath // "") != "") | .messagingSocketPath' "$file" || true
	done | sort -u
}

# list_session_records prints the project record files that currently exist.
list_session_records() {
	find "$R/cfg/projects" -type f -name '*.jsonl' 2>/dev/null | sort || true
}

# list_record_offsets writes "path<TAB>line-count" for every session record, so
# a later read can take only the lines appended after this snapshot.
list_record_offsets() {
	local file
	while IFS= read -r file; do
		[[ -n "$file" ]] || continue
		printf '%s\t%s\n' "$file" "$(wc -l <"$file" | tr -d ' ')"
	done < <(list_session_records)
}

# wait_excerpt_after_offsets polls only the lines appended after OFFSETS and
# copies those matching MARKER to $OUT/$file. Reading from the saved line count
# keeps an older record, such as the /rename line, from satisfying the wait.
wait_excerpt_after_offsets() {
	local marker="$1" offsets="$2" file="$3" seconds="$4"
	local deadline=$((SECONDS + seconds)) out path start
	out="$OUT/$file"
	while ((SECONDS < deadline)); do
		: >"$out"
		: >"$out.sources"
		while IFS=$'\t' read -r path start; do
			[[ -e "$path" ]] || continue
			if tail -n +"$((start + 1))" "$path" 2>/dev/null | grep -qF -- "$marker"; then
				printf '%s\n' "$path" >>"$out.sources"
				tail -n +"$((start + 1))" "$path" 2>/dev/null | grep -F -- "$marker" >>"$out" || true
			fi
		done <"$offsets"
		if [[ -s "$out" ]]; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}

# assistant_after_marker reports whether an assistant record follows the first
# record containing MARKER in FILE.
assistant_after_marker() {
	local file="$1" marker="$2" line
	line="$(grep -nF -- "$marker" "$file" 2>/dev/null | head -n 1 | cut -d: -f1)"
	[[ -n "$line" ]] || return 1
	tail -n +"$line" "$file" 2>/dev/null | grep -qF -- '"type":"assistant"'
}

# wait_new_record_file polls until a record file exists that is not listed in
# SNAPSHOT, and prints its path.
wait_new_record_file() {
	local snapshot="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		list_session_records >"$WORK/records-now.txt"
		comm -13 "$snapshot" "$WORK/records-now.txt" >"$WORK/records-new-now.txt" || true
		if [[ -s "$WORK/records-new-now.txt" ]]; then
			cat "$WORK/records-new-now.txt"
			return 0
		fi
		sleep 0.2
	done
	return 1
}

# wait_pending_in_new_record polls for a record file created after /clear that
# carries the held message, and copies its matching lines to $OUT.
wait_pending_in_new_record() {
	local seconds="$1"
	local deadline=$((SECONDS + seconds))
	local record
	while ((SECONDS < deadline)); do
		list_session_records >"$WORK/records-after-clear.txt"
		comm -13 "$WORK/records-before-clear.txt" "$WORK/records-after-clear.txt" >"$WORK/records-new.txt" || true
		: >"$OUT/pending-in-new-record.jsonl"
		: >"$OUT/pending-new-record-files.txt"
		while IFS= read -r record; do
			[[ -n "$record" ]] || continue
			if grep -qF -- 'pending-before-clear' "$record"; then
				printf '%s\n' "$record" >>"$OUT/pending-new-record-files.txt"
				grep -F -- 'pending-before-clear' "$record" >>"$OUT/pending-in-new-record.jsonl" || true
			fi
		done <"$WORK/records-new.txt"
		if [[ -s "$OUT/pending-in-new-record.jsonl" ]]; then
			cp "$WORK/records-new.txt" "$OUT/records-new-after-clear.txt"
			return 0
		fi
		sleep 1
	done
	cp "$WORK/records-new.txt" "$OUT/records-new-after-clear.txt" 2>/dev/null || true
	[[ -s "$OUT/pending-in-new-record.jsonl" ]]
}

# address_text FILE NAME prints NAME's ref-bearing address on hostB.
address_text() {
	local file="$1" name="$2"
	grep -oE "${name} \\[[0-9a-f]{6,12}\\]@hostB" "$file" | head -n 1 || true
}

# address_ref FILE NAME prints the ref inside NAME's address.
address_ref() {
	local file="$1" name="$2"
	local addr
	addr="$(address_text "$file" "$name")"
	if [[ -z "$addr" ]]; then
		return 1
	fi
	printf '%s\n' "$addr" | sed -E 's/^.*\[([0-9a-f]{6,12})\]@hostB$/\1/'
}

# ref_of_path FILE PATH prints the short ref rsprobe ref computed for PATH.
ref_of_path() {
	local file="$1" path="$2"
	awk -v p="$path" '$1 == p { print $3; exit }' "$file"
}

# bracket_ref TEXT NAME prints the ref in "NAME [ref]" inside the given text.
bracket_ref() {
	local text="$1" name="$2"
	local match
	match="$(printf '%s\n' "$text" | grep -oE "${name} \\[[0-9a-f]{6,12}\\]" | head -n 1 || true)"
	if [[ -z "$match" ]]; then
		return 1
	fi
	match="${match#*[}"
	printf '%s\n' "${match%]*}"
}

status_snapshot() {
	local file="$1"
	"$R/run" rsprobe status >"$OUT/$file"
}

rs_tmux_start
rs_window ref-clear-a claude --model haiku -n 'a<b' --permission-mode default --append-system-prompt "$PROMPT"
# The observer prompt asks for the built-in ListAgents tool. Pre-approve that
# one tool with --allowedTools and keep the permission mode at default, so the
# call does not open an approval dialog and no other tool is pre-approved.
rs_window ref-clear-observer claude --model haiku -n rs-observer --permission-mode default --append-system-prompt "$PROMPT" --allowedTools ListAgents
rs_agent ref-peer

if ! wait_socket 'a<b' 30; then
	rs_snap ref-clear-a a-not-registered.txt
	rs_obs "a<b did not register within 30 seconds"
	exit 1
fi
if ! rs_wait_ready ref-clear-a 45; then
	rs_obs "the a<b session did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode ref-clear-a
if ! wait_socket rs-observer 30; then
	rs_snap ref-clear-observer observer-not-registered.txt
	rs_obs "rs-observer did not register within 30 seconds"
	exit 1
fi
if ! rs_wait_ready ref-clear-observer 45; then
	rs_obs "rs-observer did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode ref-clear-observer

# X0 is computed over every session socket and X1 comes from the Broker's own
# table, so both agents and the simulated one must be registered before either
# is read; a half-built table would compare two different candidate sets.
if ! wait_status_contains 'ab [' 30; then
	rs_obs "a<b was not in the Agent table within 30 seconds"
	exit 1
fi
if ! wait_status_contains 'rs-observer@hostB' 30; then
	rs_obs "rs-observer was not in the Agent table within 30 seconds"
	exit 1
fi
if ! wait_status_contains 'ref-peer@hostB' 15; then
	rs_obs "ref-peer was not in the Agent table within 15 seconds"
	exit 1
fi

# Step 2: the address of a<b carries a ref because the name is cleaned to ab.
before_socket="$(rs_socket_of 'a<b')"
status_snapshot status-1.txt
x1_address="$(address_text "$OUT/status-1.txt" ab)"
x1="$(address_ref "$OUT/status-1.txt" ab || true)"

mapfile -t sockets < <(session_sockets)
if ((${#sockets[@]} == 0)); then
	rs_obs "no session socket paths found before computing X0"
	exit 1
fi
"$R/run" rsprobe ref "${sockets[@]}" >"$OUT/ref.txt"
x0="$(ref_of_path "$OUT/ref.txt" "$before_socket" || true)"

# Step 3: the built-in session list's own ref for a<b.
rs_send_prompt ref-clear-observer "$OBSERVER_PROMPT" 'listing the other Claude Code sessions' 60
if wait_excerpt 'a<b' observer-y1.jsonl 90; then
	y1="$(bracket_ref "$(cat "$OUT/observer-y1.jsonl")" 'a<b' || true)"
	rs_snap ref-clear-observer observer-y1-pane.txt
else
	rs_snap ref-clear-observer observer-y1-timeout.txt
	rs_obs "the session list output for a<b did not appear within 90 seconds"
fi
rs_excerpt 'listing the other Claude Code sessions' observer-y1-prompt.jsonl

# Step 4: rename and clear keep the socket path and the ref.
rs_keys ref-clear-a '/rename c<d' Enter
if wait_socket 'c<d' 15; then
	rs_obs "a<b was renamed to c<d in the session registry"
else
	rs_snap ref-clear-a rename-timeout.txt
	rs_obs "c<d did not appear in the session registry within 15 seconds"
fi
rs_keys ref-clear-a '/clear' Enter
sleep 3
status_snapshot status-2.txt
x2="$(address_ref "$OUT/status-2.txt" cd || true)"
after_socket="$(rs_socket_of 'c<d' || true)"
{
	printf 'before=%s\n' "$before_socket"
	printf 'after=%s\n' "$after_socket"
} >"$OUT/socket-paths.txt"
if ! rs_call ref-peer list_agents '{}' >"$OUT/list-agents-after-clear.json"; then
	rs_obs "ref-peer list_agents failed after /clear"
fi

# Step 5: the built-in list must show the same ref under the new name. Only the
# lines appended by this observer turn are read: the earlier /rename and /clear
# records also contain c<d, and matching them would end the wait before the
# second listing exists.
list_record_offsets >"$WORK/observer-offsets-before-y2.txt"
rs_send_prompt ref-clear-observer "$OBSERVER_PROMPT" 'listing the other Claude Code sessions' 60
if wait_excerpt_after_offsets 'c<d [' "$WORK/observer-offsets-before-y2.txt" observer-y2.jsonl 90; then
	y2="$(bracket_ref "$(cat "$OUT/observer-y2.jsonl")" 'c<d' || true)"
	rs_snap ref-clear-observer observer-y2-pane.txt
else
	rs_snap ref-clear-observer observer-y2-timeout.txt
	rs_obs "the session list output for c<d did not appear within 90 seconds"
fi

# Step 6: observation of the explicit hold policy, not a criterion. It records
# whether send_message returns at once, what the recipient shows, and what
# /clear does to a held message. Only this step changes the shared settings
# file; it is backed up to $OUT with its sha256, restored by the EXIT trap, and
# the two digests must match. The hold-time send_message runs in the background
# under RS_HOLD_CALL_WAIT, because whether it returns at once is itself an
# observation.
rs_keys ref-clear-a '/exit' Enter
if ! wait_socket_gone 'c<d' 15; then
	rs_obs "c<d was still registered 15 seconds after /exit"
fi

settings_file="$R/cfg/settings.json"
settings_backup="$OUT/settings.before.json"
settings_before_sha=''
settings_after_sha=''

# settings_sha256 PATH prints the file's sha256 digest.
settings_sha256() {
	sha256sum "$1" | awk '{print $1}'
}

# restore_settings puts the original settings file back and verifies its
# sha256. It runs explicitly before step 7 and through the library's EXIT hook
# otherwise, so a failed step 7 still restores the file and captures the
# windows.
restore_settings() {
	if ! cp "$settings_backup" "$settings_file"; then
		rs_obs "could not restore settings.json from $settings_backup"
		return 1
	fi
	settings_after_sha="$(settings_sha256 "$settings_file")"
	printf '%s\n' "$settings_after_sha" >"$OUT/settings.after.sha256"
	{
		printf 'before=%s\n' "$settings_before_sha"
		printf 'after=%s\n' "$settings_after_sha"
	} >"$OUT/settings.sha256.txt"
	if [[ "$settings_after_sha" != "$settings_before_sha" ]]; then
		rs_obs "settings.json sha256 differs after restore: before=$settings_before_sha after=$settings_after_sha"
		return 1
	fi
}

cp "$settings_file" "$settings_backup"
settings_before_sha="$(settings_sha256 "$settings_backup")"
printf '%s\n' "$settings_before_sha" >"$OUT/settings.before.sha256"
rs_on_exit restore_settings
jq '.crossSessionInbound = "hold"' "$settings_backup" >"$WORK/settings.hold.json"
cp "$WORK/settings.hold.json" "$settings_file"
rs_obs "added crossSessionInbound=hold to settings.json for step 6 (sha256 before $settings_before_sha)"

rs_window ref-clear-clr claude --model haiku -n clr --permission-mode default --append-system-prompt "$PROMPT" --allowedTools 'Bash(sleep:*)'
if ! wait_socket clr 30; then
	rs_snap ref-clear-clr clr-not-registered.txt
	rs_obs "clr did not register within 30 seconds"
	exit 1
fi
if ! rs_wait_ready ref-clear-clr 45; then
	rs_obs "clr did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode ref-clear-clr
# Background the hold-time call so the pane and the approval can be watched
# while it runs. RS_HOLD_CALL_WAIT bounds how long its reply is awaited; the
# timing and outcome are written to send-pending-timing.txt.
send_started=$SECONDS
send_deadline=$((send_started + RS_HOLD_CALL_WAIT))
rs_call ref-peer send_message '{"to":"clr@hostB","message":"pending-before-clear"}' >"$OUT/send-pending.json" &
send_pid=$!
rs_obs "ref-peer send_message pending-before-clear is running in the background (pid $send_pid)"

if ! rs_wait_pane ref-clear-clr 'pending-before-clear' 30; then
	rs_obs "pending-before-clear was not visible in clr's pane within 30 seconds"
fi
rs_snap ref-clear-clr hold-pending.txt
rs_obs "hold observation: the recipient showed only a hold notice, whose text says to set crossSessionInbound=accept to deliver the message"
if pid_running "$send_pid"; then
	rs_obs "send_message was still running while the pending message was on screen"
else
	rs_obs "send_message had returned after $((SECONDS - send_started)) seconds, while the pending message was on screen"
fi

list_session_records >"$WORK/records-before-clear.txt"
rs_keys ref-clear-clr '/clear' Enter
sleep 3
rs_snap ref-clear-clr after-clear.txt

send_returned_before_approval=no
if ! pid_running "$send_pid"; then
	send_returned_before_approval=yes
fi

# Approve the held message after /clear. The Enter key is the approval key as
# observed in the pending-message bar; the captures record whether it worked.
rs_keys ref-clear-clr Enter
send_outcome=''
send_status=''
if wait_pid_deadline "$send_pid" "$send_deadline"; then
	if [[ "$send_returned_before_approval" == yes ]]; then
		send_outcome='returned-before-approval'
	else
		send_outcome='returned-after-approval'
	fi
	if wait "$send_pid"; then
		send_status=0
	else
		send_status=$?
	fi
else
	send_outcome='still-running-at-bound'
	send_status='not-returned'
	kill "$send_pid" 2>/dev/null || true
	wait "$send_pid" 2>/dev/null || true
fi
send_elapsed=$((SECONDS - send_started))
{
	printf 'mode=background\n'
	printf 'upper_bound_seconds=%s\n' "$RS_HOLD_CALL_WAIT"
	printf 'returned_before_approval=%s\n' "$send_returned_before_approval"
	printf 'outcome=%s\n' "$send_outcome"
	printf 'elapsed_seconds=%s\n' "$send_elapsed"
	printf 'exit_status=%s\n' "$send_status"
} >"$OUT/send-pending-timing.txt"
rs_obs "send_message pending-before-clear outcome $send_outcome after ${send_elapsed} seconds (exit status $send_status)"
if ! wait_pending_in_new_record 60; then
	rs_obs "pending-before-clear did not reach a record file created after /clear within 60 seconds"
fi
rs_snap ref-clear-clr after-approval.txt
rs_excerpt 'pending-before-clear' pending-after-clear.jsonl
if [[ "$send_outcome" == returned-before-approval && ! -s "$OUT/pending-in-new-record.jsonl" ]]; then
	rs_obs "hold observation: no per-message approval appeared; pressing Enter did not deliver the held message"
fi
if [[ ! -s "$OUT/pending-in-new-record.jsonl" ]]; then
	rs_obs "hold observation: /clear discarded the held notice and the message did not reach the new conversation"
fi

# Count assistant records in the files created after /clear, which shows whether
# the held message started a model turn in the new conversation.
while IFS= read -r record; do
	[[ -n "$record" ]] || continue
	count="$(grep -cF -- '"type":"assistant"' "$record" || true)"
	count="${count:-0}"
	assistant_turns=$((assistant_turns + count))
done <"$OUT/records-new-after-clear.txt"

restore_settings
rs_obs "restored settings.json after step 6 and verified its sha256 ($settings_after_sha)"

# Step 7 (REQ-agent-messaging-033 criterion): under the default accept policy a
# message that arrives while the session is busy must appear in the conversation
# created by /clear and be handled there. Step 6 started clr while settings.json
# said hold; restore_settings has put the original file back, and clr is
# restarted so the default policy is read at startup rather than assumed to
# reload. --allowedTools covers the one tool the prompt asks for.
rs_keys ref-clear-clr '/exit' Enter
if ! wait_socket_gone clr 15; then
	rs_obs "clr was still registered 15 seconds after /exit before step 7"
fi
rs_window ref-clear-accept claude --model haiku -n clr --permission-mode default --append-system-prompt "$PROMPT" --allowedTools 'Bash(sleep:*)'
if ! wait_socket clr 30; then
	rs_snap ref-clear-accept clr-accept-not-registered.txt
	rs_obs "the default-policy clr did not register within 30 seconds"
	exit 1
fi
if ! rs_wait_ready ref-clear-accept 45; then
	rs_obs "the default-policy clr did not reach its input prompt within 45 seconds"
	exit 1
fi
rs_obs_mode ref-clear-accept

# The session's record file may appear only when the first prompt is submitted,
# so the snapshot is taken after the sleep tool is on screen and before the
# message is sent. A file that appears later is then a post-/clear file rather
# than the session's own record.
rs_send_prompt ref-clear-accept 'Run "sleep 20" with the Bash tool, then reply done.' 'Run "sleep 20"' 60
if ! rs_wait_pane ref-clear-accept 'sleep 20' 20; then
	rs_obs "the sleep 20 tool call was not visible before sending queued-before-clear"
fi
prompt_record_seen=no
prompt_deadline=$((SECONDS + 10))
while ((SECONDS < prompt_deadline)); do
	list_session_records >"$WORK/records-now.txt"
	while IFS= read -r record; do
		[[ -n "$record" ]] || continue
		if grep -qF -- 'Run "sleep 20"' "$record"; then
			prompt_record_seen=yes
		fi
	done <"$WORK/records-now.txt"
	if [[ "$prompt_record_seen" == yes ]]; then
		break
	fi
	sleep 0.2
done
if [[ "$prompt_record_seen" != yes ]]; then
	rs_obs "the sleep 20 prompt had no record file within 10 seconds before the snapshot"
fi
list_session_records >"$WORK/records-before-queued.txt"
if ! rs_call ref-peer send_message '{"to":"clr@hostB","message":"queued-before-clear"}' >"$OUT/send-queued.json"; then
	rs_obs "ref-peer send_message queued-before-clear failed"
fi

# Submit /clear while the tool is still running. The composer shows
# "ctrl+x ctrl+s to send now" while a turn is busy, so the queued command is
# sent now instead of waiting for the tool to finish. A new record file proves
# it ran; when it did not, the command is submitted again once the turn goes
# idle, before the model can finish handling the message.
queued_clear_during_tool=no
queued_clear_after_tool=no
rs_keys ref-clear-accept '/clear' Enter
sleep 1
rs_keys ref-clear-accept C-x C-s
if wait_new_record_file "$WORK/records-before-queued.txt" 25 >"$WORK/records-queued-clear.txt"; then
	queued_clear_during_tool=yes
else
	rs_wait_turn_idle ref-clear-accept 30 || true
	rs_keys ref-clear-accept '/clear' Enter
	if wait_new_record_file "$WORK/records-before-queued.txt" 20 >"$WORK/records-queued-clear.txt"; then
		queued_clear_after_tool=yes
	fi
fi
sleep 1
rs_snap ref-clear-accept queued-clear.txt

# Whether the message was already handled before /clear is recorded separately:
# in that case the run cannot answer whether an unprocessed message survives
# /clear, and the criterion is reported as such rather than as a failed
# delivery.
message_processed_before_clear=no
while IFS= read -r record; do
	[[ -n "$record" ]] || continue
	if grep -qF -- 'queued-before-clear' "$record" &&
		assistant_after_marker "$record" 'queued-before-clear'; then
		message_processed_before_clear=yes
	fi
done <"$WORK/records-before-queued.txt"
rs_excerpt 'queued-before-clear' queued-before-clear.jsonl

# Poll only the record files created after the snapshot. The criterion is
# satisfied when the message is there and an assistant record follows it.
queued_criterion='unverifiable'
queued_message_file=''
queued_assistant_ok=no
queued_deadline=$((SECONDS + 90))
while ((SECONDS < queued_deadline)); do
	list_session_records >"$WORK/records-now.txt"
	comm -13 "$WORK/records-before-queued.txt" "$WORK/records-now.txt" >"$WORK/records-new-now.txt" || true
	: >"$OUT/queued-after-clear.jsonl"
	: >"$OUT/queued-after-clear-files.txt"
	: >"$OUT/queued-assistant.txt"
	queued_message_file=''
	while IFS= read -r record; do
		[[ -n "$record" ]] || continue
		if grep -qF -- 'queued-before-clear' "$record"; then
			printf '%s\n' "$record" >>"$OUT/queued-after-clear-files.txt"
			grep -F -- 'queued-before-clear' "$record" >>"$OUT/queued-after-clear.jsonl" || true
			queued_message_file="$record"
		fi
		jq -Rr '
			fromjson?
			| select(type == "object" and .type == "assistant")
			| ((.message.content? // empty)
				| if type == "string" then .
				  elif type == "array" then [.[] | select(.type == "text") | .text] | join("\n")
				  else "" end)
		' "$record" >>"$OUT/queued-assistant.txt" 2>/dev/null || true
	done <"$WORK/records-new-now.txt"
	if [[ -n "$queued_message_file" ]] &&
		assistant_after_marker "$queued_message_file" 'queued-before-clear'; then
		queued_criterion='satisfied'
		break
	fi
	if [[ -n "$queued_message_file" ]]; then
		queued_criterion='message-present-no-assistant-turn'
	else
		queued_criterion='cleared-no-message'
	fi
	sleep 1
done
if [[ -s "$OUT/queued-assistant.txt" ]] &&
	grep -qiE '(^|[^[:alnum:]_])ok([^[:alnum:]_]|$)' "$OUT/queued-assistant.txt"; then
	queued_assistant_ok=yes
fi
if [[ ! -s "$WORK/records-queued-clear.txt" ]]; then
	queued_criterion='unverifiable'
fi
if [[ "$queued_criterion" == 'cleared-no-message' && "$message_processed_before_clear" == yes ]]; then
	queued_criterion='message-processed-before-clear'
fi
{
	printf 'clear_during_tool=%s\n' "$queued_clear_during_tool"
	printf 'clear_after_tool=%s\n' "$queued_clear_after_tool"
	printf 'prompt_record_seen=%s\n' "$prompt_record_seen"
	printf 'new_record_files=%s\n' "$(wc -l <"$WORK/records-queued-clear.txt" | tr -d ' ')"
	printf 'message_file=%s\n' "$queued_message_file"
	printf 'assistant_ok=%s\n' "$queued_assistant_ok"
	printf 'message_processed_before_clear=%s\n' "$message_processed_before_clear"
	printf 'criterion=%s\n' "$queued_criterion"
} >"$OUT/queued-criterion.txt"
rs_obs "queued-before-clear criterion: $queued_criterion (clear_during_tool=$queued_clear_during_tool, clear_after_tool=$queued_clear_after_tool, assistant_ok=$queued_assistant_ok)"

{
	printf 'X0=%s\n' "$x0"
	printf 'X1=%s\n' "$x1"
	printf 'X2=%s\n' "$x2"
	printf 'Y1=%s\n' "$y1"
	printf 'Y2=%s\n' "$y2"
	printf 'X1_address=%s\n' "$x1_address"
	printf 'before_socket=%s\n' "$before_socket"
	printf 'after_socket=%s\n' "$after_socket"
	printf 'assistant_records_in_new_files=%s\n' "$assistant_turns"
	printf 'queued_criterion=%s\n' "$queued_criterion"
	printf 'queued_clear_during_tool=%s\n' "$queued_clear_during_tool"
	printf 'queued_assistant_ok=%s\n' "$queued_assistant_ok"
} >"$OUT/refs.txt"

# No pane capture can stand in for a model turn; the run fails when the observer
# prompt never reached a conversation or no model call was recorded.
rs_require_model_turn 'listing the other Claude Code sessions' 1
