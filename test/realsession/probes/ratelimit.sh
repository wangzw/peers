#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR
# shellcheck source=lib.sh
source "$(dirname "$0")/lib.sh"

# Messages through one Broker share its token bucket; the counts come from the session transcript's user records.

LOW_CALL_PROMPT='When a cross-session message arrives, reply with the single word ok and do nothing else. Do not call any tool unless the user explicitly asks.'
SLEEP_PROMPT='Run "sleep 60" with the Bash tool, then reply done.'
TARGET='rl-rx@hostB'

# The probe's own budget check stops it when the model-call count exceeds twice this estimate.
RL_CALL_ESTIMATE=10
RL_CALL_BUDGET=$((RL_CALL_ESTIMATE * 2))

rl_now_ms() {
	date +%s%3N
}

rl_wait_broker() {
	local seconds="$1"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if "$R/run" rsprobe status >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	"$R/run" rsprobe status >/dev/null 2>&1
}

rl_wait_socket() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if rs_socket_of "$name" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.2
	done
	rs_socket_of "$name" >/dev/null 2>&1
}

rl_status_is() {
	local name="$1" want="$2"
	"$R/run" rsprobe status 2>/dev/null | grep -qE -- " ${name}@[^ ]* status=${want}( |\$)"
}

rl_wait_status() {
	local name="$1" want="$2" seconds="$3"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if rl_status_is "$name" "$want"; then
			return 0
		fi
		sleep 0.5
	done
	rl_status_is "$name" "$want"
}

rl_agent_known() {
	local name="$1"
	"$R/run" rsprobe status 2>/dev/null | grep -qE -- " ${name}@[^ ]* status="
}

rl_wait_agent() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds))
	while ((SECONDS < deadline)); do
		if rl_agent_known "$name"; then
			return 0
		fi
		sleep 0.2
	done
	rl_agent_known "$name"
}

rl_user_text() {
	local file="$1"
	jq -r '
		select(.type == "user")
		| (.message.content // "")
		| if type == "string" then . else ([.[] | .text // ""] | join("\n")) end
	' "$file" 2>/dev/null || true
}

rl_arrived() {
	local body="$1" file count=0 n
	while IFS= read -r -d '' file; do
		n="$(rl_user_text "$file" | grep -cxF -- "$body" || true)"
		count=$((count + n))
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
	printf '%s\n' "$count"
}

rl_total_user_lines() {
	local file total=0 n
	while IFS= read -r -d '' file; do
		n="$(rl_user_text "$file" | wc -l)"
		total=$((total + n))
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
	printf '%s\n' "$total"
}

# Each queued message records an enqueue, dequeue or remove operation, so a positive depth means messages still wait even when the Agent is idle.
rl_queue_counts() {
	local file op e=0 d=0 r=0
	while IFS= read -r -d '' file; do
		while IFS= read -r op; do
			case "$op" in
				enqueue) e=$((e + 1)) ;;
				dequeue) d=$((d + 1)) ;;
				remove) r=$((r + 1)) ;;
			esac
		done < <(jq -r 'select(.type == "queue-operation") | .operation // empty' "$file" 2>/dev/null)
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
	printf '%s %s %s %s\n' "$((e - d - r))" "$e" "$d" "$r"
}

rl_model_calls() {
	"$R/run" rsprobe env usage --root "$R" 2>/dev/null \
		| awk '$1 == "total" { value = $2 } END { if (value != "") print value }' || true
}

rl_check_budget() {
	local calls
	calls="$(rl_model_calls)"
	if [[ ! "$calls" =~ ^[0-9]+$ ]]; then
		return 0
	fi
	if ((calls > RL_CALL_BUDGET)); then
		rs_obs "budget: $calls model calls exceed twice the estimate of $RL_CALL_ESTIMATE; stopping"
		printf 'ratelimit probe stopped: %d model calls exceed the budget of %d\n' \
			"$calls" "$RL_CALL_BUDGET" >&2
		exit 1
	fi
}

rl_wait_quiescent() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) current last=-1 stable=0
	while ((SECONDS < deadline)); do
		current="$(rl_total_user_lines)"
		if [[ "$current" == "$last" ]] && rl_status_is "$name" idle; then
			stable=$((stable + 1))
			if ((stable >= 5)); then
				return 0
			fi
		else
			stable=0
		fi
		last="$current"
		sleep 1
	done
	return 1
}

# An idle status alone is not enough: a turn that received messages can end while they are still queued.
rl_wait_drained() {
	local name="$1" seconds="$2"
	local deadline=$((SECONDS + seconds)) counts last="" stable=0 depth
	while ((SECONDS < deadline)); do
		counts="$(rl_queue_counts)"
		depth="${counts%% *}"
		if [[ "$counts" == "$last" ]] && ((depth <= 0)) && rl_status_is "$name" idle; then
			stable=$((stable + 1))
			if ((stable >= 5)); then
				return 0
			fi
		else
			stable=0
		fi
		last="$counts"
		sleep 1
	done
	return 1
}

rl_send_checked() {
	local sender="$1" body="$2" out ok
	out="$(rs_call "$sender" send_message "{\"to\":\"$TARGET\",\"message\":\"$body\"}")"
	ok="$(printf '%s' "$out" | jq -r '.ok // false')"
	if [[ "$ok" != "true" ]]; then
		printf '%s %s\n' "$sender" "$out" >>"$OUT/send-failures.txt"
		return 1
	fi
	return 0
}

rl_burst_range() {
	local prefix="$1" first="$2" count="$3" i body sender failed=0
	for ((i = first; i < first + count; i++)); do
		if ((i % 2 == 1)); then
			sender=rl-s1
		else
			sender=rl-s2
		fi
		body="$prefix-$i"
		if ! rl_send_checked "$sender" "$body"; then
			failed=$((failed + 1))
		fi
	done
	if ((failed > 0)); then
		printf '%s %d..%d: %d of %d sends failed\n' \
			"$prefix" "$first" "$((first + count - 1))" "$failed" "$count" >>"$OUT/send-failures.txt"
	fi
}

rl_burst() {
	rl_burst_range "$1" 1 "$2"
}

rl_write_arrivals() {
	local prefix="$1" total="$2" i n
	: >"$OUT/arrivals-$prefix.tsv"
	for ((i = 1; i <= total; i++)); do
		n="$(rl_arrived "$prefix-$i")"
		printf '%s-%d\t%s\n' "$prefix" "$i" "$n" >>"$OUT/arrivals-$prefix.tsv"
	done
}

rl_write_queue() {
	rl_queue_counts >"$OUT/queue-$1.txt"
}

# rl_write_bodies records every delivered body line, in order, so a late
# delivery stays visible after the per-step count has already been taken.
rl_write_bodies() {
	local label="$1" file
	: >"$OUT/bodies-$label.txt"
	while IFS= read -r -d '' file; do
		rl_user_text "$file" | grep -E '^(rl|re|rf|q)-[0-9]+$' >>"$OUT/bodies-$label.txt" || true
	done < <(find "$R/cfg/projects" -type f -name '*.jsonl' -print0 2>/dev/null)
}

rl_count_arrivals() {
	local prefix="$1" total="$2" i n arrived_count=0
	for ((i = 1; i <= total; i++)); do
		n="$(rl_arrived "$prefix-$i")"
		if ((n >= 1)); then
			arrived_count=$((arrived_count + 1))
		fi
	done
	printf '%s\n' "$arrived_count"
}

# A test sources this script with RS_SOURCE_ONLY=1 to exercise the helpers above without starting the probe.
if [[ "${RS_SOURCE_ONLY:-}" == "1" ]]; then
	return 0
fi

# Start the Broker explicitly so its structured send log is kept as evidence.
"$R/run" peers service run --user >>"$OUT/broker.log" 2>&1 &
rl_wait_broker 15
rs_obs "broker started; its send log is $OUT/broker.log"

rs_tmux_start
rs_window rl-rx claude --model haiku -n rl-rx \
	--permission-mode default \
	--append-system-prompt "$LOW_CALL_PROMPT" \
	--allowedTools "Bash(sleep:*)"
rl_wait_socket rl-rx 30
if ! rs_wait_ready rl-rx 45; then
	rs_obs "rl-rx did not reach its input prompt within 45s"
	exit 1
fi
rs_obs_mode rl-rx
rs_obs "receiving session rl-rx registered"

rs_agent rl-s1
rs_agent rl-s2
rl_wait_agent rl-s1 15
rl_wait_agent rl-s2 15
rs_obs "simulated senders rl-s1 and rl-s2 registered"

rs_send_prompt rl-rx "$SLEEP_PROMPT" 'Run "sleep 60"' 60
rl_wait_status rl-rx busy 30
burst_start="$(rl_now_ms)"
rl_burst rl 40
burst_ms=$(( $(rl_now_ms) - burst_start ))
rs_obs "step 1: 40 messages sent through the Broker in ${burst_ms}ms"
if ((burst_ms > 5000)); then
	rs_obs "step 1: warning: the burst took longer than 5s, so more tokens refilled"
fi
sleep 0.5
rl_write_queue step1-peak
rl_wait_quiescent rl-rx 180
rl_write_queue step1
rl_write_arrivals rl 40
N1="$(rl_count_arrivals rl 40)"
rs_obs "step 1: N1=$N1 of 40 distinct rl bodies arrived"
rl_check_budget

# The bucket holds 30 tokens and refills at 0.5 per second: a 40-message burst exhausts it, and ten seconds later about 5 of 15 sends should arrive.
rs_send_prompt rl-rx "$SLEEP_PROMPT" 'Run "sleep 60"' 60
rl_wait_status rl-rx busy 30
burst_start="$(rl_now_ms)"
rl_burst re 40
burst_ms=$(( $(rl_now_ms) - burst_start ))
rs_obs "step 2: 40 messages sent in ${burst_ms}ms until the bucket was exhausted"
if ((burst_ms > 10000)); then
	rs_obs "step 2: warning: the exhausting burst took longer than 10s, so more tokens refilled"
fi
sleep 0.5
rl_write_queue step2-peak
sleep 10
burst_start="$(rl_now_ms)"
rl_burst rf 15
burst_ms=$(( $(rl_now_ms) - burst_start ))
rs_obs "step 2: 15 messages sent in ${burst_ms}ms after a 10s pause"
sleep 0.5
rl_write_queue step2-rf-peak
rl_wait_quiescent rl-rx 180
rl_write_queue step2
rl_write_arrivals re 40
rl_write_arrivals rf 15
N2_exhaust="$(rl_count_arrivals re 40)"
N2="$(rl_count_arrivals rf 15)"
rs_obs "step 2: $N2_exhaust of 40 exhausting sends arrived, so throttling occurred when below 40"
rs_obs "step 2: N2=$N2 of 15 distinct rf bodies arrived (expected about 5, +-1)"
rl_check_budget

# Identical text from one sender is dropped as a duplicate; the same text from another sender differs in the envelope and is not.
rl_send_checked rl-s1 dup-same || rs_obs "step 3: send dup-same failed"
sleep 1
rl_send_checked rl-s1 dup-same || rs_obs "step 3: second send dup-same failed"
rl_send_checked rl-s1 dup-x || rs_obs "step 3: send dup-x from rl-s1 failed"
rl_send_checked rl-s2 dup-x || rs_obs "step 3: send dup-x from rl-s2 failed"
rl_wait_quiescent rl-rx 120
rl_write_queue step3
dup_same="$(rl_arrived dup-same)"
dup_x="$(rl_arrived dup-x)"
rs_obs "step 3: dup-same arrived $dup_same time(s), dup-x arrived $dup_x time(s)"
rs_excerpt dup-same dedup-same.jsonl
rs_excerpt dup-x dedup-x.jsonl
rl_check_budget

# 60 direct frames from separate writer processes are exempt from the per-sender rate limit; the receiver's queue keeps at most 50.
rs_send_prompt rl-rx "$SLEEP_PROMPT" 'Run "sleep 60"' 60
rl_wait_status rl-rx busy 30
socket="$(rs_socket_of rl-rx)"
frame_status=0
"$R/run" rsprobe frame --socket "$socket" --from-name q-worker@x \
	--body 'q-{i}' --fork-each --count 60 \
	>"$OUT/frames.txt" 2>"$OUT/frames.err" || frame_status=$?
rs_obs "step 4: 60 fork-each frames written, exit status $frame_status"
sleep 0.5
rl_write_queue step4-peak
rl_wait_quiescent rl-rx 180
rl_write_queue step4
rl_write_arrivals q 60
N3="$(rl_count_arrivals q 60)"
rs_obs "step 4: N3=$N3 of 60 distinct q bodies arrived"
rl_check_budget

{
	printf 'N1_rl=%s\n' "$N1"
	printf 'N2_exhaust_re=%s\n' "$N2_exhaust"
	printf 'N2_rf=%s\n' "$N2"
	printf 'N3_q=%s\n' "$N3"
	printf 'dup_same=%s\n' "$dup_same"
	printf 'dup_x=%s\n' "$dup_x"
	printf 'frames_status=%s\n' "$frame_status"
} >"$OUT/counts-early.txt"

# Counts taken at idle can miss messages still in the prompt queue; count again after the queue drains.
if rl_wait_drained rl-rx 120; then
	rs_obs "final: the prompt queue drained"
else
	rs_obs "final: the prompt queue did not drain within 120s; the final counts may be partial"
fi
rl_write_queue final
rl_write_arrivals rl 40
rl_write_arrivals re 40
rl_write_arrivals rf 15
rl_write_arrivals q 60
N1="$(rl_count_arrivals rl 40)"
N2_exhaust="$(rl_count_arrivals re 40)"
N2="$(rl_count_arrivals rf 15)"
N3="$(rl_count_arrivals q 60)"
rs_obs "final: N1=$N1 of 40 rl, N2_exhaust=$N2_exhaust of 40 re, N2=$N2 of 15 rf, N3=$N3 of 60 q"
rl_write_bodies final

{
	printf 'N1_rl=%s\n' "$N1"
	printf 'N2_exhaust_re=%s\n' "$N2_exhaust"
	printf 'N2_rf=%s\n' "$N2"
	printf 'N3_q=%s\n' "$N3"
	printf 'dup_same=%s\n' "$dup_same"
	printf 'dup_x=%s\n' "$dup_x"
	printf 'frames_status=%s\n' "$frame_status"
	printf 'queue_final=%s\n' "$(rl_queue_counts)"
} >"$OUT/counts.txt"

if [[ -f "$OUT/broker.log" ]]; then
	grep -o '"result":"[^"]*"' "$OUT/broker.log" | sort | uniq -c >"$OUT/broker-sends.txt" || true
fi

# A run whose sleep prompt never submitted would still count arrivals, so the prompt must be in a user record with a model call counted.
rs_require_model_turn 'Run "sleep 60" with the Bash tool' 1
