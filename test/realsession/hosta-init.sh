#!/bin/sh
# hosta-init.sh prepares the peer host container: it writes the Broker and
# simulated Agent configuration, starts the Broker, and prints the container IP.
set -eu

# The container runs as root, so the Broker's runtime directory is /run/peers,
# not the per-uid /tmp/peers-<uid>.
BROKER_SOCKET=/run/peers/broker.sock

mkdir -p /root/.peers /rs/cfg/sessions /rs/work
# The Broker refuses a data directory that is accessible by group or other.
chmod 700 /root/.peers /rs/cfg/sessions

cat >/root/.peers/config.yaml <<'EOF'
broker:
  listen: 0.0.0.0:17443
  alias: hostA
EOF
chmod 600 /root/.peers/config.yaml

cat >/rs/work/.peers.yaml <<'EOF'
topics: [rs-probe]
EOF

peers service run --user >/rs/broker.log 2>&1 &
broker_pid=$!

# Wait for the socket: the Broker announces readiness only on standard error,
# which goes to the log.
attempt=0
while [ ! -S "$BROKER_SOCKET" ]; do
	attempt=$((attempt + 1))
	if [ "$attempt" -gt 100 ]; then
		echo "hosta-init: broker socket $BROKER_SOCKET did not appear" >&2
		exit 1
	fi
	if ! kill -0 "$broker_pid" 2>/dev/null; then
		echo "hosta-init: broker exited before creating $BROKER_SOCKET" >&2
		cat /rs/broker.log >&2 || true
		exit 1
	fi
	sleep 0.1
done

# Print only the address: the caller captures the whole output as the container IP.
hostname -i | awk '{print $1}'
