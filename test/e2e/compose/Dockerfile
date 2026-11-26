# Container image for the end-to-end tests.
#
# The system Broker and service installation unit derives its image from this
# one and keeps these names: the peer and other accounts, /usr/local/bin/peers
# and e2e-node, home /home/peer, and the control socket at /ctl/ctl.sock.
FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc

ARG UID=1000
ARG GID=1000

ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        iptables \
        iproute2 \
        netcat-openbsd \
        procps \
        tcpdump \
 && rm -rf /var/lib/apt/lists/*

# The peer account takes the host test process's uid and gid so files it writes
# under /ctl belong to the host user; reuse an existing matching account.
RUN if ! getent group "${GID}" >/dev/null; then groupadd --gid "${GID}" peer; fi \
 && if ! getent passwd "${UID}" >/dev/null; then \
        useradd --create-home --uid "${UID}" --gid "${GID}" peer; \
    fi \
 && useradd --create-home --uid 2000 other

COPY peers /usr/local/bin/peers
COPY e2e-node /usr/local/bin/e2e-node
RUN chmod 0755 /usr/local/bin/peers /usr/local/bin/e2e-node

WORKDIR /home/peer
USER peer
ENTRYPOINT ["e2e-node", "serve", "--ctl", "/ctl/ctl.sock", "--home", "/home/peer", "--bin", "/usr/local/bin/peers"]
