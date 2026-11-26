# Container image for the system Broker end-to-end tests: the base image plus
# systemd and the peers-users (gid 2001) and peers-admins (gid 2002) groups. A
# test adds members to peers-admins at run time.
ARG BASE
FROM ${BASE}

USER root

ENV LANG=C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        systemd \
        systemd-sysv \
        dbus \
        dbus-user-session \
        libpam-systemd \
        libnss-extrausers \
        acl \
        sudo \
        curl \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 2001 peers-users \
 && groupadd --gid 2002 peers-admins \
 && usermod -aG peers-users peer

# diruser and dirgroup exist only in the extrausers source so the Broker must
# resolve them through NSS. dirshare is only a supplementary group of diruser,
# so only an initgroups lookup grants access through it. The locked shadow entry
# exists because starting diruser's user manager goes through PAM, which needs
# one to set up the session.
RUN mkdir -p /var/lib/extrausers \
 && printf 'diruser:x:2100:2100:Directory Services User:/home/diruser:/bin/sh\n' \
        > /var/lib/extrausers/passwd \
 && printf 'dirgroup:x:2100:\ndirshare:x:2101:diruser\n' > /var/lib/extrausers/group \
 && printf 'diruser:*:19000:0:99999:7:::\n' > /var/lib/extrausers/shadow \
 && chgrp shadow /var/lib/extrausers/shadow \
 && chmod 0644 /var/lib/extrausers/passwd /var/lib/extrausers/group \
 && chmod 0640 /var/lib/extrausers/shadow \
 && mkdir -p /home/diruser \
 && chown 2100:2100 /home/diruser \
 && chmod 0755 /home/diruser \
 && sed -i -E \
        -e 's/^(passwd:[[:space:]]*).*/\1files extrausers/' \
        -e 's/^(group:[[:space:]]*).*/\1files extrausers/' \
        -e 's/^(shadow:[[:space:]]*).*/\1files extrausers/' \
        /etc/nsswitch.conf \
 && grep -Eq '^passwd:[[:space:]]+files[[:space:]]+extrausers' /etc/nsswitch.conf \
 && grep -Eq '^group:[[:space:]]+files[[:space:]]+extrausers' /etc/nsswitch.conf \
 && grep -Eq '^shadow:[[:space:]]+files[[:space:]]+extrausers' /etc/nsswitch.conf

STOPSIGNAL SIGRTMIN+3
ENTRYPOINT []
CMD ["/sbin/init"]
