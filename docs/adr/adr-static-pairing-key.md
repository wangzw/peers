# Take the pre-provisioned static key from the configuration file

## Context

A one-time pairing key is valid for a limited time and becomes invalid after one use, so pairing each host needs someone to generate one on the [Broker](../glossary.md) that accepts the pairing. When several hosts are deployed in bulk, no one is present to generate one each time, and the deployment needs a key that can be determined before deployment and written into each host's configuration. Such a key stays valid for a long time and can be reused, so a leak has graver consequences than a one-time pairing key, and every host paired with it knows it. Content read and written through tools enters the context and the session record of the model in the session ([ADR-config-through-tools](adr-config-through-tools.md)).

## Options Considered

### Generate it at run time in the system and take it out through a tool

The Broker generates a reusable key at random when the user enables it and saves it, and the user takes it out through a tool and distributes it to the hosts.

**Rejected**: the key exists only after the Broker starts, so it cannot be written into each host's configuration before deployment.

### Give the reusable key a validity period or a use count

The reusable key becomes invalid after a period, or after a number of uses.

**Rejected**: a pre-provisioned key has to stay usable throughout the deployment, and the period and the count would each have to be managed separately.

### Attach a piece of the acceptor's fingerprint beside the key for the dialer to check

Besides the key, the acceptor gives a piece of its own certificate fingerprint, and the dialer checks the other side's certificate fingerprint after connecting and proceeds to the handshake only when they match.

**Rejected**: the acceptor's fingerprint is produced only after its first start, so a bulk deployment would have to start each acceptor, take out its fingerprint and write it into each dialer's configuration first, and the key could no longer be determined before deployment.

### Allow it to be set or read through a tool

Like other configuration items, the reusable key can be set through a tool and its value can be read out through a tool.

**Rejected**: a long-lived secret would enter the model's context and the session record.

### Pre-provision it in the configuration file, effective when it has a value, limited only by length

The user or administrator writes the key into the Broker's configuration file; it takes effect when it has a value and is not enabled without one. The system only checks its length, does not generate it and does not save it separately; editing the configuration file sets, replaces or removes it, with no restart needed.

**Adopted**; see Decision.

## Decision

The reusable pairing key is a pre-provisioned static key taken only from the Broker's configuration file; it takes effect when it has a value, does not expire and has no use limit. The system does not generate it, does not save it separately and does not set it or return its value through a tool; the system only limits its length, and compares the value as it is.

## Consequences

A bulk deployment can write the same key into each host's configuration before deployment and reuse it together with the acceptor's address. When none is set, no long-lived pairing key exists on the Broker. Reading configuration through a tool does not return the key's value; the model in the session still reads it if it reads the configuration file through file operations. Editing the configuration file sets, replaces or removes the key, with no restart needed; removing or replacing it only stops later pairings, and a peer already paired with it is disconnected only when it is unpaired one by one.

**Costs accepted:**

- The randomness of the key is the responsibility of whoever sets it, and a minimum length cannot keep out a key that is long enough yet easy to guess.
- Before verifying the other side, the dialer sends a lookup tag computed from the key: a party that can intercept the pairing handshake, and any party the dialer connects to because of a mistyped address or a hijacked domain name, can use it to brute-force a weak key offline. The tag is bound to the dialer's fingerprint and cannot be handed to a third party for replay.
- Pairing attempts are counted by source and have a global cap; a party that can send requests from many sources, such as a whole IPv6 prefix, can still fill the global cap, which makes pairing by static key receive a temporary try-later response, while one-time pairing keys are unaffected.
- Every host paired with it knows it; such a host that can intercept or misdirect a new host's connection can impersonate the acceptor and complete pairing.
- The key is stored in plain text in the configuration file, so every account that can read the configuration file can read it and use it to make this Broker add a peer; an account with only read permission on the configuration file can therefore also complete the management operation of pairing.
- After a leak the key can only be replaced and the existing pairings then unpaired one by one; after a replacement, every deployment configuration that references the old key has to be updated with it.
- When a host pairs successfully by the pre-provisioned static key, the system only writes an audit log and does not notify the user separately.
