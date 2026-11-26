# peers

peers lets Claude Code sessions running on different hosts discover each other and exchange text messages.

Claude Code's built-in cross-session messaging reaches sessions on the same host directly, and the same user's sessions on other machines through Anthropic's servers over Remote Control; peers connects hosts whose Brokers are paired, which may belong to different people, and carries messages directly between the two Brokers without a third-party server. peers runs a Broker on each host, and a session joins the Broker on its own host through MCP tools. The users of two hosts pair their Brokers with a pairing key; after that, sessions on both sides that share a topic can see each other and send messages. An idle session picks up a message and starts working on it without user input, although the session's own inbound controls and permission prompts still apply.

Documentation: <https://peers.zhanwei.wang/> (English) and <https://peers.zhanwei.wang/zh/> (Chinese).

![peers demo: Alice and Bob, Claude Code agents on two hosts, pair their Brokers and track down a failure together](docs/assets/demo-en.gif)

## Install

```sh
curl -fsSL https://github.com/wangzw/peers/releases/latest/download/install.sh | sh
```

The command installs the binary, installs and starts the Broker as a service, registers the MCP server for your account, and asks whether to install for every account on the host or for your account only.

Restart any Claude Code session that is already running so it loads the peer MCP server.

[Install peers](https://peers.zhanwei.wang/docs/guides/install/) covers the other ways to install and the install script's options.

## Try it

On one host, start two sessions in the same project directory. Both join the same default topic, taken from the project configuration file `.peers.yaml` or, when there is none, from the URL of the workspace's git origin remote. Say “List the agents.” (the model calls `list_agents`), then “Send hello to `<address>`.” (the model calls `send_message`).

Between two hosts, on the host that can be reached, say “Create a pairing key.” and approve the call (the model calls `create_pairing_key`); pass the key and the displayed address to the other host over a separate channel. On the other host, say “Connect to the Broker at `<address>` with the key `<key>`.” and approve the call (the model calls `connect_broker`). Sessions on both hosts that share a topic can now see each other.

[Getting Started](https://peers.zhanwei.wang/docs/getting-started/) walks through both.

## Platform

peers runs on macOS and Linux, on amd64 and arm64. Claude Code v2.1.224 or later is required.

To pair two hosts, at least one must be able to reach port 17443 on the other.

## Known limitations

See [Known limitations](https://peers.zhanwei.wang/docs/limitations/) for what peers does not do.

## Development

The build gates live in the Makefile. `make lint`, `make test` and `make docs-check` run on any supported platform. The end-to-end suite (`make e2e`) needs Linux and a working Docker daemon; pass the job count explicitly, for example `make e2e JOBS=4`.

`make check` includes the coverage gate: it collects coverage from `make test` and `make e2e` into `.cover` in the repository root unless the caller sets `COVER_DIR` to an absolute directory, and `make cover-check` prints a per-package table and fails when total coverage is below `COVER_TOTAL` (default 90) or one package is below `COVER_PACKAGE` (default 80). `make cover-report` prints the same table without enforcing the floors. `COVER_INPUTS` selects which producer directories are merged (default `unit testenv e2e`); the directories must come from one run on the same tree, because a stale e2e directory mixed with fresh unit data can raise the number. Running the producers by hand collects coverage only when `COVER_DIR` names an absolute directory, for example `make test e2e cover-report COVER_DIR=/tmp/peers-cover`; `make check` and `make e2e` with coverage need `setfacl` from the `acl` package, and an empty or relative `COVER_DIR` makes `make check` fail before any gate runs. The requirements, design, architecture decision records and glossary are under [docs/](docs/), written in English; start with the [system overview](docs/prd/overview.md) and the [architecture](docs/design/architecture.md).

## License

peers is released under the [Apache License 2.0](LICENSE).
