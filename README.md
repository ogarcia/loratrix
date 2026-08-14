# meshtrix

A minimal relay between LoRa mesh networks and Matrix rooms. It bridges
**Meshtastic** and **MeshCore** — either one, or both at the same time — and
does nothing else: no plugins, no database, no maps, no telemetry.

It exists because the relay it replaces, the Python
[meshtastic-matrix-relay](https://github.com/jeremiah-k/meshtastic-matrix-relay),
carries 42k lines of source and 90-180 MB of RSS to do a job that needs neither.

## Status

Early. The routing core, configuration, prefix handling and duplicate
suppression are implemented and tested. The two mesh backends and the Matrix
`/sync` loop are skeletons that report they are not implemented yet, so the
relay does not relay anything at all so far. `--check` already works and is
useful for validating a configuration.

## What it does

- Bidirectional relay between mesh channels and Matrix rooms, mapped 1:1.
- Several mesh networks in one process, Meshtastic and MeshCore mixed freely.
- A Matrix room may carry more than one route, which also bridges those meshes
  to each other.
- Optional read-only DM inbox: direct messages sent to the relay's node are
  mirrored into a Matrix room where you can read them but not answer.
- Duplicate and echo suppression, so bridged meshes do not loop.

Deliberately out of scope: reactions, replies, media, plugins, message history.

End-to-end encrypted Matrix rooms are **not** supported yet. The Matrix side sits
behind a four-method trait so an implementation backed by `matrix-sdk` can be
added behind the `e2ee` feature without touching the rest.

## Build

```sh
cargo build --release
```

Cargo features:

| Feature      | Default | Effect                                          |
| ------------ | ------- | ----------------------------------------------- |
| `meshtastic` | yes     | Meshtastic backend                              |
| `meshcore`   | yes     | MeshCore backend                                |
| `ble`        | no      | Bluetooth Low Energy transports                 |
| `e2ee`       | no      | Reserved for Matrix end-to-end encryption       |

At least one backend is required; a build with neither is refused at compile
time. To build only what you use:

```sh
cargo build --release --no-default-features --features meshcore
```

## Configure

Copy `config.example.toml` to `~/.config/meshtrix/config.toml` and edit it. Then
check what the relay understood before starting it:

```sh
meshtrix --check
```

```text
room !bridge:example.org  (bridged: messages are also forwarded between these meshes)
  <-> lora channel 1 [meshtastic, 233 B payload, 2s min gap]
  <-> mc channel 1 [meshcore, 160 B payload, 2s min gap]
room !general:example.org
  <-> lora channel 0 [meshtastic, 233 B payload, 2s min gap]
room !dm:example.org <-- lora direct messages (read-only)
room !dm:example.org <-- mc direct messages (read-only)
```

Logging follows `RUST_LOG`, for example `RUST_LOG=meshtrix=debug`.

## The two protocols are not symmetric

Worth knowing, because it shows up in the output:

- **Meshtastic** packets carry the sender's node number, resolved to a name
  against the radio's node database.
- **MeshCore** channel messages carry no sender identity at all. The sender's
  name travels inside the message text by client convention, so the relay shows
  `unknown_sender_name` in the prefix and leaves the text untouched. MeshCore
  *direct* messages do carry identity, as a six-byte public-key prefix resolved
  against the contact list.

This is also why echo suppression is content-based rather than sender-based.

## Development

Activate the pre-commit hook once per clone; it enforces `cargo fmt` and
`cargo clippy`:

```sh
git config core.hooksPath .githooks
```

Any change to the feature flags should be checked against the whole matrix, not
just the default build:

```sh
cargo test --all-features
cargo test --no-default-features --features meshtastic
cargo test --no-default-features --features meshcore
```

## License

GPL-3.0-or-later.
