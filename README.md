# meshtrix

A minimal relay between LoRa mesh networks and Matrix rooms. It bridges
**Meshtastic** and **MeshCore** — either one, or both at the same time — and
does nothing else: no plugins, no database, no maps, no telemetry.

It exists because the relay it replaces, the Python
[meshtastic-matrix-relay](https://github.com/jeremiah-k/meshtastic-matrix-relay),
carries 42k lines of source and 90-180 MB of RSS to do a job that needs neither.

## Status

Early, but no longer inert. The routing core, the configuration, the MeshCore
backend and the Matrix side are implemented and tested; the Meshtastic backend is
still a skeleton that reports it is not implemented. So a MeshCore mesh can be
bridged and a Meshtastic one cannot, yet.

Verified against real hardware and a real homeserver: the MeshCore handshake, a
contact list of 350, a message accepted for transmission, and 80 real Matrix
events parsed. What has not been seen end to end is a message arriving *from* a
mesh, for want of anybody sending one while it was watching.

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

What ships is a **static musl binary**: one file that brings its own libc and
needs nothing installed to run, which is the point on a router or an SBC.

```sh
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

That comes to about 5.7 MB. Building it needs `cmake` and `musl-tools` on the
host, because rustls brings aws-lc-rs as its crypto provider and that compiles C
of its own.

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

## Container

```sh
podman build -t meshtrix .
podman run --rm --volume ~/.config/meshtrix:/etc/meshtrix:ro ghcr.io/ogarcia/meshtrix
```

The image is Alpine with the static binary and a CA store, running as uid 1000,
and it reads `$MESHTRIX_CONFIG`, which the image points at
`/etc/meshtrix/config.toml`. BLE is left out of it: the radios are reached over
TCP, and BLE would drag a C dependency into an image that has no need for one.

CI compiles the binary for amd64 and arm64 and the image build reuses those
artifacts rather than compiling again, which is what
`--build-arg BINARY_SOURCE=prebuilt` selects. An ordinary local build compiles in
the image and needs no `dist/`.

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

A radio, either kind, accepts **one connection at a time**, so while the relay is
attached nothing else can be. If you want to keep using a phone app against the
same radio, put a multiplexer in front of it — for MeshCore,
[coresplitter](https://github.com/ogarcia/coresplitter) does exactly that — and
point `transport` at the multiplexer instead.

## Homeservers do not all filter

The relay asks `/sync` for the bridged rooms only, messages only, and no history
on the first request. Where that is honoured it saves a lot of traffic.

It is not always honoured. conduwuit ignores `/sync` filters entirely, inline or
registered alike, and answers with the last ten events of every joined room. So
the filter is treated as an optimization and never as a guarantee: starting from
now rather than from history, and relaying only bridged rooms, are both enforced
in the relay. Without that, every restart would read the tail of each room out
over the mesh.

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
