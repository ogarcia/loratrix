# loratrix

A minimal relay between LoRa mesh networks and Matrix rooms. It bridges
**Meshtastic** and **MeshCore** — either one, or both at the same time — and
does nothing else: no plugins, no database, no maps, no telemetry.

It exists because the relay it replaces, the Python
[meshtastic-matrix-relay](https://github.com/jeremiah-k/meshtastic-matrix-relay),
carries 42k lines of source and 90-180 MB of RSS to do a job that needs neither.

## Status

Complete for what it set out to be: both backends and the Matrix side are
implemented and tested, over TCP and serial. BLE is the one transport still
missing, and encrypted Matrix rooms the one feature.

Verified against real radios and a real homeserver, not only against tests: the
MeshCore handshake and its 350 contacts, the Meshtastic session and its 200
nodes, a message sent to a LoRa channel that a stranger answered, that answer
arriving back and being parsed, and 80 real Matrix events read.

## What it does

- Bidirectional relay between mesh channels and Matrix rooms, mapped 1:1.
- Several mesh networks in one process, Meshtastic and MeshCore mixed freely.
- A Matrix room may carry more than one route, which also bridges those meshes
  to each other.
- Optional read-only DM inbox: direct messages sent to the relay's node are
  mirrored into a Matrix room where you can read them but not answer.
- Duplicate and echo suppression, so bridged meshes do not loop.
- Replies: answering a message in Matrix reaches the mesh as a reply to that
  message, in whichever way the protocol there expresses one.

Deliberately out of scope: reactions, media, plugins, message history.

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

That comes to about 6.4 MB. Building it needs `musl-tools` on the host, because
rustls brings aws-lc-rs as its crypto provider and that compiles C of its own,
with `cc` rather than cmake.

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
podman build -t loratrix .
podman run --rm --volume ~/.config/loratrix:/etc/loratrix:ro ghcr.io/ogarcia/loratrix
```

The image is Alpine with the static binary and a CA store, running as uid 1000,
and it reads `$LORATRIX_CONFIG`, which the image points at
`/etc/loratrix/config.toml`. BLE is left out of it: the radios are reached over
TCP, and BLE would drag a C dependency into an image that has no need for one.

CI compiles the binary for amd64 and arm64 and the image build reuses those
artifacts rather than compiling again, which is what
`--build-arg BINARY_SOURCE=prebuilt` selects. An ordinary local build compiles in
the image and needs no `dist/`.

## Configure

Copy `config.example.toml` to `~/.config/loratrix/config.toml` and edit it. Then
check what the relay understood before starting it:

```sh
loratrix --check
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

Logging follows `RUST_LOG`, for example `RUST_LOG=loratrix=debug`.

## The two protocols are not symmetric

Worth knowing, because it shows up in the output:

- **Meshtastic** packets carry the sender's node number, resolved to a name
  against the radio's node database.
- **MeshCore** channel messages carry no sender identity at all. Every client
  makes up for that by writing the name into the text, as `NAME: MESSAGE`, so
  that is where the relay recovers it from — the first `": "` is the one the
  client wrote, and everything after it was typed by a person. A message that
  does not follow the convention keeps its text whole and shows
  `unknown_sender_name` instead. MeshCore *direct* messages do carry identity, as
  a six-byte public-key prefix resolved against the contact list.

  So `EA1HYZ 📟: hola` on a MeshCore channel reaches Matrix as
  `[EA1HYZ 📟/mc]: hola`, which is what a Meshtastic message looks like too.

This is also why echo suppression is content-based rather than sender-based, and
why every prefix template comes in three: a general one and a per-protocol
override that wins where it is set. Meshtastic carries a short name as well as a
long one, so `[{short}/{long}]` is worth writing there and meaningless on
MeshCore. `{name}` is the placeholder that is never left blank.

A radio, either kind, accepts **one connection at a time**, so while the relay is
attached nothing else can be. If you want to keep using a phone app against the
same radio, put a multiplexer in front of it — for MeshCore,
[coresplitter](https://github.com/ogarcia/coresplitter) does exactly that — and
point `transport` at the multiplexer instead.

## Replies are two conventions, not one feature

Answer a message in Matrix and the answer reaches the mesh as an answer. What
that means differs by protocol, in the same way sender identity does.

- **Meshtastic** has a field for it. The packet carries the id of the one it
  replies to, and it costs the text nothing.
- **MeshCore** has no such field, so its clients do what they already do with
  the sender's name and write it into the text:

  ```text
  ALICE: Hola a todos
  BOB: @[ALICE] Hola Alice
  ```

  Which points at the person rather than at the message, that being all the
  protocol says. It also spends payload, so the marker is counted before the
  text is cut to fit — otherwise a reply would lose exactly the room the marker
  takes.

Tying the two together needs a memory of which mesh message each Matrix event
is. It is held in memory, keyed by event *and network* — one event bridged to
two meshes is two different messages — and entries expire after
`reply_window_secs`, twelve hours by default. Reading one does not remove it: a
message may be answered many times.

Answering something the relay no longer remembers, or that never reached that
mesh, is not an error. The message is delivered plainly, without its thread.

Matrix clients also quote the original into the body of a reply, as `> <@user>`
lines. That fallback is dropped before the text goes out, because on a payload
of a couple of hundred bytes the quote is charged against the answer.

The other direction — a reply written on the mesh arriving in Matrix as a rich
reply — is not implemented.

## A hop limit of zero is not a default

Meshtastic packets carry a hop limit: how many times another node may repeat them.
Zero is a legal value and it means *nobody repeats this*, so the message reaches
whatever is in earshot of the radio and stops there.

The firmware fills the field in for a client that left it empty, but only under one
condition:

```c
if (src == RX_SRC_USER && p->want_ack && p->hop_limit == 0)
    p->hop_limit = Default::getConfiguredOrDefaultHopLimit(config.lora.hop_limit);
```

A channel broadcast asks for no acknowledgement, so it does not qualify. The relay
therefore sets the limit itself, to whatever the radio reports in its own LoRa
config — the reference Python client does the same — and falls back to the
firmware's default of three where that is zero or above the ceiling of seven.

Worth knowing because of how it fails: the radio accepts the packet, queues it,
reports no error, and the relay logs a successful send. Everything looks right and
nothing is heard beyond the first hop.

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

### Checks that need hardware

Some things only a radio or a homeserver can answer, so those tests are ignored by
default and skip themselves when told nothing to talk to. They only listen, and
cost no airtime:

```sh
LORATRIX_MESHCORE_HOST=192.168.1.161 \
LORATRIX_MESHTASTIC_HOST=192.168.1.41 \
LORATRIX_MATRIX_CREDENTIALS=credentials.json \
LORATRIX_MATRIX_ROOMS='!one,!two' \
  cargo test --all-features -- --ignored --nocapture
```

`LORATRIX_MATRIX_HISTORY=6` additionally parses real events the rooms already
hold, which is what catches a homeserver whose JSON is not the shape we expect.

One test does transmit, and stays quiet unless told exactly what to say — on a
real mesh, real people read it:

```sh
LORATRIX_MESHCORE_HOST=192.168.1.161 LORATRIX_MESHCORE_CHANNEL=GALICIA \
LORATRIX_MESHCORE_SEND='Hello folks' \
  cargo test --all-features -- --ignored --nocapture sends_to
```
