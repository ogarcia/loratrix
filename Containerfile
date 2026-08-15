# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 Óscar García Amor <ogarcia@connectical.com>

# The versions this build rests on, written down once. Here rather than in a file
# beside it because this is the only place the build itself can read, which is
# what lets the obvious thing work:
#
#   podman build -t loratrix .
#
# Both name a minor line rather than a patch, so fixes arrive without a commit,
# the same bargain the dependencies are on.
ARG ALPINE_VERSION=3.24
ARG RUST_VERSION=1

# Which backends the compiled-here path builds. BLE is left out: it would pull
# btleplug and libdbus-sys, which is a C dependency this image has no reason to
# carry when the radios are reachable over TCP.
ARG FEATURES=meshtastic,meshcore

# Which of the two stages below the binary comes from. Declared out here because
# global scope is the only one a FROM can read.
ARG BINARY_SOURCE=builder

# --- the binary, compiled here -----------------------------------------------

FROM docker.io/library/rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG FEATURES

# Nothing to install. gcc and musl-dev come with the base image, without which
# rustc could not link at all, and they are also what aws-lc-sys needs: rustls
# brings it as its crypto provider and it compiles C of its own, but with cc
# rather than cmake unless AWS_LC_SYS_CMAKE_BUILDER says otherwise.

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# No target is named: this image's own is a musl one, whichever architecture it
# runs on, and building for the host is what makes the same file work on x86_64
# and aarch64 alike.
#
# The caches are what make a second build quick. They also mean the binary is
# produced inside a mount that does not survive the step, hence copying it out
# before the shell exits.
#
# What ships is one file that brings its own libc, which the musl target gives
# for nothing and no flag here asks for. A dynamically linked executable names an
# interpreter to load it, so finding one would mean the ground had moved — a
# patched compiler, a different base image — and that is worth hearing about here
# rather than in production.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    set -eux; \
    cargo build --release --locked --no-default-features --features "${FEATURES}"; \
    install -D target/release/loratrix /out/loratrix; \
    if readelf -l /out/loratrix | grep -q INTERP; then \
        echo "loratrix is not statically linked" >&2; \
        exit 1; \
    fi

# --- the binary, built somewhere better --------------------------------------

# CI compiles with a proper cargo cache and publishes the binary as an artifact;
# repeating that work inside a container image would be slower and would prove
# nothing. Point the build at what it already made, under dist/:
#
#   podman build --build-arg BINARY_SOURCE=prebuilt -t loratrix .
#
# Only the stage that gets used is assembled, so dist/ need not exist for an
# ordinary build from source.
#
# The name carries the architecture in the build's own vocabulary rather than
# Rust's, which is what lets one multi-platform build pick each binary out of the
# same directory. Nothing is compiled along that path, so the only thing an
# emulator has to run is the last stage's handful of shell.
FROM scratch AS prebuilt
ARG TARGETARCH
COPY dist/loratrix-${TARGETARCH} /out/loratrix

# --- whichever of those two it was -------------------------------------------

# COPY --from takes no variables, so the choice cannot be made where the copying
# happens. A stage whose only job is to have a fixed name resolves it, and the
# one it does not name is never assembled.
FROM ${BINARY_SOURCE} AS binary

# --- the image that ships ----------------------------------------------------

FROM docker.io/library/alpine:${ALPINE_VERSION}

LABEL org.opencontainers.image.title="loratrix" \
      org.opencontainers.image.description="Relay between Meshtastic/MeshCore meshes and Matrix rooms" \
      org.opencontainers.image.source="https://github.com/ogarcia/loratrix" \
      org.opencontainers.image.licenses="GPL-3.0-or-later"

# The binary brings its own libc but not its own idea of who to trust. rustls
# reads the system store to verify the homeserver, so without this every request
# fails on an unknown issuer — and having the store here is also what lets an
# operator drop a private CA in and have a self-hosted homeserver just work.
RUN apk add --no-cache ca-certificates

# The mode is set rather than inherited because a binary that has travelled
# through a CI artifact arrives without its executable bit: zip does not carry
# permissions.
COPY --from=binary --chmod=0755 /out/loratrix /usr/local/bin/loratrix

# Never root. The id is 1000 because that is the first one a Linux desktop hands
# out, so a bind mounted config usually belongs to it already; anything else is
# what --user is for.
RUN addgroup -g 1000 loratrix \
 && adduser -D -H -G loratrix -u 1000 loratrix \
 && mkdir -p /etc/loratrix

# Named in the environment rather than appended to the entrypoint, because any
# argument given to `podman run` replaces CMD wholesale and would take the path
# with it. This way `--check` and `--version` work without repeating it.
ENV LORATRIX_CONFIG=/etc/loratrix/config.toml

# No EXPOSE: the relay dials out to the radios and to the homeserver and listens
# on nothing. Nor a VOLUME, since the only thing to mount is a config file the
# relay never writes to, and where it lives is the operator's to decide.
#
# No HEALTHCHECK either. There is no endpoint to ask, and what "healthy" means
# for a relay whose radio has gone quiet is a question with more than one right
# answer.

USER loratrix
ENTRYPOINT ["/usr/local/bin/loratrix"]
