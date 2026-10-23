# Build with the x11box repository as the context. The resulting image ID is
# persisted by Bridge; acceptance runs never follow a mutable tag.
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM node:24-bookworm-slim AS node-runtime

FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg bash \
    && curl -fsSL https://xpra.org/xpra.asc -o /usr/share/keyrings/xpra.asc \
    && echo 'deb [signed-by=/usr/share/keyrings/xpra.asc] https://xpra.org bookworm main' > /etc/apt/sources.list.d/xpra.list \
    && apt-get update && apt-get install -y --no-install-recommends \
       xpra xpra-x11 xpra-html5 x11-utils xauth x11-xserver-utils \
       wmctrl xdotool scrot imagemagick openbox ffmpeg chromium \
       fonts-noto-cjk procps iproute2 dbus-x11 \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --gid 1000 x11box && useradd --uid 1000 --gid 1000 --create-home x11box \
    && mkdir -p /run/user/1000 /var/lib/x11box/state /artifacts /workspace \
    && chown -R x11box:x11box /run/user/1000 /var/lib/x11box /artifacts /workspace \
    && chmod 700 /run/user/1000
COPY --from=builder /build/target/release/x11box /usr/local/bin/x11box
COPY --from=node-runtime /usr/local/bin/node /usr/local/bin/node
COPY --from=node-runtime /usr/local/bin/nodejs /usr/local/bin/nodejs
COPY --from=node-runtime /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -sf /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm \
    && ln -sf /usr/local/lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx \
    && ln -sf /usr/local/lib/node_modules/corepack/dist/corepack.js /usr/local/bin/corepack \
    && corepack enable
ENV HOME=/home/x11box LANG=C.UTF-8 XDG_RUNTIME_DIR=/run/user/1000
ENV XDG_STATE_HOME=/var/lib/x11box/state X11BOX_STATE_DIR=/var/lib/x11box/state/x11box
ENV X11ACT_STATE_DIR=/var/lib/x11box/state/x11box
USER x11box
WORKDIR /workspace
EXPOSE 27811 14500
CMD ["x11box", "agent", "serve", "--listen", "0.0.0.0:27811"]
