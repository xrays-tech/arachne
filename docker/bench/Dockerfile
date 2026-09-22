# Runtime image for the statically-linked `arachne-node` binary.
#
# The binary is cross-compiled on the host with
#   cargo zigbuild --release -p arachne-node --target aarch64-unknown-linux-musl
# and copied into `bin/` before `docker build` (see the README note in this
# directory). Alpine is the base only for a shell + busybox `wget` healthcheck;
# the binary itself needs nothing from the image.
FROM alpine:latest

# Non-root runtime user.
RUN adduser -D -u 10001 arachne

# The WAL data dir. A named volume's ownership is copied up from this image
# dir on first mount, so pre-owning it lets the non-root user write.
RUN mkdir -p /data && chown arachne:arachne /data

COPY bin/arachne-node /usr/local/bin/arachne-node
RUN chmod +x /usr/local/bin/arachne-node

USER arachne

# Config is bind-mounted per service at /etc/arachne/node.toml; the WAL data
# dir is a named volume mounted at /data.
ENTRYPOINT ["/usr/local/bin/arachne-node"]
