FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/zfs-csi ./cmd/zfs-csi

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        openssh-client \
        util-linux \
        zfsutils-linux \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/zfs-csi /usr/local/bin/zfs-csi
ENTRYPOINT ["/usr/local/bin/zfs-csi"]
