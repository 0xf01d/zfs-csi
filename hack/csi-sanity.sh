#!/usr/bin/env bash
# Run csi-sanity against a running driver on a real ZFS node.
#
# Start the driver first, for example:
#   sudo ./bin/zfs-csi --mode=node --node-id=n1 \
#       --endpoint=unix:///tmp/csi.sock --pool=tank/k8s \
#       --peer-listen=127.0.0.1:9809 --peer=controller=127.0.0.1:9809
#
# csi-sanity exercises controller and node RPCs; it does not run Kubernetes.
set -euo pipefail

ENDPOINT=${CSI_ENDPOINT:-unix:///tmp/csi.sock}
SANITY=${CSI_SANITY:-csi-sanity}

if ! command -v "$SANITY" >/dev/null 2>&1; then
  cat >&2 <<EOF
csi-sanity not found. Install it with:
  go install github.com/kubernetes-csi/csi-sanity/cmd/csi-sanity@v0.11.0
then rerun with CSI_SANITY=\$(go env GOPATH)/bin/csi-sanity
EOF
  exit 1
fi

exec "$SANITY" \
  --csi.endpoint "$ENDPOINT" \
  --csi.testvolumeparameters '{"zfs-csi/test":"true"}' \
  --ginkgo.v
