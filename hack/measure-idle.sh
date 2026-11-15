#!/usr/bin/env bash
# Measure resident memory of the driver processes with no CSI requests.
# This is not the sidecar total: sidecar images are separate processes and
# only exist in a real deployment. Use the kubectl variant below for those.
set -euo pipefail

cd "$(dirname "$0")/.."
GO=${GO:-go}
make build >/dev/null

tmp=$(mktemp -d)
trap 'kill ${CTL_PID:-} ${NODE_PID:-} 2>/dev/null || true; rm -rf "$tmp"' EXIT

./bin/zfs-csi --mode=controller --endpoint="unix://$tmp/controller.sock" \
  --pool=tank/k8s --peer-discovery=false >"$tmp/controller.log" 2>&1 &
CTL_PID=$!

./bin/zfs-csi --mode=node --node-id=measure-node \
  --endpoint="unix://$tmp/node.sock" --pool=tank/k8s \
  --peer-listen=127.0.0.1:19809 --peer-discovery=false >"$tmp/node.log" 2>&1 &
NODE_PID=$!

for _ in $(seq 1 50); do
  [ -S "$tmp/controller.sock" ] && [ -S "$tmp/node.sock" ] && break
  sleep 0.1
done
sleep 3

rss_mib() {
  local pid=$1
  local kb
  kb=$(awk '/VmRSS:/ {print $2}' "/proc/$pid/status")
  awk -v kb="$kb" 'BEGIN {printf "%.1f", kb/1024}'
}

echo "controller plugin: $(rss_mib "$CTL_PID") MiB RSS"
echo "node plugin:       $(rss_mib "$NODE_PID") MiB RSS"
echo
echo "For sidecars in a cluster:"
echo "  kubectl -n zfs-csi top pod -l app.kubernetes.io/name=zfs-csi"
echo "  kubectl -n zfs-csi get pod -l app.kubernetes.io/name=zfs-csi -o json \\"
echo "    | jq '[.items[].spec.containers[].resources]'"
echo
echo "Logs kept at $tmp (removed on exit)."
