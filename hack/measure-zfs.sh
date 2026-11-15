#!/usr/bin/env bash
# Real-ZFS measurements: snapshot latency, clone latency, incremental import.
# Run as root on a node with ZFS. Creates and destroys temporary datasets
# below $POOL. It never touches non-test datasets.
set -euo pipefail

POOL=${POOL:-tank/k8s}
SIZE_MIB=${SIZE_MIB:-64}
NAME="pvc-measure-$$"
SRC="$POOL/$NAME"
DST="$POOL/$NAME-dst"
CLONE="$POOL/$NAME-clone"
MNT=$(mktemp -d)
MNT2=$(mktemp -d)

cleanup() {
  umount "$MNT" 2>/dev/null || true
  umount "$MNT2" 2>/dev/null || true
  zfs destroy -r "$SRC" 2>/dev/null || true
  zfs destroy -r "$DST" 2>/dev/null || true
  zfs destroy -r "$CLONE" 2>/dev/null || true
  rmdir "$MNT" "$MNT2" 2>/dev/null || true
}
trap cleanup EXIT

if ! command -v zfs >/dev/null || ! zfs list "$POOL" >/dev/null 2>&1; then
  echo "POOL=$POOL does not exist or zfs is unavailable" >&2
  exit 1
fi

now_ns() { date +%s%N; }
gap_ms() { awk -v a="$1" -v b="$2" 'BEGIN {printf "%.3f", (b-a)/1000000}'; }

zfs create -o mountpoint=legacy "$SRC"
mount -t zfs "$SRC" "$MNT"
dd if=/dev/urandom of="$MNT/data" bs=1M count="$SIZE_MIB" status=none
sync

echo "== snapshot latency (dataset with ${SIZE_MIB} MiB data) =="
for i in 1 2 3 4 5; do
  a=$(now_ns); zfs snapshot "$SRC@vs-measure-$i"; b=$(now_ns)
  echo "snapshot $i: $(gap_ms "$a" "$b") ms"
done

echo "== clone latency =="
for i in 1 2 3; do
  a=$(now_ns)
  zfs clone -o mountpoint=legacy "$SRC@vs-measure-1" "$CLONE"
  b=$(now_ns)
  echo "clone $i: $(gap_ms "$a" "$b") ms"
  zfs destroy -r "$CLONE"
done

echo "== incremental import (${SIZE_MIB} MiB delta) =="
zfs snapshot "$SRC@rep-a"
dd if=/dev/urandom of="$MNT/delta" bs=1M count="$SIZE_MIB" status=none
sync
zfs snapshot "$SRC@rep-b"
a=$(now_ns)
zfs send -i "$SRC@rep-a" "$SRC@rep-b" | zfs recv -F "$DST"
b=$(now_ns)
echo "incremental ${SIZE_MIB} MiB: $(gap_ms "$a" "$b") ms"

stream_bytes=$(zfs send -i "$SRC@rep-a" "$SRC@rep-b" | wc -c)
echo "stream bytes: $stream_bytes"
echo "stream MiB: $(awk -v b="$stream_bytes" 'BEGIN {printf "%.2f", b/1048576}')"

mount -t zfs "$DST" "$MNT2"
if command -v sha256sum >/dev/null; then
  echo "source tree sha256: $(cd "$MNT" && find . -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum)"
  echo "target tree sha256: $(cd "$MNT2" && find . -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum)"
fi
