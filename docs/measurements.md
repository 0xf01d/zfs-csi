# Measurement report

Date: 2026-11-15
Host: Debian 13 sandbox, no `/dev/zfs`, no root, no Kubernetes cluster.
Go: 1.24.10 linux/amd64.

This report separates **measured** from **not measured**. Nothing below is
extrapolated into a claim that was not actually observed.

## 1. Idle memory

### Measured

Command:

```sh
make measure
```

which builds `bin/zfs-csi`, starts a controller process and a node process with
no CSI requests (`--peer-discovery=false`, no ZFS access), waits 3 seconds and
reads `VmRSS` from `/proc/<pid>/status`.

| process | RSS |
|---|---|
| controller plugin | 28.6 MiB |
| node plugin | 28.8 MiB |
| **sum of driver processes** | **57.4 MiB** |
| binary on disk | 39 MB |

The node process includes the gRPC server, the peer HTTP server and the
replicator goroutine, but no active mounts or datasets.

### Not measured

The five sidecars (`csi-provisioner`, `csi-attacher`, `csi-snapshotter`,
`livenessprobe`, `node-driver-registrar`) are separate containers. They were
not run here. Their RSS depends on the image versions in `deploy/` and on the
cluster size, and is deliberately not guessed.

To measure them in a real cluster:

```sh
kubectl -n zfs-csi top pod -l app.kubernetes.io/name=zfs-csi --containers
# or, on the node:
for p in $(crictl pods --name zfs-csi -q); do
  crictl inspectp "$p" >/dev/null
done
```

Recommended report table:

| pod | container | RSS |
|---|---|---|
| zfs-csi-controller-* | csi-provisioner | not measured |
| zfs-csi-controller-* | csi-attacher | not measured |
| zfs-csi-controller-* | csi-snapshotter | not measured |
| zfs-csi-controller-* | liveness-probe | not measured |
| zfs-csi-controller-* | zfs-csi | 28.6 MiB (sandbox) |
| zfs-csi-node-* | node-driver-registrar | not measured |
| zfs-csi-node-* | liveness-probe | not measured |
| zfs-csi-node-* | zfs-csi | 28.8 MiB (sandbox) |

Reference: the brief gives LINSTOR at roughly **770 MiB** on the test cluster.
The driver processes alone are far below that, but the full sidecar total was
not measured, so a full-deployment win is **not proven** here.

## 2. Snapshot latency

Not measured (no ZFS in the sandbox).

On a real node:

```sh
POOL=tank/k8s
DS=$POOL/pvc-measure
zfs create -o mountpoint=legacy "$DS"
mkdir -p /mnt/measure && mount -t zfs "$DS" /mnt/measure
dd if=/dev/urandom of=/mnt/measure/data bs=1M count=256
sync

for i in 1 2 3 4 5; do
  /usr/bin/time -f "%e s" zfs snapshot "$DS@vs-measure-$i"
done
```

Report the five wall-clock values. A ZFS snapshot of an unchanged dataset is
normally milliseconds or less; the number that matters is the distribution,
not a single best case.

## 3. Clone latency

Not measured.

```sh
DS=tank/k8s/pvc-measure
SNAP=$DS@vs-measure-1
for i in 1 2 3; do
  /usr/bin/time -f "%e s" zfs clone -o mountpoint=legacy "$SNAP" "$DS-clone-$i"
  zfs destroy -r "$DS-clone-$i"
done
```

A clone is metadata-only. Time it with a cold page cache and with a warm one if
you want to distinguish ZFS bookkeeping from kernel cache effects.

## 4. Incremental import of N MiB

Not measured. This is the number that matters for the design: moving state
must cost the size of the delta, not the size of the volume.

Single-node two-pool loopback:

```sh
SRC=tank/k8s/pvc-measure
DST=backup/k8s/pvc-measure

zfs snapshot "$SRC@rep-a"
dd if=/dev/urandom of=/mnt/measure/delta bs=1M count=64
sync
zfs snapshot "$SRC@rep-b"

/usr/bin/time -f "%e s" sh -c \
  "zfs send -i '$SRC@rep-a' '$SRC@rep-b' | zfs recv -F '$DST'"
```

Also run the full-stream case for comparison, and record:

| N (delta) | full stream | incremental stream | wall time | MiB/s |
|---|---|---|---|---|
| 64 MiB | not measured | not measured | not measured | not measured |
| 512 MiB | not measured | not measured | not measured | not measured |

The test in `test/e2e/zfs_e2e_test.go` verifies correctness (SHA-256 of a
sample tree before/after) and logs the incremental stream size, but it does not
claim a throughput number.

## 5. Real cross-node failover

**Not executed.** The cluster available for this work has one node; the sandbox
has no ZFS at all. The cross-node implementation was exercised only with two
peer servers on 127.0.0.1 (fake backends in unit tests, real ZFS in the gated
e2e test). A pod actually rescheduled onto another physical node, with its
dataset imported from a peer and the SHA-256 tree compared, remains to be done
on the target cluster.

Suggested procedure on the real cluster:

1. create a PVC, mount it, write a known tree, take a `VolumeSnapshot`;
2. wait for the `@rep-*` snapshot to appear on the peer (`zfs list -t snapshot`);
3. delete the pod, force the replacement onto the other node
   (`nodeSelector`/`nodeAffinity`)
4. verify the pod mounts, then compare the tree hash with the original;
5. record `zfs send` bytes and elapsed time for the import.

Report the result only if all five steps ran.
