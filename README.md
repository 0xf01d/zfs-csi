# zfs-csi

A CSI driver for Kubernetes where **one ZFS dataset per volume** is the unit of
everything: provisioning, snapshots, cloning, and moving state between nodes.

The working name of the design is **zdataset**. This repository is `zfs-csi`
and that is also the CSI driver name:
`zfs-csi.0xf01d.github.io`.

## The idea

A `ReadWriteOnce` PVC becomes a new ZFS dataset on whichever node the pod lands
on (`WaitForFirstConsumer`). Snapshots of a dataset are cheap, so they are
taken often — they are the restore points. When the pod goes away, at least one
of those snapshots should already exist on another node, so a replacement pod
can start there by cloning or importing it, without copying gigabytes of
unchanged data.

State moves, not the live volume: there is no continuous replication of a
mounted device, no shared block device, no split-brain to arbitrate.
`zfs send -i @before @after | zfs recv` carries only the delta.

This is different from DRBD/VolSync-style replication:

| | DRBD / block replication | this driver |
|---|---|---|
| unit | live block device | ZFS dataset and snapshots |
| consistency | needs fencing/quorum | snapshot is a point in time |
| move | continuous mirroring | delta between snapshots |
| failure mode | split-brain | last replicated snapshot wins |
| restore | replay device state | `zfs clone` or `zfs recv` |

## Architecture

```
                  Kubernetes API
                       |
       +---------------+----------------+
       |                                |
 external-provisioner            external-snapshotter
 external-attacher              node-driver-registrar
 livenessprobe                  livenessprobe
       |                                |
+------+-------+                +-------+----------------------+
| controller   |                | node plugin (DaemonSet)      |
| Deployment   |                | one per node                 |
| 1 replica    |                |                              |
|              |   HTTP/TLS     |  ZFS CLI backend             |
| peer client  +--------------->+  peer server :9809          |
+------+-------+                |  replicator goroutine        |
       |                        |  mount / bind mount          |
       |                        +-------+----------------------+
       |                                |
       +----------- peer clients --------+--- other nodes
                                        |
                                   /dev/zfs, zpool tank/k8s
```

- **Controller plugin** (`--mode=controller`): CreateVolume, DeleteVolume,
  CreateSnapshot, DeleteSnapshot, ListSnapshots, ListVolumes,
  ControllerExpandVolume, ControllerPublish/Unpublish (no-op), topology.
- **Node plugin** (`--mode=node`): NodeStage/Unstage, NodePublish/Unpublish,
  NodeGetVolumeStats, NodeExpand, local dataset creation, snapshots, send/recv.
- **Peer API** (`--peer-listen`, default `:9809`): a small HTTP API used to ask
  a node about datasets and snapshots, create/destroy/clone, stream
  `zfs send`, and pull a snapshot from another node.
- **Replicator**: after every snapshot (and on pod unpublish, plus optionally
  on a timer) it finds the newest common snapshot, sends the delta to every
  peer, then prunes old `@rep-*` snapshots.

There is no local database. Given `--pool` and a volume handle, any node can
reconstruct every name with `zfs list`.

## Naming and idempotence

```
volume handle:  pvc-<kubernetes-pvc-uid>            (returned by CreateVolume)
dataset:        <pool>/pvc-<uid>                    e.g. tank/k8s/pvc-1234
ZFS snapshot:   <dataset>@vs-<volumesnapshot-uid>   requested via VolumeSnapshot
                <dataset>@rep-<unixnano>            taken by the replicator
snapshot ID:    <volume-handle>@<snapshot-name>     e.g. pvc-1234@vs-abcd
```

Everything is derived from the CSI IDs, so the replicator can restart after a
crash and simply read `zfs list`. Names are sanitized into a single ZFS-safe
path component.

## Data flow

### Provision (late binding)

1. A PVC uses the `zfs-csi` StorageClass with
   `volumeBindingMode: WaitForFirstConsumer`.
2. When the pod is scheduled, external-provisioner calls `CreateVolume` with
   the selected topology. The controller asks that node to create
   `<pool>/pvc-<uid>` (mountpoint `legacy`).
3. `NodeStageVolume` mounts the dataset on a staging path; `NodePublishVolume`
   bind-mounts it into the pod.

If no topology is selected yet, `CreateVolume` only returns an ID and the node
plugin creates the dataset lazily in `NodeStageVolume`.

### Snapshot

`VolumeSnapshot` → external-snapshotter → `CreateSnapshot` → controller picks a
node that has the dataset and calls `POST /v1/snapshots`. The node runs
`zfs snapshot <dataset>@vs-<uid>` and triggers replication. No data is copied
in the snapshot call.

If a volume was restored by cloning that snapshot, ZFS keeps the clone
dependent on it. `DeleteSnapshot` then uses `zfs destroy -d`: deletion is
deferred until the last clone is gone, so the CSI call succeeds without
copying the snapshot first.

### Replicate

For each peer:

1. read local and remote `zfs list -t snapshot`;
2. `base` = newest snapshot both sides have;
3. if `latest` is already there, do nothing;
4. `zfs send -i @base @latest` (or full `zfs send @latest` if no base) is
   streamed to the peer's `/v1/receive`, which pipes it into `zfs recv -F`.

The replicator itself takes `@rep-<timestamp>` snapshots. `--keep-snapshots`
(default 10) prunes old `@rep-*` snapshots on all peers; Kubernetes `@vs-*`
snapshots are never pruned by the replicator.

### Failover / move state

`NodeStageVolume` on the chosen node calls `EnsureLocal`:

- dataset already local and up to date: mount it;
- dataset local but a peer has newer snapshots: import the incremental delta;
- dataset missing, volume has a source snapshot: clone it if it is local,
  otherwise pull it from a peer;
- dataset missing, no source: ask all peers for the newest snapshot of this
  volume and import it (full if there is no base); only a volume no node has
  ever seen is created empty.

The same path is used by `CreateVolume` when a new volume is created from a
`VolumeSnapshot`: on the same node it is an instant `zfs clone`; on another
node the snapshot is streamed once.

There is no live replication. The newest replicated snapshot is the recovery
point.

## Backend

Only ZFS. No LVM, DRBD, LINSTOR or Piraeus, and no fallback backend.

`pkg/backend.Backend` is the complete storage surface:

```go
CreateDataset(name)
DestroyDataset(name, recursive)
Snapshot(ds, snap)
DestroySnapshot(ds, snap)
Clone(snap, target)
ListSnapshots(ds)
SendIncremental(from, to, io.Writer)
Receive(io.Reader, target)
Mountpoint(ds)
UsedBytes(ds)
DatasetExists(ds)
ListDatasets(root)
SetQuota(ds, bytes)
```

The production implementation shells out to `zfs(8)`; there are no libzfs
bindings. A fake in-memory implementation exists only for unit tests.
`CreateDataset` uses `mountpoint=legacy`, so the node mount is explicit and
ZFS does not auto-mount datasets behind the driver's back. `SetQuota` implements
expansion; ZFS applies it online, so `NodeExpandVolume` is a no-op.

## Deployment

Requirements on every node:

- Linux with ZFS 2.3.x (the driver only uses stable CLI commands);
- the `zfs` kernel module loaded and `/dev/zfs` present;
- the parent dataset (`--pool`, default `tank/k8s`) **already exists** — the
  driver does not create pools;
- `zfs`, `mount`, `umount` in the container image;
- network reachability to peers on port 9809 (or your `--peer-port`);
- privileged node containers (mount propagation, `/dev/zfs`).

Install the VolumeSnapshot CRDs from external-snapshotter v8.2.x before
applying the manifests.

```sh
# build the driver image with ZFS userspace in it
make image IMAGE=registry.example/zfs-csi TAG=dev

# edit deploy/controller.yaml and deploy/node.yaml:
#   image: registry.example/zfs-csi:dev
#   --pool=tank/k8s

kubectl apply -k deploy
```

The DaemonSet uses `hostNetwork: true` so the peer API is reachable at
`<node-internal-ip>:9809`. If no `--peer` flags are given, both controller and
nodes discover peers from the Kubernetes `Node` list on `--peer-port`. Static
peers can be supplied instead, repeatably:

```sh
--peer=node-a=10.0.0.11:9809 --peer=node-b=10.0.0.12:9809
```

For the single-node / two-pool test described below, point a second pool at a
separate peer port and use explicit peers:

```sh
# node plugin on pool 1, peer API on 127.0.0.1:9809
--pool=tank/k8s --peer-listen=127.0.0.1:9809 --peer=peerB=127.0.0.1:9810
# node plugin on pool 2, peer API on 127.0.0.1:9810
--pool=backup/k8s --peer-listen=127.0.0.1:9810 --peer=peerA=127.0.0.1:9809
```

### TLS

The peer API is plain TCP by default. That is convenient for a first test but
it lets anyone who can reach port 9809 delete datasets and stream data.
For anything real, enable mutual TLS on both sides:

```sh
--tls-cert=/etc/zfs-csi/tls/tls.crt \
--tls-key=/etc/zfs-csi/tls/tls.key \
--tls-ca=/etc/zfs-csi/tls/ca.crt
```

The same flags are used for the peer server and for outgoing clients. Mount
the Secret into every driver container and add the flags to the manifests.
There is no SSH transport in this version; mTLS over HTTP is the only peer
transport.

### Snapshot usage

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: data-1
spec:
  volumeSnapshotClassName: zfs-csi
  source:
    persistentVolumeClaimName: data
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-restored
spec:
  storageClassName: zfs-csi
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
  dataSource:
    kind: VolumeSnapshot
    name: data-1
```

## Configuration

| flag | default | meaning |
|---|---|---|
| `--mode` | required | `controller`, `node`, or `all` (combined, for csi-sanity) |
| `--endpoint` | `unix:///csi/csi.sock` | CSI gRPC endpoint |
| `--driver-name` | `zfs-csi.0xf01d.github.io` | CSI driver name |
| `--node-id` | `$NODE_NAME` | node identity (node mode) |
| `--pool` | `tank/k8s` | existing parent dataset |
| `--topology-key` | `topology.zfs-csi.io/node` | topology key |
| `--peer` | none | `node=host:port`, repeatable |
| `--peer-listen` | `:9809` | peer API listen address (node mode) |
| `--peer-port` | `9809` | port used by node discovery |
| `--peer-discovery` | `true` | discover peers from Kubernetes Nodes |
| `--tls-cert/key/ca` | empty | mutual TLS for the peer API |
| `--replicate-interval` | `0` | periodic replication; 0 = event driven |
| `--keep-snapshots` | `10` | number of `@rep-*` snapshots to keep; 0 = all |
| `--max-volumes-per-node` | `0` | advertised CSI limit |
| `--zfs-path` | `zfs` | path to `zfs(8)` |
| `--kubeconfig` | empty | node discovery outside a cluster |

## What is tested here

The sandbox used for this implementation has **no ZFS and no root**. It does
have Go, so the following are real, executed tests:

- `pkg/backend`: fake backend and CLI name/error helpers.
- `pkg/driver`: CreateVolume (late binding and topology), idempotent create,
  Create/Delete/ListSnapshot, clone from local snapshot, import from another
  node, cross-peer DeleteVolume, expansion, capabilities, NodeStage/Publish/
  Unpublish/Unstage with an in-memory mounter and backend.
- `pkg/replication`: full replication, incremental replication, idempotent
  re-replication, failover import of the newest snapshot, catch-up of an
  existing stale dataset, local clone, `@rep-*` snapshot creation.
- `pkg/peer`: mutual-TLS handshake (a generated CA, server and client cert;
  a client without a certificate is rejected).
- `pkg/naming`: name derivation and sanitization.

Run them with:

```sh
make test        # or: go test ./...
make vet
```

ZFS-only tests skip unless the environment has ZFS:

- `pkg/backend/zfs_test.go` runs against `ZFS_CSI_TEST_POOL`, for example
  `ZFS_CSI_TEST_POOL=tank/k8s go test -v ./pkg/backend`.
- `test/e2e/zfs_e2e_test.go` is the real end-to-end test: create dataset,
  mount, write a sample tree, snapshot, modify, snapshot, send incremental,
  receive into a second pool, mount, compare SHA-256 of the tree. It also runs
  the peer/replicator path with real ZFS over 127.0.0.1. Run it with
  `ZFS_CSI_E2E=1 go test -v ./test/e2e` (as root, with two temporary
  file-backed pools) or set `ZFS_CSI_E2E_POOL_A` and `ZFS_CSI_E2E_POOL_B`.

`csi-sanity` is **not run here** (no ZFS, and the binary is not installed).
`hack/csi-sanity.sh` is provided and expects a running driver on a real ZFS
node. Run the driver with `--mode=all --peer=self=127.0.0.1:9809` (and
`--peer-listen=127.0.0.1:9809`) to expose both CSI services on one socket.

## What is not tested / not claimed

- **A pod restarting on a different physical node was not executed.** The
  cross-node code path is implemented and tested with two peer servers on
  127.0.0.1 and with fake backends. The single-node test cluster cannot prove
  the physical move.
- **Sidecar memory was not measured**, because the sandbox has no cluster and
  no images. See `docs/measurements.md`; the driver process RSS is measured,
  the sidecar total is an explicit unknown.
- Mutually authenticated TLS is covered by a unit test that generates its own
  CA and certificates. It was not exercised in a real cluster with a mounted
  Secret.
- Snapshots are ZFS crash-consistent points in time, not application-consistent
  database backups. Use `fsfreeze` or application quiescing if you need that.
- The last snapshot on another node is a best-effort recovery point. The
  snapshot on `NodeUnpublish` is asynchronous. If a node dies without
  unpublishing and `--replicate-interval=0`, data since the last snapshot is
  lost. Enable a periodic interval for a bounded RPO; there is no continuous
  replication by design.
- There is no fencing. Kubernetes RWO semantics are the only protection
  against the same volume being mounted on two nodes at once. The driver does
  not implement a distributed lease.
- No pool management, no capacity reporting (`GetCapacity` is not advertised),
  no compression/encryption policy management, no snapshot schedules, no
  VolumeGroupSnapshot, no block volumes.
- `DeleteVolume` and `DeleteSnapshot` contact every configured peer. If a peer
  is down, deletion returns an error instead of silently leaving state behind.
- The peer API is plain HTTP unless TLS is configured. Use mTLS or a network
  policy that restricts port 9809 to the nodes.
- ZFS feature flags must be compatible across nodes for `zfs send`/`recv`.
  The driver does not check them.
- Multi-pool per driver instance is not supported: one `--pool` per driver.
  The two-pool tests use two driver instances.

## Measurements

Measured in this sandbox with `make measure` (Go 1.24, no ZFS, idle, no CSI
requests):

| process | RSS |
|---|---|
| controller plugin | **28.6 MiB** |
| node plugin | **28.8 MiB** |
| sidecars (5 containers) | **not measured** |

The reference to beat was LINSTOR at roughly 770 MiB. The two driver processes
are about 57 MiB together, and the static Go binary is 39 MB on disk. That is
a strong indication but **not proof** that a full deployment beats LINSTOR,
because the five upstream sidecars are separate processes and were not
measured here. The honest claim is: the driver itself is small; the complete
pod total is unverified.

Snapshot latency, clone latency and incremental import throughput need a real
ZFS host and were not measured. `docs/measurements.md` contains the exact
commands and the report template; `hack/measure-zfs.sh` automates them.

## Layout

```
cmd/zfs-csi/          main, flags, gRPC and peer servers
pkg/backend/          Backend interface, ZFS CLI implementation, test fake
pkg/driver/           CSI Identity, Controller and Node services
pkg/mount/            Mounter interface and Linux implementation
pkg/naming/           dataset, snapshot and handle naming
pkg/peer/             peer HTTP API and client, optional mutual TLS
pkg/replication/      delta replication, failover import, catch-up
deploy/               RBAC, CSIDriver, controller, node, StorageClass, SnapshotClass
test/e2e/             real-ZFS end-to-end test (skipped without ZFS)
hack/                 csi-sanity, idle and ZFS measurement helpers
docs/                 measurement report
```

## License

Not specified yet.
