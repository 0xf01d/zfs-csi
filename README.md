# zfs-cni

> The name is a placeholder. It is a **CSI** driver, not a CNI plugin.

A CSI driver for Kubernetes where **one ZFS dataset per volume** is the unit of
everything: provisioning, snapshots, cloning, and moving state between nodes.

## The idea

A `ReadWriteOnce` PVC becomes a new ZFS dataset on whichever node the pod lands
on. Snapshots of a dataset are free, so they are taken often — they are the
restore points. When the pod goes away, at least one of those snapshots already
exists on another node, so a replacement pod can start there by cloning it,
without copying gigabytes.

State moves, not the live volume: no continuous replication of a mounted
device, no shared block device, no split-brain to arbitrate. `zfs send -i
@before @after | zfs recv` carries only the delta.

## Status

Design phase — nothing runs yet. See the issue tracker for the plan.
