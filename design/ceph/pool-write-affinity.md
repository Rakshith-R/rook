---
title: pool-write-affinity
target-version: release-1.22
---

# Write Affinity for Replicated Pools

Target version: 1.22

## Summary

Add an ordered list of CRUSH failure domains (zones) to the replicated settings of the pool spec used by
`CephBlockPool`. Rook turns the list into a CRUSH rule with one `take` per zone, so the **primary OSD of every
PG is in the first zone of the list** and each remaining zone holds its replica(s).

With one such pool per zone and a single topology-aware RBD StorageClass, a volume is provisioned in the pool
whose primaries are in the zone of the consuming pod. Writes then go to an OSD in the same zone and are
replicated from there.

```yaml
apiVersion: ceph.rook.io/v1
kind: CephBlockPool
metadata:
  name: replicapool-us-east-1a
  namespace: rook-ceph
spec:
  failureDomain: zone
  replicated:
    size: 3
    writeAffinity:
      failureDomains: # ordered, the first entry hosts the primary OSD
        - us-east-1a
        - us-east-1b
        - us-east-1c
```

## Motivation

In a cluster stretched across zones with `failureDomain: zone`, the primary OSD of a PG is in a
pseudo-random zone. An RBD client always sends writes to the primary, which then replicates to the other OSDs.
For a client in zone A and a `size: 3` pool spread over three zones:

| Primary location | Probability today | Cross-zone payload transfers per write | Sequential cross-zone hops |
|---|---|---|---|
| Zone A (client zone) | 1/3 | 2 (primary to two replicas) | 1 |
| Zone B or C | 2/3 | 3 (client to primary, primary to two replicas) | 2 |

Pinning the primary to the client zone makes every write follow the first row: cross-zone write traffic drops
from an average of 2.67 to 2 payload transfers (-25%) and the write path always has one cross-zone hop instead
of an average of 1.67.

Reads are already served in-zone for krbd clients through the CSI read affinity feature
(`read_from_replica=localize`). With write affinity the primary itself is local, so reads that a replica
bounces to the primary, and clients that do not localize reads, stay in-zone too.

### Goals

- Describe, in the pool spec, which zone holds the primary and which zones hold the other replicas.
- Have Rook create and maintain the corresponding CRUSH rule; no toolbox steps.
- Work with the existing ceph-csi topology provisioning (`topologyConstrainedPools`) with no ceph-csi change.
- Cover clusters stretched over three or more zones, and Ceph stretch mode (two zones plus arbiter).

### Non-goals

- Erasure coded pools.
- Creating the StorageClass, or changing how ceph-csi pins volumes to a zone.
- One CR creating several pools. The number of pools equals the number of zones and each pool keeps its own
  `CephBlockPool` CR.

## Proposal

### API

`CephBlockPool.spec` inlines `PoolSpec`; the new field is added to `ReplicatedSpec`, next to `hybridStorage`,
which is the existing precedent for a spec setting that selects the primary OSD through a multi-`take` rule.
The field is `+optional` and deliberately not `+nullable`: an unset `writeAffinity` is simply absent, so the
CEL rules can use `has(self.writeAffinity)`.

```go
// +kubebuilder:validation:XValidation:message="writeAffinity and hybridStorage cannot be specified together",rule="!has(self.writeAffinity) || !has(self.hybridStorage)"
// +kubebuilder:validation:XValidation:message="size must equal the number of writeAffinity failureDomains multiplied by replicasPerFailureDomain",rule="!has(self.writeAffinity) || self.size == size(self.writeAffinity.failureDomains) * (has(self.replicasPerFailureDomain) ? self.replicasPerFailureDomain : 1)"
type ReplicatedSpec struct {
    // ... existing fields: size, replicasPerFailureDomain, subFailureDomain, hybridStorage, ...

    // WriteAffinity places the replicas of the pool in an explicit, ordered list of CRUSH failure domains.
    // The primary OSD of every PG is in the first failure domain of the list. Only supported for CephBlockPool.
    // +optional
    WriteAffinity *WriteAffinitySpec `json:"writeAffinity,omitempty"`
}

// WriteAffinitySpec represents the write affinity settings of a replicated pool
type WriteAffinitySpec struct {
    // FailureDomains is the ordered list of CRUSH buckets that hold the replicas of the pool. The buckets
    // must be of the type set in the pool failureDomain, for example the names of the zones. The first
    // entry holds the primary OSD. If the first failure domain is unavailable, the primary moves to the
    // next entry of the list.
    // +kubebuilder:validation:MinItems=2
    // +kubebuilder:validation:MaxItems=16
    // +kubebuilder:validation:items:MinLength=1
    // +kubebuilder:validation:items:MaxLength=63
    // +listType=set
    FailureDomains []string `json:"failureDomains"`
}
```

Uniqueness is enforced with `+listType=set`, not a CEL `exists_one` rule. The `ReplicatedSpec` is embedded in
the unbounded `CephFilesystem.dataPools` list, and the nested `all(x, self.exists_one(y, ...))` comparison
exceeds the apiserver's CEL cost budget once the list is treated as unbounded. Set semantics reject duplicates
at no CEL cost. `MaxLength=63` bounds every entry, matching the topology label value limit the bucket names
come from and bounding the generated rule text.

Semantics:

- `failureDomains` holds bucket names, not a bucket type. The type is the existing `spec.failureDomain`
  (`zone` in the target use case; `rack`, `datacenter`, ... work the same way). `failureDomain` becomes
  mandatory for a write affinity pool; this is enforced in operator validation rather than in the CRD because
  it is a cross-field requirement of the shared, embedded `PoolSpec`.
- Entries are the values of the node topology label, the same strings used in the StorageClass. Rook applies
  its existing CRUSH name normalization (`.` replaced by `-`) before looking up the bucket.
- The list is exhaustive: the pool has replicas in exactly these failure domains, `replicasPerFailureDomain`
  (default 1) in each, in `subFailureDomain` (default `host`) buckets. `size` must match, which the CRD
  enforces. The Go zero value of `ReplicasPerFailureDomain` (`0`, omitted from the CR) is treated as `1`
  everywhere, including in this CEL rule.
- `deviceClass` and `crushRoot` keep their meaning and apply to every entry.
- The field is optional. Pools without it are reconciled exactly as today.
- `writeAffinity` is rejected for pools of a `CephFilesystem`, `CephObjectStore` or `CephObjectZone`, and for
  the built-in pools `.mgr`, `.nfs` and `.rgw.root`. There is no client-side zone pinning for them.

### Example

One pool per zone. Only the order of the list differs:

```yaml
apiVersion: ceph.rook.io/v1
kind: CephBlockPool
metadata:
  name: replicapool-us-east-1a
  namespace: rook-ceph
spec:
  failureDomain: zone
  replicated:
    size: 3
    writeAffinity:
      failureDomains: [us-east-1a, us-east-1b, us-east-1c]
---
apiVersion: ceph.rook.io/v1
kind: CephBlockPool
metadata:
  name: replicapool-us-east-1b
  namespace: rook-ceph
spec:
  failureDomain: zone
  replicated:
    size: 3
    writeAffinity:
      failureDomains: [us-east-1b, us-east-1c, us-east-1a]
---
apiVersion: ceph.rook.io/v1
kind: CephBlockPool
metadata:
  name: replicapool-us-east-1c
  namespace: rook-ceph
spec:
  failureDomain: zone
  replicated:
    size: 3
    writeAffinity:
      failureDomains: [us-east-1c, us-east-1a, us-east-1b]
```

A single StorageClass selects the pool from the zone of the node chosen for the first consumer. This is
existing ceph-csi behavior:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: rook-ceph-block-write-affinity
provisioner: rook-ceph.rbd.csi.ceph.com
volumeBindingMode: WaitForFirstConsumer
parameters:
  clusterID: rook-ceph
  topologyConstrainedPools: |
    [
      {"poolName":"replicapool-us-east-1a","domainSegments":[{"domainLabel":"zone","value":"us-east-1a"}]},
      {"poolName":"replicapool-us-east-1b","domainSegments":[{"domainLabel":"zone","value":"us-east-1b"}]},
      {"poolName":"replicapool-us-east-1c","domainSegments":[{"domainLabel":"zone","value":"us-east-1c"}]}
    ]
  # ... image features, secrets and fstype as in any other RBD StorageClass
```

Prerequisites, all existing features:

- Nodes carry the zone label (`topology.kubernetes.io/zone`), which Rook already uses to build the CRUSH
  hierarchy.
- CSI topology is enabled with that label as a domain label: `nodePlugin.topology.domainLabels` in the
  ceph-csi-operator `Driver` (or `OperatorConfig` `driverSpecDefaults`) CR.
- Optionally, `csi.readAffinity.enabled: true` in the `CephCluster` CR.

### Generated CRUSH rule

For the first pool above:

```text
rule replicapool-us-east-1a_wa_<hash> {
        id <next free id>
        type replicated
        step take us-east-1a
        step chooseleaf firstn 1 type host
        step emit
        step take us-east-1b
        step chooseleaf firstn 1 type host
        step emit
        step take us-east-1c
        step chooseleaf firstn 1 type host
        step emit
}
```

In general, one block per entry of `failureDomains`, in order:

```text
step take <failure domain bucket> [class <deviceClass>]
step chooseleaf firstn <replicasPerFailureDomain, default 1> type <subFailureDomain, default host>
step emit
```

The rule omits `min_size` and `max_size`; both options are deprecated in Ceph and have no effect.

`firstn` rules emit OSDs in rule order and the first OSD of a replicated PG is its primary, so the primary is
always chosen from the first bucket. Enumerating every zone, instead of taking the root for the non-primary
replicas, is what keeps one replica per zone: CRUSH only avoids duplicates within one `choose` step, not across
`emit` blocks (see [Validation of the rule](#validation-of-the-rule)).

### Operator changes

The rule is injected the same way as the existing hybrid storage and two-step rules: decompile the CRUSH map,
append the rule, compile and inject (`updateCrushMap()` in `pkg/daemon/ceph/client/pool.go`).

**Concurrency.** `updateCrushMap()` was previously called while only holding the read lock of
`crushRuleMutex`, so two concurrent pool reconciles could each read the CRUSH map, add their rule and write
back a map missing the other's rule, or allocate the same free rule ID. A new `crushMapMutex` serializes the
whole check-exists, ID allocation and inject sequence in a new `addCrushRule()` helper. All crushtool rule
creation goes through it: write affinity, hybrid, two-step and the stretch default rule.

**Validation** (`ValidatePoolSpec()` / `validateWriteAffinity()`), in addition to the CRD rules:

- `failureDomain` is set and the pool is replicated.
- Every entry is normalized and must exist in the CRUSH map as a bucket of type `failureDomain` below the pool
  CRUSH root (default `crushRoot` from the cluster spec, falling back to `default`). A missing bucket, for
  example a zone without OSDs yet, fails the reconcile with a clear message and is retried.
- The effective `subFailureDomain` (pool setting, else the stretch cluster setting, else `host`) must differ
  from `failureDomain`.
- If `deviceClass` is set, the subtree of every listed bucket is walked and must contain at least one OSD of
  that class in the CRUSH map device list.
- In a stretch cluster (see below), the list is exactly the two non-arbiter zones, `failureDomain` matches the
  stretch failure domain label suffix, and `deviceClass` is rejected.
- `writeAffinity` with `hybridStorage`, or on the pools named in the non-goals above, is rejected.

The CRUSH map is fetched only when `failureDomain` or `crushRoot` is set, which is always true for a validated
write affinity pool.

**Rule name.** `<pool name>_wa_<hash>`, where the hash is the first 8 hex characters of the SHA-256 of the
rendered steps. The name is a function of the desired rule, so creation is idempotent, and any change of the
list, device class or sub failure domain results in a new rule. The previous rule is deleted directly, see
below; the rule of a deleted pool is removed by `DeletePool()`.

**Cleanup.** `CleanupUnusedCrushRules()` only runs at mgr startup, so it cannot be relied on after a rule
switch. When a pool stops using a write affinity rule, Rook deletes the previous rule itself, and only when the
name matches exactly the `<pool>_wa_<8 hex>` pattern for that same pool, so an unrelated rule is never removed.

**Reconcile** (`createReplicatedPoolForApp()`): a new branch, evaluated before the stretch cluster, hybrid
storage and two-step branches:

1. In a stretch cluster, wait for stretch mode to be enabled (see below).
2. Compute the rule name and create the rule if it does not exist.
3. New pool: create it with that rule.
4. Existing pool using another rule: if `enableCrushUpdates` is `true`, run
   `ceph osd pool set <pool> crush_rule <rule>`, then delete the old write affinity rule if it matches the
   pattern above; otherwise keep the current rule and report the mismatch (see Status). This matches how
   failure domain and device class changes are gated today.
5. Skip `updatePoolCrushRuleLocked()`. It derives the failure domain from the first two steps of the rule and
   would read `host` from a write affinity rule, then replace it with a plain `create-replicated` rule.

**Removing `writeAffinity`** is gated by `enableCrushUpdates` and takes the path of the spec that remains:

- Plain replicated (`replicasPerFailureDomain` at most 1): handled by `updatePoolCrushRuleLocked()`, which
  sets the standard rule and deletes the previous write affinity rule.
- Stretch cluster: the desired rule is `default_stretch_cluster_rule`; the pool is moved to it and the old
  write affinity rule is deleted.
- `replicasPerFailureDomain > 1` and hybrid storage: the target rule is the `<pool>` rule, which may already
  exist from an older spec. Rook deletes it and recreates it before setting it, because the old code never
  set a rule on an existing pool in these paths. Then the old write affinity rule is deleted.

Without `enableCrushUpdates`, none of these moves happen and the mismatch is reported instead.

**Status.** `status.info` gains `primaryFailureDomain` (the first entry) next to the existing `failureDomain`
key, so tools that build the StorageClass can map pools to zones without parsing the spec. The status message
is unchanged. When `enableCrushUpdates` is off and the pool's rule differs from the desired one, the operator
logs a warning and records a `Warning` event `CrushRuleNotUpdated` on the `CephBlockPool` explaining that the
change moves data and needs `enableCrushUpdates`. There was no existing status mechanism for this kind of
mismatch; the event is the least invasive way to surface it.

**Ceph stretch mode** (`mon.stretchCluster`). `ceph mon enable_stretch_mode` overwrites the `crush_rule` of
every existing pool with the default stretch rule when it runs. If Rook created a write affinity rule and set
it on pools while stretch mode was still being enabled, that setting would be lost. Rook therefore requeues
(getting `mon dump` and waiting for `stretch_mode` to be reported) before it creates or updates a write
affinity pool in a stretch cluster.

With `writeAffinity` the pool gets its own rule instead of the shared default:

```yaml
spec:
  failureDomain: zone
  replicated:
    size: 4
    replicasPerFailureDomain: 2
    writeAffinity:
      failureDomains: [b, c] # a second pool uses [c, b]
```

```text
step take b
step chooseleaf firstn 2 type host
step emit
step take c
step chooseleaf firstn 2 type host
step emit
```

Ceph permits this: stretch mode marks every replicated pool created while it is enabled as a stretch pool
whatever its rule, and the Ceph stretch mode documentation shows this exact rule shape. `subFailureDomain`
defaults to the one of the stretch cluster. `deviceClass` is rejected together with `writeAffinity` in a
stretch cluster because Ceph warns that PGs of a device class rule fail to peer after a site loss.

### Interaction with other features

| Feature | Interaction |
|---|---|
| CSI read affinity | Complementary. Reads are local with or without it for these pools; it still helps clients in a non-primary zone. |
| Balancer `upmap` mode (default) | Compatible. Upmap remaps stay inside the subtree of each `take`. |
| Balancer `read` and `upmap-read` modes | Compatible. Ceph v18 to v20 validate every candidate primary with `CrushWrapper::verify_upmap`, which at each `emit` checks the OSD is inside that `take` subtree. `balance_primaries` therefore cannot move a primary out of the first failure domain; with `replicasPerFailureDomain > 1` it only balances among replicas inside it. No balancer-mode validation is needed. |
| PG autoscaler | The autoscaler still attributes a pool to the bucket of the first `take` (the `FIXME` in `pg_autoscaler` remains), so a write affinity pool is sized against the OSDs of its first zone. Overlapping-roots support landed in main (v22) and was backported to tentacle and umbrella after v20.3.0 (PR #70730), so Squid v19 and Tentacle up to v20.3.0 skip autoscaling for pools with overlapping roots. This is documented rather than version-gated: set `pg_num` through `parameters` on those versions. |
| `ceph df` MAX AVAIL | Correct on Ceph v20 and newer: the weight merging across `take` steps (commit `3bd34683e79`, tracker 56650) gives the same value as a root-based rule. Squid v19 normalizes per `take` and under-reports, close to the capacity of one zone. The warning in the Ceph stretch mode documentation is stale for v20 and newer. |
| OSD `primary-affinity` | An OSD with primary affinity below 1 can hand the primary role to an OSD of another zone. Leave it at the default for OSDs backing these pools. |
| Mirroring, quotas, RADOS namespaces | Unaffected. |

### Failure behavior

| Event | Result |
|---|---|
| OSD or host of the first zone fails | Until the OSD is marked out, the primary is the replica in the second zone. After that CRUSH picks another host of the first zone and the primary returns there. |
| First zone fails | PGs stay active with the remaining replicas (subject to `min_size`); the primary is in the second zone of the list. Nothing is remapped to the surviving zones, as with any `failureDomain: zone` pool. |
| Another zone fails | The primary stays in the first zone; PGs are degraded. |
| Zone returns | Replicas recover and the primary returns to the first zone. |

### Day-2 operations

- **New pools** are the recommended path.
- **Enabling on an existing pool** needs `enableCrushUpdates: true` and moves data: placement inside each zone
  is computed differently from the root-based rule. In the test below 55% of the replicas were re-placed.
- **Reordering the list** (changing the primary zone) does not move data. Placement inside a zone depends only
  on the PG and the zone bucket, not on the position in the rule, so PGs only re-peer with a new primary.
- **Replacing a zone** in the list backfills the replicas of that zone only.
- **Removing `writeAffinity`** with `enableCrushUpdates: true` returns the pool to a standard rule through the
  path described under Operator changes, with data movement.

## Validation of the rule

Rules were compiled and tested with `crushtool` from Ceph 19.2.3 on a map with 3 zones, 3 hosts per zone and
2 OSDs per host, for 4096 inputs each.

| Rule | Primary in first zone | PGs with two replicas in one zone |
|---|---|---|
| Root based rule (today) | 35% | 0 |
| First zone, then `take default` with `chooseleaf firstn -1 type zone` | 100% | 2759 (67%) |
| Proposed, list `[zone0, zone1, zone2]` | 100% | 0 |
| Proposed, rotated lists for the two other pools | 100% | 0 |
| Proposed with `class ssd` | 100% | 0 |
| Proposed, 2 zones, 2 replicas per zone | 100% | by design, 0 on the same host |

Further results for the proposed rule:

- All OSDs of the first zone out: every PG maps to 2 OSDs, with the primary in the second zone.
- All OSDs of the second zone out: 2 OSDs, primary still in the first zone.
- One host of the first zone out: 3 OSDs, primary in the first zone.
- No bad mappings; per-OSD PG counts stayed within 5% of the mean.
- Switching from the root based rule: 55% of replicas change OSD. Rotating the list: 0%.

The CRD rules were evaluated with the Kubernetes CEL validator (`k8s.io/apiextensions-apiserver` v0.37.2)
against a schema equivalent to the generated one: valid specs pass; a size mismatch and the combination with
`hybridStorage` are rejected. Duplicate entries are rejected by `listType=set`, which was chosen over the CEL
uniqueness rule because the latter exceeds the cost budget for the embedded, unbounded `dataPools` list.

## Alternatives considered

| Alternative | Why not |
|---|---|
| Only name the primary zone and take the root for the rest | Places two replicas in the primary zone for 67% of PGs in the test above. |
| Only name the primary zone and let Rook enumerate the other zones | The failover order becomes implicit and the rule changes whenever a zone appears in the CRUSH map. Can be added later as a shorthand that expands to the explicit list. |
| List of objects (`- name: us-east-1a`) | Easier to extend per zone, but nothing per-zone is needed now: replica count, sub failure domain and device class are uniform and already have fields. The `writeAffinity` struct is the extension point. |
| CEL `exists_one` uniqueness rule | Exceeds the apiserver CEL cost budget once the list is part of the unbounded `CephFilesystem.dataPools` list. `listType=set` rejects duplicates without CEL. |
| One CR producing one pool per zone | Breaks the one CR to one pool model that status, pool ID, mirroring, deletion and `CephBlockPoolRadosNamespace` rely on. |
| OSD `primary-affinity` | Set per OSD for all pools, so it cannot give each pool a different primary zone. |
| `pg-upmap-primary` managed by Rook | Per-PG state to reconcile on every PG split or merge, competes with the balancer, and raises the minimum client compatibility level to Reef. |
| Free-form `crushRule` name with a rule created by the admin | Requires the toolbox and offers no validation. Orthogonal; could exist as a general escape hatch. |

## Limitations

- **Pods stay in their zone.** ceph-csi topology provisioning sets a node affinity for the zone on the PV. If
  the zone fails, the data remains available from the other zones but the pod cannot be rescheduled there.
  This is existing behavior of `topologyConstrainedPools`, not of this change, but it is a real trade-off
  against a non-topology StorageClass and should be documented. Using topology only for pool selection would
  be a ceph-csi follow-up.
- **PVC-to-PVC clones** across zones work: ceph-csi selects the destination pool from the request topology and
  performs a cross-pool `librbd.CloneImage`.
- **VolumeSnapshot restores** into another zone are rejected. ceph-csi `MatchPoolAndTopology` forces the
  snapshot's pool, so a restore whose topology resolves to a different zone fails with `codes.Internal`. This
  affects `VolumeGroupSnapshot` members too.
- **Autoscaling on Ceph v19, and Tentacle up to v20.3.0**, is the largest operational cost. It is documented;
  set `pg_num` through `parameters` on those versions. There is no hard version gate.

## Implementation and test plan

1. API: types, `IsWriteAffinityPool()` helper, generated CRDs and Helm resources, CRD documentation.
2. Rule builder, rule naming, `crushMapMutex` / `addCrushRule()`, validation and the reconcile branch, with
   unit tests for the generated rule text, name stability, each validation error, the `enableCrushUpdates`
   gating, removal of `writeAffinity` on each path, and rule cleanup.
3. Stretch cluster support, including the wait for `stretch_mode`, with unit tests.
4. Integration test on a multi-node cluster with zone labels: three pools, then check with `ceph pg ls-by-pool`
   and `ceph osd tree` that every acting primary is in the first zone of its pool, and provision PVCs through
   the topology StorageClass in each zone.
5. Documentation: `ceph-block-pool-crd.md`, an example manifest with the three pools and the StorageClass, and
   `PendingReleaseNotes.md`.
