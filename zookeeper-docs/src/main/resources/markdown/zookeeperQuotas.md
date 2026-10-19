<!--
Copyright 2002-2004 The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
//-->

# ZooKeeper Quota's Guide

### A Guide to Deployment and Administration

* [Quotas](#zookeeper_quotas)
    * [Setting Quotas](#Setting+Quotas)
    * [Listing Quotas](#Listing+Quotas)
    * [Deleting Quotas](#Deleting+Quotas)
    * [Enforcement and Limitations](#Enforcement)
    * [Quota Metrics](#Quota+Metrics)
    * [Fork Compatibility and Rollout](#Compatibility)

<a name="zookeeper_quotas"></a>

## Quotas

ZooKeeper has both namespace and bytes quotas. You can use the ZooKeeperMain class to set up quotas.
This fork backports the native quota behavior released in Apache ZooKeeper 3.8.7.
Soft limits produce warnings; hard limits reject writes with `QuotaExceededException`.
Both checks require the startup JVM property `-Dzookeeper.enforceQuota=true`, which defaults to
`false`. Usage accounting continues when enforcement is disabled.

Notice: What the `namespace` quota means is the count quota which limits the number of children
under the path (including itself). Bytes quotas count znode payload bytes, not path names,
heap overhead, or disk usage.

    $ bin/zkCli.sh -server host:port**

The above command gives you a command line option of using quotas.

<a name="Setting+Quotas"></a>

### Setting Quotas

- You can use `setquota` to set a quota on a ZooKeeper node. It has an option of setting quota with
`-n` (soft count), `-b` (soft bytes), `-N` (hard count), or `-B` (hard bytes).
Count limits must be positive integers; byte limits may be zero. Set one dimension per command.
When a hard limit is present, it takes precedence over the soft limit for that dimension.

- Quotas are stored under **/zookeeper/quota** in `zookeeper_limits` and `zookeeper_stats` znodes.
Restrict administrative access to the management hierarchy and its descendant metadata znodes.
ACLs are not inherited: securing only **/zookeeper/quota** does not protect existing children.
The CLI can create children with open ACLs, so protect newly created metadata before relying on enforcement.

- If the quota doesn't exist in the specified path,create the quota, otherwise update the quota.

- The Scope of the quota users set is all the nodes under the path specified (included itself).

- In order to simplify the calculation of quota in the current directory/hierarchy structure, a complete tree path(from root to leaf node)
can be set only one quota. In the situation when setting a quota in a path which its parent or child node already has a quota. `setquota` will
reject and tell the specified parent or child path, users can adjust allocations of quotas(delete/move-up/move-down the quota)
according to specific circumstances.

- Combined with the Chroot, the quota will have a better isolation effectiveness between different applications.For example:

    ```bash
    # Chroot is:
    192.168.0.1:2181,192.168.0.2:2181,192.168.0.3:2181/apps/app1
    setquota -n 100000 /apps/app1
    ```

- Users cannot set the quota on the path under **/zookeeper/quota**

- The target path must already exist. A quota on `/` is not enforced.
Setting a limit below current usage warns but does not remove data or reject the quota configuration.

<a name="Listing+Quotas"></a>

### Listing Quotas

You can use _listquota_ to list a quota on a ZooKeeper node.

<a name="Deleting+Quotas"></a>

### Deleting Quotas

You can use _delquota_ to delete quota on a ZooKeeper node.

Use `delquota -n`, `-b`, `-N`, or `-B` to unset just the selected limit and preserve the other
settings. Without a flag, `delquota` removes the quota metadata.

<a name="Enforcement"></a>

### Enforcement and Limitations

Enable `-Dzookeeper.enforceQuota=true` on every ensemble member before relying on hard limits.
This is a startup property, not a dynamic switch. With only soft limits configured, enabling it
produces warnings without introducing hard rejections. Unlike this fork's older soft-only
implementation, disabling this property also disables soft-warning checks.

Checks run before create and `setData` transactions are prepared. All create modes use the same
check, including ephemeral, sequential, container, and TTL nodes. Deletes are not quota-rejected,
so they can release capacity. A same-size update skips the byte check; a shrinking update can
still be rejected if the resulting usage remains above the hard byte limit.

The backport preserves the released 3.8 limitations. Admission uses committed usage, not
reservations for outstanding writes. Individually admissible operations in a single `multi()`
can collectively exceed a limit; a delete followed by a create can also be rejected even if the
combined result would fit. Pipelined requests can exceed limits for the same reason. Do not treat
these quotas as a strict no-overshoot guarantee. Quota metadata is not automatically removed
when the application subtree is deleted.

<a name="Quota+Metrics"></a>

### Quota Metrics

The default and Prometheus providers expose these logical metrics:

| Metric | Meaning |
|---|---|
| `quota_count_limit_per_namespace` | Effective count limit, preferring hard over soft |
| `quota_bytes_limit_per_namespace` | Effective payload-byte limit, preferring hard over soft |
| `quota_count_usage_per_namespace` | Committed znode count |
| `quota_bytes_usage_per_namespace` | Committed payload bytes |
| `quota_exceeded_error_per_namespace` | Hard-quota rejection count |

Metrics aggregate by the top-level namespace: `/apps/a` and `/apps/b` both contribute to `apps`.
They do not provide separate full-path or application-tenant labels. Soft warnings do not increment
the rejection counter. Usage and limit gauges remain available with enforcement disabled.

<a name="Compatibility"></a>

### Fork Compatibility and Rollout

The native quota error uses `QUOTAEXCEEDED = -125`. This fork's separate per-session ephemeral
serialized-path byte limiter now uses `TOTALEPHEMERALLIMITEXCEEDED = -128`, matching the existing
LinkedIn 3.8-based fork. Its limit configuration, accounting, and violation metric are unchanged.

This mapping requires client migration even when native quota enforcement is disabled:

| Combination | Error interpretation |
|---|---|
| Old fork client and old server | `-125` is the ephemeral limit |
| New fork client and new server | `-125` is native quota; `-128` is the ephemeral limit |
| New fork client and old server | An old ephemeral rejection is decoded as `QuotaExceededException` |
| Old fork client and new server | Quota errors can be misclassified, and `-128` is unknown |

There is no information in a numeric `-125` response that distinguishes the old and new meanings.
Migrate application handling to tolerate both resource-limit exception types during a mixed-version
transition, or use a controlled cutover. An unmodified upstream 3.8 client understands native
quotas but not the fork-only ephemeral error.

Rebuild consumers of quota helper APIs: `StatsTrack` count accessors now use `long`,
`Quotas.quotaPath()` now returns the management directory (use `limitPath()` for the limit leaf),
and the set/delete quota helpers accept `StatsTrack`. Use the matching admin CLI; an older CLI
can discard hard-limit fields when rewriting quota records.

Custom metrics providers must implement the new `CounterSet` and `GaugeSet` methods in
`MetricsContext`. Deploy matching provider and server binaries together; flag-off does not make
an old provider compatible with the new SPI.

After migrating clients, roll all servers and matching providers with enforcement disabled.
Secure quota metadata, inspect usage, and establish soft limits first. Then enable the property
on all members and verify the ensemble before adding hard limits to a controlled subtree:

```text
setquota -n 80000 /apps/example
setquota -b 83886080 /apps/example
listquota /apps/example
setquota -N 100000 /apps/example
setquota -B 104857600 /apps/example
```

The path must exist; these values are examples, not production recommendations. Before a rollback,
remove hard fields with `delquota -N` and `delquota -B` using the new CLI. Preserve the client
migration during mixed-version rollback, and roll provider binaries with their corresponding server.
Disabling the startup flag requires restarting members; restarting one member is not an
ensemble-wide disablement.
