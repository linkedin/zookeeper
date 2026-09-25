<!--
Copyright 2002-2022 The Apache Software Foundation

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

# A series of tools for ZooKeeper

* [Scripts](#Scripts)
    * [zkServer.sh](#zkServer)
    * [zkCli.sh](#zkCli)
    * [zkEnv.sh](#zkEnv)
    * [zkCleanup.sh](#zkCleanup)
    * [zkTxnLogToolkit.sh](#zkTxnLogToolkit)
    * [zkSnapShotToolkit.sh](#zkSnapShotToolkit)
    * [zkSnapshotComparer.sh](#zkSnapshotComparer)
    * [zkSnapshotRecursiveSummaryToolkit.sh](#zkSnapshotRecursiveSummaryToolkit)
    * [zkOfflineAudit.sh](#zkOfflineAudit)
    
* [Testing](#Testing)
    * [Jepsen Test](#jepsen-test)
    
<a name="Scripts"></a>

## Scripts

<a name="zkServer"></a>

### zkServer.sh
A command for the operations for the ZooKeeper server.

```bash
Usage: ./zkServer.sh {start|start-foreground|stop|version|restart|status|upgrade|print-cmd}
# start the server
./zkServer.sh start

# start the server in the foreground for debugging
./zkServer.sh start-foreground

# stop the server
./zkServer.sh stop

# restart the server
./zkServer.sh restart

# show the status,mode,role of the server
./zkServer.sh status
JMX enabled by default
Using config: /data/software/zookeeper/conf/zoo.cfg
Mode: standalone

# Deprecated
./zkServer.sh upgrade

# print the parameters of the start-up
./zkServer.sh print-cmd

# show the version of the ZooKeeper server
./zkServer.sh version
Apache ZooKeeper, version 3.6.0-SNAPSHOT 06/11/2019 05:39 GMT

```

The `status` command establishes a client connection to the server to execute diagnostic commands. 
When the ZooKeeper cluster is started in client SSL only mode (by omitting the clientPort
from the zoo.cfg), then additional SSL related configuration has to be provided before using 
the `./zkServer.sh status` command to find out if the ZooKeeper server is running. An example:

    CLIENT_JVMFLAGS="-Dzookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty -Dzookeeper.ssl.trustStore.location=/tmp/clienttrust.jks -Dzookeeper.ssl.trustStore.password=password -Dzookeeper.ssl.keyStore.location=/tmp/client.jks -Dzookeeper.ssl.keyStore.password=password -Dzookeeper.client.secure=true" ./zkServer.sh status


<a name="zkCli"></a>

### zkCli.sh
Look at the [ZooKeeperCLI](zookeeperCLI.html)

<a name="zkEnv"></a>

### zkEnv.sh
The environment setting for the ZooKeeper server

```bash
# the setting of log property
ZOO_LOG_DIR: the directory to store the logs
```

<a name="zkCleanup"></a>

### zkCleanup.sh
Clean up the old snapshots and transaction logs.

```bash
Usage:
     * args dataLogDir [snapDir] -n count
     * dataLogDir -- path to the txn log directory
     * snapDir -- path to the snapshot directory
     * count -- the number of old snaps/logs you want to keep, value should be greater than or equal to 3
# Keep the latest 5 logs and snapshots
./zkCleanup.sh -n 5
```

<a name="zkTxnLogToolkit"></a>

### zkTxnLogToolkit.sh
TxnLogToolkit is a command line tool shipped with ZooKeeper which
is capable of recovering transaction log entries with broken CRC.

Running it without any command line parameters or with the `-h,--help` argument, it outputs the following help page:

    $ bin/zkTxnLogToolkit.sh
    usage: TxnLogToolkit [-dhrv] txn_log_file_name
    -d,--dump      Dump mode. Dump all entries of the log file. (this is the default)
    -h,--help      Print help message
    -r,--recover   Recovery mode. Re-calculate CRC for broken entries.
    -v,--verbose   Be verbose in recovery mode: print all entries, not just fixed ones.
    -y,--yes       Non-interactive mode: repair all CRC errors without asking

The default behaviour is safe: it dumps the entries of the given
transaction log file to the screen: (same as using `-d,--dump` parameter)

    $ bin/zkTxnLogToolkit.sh log.100000001
    ZooKeeper Transactional Log File with dbid 0 txnlog format version 2
    4/5/18 2:15:58 PM CEST session 0x16295bafcc40000 cxid 0x0 zxid 0x100000001 createSession 30000
    CRC ERROR - 4/5/18 2:16:05 PM CEST session 0x16295bafcc40000 cxid 0x1 zxid 0x100000002 closeSession null
    4/5/18 2:16:05 PM CEST session 0x16295bafcc40000 cxid 0x1 zxid 0x100000002 closeSession null
    4/5/18 2:16:12 PM CEST session 0x26295bafcc90000 cxid 0x0 zxid 0x100000003 createSession 30000
    4/5/18 2:17:34 PM CEST session 0x26295bafcc90000 cxid 0x0 zxid 0x200000001 closeSession null
    4/5/18 2:17:34 PM CEST session 0x16295bd23720000 cxid 0x0 zxid 0x200000002 createSession 30000
    4/5/18 2:18:02 PM CEST session 0x16295bd23720000 cxid 0x2 zxid 0x200000003 create '/andor,#626262,v{s{31,s{'world,'anyone}}},F,1
    EOF reached after 6 txns.

There's a CRC error in the 2nd entry of the above transaction log file. In **dump**
mode, the toolkit only prints this information to the screen without touching the original file. In
**recovery** mode (`-r,--recover` flag) the original file still remains
untouched and all transactions will be copied over to a new txn log file with ".fixed" suffix. It recalculates
CRC values and copies the calculated value, if it doesn't match the original txn entry.
By default, the tool works interactively: it asks for confirmation whenever CRC error encountered.

    $ bin/zkTxnLogToolkit.sh -r log.100000001
    ZooKeeper Transactional Log File with dbid 0 txnlog format version 2
    CRC ERROR - 4/5/18 2:16:05 PM CEST session 0x16295bafcc40000 cxid 0x1 zxid 0x100000002 closeSession null
    Would you like to fix it (Yes/No/Abort) ?

Answering **Yes** means the newly calculated CRC value will be outputted
to the new file. **No** means that the original CRC value will be copied over.
**Abort** will abort the entire operation and exits.
(In this case the ".fixed" will not be deleted and left in a half-complete state: contains only entries which
have already been processed or only the header if the operation was aborted at the first entry.)

    $ bin/zkTxnLogToolkit.sh -r log.100000001
    ZooKeeper Transactional Log File with dbid 0 txnlog format version 2
    CRC ERROR - 4/5/18 2:16:05 PM CEST session 0x16295bafcc40000 cxid 0x1 zxid 0x100000002 closeSession null
    Would you like to fix it (Yes/No/Abort) ? y
    EOF reached after 6 txns.
    Recovery file log.100000001.fixed has been written with 1 fixed CRC error(s)

The default behaviour of recovery is to be silent: only entries with CRC error get printed to the screen.
One can turn on verbose mode with the `-v,--verbose` parameter to see all records.
Interactive mode can be turned off with the `-y,--yes` parameter. In this case all CRC errors will be fixed
in the new transaction file.

<a name="zkSnapShotToolkit"></a>

### zkSnapShotToolkit.sh
Dump a snapshot file to stdout, showing the detailed information of the each zk-node.

```bash
# help
./zkSnapShotToolkit.sh
/usr/bin/java
USAGE: SnapshotFormatter [-d|-json] snapshot_file
       -d dump the data for each znode
       -json dump znode info in json format

# show the each zk-node info without data content
./zkSnapShotToolkit.sh /data/zkdata/version-2/snapshot.fa01000186d
/zk-latencies_4/session_946
  cZxid = 0x00000f0003110b
  ctime = Wed Sep 19 21:58:22 CST 2018
  mZxid = 0x00000f0003110b
  mtime = Wed Sep 19 21:58:22 CST 2018
  pZxid = 0x00000f0003110b
  cversion = 0
  dataVersion = 0
  aclVersion = 0
  ephemeralOwner = 0x00000000000000
  dataLength = 100

# [-d] show the each zk-node info with data content
./zkSnapShotToolkit.sh -d /data/zkdata/version-2/snapshot.fa01000186d
/zk-latencies2/session_26229
  cZxid = 0x00000900007ba0
  ctime = Wed Aug 15 20:13:52 CST 2018
  mZxid = 0x00000900007ba0
  mtime = Wed Aug 15 20:13:52 CST 2018
  pZxid = 0x00000900007ba0
  cversion = 0
  dataVersion = 0
  aclVersion = 0
  ephemeralOwner = 0x00000000000000
  data = eHh4eHh4eHh4eHh4eA==

# [-json] show the each zk-node info with json format
./zkSnapShotToolkit.sh -json /data/zkdata/version-2/snapshot.fa01000186d
[[1,0,{"progname":"SnapshotFormatter.java","progver":"0.01","timestamp":1559788148637},[{"name":"\/","asize":0,"dsize":0,"dev":0,"ino":1001},[{"name":"zookeeper","asize":0,"dsize":0,"dev":0,"ino":1002},{"name":"config","asize":0,"dsize":0,"dev":0,"ino":1003},[{"name":"quota","asize":0,"dsize":0,"dev":0,"ino":1004},[{"name":"test","asize":0,"dsize":0,"dev":0,"ino":1005},{"name":"zookeeper_limits","asize":52,"dsize":52,"dev":0,"ino":1006},{"name":"zookeeper_stats","asize":15,"dsize":15,"dev":0,"ino":1007}]]],{"name":"test","asize":0,"dsize":0,"dev":0,"ino":1008}]]
```

<a name="zkSnapshotComparer"></a>

### zkSnapshotComparer.sh

Compare the subtree data sizes and descendant counts in two snapshot files.
This is the native backport of [ZOOKEEPER-3427](https://github.com/apache/zookeeper/commit/f90060b83da4bfcca58ada93a57fedb40a069387).
The tool reports paths found only in the left or right snapshot and size/count
deltas for paths present in both. Deltas are **right minus left**.

```bash
bin/zkSnapshotComparer.sh --left snapshot.1 --right snapshot.2.gz --bytes 0 --nodes 0
```

All four options are required:

* `-l`, `--left`: left snapshot file.
* `-r`, `--right`: right snapshot file.
* `-b`, `--bytes`: non-negative byte-difference threshold.
* `-n`, `--nodes`: non-negative descendant-count-difference threshold.

Thresholds are integers from 0 through 2147483647. A path is printed when
**either** absolute difference is **strictly greater** than its threshold.
For a path present in only one snapshot, its subtree size and descendant count
are compared to the thresholds. Data sizes include the node's own payload and
all descendant payloads; descendant counts exclude the node itself. Null data
contributes zero bytes. A zero-byte leaf present in only one snapshot is
therefore filtered even with both thresholds set to zero.

Use `-d`, `--debug` to display filtered paths and comparison details. Use
`-i`, `--interactive` to explore the snapshots interactively:

* Press Enter to print the current depth and advance.
* Enter a non-negative depth to jump to that layer (the root is depth 0).
* Enter an absolute path, including `/`, to compare its immediate children.
* End input or press Ctrl-C to stop before all layers have been compared.

The batch report visits each depth and sorts paths alphabetically within it.
It retains the upstream empty label for the root in comparison lines.

<a name="zkSnapshotRecursiveSummaryToolkit"></a>

### zkSnapshotRecursiveSummaryToolkit.sh

Recursively summarize one snapshot subtree. This is the native backport of
[ZOOKEEPER-4566](https://github.com/apache/zookeeper/commit/05b215994f5e145c2758c4089828b57ba471b329).

```bash
bin/zkSnapshotRecursiveSummaryToolkit.sh snapshot.1 /app 1
```

Usage: `SnapshotRecursiveSummary <snapshot_file> <starting_node> <max_depth>`.
The starting node must be an existing absolute znode path. The maximum depth is
a non-negative integer: 0 prints every non-leaf node, 1 prints the starting
node and its non-leaf children, 2 adds another level, and so on.
**Depth limits output only, not traversal or totals.**

`children` is the number of all descendants, not just immediate children.
`data` is the sum of payload bytes for the node itself and all descendants.
Null data contributes zero bytes. Leaves contribute to their ancestors' totals
but are not printed; selecting a leaf produces no summary entries.

For example, if `/app` has 2 payload bytes, `/app/branch` has 3, and
`/app/branch/leaf` has 5, the summary is:

```text
 /app
   children: 2
   data: 10
-- /app/branch
--   children: 1
--   data: 8
```

#### Snapshot analysis limitations

Both tools run offline, read files without modifying them, and support
uncompressed, `.gz`, and `.snappy` snapshots (including mixed formats in the
comparer). They validate snapshot checksums and report unreadable or corrupt
input rather than silently producing a successful analysis. Invalid arguments,
file paths or summary starting paths exit with code 2; snapshot read failures
exit with code 1. Windows launchers with the same names and a `.cmd` extension
are also included. The launchers use the existing `zkEnv` configuration.

Both tools **include ephemeral znodes** present in the snapshot; they do not
report session records. This reflects the upstream traversal behavior, despite
the original comparer's description claiming that ephemerals were ignored.
Neither tool compares payload contents, ACLs, versions or other znode metadata.
**Equal sizes/counts do not prove identical contents.** Snapshots may be fuzzy:
these tools do not replay transaction logs, reconstruct point-in-time state,
or establish transaction-consistent equality. The tools load snapshots into
memory, and traversal still visits the full selected subtree.

<a name="zkOfflineAudit"></a>

### zkOfflineAudit.sh

Export versioned, non-value metadata from **one explicitly selected snapshot**.
The Windows equivalent is `bin/zkOfflineAudit.cmd`; the Java entry point is
`org.apache.zookeeper.server.OfflineAuditExporter`.

```bash
bin/zkOfflineAudit.sh --snapshot-file snapshot.100000001.gz --output-dir audit-run
```

The output directory **must not already exist**, even if empty, and its parent
must exist. Use a new, owned directory for each invocation. Existing files,
directories, hard links and symbolic links at the destination are rejected
without modifying them. Filesystem path resolution preserves symbolic-link
parent semantics; it does not lexically remove `..` and select a different file.
The tool never renames, rewrites or deletes the source, connects to a ZooKeeper
server, replays transaction logs, or selects a fallback snapshot.

The shared native reader accepts format-version 2 snapshots, uncompressed or
with the native `.gz`/`.snappy` suffix. It checks the header, body seal and any
optional digest seal, rejects partial trailers, invalid roots and duplicate node
records, and consumes the decoded stream to its end. A digest is recorded as metadata, **not** compared
as a transaction-consistent recovery digest. Fuzzy snapshots do not need
transaction-log coverage for this export.

#### Resources, publication and failures

The native reader loads one complete `DataTree`, its ACL cache and session
indexes. Budget heap for the **decoded** tree, not just the compressed file or
stored value bytes. The exporter adds an iterative traversal stack, sorted child
name references for the active ancestors, one JSON record at a time, and a copy
of one session's ephemeral path set at a time. It does not build a second tree
or retain all output records. Native quota initialization can still recurse
during loading; an unusually deep quota tree can exhaust the JVM stack.

Output can exceed snapshot size substantially: every node includes full paths
and metadata, and deep paths are repeated. Disk policy must also allow filesystem
allocation overhead. The optional `--max-output-bytes BYTES` is a **positive,
decimal signed-64-bit integer** bounding the combined UTF-8 bytes of the three
NDJSON files and the manifest, including newlines. No production budget is
assumed when it is absent. This is not an input-size, heap, stack, runtime or
filesystem-block quota. Supply approved worker budgets and JVM limits through
the existing launcher environment; do not infer safe limits from this example.

The tool creates a private directory (mode 0700 on POSIX), creates output files
without replacement, closes and forces the data files, stages the manifest,
rechecks source identity, and **atomically publishes `manifest.json` last**.
Atomic move support is required; there is no non-atomic fallback. Source checks
compare SHA-256 of the original file bytes, resolved path, size, precise mtime,
creation time, available file key and Unix change time where supported, including
stat checks around hashing. Access time is deliberately not compared because
reading may update it. Use immutable, owned input copies: these checks are not
a lock against concurrent writers, and identity/time precision depends on the
filesystem.

Exit 0 means publication succeeded. Invalid arguments, decoder properties or an
existing destination exit 2 with usage; snapshot, source-change, output-budget
and I/O failures exit 1 with diagnostics. JVM resource errors remain visible
and nonzero rather than becoming an empty-database success. A failed started
export can leave partial files and `.manifest.json.inprogress` for diagnosis;
**neither is a published result** and the directory cannot be reused. An
existing destination is rejected *before an export is attempted*: its prior
manifest is left untouched, not relabeled as this invocation's result. Consumers
must require a successful invocation into their unique run directory, read only
`manifest.json`, and verify its source identity, schema, file counts and hashes.
The file descriptors omit the manifest itself to avoid a checksum cycle.

#### Decoder configuration and owner uncertainty

Use JVM system properties (for example through `JVMFLAGS`) only when the source
settings are known:

* `zookeeper.extendedTypesEnabled`: `true` or `false`, case-insensitive.
* `zookeeper.emulate353TTLNodes`: `true` or `false`, case-insensitive. Explicit
  `true` requires explicit `zookeeper.extendedTypesEnabled=true`.
* `jute.maxbuffer`: positive 32-bit integer in the native Java numeric-property
  syntax (`Integer.decode`: decimal, `0x`/`#` hexadecimal or leading-zero octal).
  When absent, the native `BinaryInputArchive.maxBuffer` default applies.
* `zookeeper.jute.maxbuffer.extrasize`: non-negative 32-bit integer in the same
  syntax. When absent it defaults to `jute.maxbuffer`; the native minimum extra
  padding of 1024 bytes is applied. The effective sum must fit a signed 32-bit
  length. These decoder bounds are not estimates of server response limits.

Malformed properties are rejected instead of silently using Java's fallback.
Compression is detected by the selected file's suffix, not the output
compression system property. The optional digest is read regardless of the
local digest-calculation setting. Arbitrary JVM properties are not exported.

Snapshots do not persist the source's type settings. Absent source properties
are therefore `null` in the manifest, although the native decoder's effective
boolean defaults are false. Owner 0 is persistent, `0x8000000000000000` is a
container, and nonzero sign-bit-clear owners are normal ephemerals. Other owners
are classified as follows:

| Known source configuration | Classification of other negative owners |
| --- | --- |
| Extended types unspecified | `unknown`; never grouped as sessions |
| Extended types explicitly false | Native normal ephemeral/session |
| Extended types true, emulation unspecified | `unknown`; legacy interpretation is unresolved |
| Extended types true, emulation false | Native modern TTL or normal ephemeral; unsupported extended feature bits fail |
| Extended types true, emulation true | Native 3.5.3-emulated TTL |

`ttl_ms` uses this decoder's `EphemeralType.TTL.getValue(owner)` (low 40 bits),
including emulation; it does not reconstruct a historical implementation's
different value mask. It is a duration, **not** a calculated expiry time.
Neither an owner in `sessions.ndjson` nor membership in the snapshot session
table proves a currently live session.

#### Schema version 1

All files are UTF-8. Each NDJSON line is one complete JSON object; `manifest.json`
is one JSON object. `null` means unknown/not applicable, not zero. `integer`
below means a JSON integral number (64-bit arithmetic for counts, lengths,
byte totals and times); schema/format/digest versions, node version counters,
session timeouts and quota counts are 32-bit integers. No field is a JVM-memory
estimate. **All 64-bit zxids, owner/session identifiers and digest values are
lowercase, zero-padded, 16-digit hex strings prefixed with `0x`**, preserving
their unsigned bit patterns. SHA-256 values are 64 lowercase hex digits without
a prefix. Consumers must not convert identifiers to floating-point numbers.

**`nodes.ndjson`** includes the root exactly once as `/`, all customer nodes and
the reserved subtree. Records are emitted in deterministic depth-first
**preorder** with sorted children. Each parent's descendants are contiguous,
supporting streaming subtree intervals. This is not a global lexical path sort:
`/a/x` follows `/a` before the traversal advances to `/a-`.

| Field | Type | Exact meaning |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `path` | string | Full absolute decoded znode path; root is `/`, never `""` |
| `data_length` | integer | This decoded node's payload length in bytes; null data contributes 0 |
| `num_children` | integer | Number of **immediate** children, not descendants |
| `getchildren_response_bytes` | integer | Full successful getChildren reply, defined below |
| `getchildren2_response_bytes` | integer | Same reply plus the 68-byte Stat |
| `ctime_ms`, `mtime_ms` | integer | Stored node creation/data-modification timestamps, epoch milliseconds |
| `czxid`, `mzxid`, `pzxid` | hex string | Stored creation, data-modification and child-modification zxids |
| `data_version`, `acl_version` | integer | Persisted data/ACL version counters |
| `persisted_cversion` | integer | Raw persisted child-create counter; not the client Stat cversion conversion |
| `path_utf8_bytes` | integer | Full absolute path's UTF-8 length, including leading slash, without a length prefix |
| `node_type` | string | `persistent`, `ephemeral`, `container`, `ttl` or `unknown` |
| `ephemeral_owner` | hex string | Raw persisted owner bits, including 0, TTL/container and unknown encodings |
| `owner_encoding` | string | `none`, `session`, `container`, `ttl`, `ttl-3.5.3` or `unknown` |
| `ttl_ms` | integer or null | Native decoded TTL duration for known TTL types only |
| `acl_risk_flags` | array of strings | Non-secret review hints below; empty means no listed hint, not proven safe |

The getChildren size is
`16 + 4 + sum(4 + UTF-8(child-name).length)`: the 16-byte ReplyHeader, four-byte
vector count, and four-byte length plus bytes for each **child name**, not full
path. getChildren2 adds 68 bytes. Both **exclude** the outer four-byte framing
prefix and transport/TLS overhead. Empty vectors are 20/88 bytes; names `é`, `a`
are 31/99 bytes. Counts use `long` arithmetic, independent of any packet limit.
Actual Jute serialization agrees for valid znode names. This branch's older
Jute encoder treats surrogate pairs differently from standard UTF-8, but native
`PathUtils` prohibits those characters in znode paths; malformed decoded paths
are rejected, not silently relabeled.

ACL hints do not disclose ACL scheme strings or IDs, change permissions, or
perform authentication/authorization:

| Flag | Meaning |
| --- | --- |
| `world_read` | `world:anyone` has READ |
| `world_write` | `world:anyone` has any of WRITE, CREATE or DELETE |
| `world_admin` | `world:anyone` has ADMIN |
| `auth` | An `auth` scheme entry remains in the decoded ACL |
| `unknown_scheme` | Scheme outside the known set `world`, `auth`, `digest`, `ip`, `sasl`, `x509`; custom providers require review |
| `missing_acl` | Decoded ACL list is null or empty |
| `invalid_permissions` | Permission bits outside `ZooDefs.Perms.ALL` |
| `invalid_identity` | Missing identity/scheme/ID, or a `world` identity other than `anyone` |

Native deserialization can reject malformed ACL structures before flags can be
produced. Node values and raw ACL identities are never written to the export.
Paths themselves can be sensitive; protect the output as metadata.

**`namespaces.ndjson`** contains **every** immediate root child except exactly
`/zookeeper`, including leaves and `/zookeeper-client`. Counts/bytes include the
namespace node and all descendants of every node type. Root data is not assigned
to a namespace. Records follow traversal order.

| Field | Type | Exact meaning |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `path` | string | Top-level customer namespace path |
| `subtree_node_count` | integer | Inclusive subtree node count |
| `payload_bytes` | integer | Sum of decoded payload lengths; null contributes 0 |
| `path_utf8_bytes` | integer | Sum of full absolute path lengths in UTF-8 |
| `quota_status` | string | `absent`, `valid` or `invalid` for the exact namespace limit znode |
| `quota_limit_count` | integer or null | Parsed `count`, including -1 (unset); null if absent/invalid |
| `quota_limit_bytes` | integer or null | Parsed `bytes`, including -1 (unset); null if absent/invalid |
| `quota_stat_present` | boolean | Exact namespace quota stat znode exists; not evidence of freshness |

Quota availability refers specifically to
`/zookeeper/quota<namespace>/zookeeper_limits` and `zookeeper_stats`; it does not
claim absence of quotas on descendants or model inherited enforcement. A valid
limit has the native `count=<int>,bytes=<long>` format with values at least -1.
Malformed limits are flagged without exporting their raw value. Native DataTree
loading **rebuilds quota-stat payloads in memory**; node lengths under the
reserved quota subtree describe that decoded tree, not necessarily the original
stat-node payload lengths. Customer subtree totals are computed independently
from the decoded customer nodes; no rebuilt quota count is trusted for them.

**`sessions.ndjson`** has one record per owner with decoded normal ephemerals.
It excludes containers, known TTLs, unknown encodings and session-table entries
with no normal ephemeral nodes. Session ordering is unspecified; records are
streamed from the native index, including ephemerals in the reserved subtree.

| Field | Type | Exact meaning |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `session_id` | hex string | Normal ephemeral owner under the known/unambiguous decoder interpretation |
| `ephemeral_node_count` | integer | Number of nodes owned by that session |
| `payload_bytes` | integer | Sum of those nodes' payload lengths |
| `path_utf8_bytes` | integer | Sum of those nodes' full UTF-8 path lengths |
| `present_in_snapshot_session_table` | boolean | Owner also appears in the serialized session/timeout table |
| `timeout_ms` | integer or null | Serialized timeout if present; null otherwise, not a remaining lifetime |

**`manifest.json`** is the sole publication marker. Export completeness means
complete traversal/output of this decoded snapshot, **not** current-state,
backup completeness or transaction-consistent recovery.

| Field | Type | Exact meaning |
| --- | --- | --- |
| `schema_version` | integer | `1` |
| `tool` | object | `name` = `OfflineAuditExporter`; `version` = native `Version.getFullVersion()` string |
| `export_id` | string | New UUID identifying this published export |
| `export_complete` | boolean | `true` in a successfully published manifest only |
| `recovery_scope` | string | Always `snapshot-only` |
| `transaction_logs_replayed` | boolean | Always `false` |
| `source_capture_time_ms` | null | Capture time unknown; never inferred from file or node timestamps |
| `source_capture_provenance` | null | No authoritative capture provenance supplied to this CLI |
| `source_server_version` | null | Snapshot header version does not identify the source server release |
| `source` | object | Source descriptor defined below |
| `decoder` | object | Decoder settings defined below |
| `snapshot_zxid` | hex string or null | Label parsed only from `snapshot.<1..16 hex digits>[.gz or .snappy]`; renamed/range filenames yield null; not a consistent endpoint |
| `last_observed_zxid` | hex string | Unsigned maximum of all exported nodes' `czxid`, `mzxid`, `pzxid`; excludes filename/digest zxids |
| `last_observed_zxid_scope` | string | Always `maximum-node-czxid-mzxid-pzxid-unsigned` |
| `snapshot_digest` | object or null | Optional stored digest metadata, not recovery evidence |
| `unknown_owner_nodes` | integer | Count of node records with `node_type=unknown` |
| `max_output_bytes` | integer or null | Requested output byte budget, or no CLI budget |
| `files` | array of objects | Exactly three output descriptors, in nodes/namespaces/sessions order |

Nested objects have these exact fields:

* `source`: `path` (resolved absolute filename string), `sha256` (hash of the
  original **compressed bytes**, if compressed), `size_bytes` (integer),
  `mtime_ms` (integer epoch milliseconds), `change_time_ms` (Unix inode-change
  time in epoch milliseconds, or null if unavailable), `compression` (`checked`
  = uncompressed, `gzip` or `snappy`), `format_version` (integer, currently 2),
  `database_id` (raw header dbid as a hex string). None of the file times is
  snapshot capture time.
* `decoder`: `source_extended_types_enabled` and `source_emulate_353_ttl_nodes`
  (boolean or null); `effective_extended_types_enabled` and
  `effective_emulate_353_ttl_nodes` (boolean); `jute_maxbuffer` and
  `jute_extra_maxbuffer` (integers after native default/minimum handling).
* `snapshot_digest`, when present: `zxid` (hex string), `version` (integer),
  `value` (hex string), `seal_validated` (true),
  `transaction_consistency_verified` (false). A native zero/empty digest is
  retained as zero, not promoted to completeness evidence.
* Each `files` entry: `name` (exactly `nodes.ndjson`, `namespaces.ndjson` or
  `sessions.ndjson`), `records` (integer line count), `size_bytes` (integer
  physical file length), `sha256` (hash of all file bytes, including newlines).

<a name="Testing"></a>

## Testing

<a name="jepsen-test"></a>

### Jepsen Test
A framework for distributed systems verification, with fault injection.
Jepsen has been used to verify everything from eventually-consistent commutative databases to linearizable coordination systems to distributed task schedulers.
more details can be found in [jepsen-io](https://github.com/jepsen-io/jepsen)

Running the [Dockerized Jepsen](https://github.com/jepsen-io/jepsen/blob/master/docker/README.md) is the simplest way to use the Jepsen.

Installation:

```bash
git clone git@github.com:jepsen-io/jepsen.git
cd docker
# maybe a long time for the first init.
./up.sh
# docker ps to check one control node and five db nodes are up
docker ps
     CONTAINER ID        IMAGE               COMMAND                 CREATED             STATUS              PORTS                     NAMES
     8265f1d3f89c        docker_control      "/bin/sh -c /init.sh"   9 hours ago         Up 4 hours          0.0.0.0:32769->8080/tcp   jepsen-control
     8a646102da44        docker_n5           "/run.sh"               9 hours ago         Up 3 hours          22/tcp                    jepsen-n5
     385454d7e520        docker_n1           "/run.sh"               9 hours ago         Up 9 hours          22/tcp                    jepsen-n1
     a62d6a9d5f8e        docker_n2           "/run.sh"               9 hours ago         Up 9 hours          22/tcp                    jepsen-n2
     1485e89d0d9a        docker_n3           "/run.sh"               9 hours ago         Up 9 hours          22/tcp                    jepsen-n3
     27ae01e1a0c5        docker_node         "/run.sh"               9 hours ago         Up 9 hours          22/tcp                    jepsen-node
     53c444b00ebd        docker_n4           "/run.sh"               9 hours ago         Up 9 hours          22/tcp                    jepsen-n4
```

Running & Test

```bash
# Enter into the container:jepsen-control
docker exec -it jepsen-control bash
# Test
cd zookeeper && lein run test --concurrency 10
# See something like the following to assert that ZooKeeper has passed the Jepsen test
INFO [2019-04-01 11:25:23,719] jepsen worker 8 - jepsen.util 8	:ok	:read	2
INFO [2019-04-01 11:25:23,722] jepsen worker 3 - jepsen.util 3	:invoke	:cas	[0 4]
INFO [2019-04-01 11:25:23,760] jepsen worker 3 - jepsen.util 3	:fail	:cas	[0 4]
INFO [2019-04-01 11:25:23,791] jepsen worker 1 - jepsen.util 1	:invoke	:read	nil
INFO [2019-04-01 11:25:23,794] jepsen worker 1 - jepsen.util 1	:ok	:read	2
INFO [2019-04-01 11:25:24,038] jepsen worker 0 - jepsen.util 0	:invoke	:write	4
INFO [2019-04-01 11:25:24,073] jepsen worker 0 - jepsen.util 0	:ok	:write	4
...............................................................................
Everything looks good! ヽ(‘ー`)ノ

```

Reference:
read [this blog](https://aphyr.com/posts/291-call-me-maybe-zookeeper) to learn more about the Jepsen test for the Zookeeper.
