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

# ZooKeeper Audit Logging

* [ZooKeeper Audit Logs](#ch_auditLogs)
* [ZooKeeper Audit Log Configuration](#ch_auditConfig)
* [Enhanced audit metadata (schema v2)](#ch_auditV2)
* [Who is taken as user in audit logs?](#ch_zkAuditUser)
<a name="ch_auditLogs"></a>

## ZooKeeper Audit Logs

Apache ZooKeeper supports audit logs from version 3.6.0. By default audit logs are disabled. To enable audit logs
 configure audit.enable=true in conf/zoo.cfg. Audit logs are not logged on all the ZooKeeper servers, but logged only on the servers where client is connected as depicted in below figure.

![Audit Logs](images/zkAuditLogs.jpg)


The audit log captures detailed information for the operations that are selected to be audited. The audit information is written as a set of key=value pairs for the following keys

| Key   | Value |
| ----- | ----- |
|session | client session id |
|user | comma separated list of users who are associate with a client session. For more on this, see [Who is taken as user in audit logs](#ch_zkAuditUser).
|ip | client IP address
|operation | any one of the selected operations for audit. Possible values are(serverStart, serverStop, create, delete, setData, setAcl, multiOperation, reconfig, ephemeralZNodeDeletionOnSessionCloseOrExpire)
|znode | path of the znode
|znode_type | type of znode in case of creation operation
|acl | String representation of znode ACL like cdrwa(create, delete,read, write, admin). This is logged only for setAcl operation
|result | result of the operation. Possible values are (success/failure/invoked). Result "invoked" is used for serverStop operation because stop is logged before ensuring that server actually stopped.

Below are sample audit logs for all operations, where client is connected from 192.168.1.2, client principal is zkcli@HADOOP.COM, server principal is zookeeper/192.168.1.3@HADOOP.COM

    user=zookeeper/192.168.1.3 operation=serverStart   result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=create    znode=/a    znode_type=persistent  result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=create    znode=/a    znode_type=persistent  result=failure
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=setData   znode=/a    result=failure
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=setData   znode=/a    result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=setAcl    znode=/a    acl=world:anyone:cdrwa  result=failure
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=setAcl    znode=/a    acl=world:anyone:cdrwa  result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=create    znode=/b    znode_type=persistent  result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=setData   znode=/b    result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=delete    znode=/b    result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=multiOperation    result=failure
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=delete    znode=/a    result=failure
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=delete    znode=/a    result=success
    session=0x19344730001   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=create   znode=/ephemral znode_type=ephemral result=success
    session=0x19344730001   user=zookeeper/192.168.1.3   operation=ephemeralZNodeDeletionOnSessionCloseOrExpire  znode=/ephemral result=success
    session=0x19344730000   user=192.168.1.2,zkcli@HADOOP.COM  ip=192.168.1.2    operation=reconfig  znode=/zookeeper/config result=success
    user=zookeeper/192.168.1.3 operation=serverStop    result=invoked

<a name="ch_auditConfig"></a>

## ZooKeeper Audit Log Configuration

By default audit logs are disabled. To enable audit logs configure `audit.enable=true` in _conf/zoo.cfg_. 
Audit logging is done using logback. Following is the default logback configuration for audit logs in `conf/logback.xml`

    <!--
      zk audit logging
    -->
    <!--property name="zookeeper.auditlog.file" value="zookeeper_audit.log" />
    <property name="zookeeper.auditlog.threshold" value="INFO" />
    <property name="audit.logger" value="INFO, RFAAUDIT" />
    
    <appender name="RFAAUDIT" class="ch.qos.logback.core.rolling.RollingFileAppender">
      <File>${zookeeper.log.dir}/${zookeeper.auditlog.file}</File>
      <encoder>
        <pattern>%d{ISO8601} %p %c{2}: %m%n</pattern>
      </encoder>
      <filter class="ch.qos.logback.classic.filter.ThresholdFilter">
        <level>${zookeeper.auditlog.threshold}</level>
      </filter>
      <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
        <maxIndex>10</maxIndex>
        <FileNamePattern>${zookeeper.log.dir}/${zookeeper.auditlog.file}.%i</FileNamePattern>
      </rollingPolicy>
      <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
        <MaxFileSize>10MB</MaxFileSize>
      </triggeringPolicy>
    </appender>
    
    <logger name="org.apache.zookeeper.audit.Slf4jAuditLogger" additivity="false" level="${audit.logger}">
      <appender-ref ref="RFAAUDIT" />
    </logger-->

Change above configuration to customize the auditlog file, number of backups, max file size, custom audit logger etc.

<a name="ch_auditV2"></a>

## Enhanced audit metadata (schema v2)

The optional Java system property `zookeeper.audit.enhanced.enable` defaults to
`false`. Set `-Dzookeeper.audit.enhanced.enable=true` in addition to enabling
`zookeeper.audit.enable` to emit v2 records. Enhanced logging does not enable
audit logging by itself. With enhanced logging off, ordinary legacy formatting
and the existing logging APIs are preserved. TTL creates are audited in both
modes, including sequential TTL creates.

V2 keeps the existing field names and `result=success/failure/invoked`, and adds:

| Key | Meaning |
| --- | --- |
| `schema_version` | `2`. Its absence identifies legacy output. |
| `data_length` | Attempted payload size in bytes for create variants and `setData`, not request size, character count, or resulting znode size. Null and empty payloads are `0`; unavailable or undecodable payloads omit this field. A failed attempt can have a nonzero length. |
| `error_code` | Known numeric ZooKeeper code, such as `0` (OK), `-101` (NONODE), `-102` (NOAUTH), `-103` (BADVERSION), or `-110` (NODEEXISTS). Omitted when unavailable. |
| `outcome` | `committed`, `failed`, `rolled_back`, or `unknown`, as described below. |
| `cxid` | Known client request ID in decimal. All members of a multi share this ID. |
| `zxid` | Known transaction ID in decimal, not the server's latest zxid at reply time. A failed transaction may also have a zxid. |
| `multi_index` | Zero-based position in the **complete** multi request, including non-mutating checks. Omitted on single operations and multi parent records. |

No znode payloads are included. For enhanced `setAcl` records, `acl` retains
schemes and permissions, but identities are conservative: `world:anyone` is
retained, valid built-in digest identities use the provider's username extraction
(not the digest), and other identities are `[redacted]`. This also applies to
failed ACL attempts, where identities might be malformed or contain credentials.
Unknown custom identity representations are not printed as ACL identities.

For server-generated v2 records, the `user` field also uses a conservative
provider policy rather than trusting a custom provider's default `getUserName`.
Only the concrete built-in digest, IP, SASL and X509 providers are trusted, and
their identity syntax must validate before their username extraction is used.
Digest credentials are reduced to usernames; X509 identities are distinguished
names, not certificate bodies. Unknown or malformed identities, custom providers
and provider subclasses are represented as `[redacted]`. Merely overriding
`getUserName` does not opt a custom provider into this trust policy. Legacy user
extraction is unchanged when enhanced logging is off.

Custom callers of the logging APIs must supply sanitized user and ACL metadata;
the string-based APIs cannot infer credentials from arbitrary strings.

### Results and transaction outcomes

* `committed` means the available transaction result reports an applied write.
  These records have `result=success` and `error_code=0`. It does not guarantee
  that the reply reached the client or the log reached durable storage.
* `failed` means the operation or whole multi failed according to its known
  result. For rejected single transactions, the request exception takes
  precedence when the reply path uses it instead of the transaction error.
* `rolled_back` means a mutation was not applied because its atomic multi
  failed. This includes prepared members discarded by rollback and members
  skipped after the failure. They have `result=failure`, **even when their
  individual `error_code` is `0`**. Later skipped members normally have code
  `-2` (RUNTIMEINCONSISTENCY); a first failure with that code is still `failed`.
* `unknown` means the available metadata cannot establish an operation's
  outcome. Missing or mismatched multi results are not treated as commits,
  and untrustworthy per-member error codes are omitted. If the whole multi
  is known to have failed, these members still have `result=failure`;
  otherwise unknown operations use `result=invoked`.

Failed multis keep their `multiOperation` parent failure record, followed in v2
by records for the attempted mutations when the request is decodable. The parent
is retained even if request decoding fails. Successful multis emit individual
mutation records, not a parent success record. Request and result members are
paired by position, so repeated paths cannot overwrite each other's metadata.
Successful sequential creates use the final path returned by the transaction;
failed or rolled-back creates use the attempted path.

### Supported operations and identity

The audited operations remain creates (`create`, `create2`, container and TTL
variants), deletes (including container deletes), `setData`, `setAcl`, write
multis, reconfiguration, server start/stop, and existing system ephemeral-node
deletions. Ordinary reads, read multis and checks do not produce audit records.
Checks inside a write multi still occupy an index.

Existing lifecycle events and callers of the legacy logging overloads receive
`outcome=unknown` in v2 when they do not supply transaction metadata; their
existing `result` is unchanged. No session/authentication binding events are
added by this schema.

System ephemeral-node deletion records preserve the **server actor**, the
affected session and path, and omit client IP. V2 adds the deletion transaction's
zxid, known error code and outcome. A client request ID is not available at this
deletion hook, so it is omitted. These system records can be emitted on multiple
replicas or during replay. Downstream consumers can deduplicate using ensemble
identity plus `zxid`, `operation`, `session` and `znode`; client multi mutations
also require `multi_index` to distinguish repeated paths.

### Escaping and parsing

Fields are separated by literal tabs. Split each field at its **first** equals
sign; equals signs inside values are unchanged. In v2 only, value formatting
reversibly escapes backslash as `\\`, tab as `\t`, carriage return as `\r`, and
newline as `\n`. A literal backslash followed by `t` is therefore `\\t`, distinct
from an escaped tab. Decode left to right, consuming one escape pair at a time;
do not use successive global replacements to unescape. For example, the user
value containing a tab and newline is rendered as `user=team\tname\n` on a single
physical log line. Enum-derived keys and values are independent of the server's
default locale. Custom `AuditLogger` implementations receive raw event values;
`AuditEvent.toString()` is the production text-formatting path.

### Mixed versions, rollout, rollback and coverage

Consumers should recognize v2 per record using `schema_version=2`, tolerate
unknown additive fields, and accept omitted optional fields. Do not apply v2
unescaping to legacy records. Prepare consumers for mixed legacy/v2 output before
enabling the property on servers, then roll it out gradually. Roll back emission
by removing the enhanced property or setting it to `false` on restart. The
enhanced gate is checked when emitting events; the base audit enablement retains
its startup behavior. No wire, persistence, authentication, ACL, quota or payload
limit changes are required.

The bounded-cardinality server counter `audit_errors` counts detected audit
metadata extraction/correlation failures and runtime failures reported by the
audit logger. Such failures are logged without rejecting an otherwise valid
client operation. A metadata error can still yield an audit record with omitted
fields, so this counter is **not** a dropped-record count. It has no per-path,
per-user or per-session labels.

Audit coverage is limited to the existing transaction-audit hooks. Rejections
before a hook, connection-level failures and later reply/send failures need
separate instrumentation; these records are not a complete request-delivery
ledger. Log delivery is also a separate concern: an asynchronous appender using
`neverBlock=true` can discard records without reporting an exception.
`audit_errors` cannot measure those silent drops, filtering, retention loss, or
downstream delivery gaps. Validate source-log coverage and delivery independently
before relying on the stream for complete accounting.

<a name="ch_zkAuditUser"></a>

## Who is taken as user in audit logs?

By default there are only four authentication provider:

* IPAuthenticationProvider
* SASLAuthenticationProvider
* X509AuthenticationProvider
* DigestAuthenticationProvider

User is decided based on the configured authentication provider:

* When IPAuthenticationProvider is configured then authenticated IP is taken as user
* When SASLAuthenticationProvider is configured then client principal is taken as user
* When X509AuthenticationProvider is configured then client certificate is taken as user
* When DigestAuthenticationProvider is configured then authenticated user is user 

Custom authentication provider can override org.apache.zookeeper.server.auth.AuthenticationProvider.getUserName(String id)
 to provide user name. If authentication provider is not overriding this method then whatever is stored in 
 org.apache.zookeeper.data.Id.id is taken as user. 
 Generally only user name is stored in this field but it is up to the custom authentication provider what they store in it. 
 For audit logging value of org.apache.zookeeper.data.Id.id would be taken as user.

In ZooKeeper Server not all the operations are done by clients but some operations are done by the server itself. For example when client closes the session, ephemeral znodes are deleted by the Server. These deletion are not done by clients directly but it is done the server itself these are called system operations. For these system operations the user associated with the ZooKeeper server are taken as user while audit logging these operations. For example if in ZooKeeper server principal is zookeeper/hadoop.hadoop.com@HADOOP.COM then this becomes the system user and all the system operations will be logged with this user name.

	user=zookeeper/hadoop.hadoop.com@HADOOP.COM operation=serverStart result=success


If there is no user associate with ZooKeeper server then the user who started the ZooKeeper server is taken as the user. For example if server started by root then root is taken as the system user

	user=root operation=serverStart result=success


Single client can attach multiple authentication schemes to a session, in this case all authenticated schemes will taken taken as user and will be presented as comma separated list. For example if a client is authenticate with principal zkcli@HADOOP.COM and ip 127.0.0.1 then create znode audit log will be as:		

	session=0x10c0bcb0000 user=zkcli@HADOOP.COM,127.0.0.1 ip=127.0.0.1 operation=create znode=/a result=success

	
