/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.audit;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.audit.AuditEvent.Outcome;
import org.apache.zookeeper.audit.AuditEvent.Result;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.auth.IPAuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.SASLAuthenticationProvider;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to decouple audit log code.
 */
public final class AuditHelper {
    private static final Logger LOG = LoggerFactory.getLogger(AuditHelper.class);
    private static final String REDACTED = "[redacted]";

    /**
     * Records the identities on a valid session attachment, including reconnects.
     * The caller must have validated and assigned the session before invoking this hook.
     */
    public static void addSessionEstablishedLog(ServerCnxn cnxn) {
        logConnection(cnxn, AuditConstants.OP_SESSION_ESTABLISHED, null, Result.SUCCESS, null, null);
    }

    /**
     * Records an actual authentication outcome, not an intermediate SASL challenge.
     * Success includes the accepted scheme's current bindings; failure does not infer a principal.
     */
    public static void addAuthenticationLog(ServerCnxn cnxn, String scheme, Code code, int cxid) {
        logConnection(cnxn, AuditConstants.OP_AUTHENTICATION, scheme,
                code == Code.OK ? Result.SUCCESS : Result.FAILURE, code == null ? null : code.intValue(), cxid);
    }

    private static void logConnection(ServerCnxn cnxn, String operation, String scheme,
                                      Result result, Integer error, Integer cxid) {
        try {
            boolean enhanced = ZKAuditProvider.isEnhancedAuditEnabled();
            if (!enhanced) {
                return;
            }
            long sessionId = cnxn.getSessionId();
            String session = sessionId == 0 ? null : "0x" + Long.toHexString(sessionId);
            String ip = cnxn.getHostAddress();
            Map<String, Set<String>> bindings = connectionBindings(cnxn, operation, scheme, result);
            for (Map.Entry<String, Set<String>> binding : bindings.entrySet()) {
                for (String user : binding.getValue()) {
                    ZKAuditProvider.logConnection(user, operation, binding.getKey(),
                            session, ip, result, error, cxid, enhanced);
                }
            }
        } catch (RuntimeException e) {
            ZKAuditProvider.reportAuditError(LOG, "Failed to audit log operation {}", operation, e);
        }
    }

    private static Map<String, Set<String>> connectionBindings(
            ServerCnxn cnxn, String operation, String scheme, Result result) {
        boolean attachment = AuditConstants.OP_SESSION_ESTABLISHED.equals(operation);
        Map<String, Set<String>> bindings = new LinkedHashMap<>();
        if (result == Result.SUCCESS) {
            for (Id id : cnxn.getAuthInfo()) {
                if (attachment || (id != null && scheme != null && scheme.equals(id.getScheme()))) {
                    bindings.computeIfAbsent(id == null ? null : id.getScheme(), key -> new LinkedHashSet<>())
                            .add(safeUser(id));
                }
            }
        }
        if (bindings.isEmpty()) {
            bindings.put(scheme, Collections.singleton(null));
        }
        return bindings;
    }

    public static void addAuditLog(Request request, ProcessTxnResult rc) {
        addAuditLog(request, rc, false);
    }

    /**
     * Add audit log if audit log is enabled and operation is of type which to be audit logged.
     *
     * @param request   user request
     * @param txnResult ProcessTxnResult
     * @param failedTxn whether the transaction was rejected before applying the requested operation
     */
    public static void addAuditLog(Request request, ProcessTxnResult txnResult, boolean failedTxn) {
        if (!ZKAuditProvider.isAuditEnabled()) {
            return;
        }
        try {
            if (request.type == OpCode.multi) {
                logMultiOperation(request, txnResult, failedTxn);
                return;
            }
            String operation = operationFor(request.type);
            if (operation == null) {
                return;
            }
            Integer error = errorCode(request, txnResult, failedTxn);
            Outcome outcome = outcome(txnResult, failedTxn, error);
            RequestMetadata metadata = new RequestMetadata();
            boolean enhanced = ZKAuditProvider.isEnhancedAuditEnabled();
            if (isCreate(request.type) || request.type == OpCode.setACL
                    || (request.type != OpCode.reconfig && (enhanced || outcome != Outcome.COMMITTED))) {
                try {
                    metadata = metadata(request.type, readRequestRecord(request), enhanced);
                } catch (IOException e) {
                    auditError(request.type, e);
                }
            }
            String path = txnResult == null ? null : txnResult.path;
            if (outcome != Outcome.COMMITTED || path == null) {
                path = metadata.path == null ? path : metadata.path;
            }
            log(request, txnResult, path, operation, metadata, result(outcome), error, outcome, null, enhanced);
        } catch (RuntimeException e) {
            auditError(request.type, e);
        }
    }

    private static Record readRequestRecord(Request request) throws IOException {
        Record record;
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createContainer:
                record = new CreateRequest();
                break;
            case OpCode.createTTL:
                record = new CreateTTLRequest();
                break;
            case OpCode.delete:
            case OpCode.deleteContainer:
                record = new DeleteRequest();
                break;
            case OpCode.setData:
                record = new SetDataRequest();
                break;
            case OpCode.setACL:
                record = new SetACLRequest();
                break;
            case OpCode.multi:
                record = new MultiOperationRecord();
                break;
            default:
                throw new IOException("Unsupported audit request type: " + request.type);
        }
        if (request.request == null) {
            throw new IOException("Audit request record is unavailable");
        }
        ByteBuffer buffer = request.request.duplicate();
        buffer.rewind();
        ByteBufferInputStream.byteBuffer2Record(buffer, record);
        return record;
    }

    private static void logMultiOperation(Request request, ProcessTxnResult rc, boolean failedTxn) {
        boolean enhanced = ZKAuditProvider.isEnhancedAuditEnabled();
        Integer error = errorCode(request, rc, failedTxn);
        boolean failed = failedTxn || (error != null && error != Code.OK.intValue());
        int failureIndex = -1;
        if (rc != null && rc.multiResult != null) {
            for (int index = 0; index < rc.multiResult.size(); index++) {
                ProcessTxnResult subResult = rc.multiResult.get(index);
                if (subResult != null && (subResult.type == OpCode.error || subResult.err != Code.OK.intValue())) {
                    failed = true;
                    if (failureIndex < 0 && subResult.err != Code.OK.intValue()) {
                        failureIndex = index;
                    }
                    if (error == null || error == Code.OK.intValue()) {
                        error = subResult.err;
                    }
                }
            }
        }
        // Emit the failed parent before decoding so malformed metadata cannot hide the failure.
        if (failed) {
            log(request, rc, rc == null ? null : rc.path, AuditConstants.OP_MULTI_OP,
                    new RequestMetadata(), Result.FAILURE, error, Outcome.FAILED, null, enhanced);
            if (!enhanced) {
                return;
            }
        }
        MultiOperationRecord multiRequest;
        try {
            multiRequest = (MultiOperationRecord) readRequestRecord(request);
        } catch (IOException e) {
            auditError(request.type, e);
            return;
        }
        boolean complete = rc != null && rc.type == OpCode.multi && rc.multiResult != null
                && rc.multiResult.size() == multiRequest.size();
        if (complete) {
            int index = 0;
            for (Op op : multiRequest) {
                ProcessTxnResult subResult = rc.multiResult.get(index++);
                if (subResult == null || (subResult.type != OpCode.error
                        && subResult.type != op.getType()
                        && !(subResult.type == OpCode.create2 && op.getType() == OpCode.create))) {
                    complete = false;
                    break;
                }
            }
        }
        if (!complete) {
            auditError(request.type, new IOException("Audit multi request/result positions do not match"));
        }
        int index = 0;
        for (Op op : multiRequest) {
            ProcessTxnResult subResult = complete ? rc.multiResult.get(index) : null;
            String operation = operationFor(op.getType());
            if (operation != null) {
                Outcome outcome;
                if (!complete) {
                    outcome = Outcome.UNKNOWN;
                } else if (failed) {
                    // A zero-code error member was rolled back, not committed. Later members are not applied.
                    outcome = subResult.err == Code.OK.intValue()
                            || (index > failureIndex && subResult.err == Code.RUNTIMEINCONSISTENCY.intValue())
                            ? Outcome.ROLLED_BACK : Outcome.FAILED;
                } else {
                    outcome = Outcome.COMMITTED;
                }
                RequestMetadata metadata = metadata(op.getType(), op.toRequestRecord(), enhanced);
                String path = outcome == Outcome.COMMITTED ? subResult.path : op.getPath();
                log(request, rc, path, operation, metadata, failed ? Result.FAILURE : result(outcome),
                        subResult == null ? null : subResult.err, outcome, index, enhanced);
            }
            index++;
        }
    }

    private static RequestMetadata metadata(int type, Record record, boolean enhanced) {
        RequestMetadata metadata = new RequestMetadata();
        switch (type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createContainer:
                CreateRequest create = (CreateRequest) record;
                metadata.path = create.getPath();
                metadata.dataLength = length(create.getData());
                metadata.createMode = createMode(type, create.getFlags());
                break;
            case OpCode.createTTL:
                CreateTTLRequest ttl = (CreateTTLRequest) record;
                metadata.path = ttl.getPath();
                metadata.dataLength = length(ttl.getData());
                metadata.createMode = createMode(type, ttl.getFlags());
                break;
            case OpCode.setData:
                SetDataRequest setData = (SetDataRequest) record;
                metadata.path = setData.getPath();
                metadata.dataLength = length(setData.getData());
                break;
            case OpCode.delete:
            case OpCode.deleteContainer:
                metadata.path = ((DeleteRequest) record).getPath();
                break;
            case OpCode.setACL:
                SetACLRequest setAcl = (SetACLRequest) record;
                metadata.path = setAcl.getPath();
                if (setAcl.getAcl() != null) {
                    metadata.acl = enhanced ? safeAclToString(setAcl.getAcl()) : ZKUtil.aclToString(setAcl.getAcl());
                }
                break;
            default:
                break;
        }
        return metadata;
    }

    private static String safeAclToString(List<ACL> acls) {
        StringBuilder value = new StringBuilder();
        for (ACL acl : acls) {
            Id id = acl.getId();
            String user = REDACTED;
            if ("world".equals(id.getScheme()) && "anyone".equals(id.getId())) {
                user = "anyone";
            } else if ("digest".equals(id.getScheme())) {
                AuthenticationProvider provider = ProviderRegistry.getProvider(id.getScheme());
                if (provider != null && provider.getClass() == DigestAuthenticationProvider.class
                        && id.getId() != null && provider.isValid(id.getId())) {
                    user = provider.getUserName(id.getId());
                }
            }
            value.append(id.getScheme()).append(':').append(user).append(':')
                    .append(ZKUtil.getPermString(acl.getPerms()));
        }
        return value.toString();
    }

    private static String getUsers(Request request, boolean enhanced) {
        if (!enhanced) {
            return request.getUsers();
        }
        if (request.authInfo == null) {
            return null;
        }
        StringBuilder users = new StringBuilder();
        boolean first = true;
        for (Id id : request.authInfo) {
            if (!first) {
                users.append(',');
            }
            first = false;
            users.append(safeUser(id));
        }
        return users.toString();
    }

    private static String safeUser(Id id) {
        if (id == null || id.getScheme() == null || id.getId() == null) {
            return REDACTED;
        }
        AuthenticationProvider provider = ProviderRegistry.getProvider(id.getScheme());
        if (provider == null) {
            return REDACTED;
        }
        Class<?> providerClass = provider.getClass();
        // Custom providers, including subclasses, do not establish safe identity representations.
        if ((providerClass == DigestAuthenticationProvider.class
                || providerClass == IPAuthenticationProvider.class
                || providerClass == SASLAuthenticationProvider.class
                || providerClass == X509AuthenticationProvider.class)
                && provider.isValid(id.getId())) {
            return provider.getUserName(id.getId());
        }
        return REDACTED;
    }

    private static String createMode(int type, int flags) {
        try {
            return CreateMode.fromFlag(flags).name().toLowerCase(Locale.ROOT);
        } catch (KeeperException e) {
            auditError(type, e);
            return null;
        }
    }

    private static int length(byte[] data) {
        return data == null ? 0 : data.length;
    }

    private static Integer errorCode(Request request, ProcessTxnResult rc, boolean failedTxn) {
        if (failedTxn && request.getException() != null) {
            return request.getException().code().intValue();
        }
        return rc == null || rc.type == 0 ? null : rc.err;
    }

    private static Outcome outcome(ProcessTxnResult rc, boolean failedTxn, Integer error) {
        if (failedTxn || (rc != null && rc.type == OpCode.error)
                || (error != null && error != Code.OK.intValue())) {
            return Outcome.FAILED;
        }
        return error == null ? Outcome.UNKNOWN : Outcome.COMMITTED;
    }

    private static Result result(Outcome outcome) {
        return outcome == Outcome.COMMITTED ? Result.SUCCESS : outcome == Outcome.UNKNOWN ? Result.INVOKED : Result.FAILURE;
    }

    private static String operationFor(int type) {
        switch (type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
                return AuditConstants.OP_CREATE;
            case OpCode.delete:
            case OpCode.deleteContainer:
                return AuditConstants.OP_DELETE;
            case OpCode.setData:
                return AuditConstants.OP_SETDATA;
            case OpCode.setACL:
                return AuditConstants.OP_SETACL;
            case OpCode.reconfig:
                return AuditConstants.OP_RECONFIG;
            default:
                return null;
        }
    }

    private static boolean isCreate(int type) {
        return type == OpCode.create || type == OpCode.create2 || type == OpCode.createTTL || type == OpCode.createContainer;
    }

    private static void log(Request request, ProcessTxnResult rc, String path, String operation,
                            RequestMetadata metadata, Result result, Integer error, Outcome outcome,
                            Integer index, boolean enhanced) {
        Long zxid = null;
        if (request.getHdr() != null) {
            zxid = request.getHdr().getZxid();
        } else if (rc != null && rc.type != 0) {
            zxid = rc.zxid;
        } else if (request.zxid >= 0) {
            zxid = request.zxid;
        }
        ZKAuditProvider.log(getUsers(request, enhanced), operation, path, metadata.acl, metadata.createMode,
                request.cnxn.getSessionIdHex(), request.cnxn.getHostAddress(), result,
                metadata.dataLength, error, outcome, request.cxid, zxid, index, enhanced);
    }

    private static void auditError(int type, Exception e) {
        ZKAuditProvider.reportAuditError(LOG, "Failed to audit log request {}", type, e);
    }

    private static final class RequestMetadata {
        private String path;
        private String acl;
        private String createMode;
        private Integer dataLength;
    }
}
