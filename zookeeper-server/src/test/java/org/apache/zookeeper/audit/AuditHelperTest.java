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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.audit.AuditEvent.Result;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.CreateTTLRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.CloseSessionTxn;
import org.apache.zookeeper.txn.CreateTTLTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class AuditHelperTest {
    static final String ENHANCED_ENABLE = "zookeeper.audit.enhanced.enable";
    private static final long SESSION = 0x123;
    private AuditCapture capture;
    private DataTree tree;
    private ServerCnxn cnxn;
    private String previousEnhanced;
    private String previousAudit;
    private String previousExtendedTypes;

    @Before
    public void setUp() {
        previousAudit = System.getProperty(ZKAuditProvider.AUDIT_ENABLE);
        previousEnhanced = System.getProperty(ENHANCED_ENABLE);
        previousExtendedTypes = System.getProperty("zookeeper.extendedTypesEnabled");
        System.setProperty(ZKAuditProvider.AUDIT_ENABLE, "true");
        System.setProperty(ENHANCED_ENABLE, "true");
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
        assertTrue(ZKAuditProvider.isAuditEnabled());
        capture = new AuditCapture();
        tree = new DataTree();
        cnxn = mock(ServerCnxn.class);
        when(cnxn.getSessionIdHex()).thenReturn("0x123");
        when(cnxn.getHostAddress()).thenReturn("127.0.0.1");
    }

    @After
    public void tearDown() {
        capture.close();
        restoreProperty(ZKAuditProvider.AUDIT_ENABLE, previousAudit);
        restoreProperty(ENHANCED_ENABLE, previousEnhanced);
        restoreProperty("zookeeper.extendedTypesEnabled", previousExtendedTypes);
    }

    @Test
    public void testCreateAndSetDataLengthsArePayloadBytes() throws Exception {
        byte[][] data = {null, new byte[0], new byte[1], new byte[1024],
            new byte[65536], new byte[1048575], "\u00e9\ud83d\ude00".getBytes(StandardCharsets.UTF_8)};
        String[] lengths = {"0", "0", "1", "1024", "65536", "1048575", "6"};
        for (int i = 0; i < data.length; i++) {
            String path = "/length-" + i;
            Request create = request(OpCode.create, createRecord(path, data[i], CreateMode.PERSISTENT));
            ProcessTxnResult created = apply(create, OpCode.create, createTxn(path, data[i], false));
            assertEquals(0, created.err);
            AuditHelper.addAuditLog(create, created);
            Map<String, String> fields = fields(capture.read(1).get(0));
            assertWrite(fields, "create", path, lengths[i], "committed", "0");
            assertEquals("41", fields.get("cxid"));
            assertEquals("66", fields.get("zxid"));
            assertNull(fields.get("multi_index"));

            Request setData = request(OpCode.setData, new SetDataRequest(path, data[i], -1));
            ProcessTxnResult changed = apply(setData, OpCode.setData, new SetDataTxn(path, data[i], 1));
            assertEquals(0, changed.err);
            AuditHelper.addAuditLog(setData, changed);
            assertWrite(fields(capture.read(1).get(0)), "setData", path, lengths[i], "committed", "0");
        }
    }

    @Test
    public void testCreateTtlUsesTypedRecord() throws Exception {
        byte[] data = {1, 2, 3};
        Request request = request(OpCode.createTTL, new CreateTTLRequest(
                "/ttl", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_WITH_TTL.toFlag(), 60000));
        ProcessTxnResult result = apply(request, OpCode.createTTL,
                new CreateTTLTxn("/ttl", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1, 60000));
        assertEquals(0, result.err);
        AuditHelper.addAuditLog(request, result);
        Map<String, String> fields = fields(capture.read(1).get(0));
        assertWrite(fields, "create", "/ttl", "3", "committed", "0");
        assertEquals("persistent_with_ttl", fields.get("znode_type"));
    }

    @Test
    public void testFailureUsesAttemptedLengthAndReplyException() throws Exception {
        Request request = request(OpCode.create, createRecord("/denied", new byte[7], CreateMode.PERSISTENT));
        ProcessTxnResult result = apply(request, OpCode.error, new ErrorTxn(Code.SESSIONEXPIRED.intValue()));
        request.setException(KeeperException.create(Code.NOAUTH));
        AuditHelper.addAuditLog(request, result, true);
        assertWrite(fields(capture.read(1).get(0)), "create", "/denied", "7", "failed", "-102");
        assertNull(tree.getNode("/denied"));
    }

    @Test
    public void testFailedMultiNeverCommitsZeroCodeErrorMembers() throws Exception {
        Request request = request(OpCode.multi, new MultiOperationRecord(Arrays.asList(
                Op.create("/rolled", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.check("/", -1),
                Op.setData("/missing", new byte[3], -1),
                Op.create("/later", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))));
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Arrays.asList(
                txn(OpCode.create, createTxn("/rolled", new byte[1], false)),
                txn(OpCode.check, new CheckVersionTxn("/", -1)),
                txn(OpCode.error, new ErrorTxn(Code.NONODE.intValue())),
                txn(OpCode.error, new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue())))));
        assertEquals(-101, result.err);
        assertEquals(OpCode.error, result.multiResult.get(0).type);
        assertEquals(0, result.multiResult.get(0).err);
        assertNull(tree.getNode("/rolled"));
        assertNull(tree.getNode("/later"));

        AuditHelper.addAuditLog(request, result);
        List<String> logs = capture.read(4);
        assertWrite(fields(logs.get(0)), "multiOperation", null, null, "failed", "-101");
        assertWrite(fields(logs.get(1)), "create", "/rolled", "1", "rolled_back", "0");
        assertWrite(fields(logs.get(2)), "setData", "/missing", "3", "failed", "-101");
        assertWrite(fields(logs.get(3)), "create", "/later", "2", "rolled_back", "-2");
        assertEquals("0", fields(logs.get(1)).get("multi_index"));
        assertEquals("2", fields(logs.get(2)).get("multi_index"));
        assertEquals("3", fields(logs.get(3)).get("multi_index"));
        for (String log : logs) {
            assertFalse(log, log.contains("result=success"));
            assertFalse(log, log.contains("outcome=committed"));
        }
    }

    @Test
    public void testMultiMatchesRepeatedPathsAndTtlByPosition() throws Exception {
        Request request = request(OpCode.multi, new MultiOperationRecord(Arrays.asList(
                Op.create("/same", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.delete("/same", -1),
                Op.check("/", -1),
                Op.create("/same", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
                Op.create("/seq-", new byte[3], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, 60000))));
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Arrays.asList(
                txn(OpCode.create, createTxn("/same", new byte[1], false)),
                txn(OpCode.delete, new DeleteTxn("/same")),
                txn(OpCode.check, new CheckVersionTxn("/", -1)),
                txn(OpCode.create, createTxn("/same", new byte[2], true)),
                txn(OpCode.createTTL, new CreateTTLTxn(
                        "/seq-0000000002", new byte[3], ZooDefs.Ids.OPEN_ACL_UNSAFE, -1, 60000)))));
        assertEquals(0, result.err);
        assertNotNull(tree.getNode("/seq-0000000002"));

        AuditHelper.addAuditLog(request, result);
        List<String> logs = capture.read(4);
        assertWrite(fields(logs.get(0)), "create", "/same", "1", "committed", "0");
        assertEquals("persistent", fields(logs.get(0)).get("znode_type"));
        assertEquals("ephemeral", fields(logs.get(2)).get("znode_type"));
        assertEquals("3", fields(logs.get(2)).get("multi_index"));
        assertWrite(fields(logs.get(3)), "create", "/seq-0000000002", "3", "committed", "0");
        assertEquals("persistent_sequential_with_ttl", fields(logs.get(3)).get("znode_type"));
        assertEquals("4", fields(logs.get(3)).get("multi_index"));
    }

    @Test
    public void testIncompleteMultiResultsDoNotMisattributeCodes() throws Exception {
        Request request = request(OpCode.multi, new MultiOperationRecord(Arrays.asList(
                Op.create("/incomplete", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.check("/", -1))));
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Arrays.asList(
                txn(OpCode.create, createTxn("/incomplete", new byte[2], false)),
                txn(OpCode.check, new CheckVersionTxn("/", -1)))));
        assertEquals(0, result.err);
        result.multiResult.remove(0);
        long before = auditErrors();
        AuditHelper.addAuditLog(request, result);
        assertWrite(fields(capture.read(1).get(0)), "create", "/incomplete", "2", "unknown", null);
        assertEquals(before + 1, auditErrors());
    }

    @Test
    public void testFirstRuntimeInconsistencyIsFailureNotSkippedMember() throws Exception {
        Request request = request(OpCode.multi, new MultiOperationRecord(Arrays.asList(
                Op.create("/first", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.create("/skipped", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))));
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Arrays.asList(
                txn(OpCode.error, new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue())),
                txn(OpCode.error, new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue())))));
        assertNull(tree.getNode("/first"));
        assertNull(tree.getNode("/skipped"));
        AuditHelper.addAuditLog(request, result);
        List<String> logs = capture.read(3);
        assertWrite(fields(logs.get(1)), "create", "/first", "1", "failed", "-2");
        assertWrite(fields(logs.get(2)), "create", "/skipped", "2", "rolled_back", "-2");
    }

    @Test
    public void testDecodingPreservesPositionLimitAndMark() throws Exception {
        for (int kind = 0; kind < 3; kind++) {
            String path = "/buffer-" + kind;
            byte[] bytes = serialize(createRecord(path, new byte[3], CreateMode.PERSISTENT));
            ByteBuffer storage = kind == 0
                    ? ByteBuffer.allocateDirect(bytes.length + 12) : ByteBuffer.allocate(bytes.length + 12);
            storage.position(4);
            storage.put(bytes);
            storage.position(4);
            ByteBuffer buffer = storage.slice();
            buffer.limit(bytes.length);
            if (kind == 2) {
                buffer = buffer.asReadOnlyBuffer();
            }
            buffer.position(2);
            buffer.mark();
            buffer.position(bytes.length);
            Request request = request(OpCode.create, buffer);
            ProcessTxnResult result = apply(request, OpCode.create, createTxn(path, new byte[3], false));
            AuditHelper.addAuditLog(request, result);
            assertEquals(bytes.length, buffer.position());
            assertEquals(bytes.length, buffer.limit());
            buffer.reset();
            assertEquals(2, buffer.position());
            assertWrite(fields(capture.read(1).get(0)), "create", path, "3", "committed", "0");
        }
    }

    @Test
    public void testMultiDecodingPreservesPositionLimitAndMark() throws Exception {
        Request request = request(OpCode.multi, new MultiOperationRecord(Collections.singletonList(
                Op.create("/buffer-multi", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))));
        request.request.position(3);
        request.request.mark();
        int limit = request.request.limit();
        request.request.position(limit);
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Collections.singletonList(
                txn(OpCode.create, createTxn("/buffer-multi", new byte[2], false)))));
        AuditHelper.addAuditLog(request, result);
        assertEquals(limit, request.request.position());
        assertEquals(limit, request.request.limit());
        request.request.reset();
        assertEquals(3, request.request.position());
        assertWrite(fields(capture.read(1).get(0)), "create", "/buffer-multi", "2", "committed", "0");
    }

    @Test
    public void testUnavailablePayloadIsOmittedAndDecodeErrorsAreCounted() throws Exception {
        for (ByteBuffer buffer : Arrays.asList(null, ByteBuffer.wrap(new byte[] {0, 0, 0, 10, 1}))) {
            String path = buffer == null ? "/unavailable" : "/undecodable";
            Request request = request(OpCode.create, buffer);
            ProcessTxnResult result = apply(request, OpCode.create, createTxn(path, new byte[5], false));
            long before = auditErrors();
            AuditHelper.addAuditLog(request, result);
            assertWrite(fields(capture.read(1).get(0)), "create", path, null, "committed", "0");
            assertEquals(before + 1, auditErrors());
            assertNotNull(tree.getNode(path));
        }
    }

    @Test
    public void testFailedMultiParentSurvivesUndecodableRequest() throws Exception {
        Request request = request(OpCode.multi, ByteBuffer.wrap(new byte[] {1}));
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Collections.singletonList(
                txn(OpCode.error, new ErrorTxn(Code.NOAUTH.intValue())))));
        long before = auditErrors();
        AuditHelper.addAuditLog(request, result);
        assertWrite(fields(capture.read(1).get(0)), "multiOperation", null, null, "failed", "-102");
        assertEquals(before + 1, auditErrors());
    }

    @Test
    public void testMissingResultDoesNotInventSuccessOrZxid() throws Exception {
        Request request = request(OpCode.setData, new SetDataRequest("/unknown", new byte[4], -1));
        try {
            AuditHelper.addAuditLog(request, null);
        } catch (RuntimeException e) {
            fail("Unavailable transaction results must not escape the audit boundary: " + e);
        }
        Map<String, String> fields = fields(capture.read(1).get(0));
        assertWrite(fields, "setData", "/unknown", "4", "unknown", null);
        assertEquals("invoked", fields.get("result"));
        assertNull(fields.get("zxid"));
    }

    @Test
    public void testEnhancedSetAclDoesNotExposeCredentials() throws Exception {
        List<ACL> acls = Arrays.asList(
                new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")),
                new ACL(ZooDefs.Perms.READ, new Id("digest", "alice:synthetic-digest-secret")),
                new ACL(ZooDefs.Perms.WRITE, new Id("custom", "synthetic-token-secret")),
                new ACL(ZooDefs.Perms.READ, new Id("x509", "-----BEGIN CERTIFICATE-----synthetic-body")));
        Request request = request(OpCode.setACL, new SetACLRequest("/acl", acls, -1));
        ProcessTxnResult result = apply(request, OpCode.error, new ErrorTxn(Code.INVALIDACL.intValue()));
        AuditHelper.addAuditLog(request, result, true);
        String log = capture.read(1).get(0);
        assertWrite(fields(log), "setAcl", "/acl", null, "failed", "-114");
        assertTrue(log, log.contains("world:anyone:cdrwa"));
        assertTrue(log, log.contains("digest:alice:r"));
        assertFalse(log, log.contains("synthetic-digest-secret"));
        assertFalse(log, log.contains("synthetic-token-secret"));
        assertFalse(log, log.contains("BEGIN CERTIFICATE"));
        assertFalse(log, log.contains("synthetic-body"));
    }

    @Test
    public void testMalformedDigestIdentityIsNotMistakenForAUser() throws Exception {
        Request request = request(OpCode.setACL, new SetACLRequest("/acl", Collections.singletonList(
                new ACL(ZooDefs.Perms.READ, new Id("digest", "synthetic-digest-token"))), -1));
        ProcessTxnResult result = apply(request, OpCode.error, new ErrorTxn(Code.INVALIDACL.intValue()));
        AuditHelper.addAuditLog(request, result, true);
        String log = capture.read(1).get(0);
        assertFalse(log, log.contains("synthetic-digest-token"));
        assertWrite(fields(log), "setAcl", "/acl", null, "failed", "-114");
    }

    @Test
    public void testEnhancedUsersRedactRegisteredDefaultProvider() throws Exception {
        String property = ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "c1-audit-user";
        String previous = System.getProperty(property);
        System.setProperty(property, CredentialAuthenticationProvider.class.getName());
        ProviderRegistry.initialize();
        try {
            Request request = new Request(cnxn, SESSION, 41, OpCode.create,
                    ByteBuffer.wrap(serialize(createRecord("/custom-user", new byte[1], CreateMode.PERSISTENT))),
                    Arrays.asList(new Id("ip", "127.0.0.1"),
                            new Id("audit-test-custom", "alice:synthetic-password")));
            ProcessTxnResult result = apply(request, OpCode.create, createTxn("/custom-user", new byte[1], false));
            assertEquals(0, result.err);
            AuditHelper.addAuditLog(request, result);
            String log = capture.read(1).get(0);
            assertWrite(fields(log), "create", "/custom-user", "1", "committed", "0");
            assertEquals("127.0.0.1,[redacted]", fields(log).get("user"));
            assertFalse(log.contains("synthetic-password"));
        } finally {
            ProviderRegistry.removeProvider("audit-test-custom");
            restoreProperty(property, previous);
        }
    }

    @Test
    public void testEnhancedUsersRedactUnknownAndMalformedIdentities() throws Exception {
        Request request = new Request(cnxn, SESSION, 41, OpCode.create,
                ByteBuffer.wrap(serialize(createRecord("/unknown-users", new byte[1], CreateMode.PERSISTENT))),
                Arrays.asList(new Id("unregistered", "synthetic-token"),
                        new Id("digest", "synthetic-digest-token"),
                        new Id("ip", "alice:synthetic-password")));
        ProcessTxnResult result = apply(request, OpCode.create, createTxn("/unknown-users", new byte[1], false));
        AuditHelper.addAuditLog(request, result);
        String log = capture.read(1).get(0);
        assertWrite(fields(log), "create", "/unknown-users", "1", "committed", "0");
        assertEquals("[redacted],[redacted],[redacted]", fields(log).get("user"));
        assertFalse(log.contains("synthetic"));
    }

    @Test
    public void testAuditDisabledSkipsRequestsAndProvider() throws Exception {
        Object previous = replaceProviderField("auditEnabled", false);
        long before = auditErrors();
        try {
            AuditHelper.addAuditLog(null, null);
            ZKAuditProvider.log("user", "create", "/disabled", null, null, null, null, Result.SUCCESS);
            capture.read(0);
            assertEquals(before, auditErrors());
        } finally {
            replaceProviderField("auditEnabled", previous);
        }
    }

    @Test
    public void testDefaultAndExplicitLegacyOutputAreUnchanged() throws Exception {
        for (String setting : Arrays.asList(null, "false")) {
            restoreProperty(ENHANCED_ENABLE, setting);
            ZKAuditProvider.log("user", "create", "/legacy", null, "persistent", "0x123",
                    "127.0.0.1", Result.SUCCESS);
            assertEquals("session=0x123\tuser=user\tip=127.0.0.1\toperation=create"
                    + "\tznode=/legacy\tznode_type=persistent\tresult=success", capture.read(1).get(0));
        }
    }

    @Test
    public void testLegacyFailedMultiStillHasOnlyParent() throws Exception {
        System.clearProperty(ENHANCED_ENABLE);
        Request request = request(OpCode.multi, new MultiOperationRecord(Collections.singletonList(
                Op.setData("/absent", new byte[2], -1))));
        ProcessTxnResult result = apply(request, OpCode.multi, new MultiTxn(Collections.singletonList(
                txn(OpCode.error, new ErrorTxn(Code.NONODE.intValue())))));
        AuditHelper.addAuditLog(request, result);
        Map<String, String> fields = fields(capture.read(1).get(0));
        assertEquals("multiOperation", fields.get("operation"));
        assertEquals("failure", fields.get("result"));
        assertNull(fields.get("schema_version"));
    }

    @Test
    public void testReadsRemainUnaudited() {
        for (int type : new int[] {OpCode.getData, OpCode.getChildren, OpCode.exists, OpCode.multiRead}) {
            AuditHelper.addAuditLog(request(type, ByteBuffer.wrap(new byte[] {1})), new ProcessTxnResult());
        }
        capture.read(0);
    }

    @Test
    public void testSystemDeletionHasStableIdentityAndSystemActor() throws Exception {
        Request create = request(OpCode.create, createRecord("/ephemeral", new byte[0], CreateMode.EPHEMERAL));
        assertEquals(0, apply(create, OpCode.create, createTxn("/ephemeral", new byte[0], true)).err);
        tree.processTxn(new TxnHeader(SESSION, -11, 80, 1000, OpCode.closeSession),
                new CloseSessionTxn(Collections.singletonList("/ephemeral")));
        Map<String, String> fields = fields(capture.read(1).get(0));
        assertWrite(fields, "ephemeralZNodeDeletionOnSessionCloseOrExpire",
                "/ephemeral", null, "committed", "0");
        assertEquals(ZKAuditProvider.getZKUser(), fields.get("user"));
        assertEquals("0x123", fields.get("session"));
        assertEquals("80", fields.get("zxid"));
        assertNull(fields.get("cxid"));
        assertNull(fields.get("ip"));
        assertNull(tree.getNode("/ephemeral"));
    }

    @Test
    public void testLoggerFailureDoesNotInterruptSystemDeletion() throws Exception {
        for (String path : Arrays.asList("/ephemeral-a", "/ephemeral-b")) {
            Request request = request(OpCode.create, createRecord(path, new byte[0], CreateMode.EPHEMERAL));
            assertEquals(0, apply(request, OpCode.create, createTxn(path, new byte[0], true)).err);
        }
        AuditLogger failingLogger = event -> {
            throw new IllegalStateException("synthetic audit sink failure");
        };
        Object previous = replaceProviderField("auditLogger", failingLogger);
        long before = auditErrors();
        try {
            try {
                tree.processTxn(new TxnHeader(SESSION, -11, 81, 1000, OpCode.closeSession),
                        new CloseSessionTxn(Arrays.asList("/ephemeral-a", "/ephemeral-b")));
            } catch (RuntimeException e) {
                fail("Audit sink failure must not interrupt committed deletion: " + e);
            }
            assertNull(tree.getNode("/ephemeral-a"));
            assertNull(tree.getNode("/ephemeral-b"));
            assertEquals(before + 2, auditErrors());
        } finally {
            replaceProviderField("auditLogger", previous);
        }
    }

    @Test
    public void testFailingErrorCounterDoesNotEscapeMetadataFailure() throws Exception {
        Request request = request(OpCode.create, ByteBuffer.wrap(new byte[] {1}));
        ProcessTxnResult result = apply(request, OpCode.create, createTxn("/reporter-metadata", new byte[2], false));
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = replaceAuditErrorCounter(counter);
        try {
            RuntimeException escaped = null;
            try {
                AuditHelper.addAuditLog(request, result);
            } catch (RuntimeException e) {
                escaped = e;
            }
            assertNotNull(tree.getNode("/reporter-metadata"));
            assertNull("The error reporter must not escape the audit boundary", escaped);
            assertWrite(fields(capture.read(1).get(0)), "create", "/reporter-metadata", null, "committed", "0");
            assertEquals("Do not retry a failing error reporter", 1, counter.get());
        } finally {
            replaceAuditErrorCounter(previousCounter);
        }
    }

    @Test
    public void testFailingErrorCounterDoesNotInterruptSystemDeletions() throws Exception {
        for (String path : Arrays.asList("/reporter-a", "/reporter-b")) {
            Request request = request(OpCode.create, createRecord(path, new byte[0], CreateMode.EPHEMERAL));
            assertEquals(0, apply(request, OpCode.create, createTxn(path, new byte[0], true)).err);
        }
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = replaceAuditErrorCounter(counter);
        Object previousLogger = replaceProviderField("auditLogger", (AuditLogger) event -> {
            throw new IllegalStateException("synthetic audit sink failure");
        });
        try {
            RuntimeException escaped = null;
            try {
                tree.processTxn(new TxnHeader(SESSION, -11, 81, 1000, OpCode.closeSession),
                        new CloseSessionTxn(Arrays.asList("/reporter-a", "/reporter-b")));
            } catch (RuntimeException e) {
                escaped = e;
            }
            assertNull("Reporting a sink failure must not escape system deletion", escaped);
            assertNull(tree.getNode("/reporter-a"));
            assertNull(tree.getNode("/reporter-b"));
            assertEquals("One best-effort report per deletion, with no retries", 2, counter.get());
        } finally {
            replaceProviderField("auditLogger", previousLogger);
            replaceAuditErrorCounter(previousCounter);
        }
    }

    private Request request(int type, Record record) throws IOException {
        return request(type, ByteBuffer.wrap(serialize(record)));
    }

    private Request request(int type, ByteBuffer buffer) {
        return new Request(cnxn, SESSION, 41, type, buffer,
                Collections.singletonList(new Id("ip", "127.0.0.1")));
    }

    private ProcessTxnResult apply(Request request, int type, Record txn) {
        TxnHeader header = new TxnHeader(SESSION, 41, 66, 1000, type);
        request.setHdr(header);
        request.setTxn(txn);
        return tree.processTxn(header, txn);
    }

    private static CreateRequest createRecord(String path, byte[] data, CreateMode mode) {
        return new CreateRequest(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode.toFlag());
    }

    private static CreateTxn createTxn(String path, byte[] data, boolean ephemeral) {
        return new CreateTxn(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, ephemeral, -1);
    }

    private static Txn txn(int type, Record record) throws IOException {
        return new Txn(type, serialize(record));
    }

    private static byte[] serialize(Record record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        record.serialize(BinaryOutputArchive.getArchive(out), "request");
        return out.toByteArray();
    }

    static long auditErrors() {
        return ((Number) MetricsUtils.currentServerMetrics().getOrDefault("audit_errors", 0L)).longValue();
    }

    static Object replaceProviderField(String name, Object value) throws ReflectiveOperationException {
        Field field = ZKAuditProvider.class.getDeclaredField(name);
        field.setAccessible(true);
        Object previous = field.get(null);
        field.set(null, value);
        return previous;
    }

    static Counter replaceAuditErrorCounter(Counter counter) throws ReflectiveOperationException {
        ServerMetrics metrics = ServerMetrics.getMetrics();
        Field field = ServerMetrics.class.getField("AUDIT_ERRORS");
        field.setAccessible(true);
        Counter previous = (Counter) field.get(metrics);
        field.set(metrics, counter);
        return previous;
    }

    static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    static Map<String, String> fields(String log) {
        Map<String, String> fields = new LinkedHashMap<>();
        for (String pair : log.split("\t")) {
            int separator = pair.indexOf('=');
            assertTrue(log, separator > 0);
            String previous = fields.put(pair.substring(0, separator), pair.substring(separator + 1));
            assertNull("Duplicate audit field in " + log, previous);
        }
        return fields;
    }

    static void assertWrite(Map<String, String> fields, String operation, String path,
                            String length, String outcome, String error) {
        assertEquals("2", fields.get("schema_version"));
        assertEquals(operation, fields.get("operation"));
        assertEquals(path, fields.get("znode"));
        assertEquals(length, fields.get("data_length"));
        assertEquals(outcome, fields.get("outcome"));
        assertEquals(error, fields.get("error_code"));
        assertEquals("committed".equals(outcome) ? "success" : "unknown".equals(outcome) ? "invoked" : "failure",
                fields.get("result"));
    }

    public static class CredentialAuthenticationProvider implements AuthenticationProvider {
        @Override
        public String getScheme() {
            return "audit-test-custom";
        }

        @Override
        public Code handleAuthentication(ServerCnxn connection, byte[] authData) {
            connection.addAuthInfo(new Id(getScheme(), new String(authData, StandardCharsets.UTF_8)));
            return Code.OK;
        }

        @Override
        public boolean matches(String id, String aclExpr) {
            return id.equals(aclExpr);
        }

        @Override
        public boolean isAuthenticated() {
            return true;
        }

        @Override
        public boolean isValid(String id) {
            return true;
        }
    }

    static final class FailingCounter implements Counter {
        private final AtomicInteger attempts = new AtomicInteger();

        @Override
        public void add(long delta) {
            attempts.incrementAndGet();
            throw new IllegalStateException("synthetic audit counter failure");
        }

        @Override
        public long get() {
            return attempts.get();
        }
    }

    static final class AuditCapture extends AppenderBase<ILoggingEvent> implements AutoCloseable {
        private final Logger logger = (Logger) LoggerFactory.getLogger(Slf4jAuditLogger.class);
        private final Level previousLevel = logger.getLevel();
        private final List<String> messages = new ArrayList<>();
        private boolean overflow;

        AuditCapture() {
            setContext(logger.getLoggerContext());
            logger.setLevel(Level.INFO);
            logger.addAppender(this);
            start();
        }

        @Override
        protected synchronized void append(ILoggingEvent event) {
            if (messages.size() < 128) {
                messages.add(event.getFormattedMessage());
            } else {
                overflow = true;
            }
            notifyAll();
        }

        synchronized List<String> read(int expected) {
            assertFalse("Audit capture exceeded its bounded capacity", overflow);
            List<String> result = new ArrayList<>(messages);
            messages.clear();
            assertEquals(result.toString(), expected, result.size());
            return result;
        }

        synchronized void clear() {
            assertFalse("Audit capture exceeded its bounded capacity", overflow);
            messages.clear();
        }

        synchronized List<String> await(int expected, long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (messages.size() < expected) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return read(expected);
        }

        @Override
        public void close() {
            logger.detachAppender(this);
            logger.setLevel(previousLevel);
            stop();
        }
    }
}
