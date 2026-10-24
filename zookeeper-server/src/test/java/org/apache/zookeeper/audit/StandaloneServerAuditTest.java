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


import static org.apache.zookeeper.audit.AuditHelperTest.assertWrite;
import static org.apache.zookeeper.audit.AuditHelperTest.fields;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.audit.AuditHelperTest.AuditCapture;
import org.apache.zookeeper.audit.AuditHelperTest.CredentialAuthenticationProvider;
import org.apache.zookeeper.audit.AuditHelperTest.FailingCounter;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;



public class StandaloneServerAuditTest extends ClientBase {
    private AuditCapture capture;
    private String previousEnhanced;
    private String previousExtendedTypes;

    @BeforeClass
    public static void setup() {
        System.setProperty(ZKAuditProvider.AUDIT_ENABLE, "true");
    }

    @Before
    public void captureAuditLogs() {
        previousEnhanced = System.getProperty(AuditHelperTest.ENHANCED_ENABLE);
        previousExtendedTypes = System.getProperty("zookeeper.extendedTypesEnabled");
        capture = new AuditCapture();
    }

    @After
    public void restoreAuditSettings() {
        capture.close();
        AuditHelperTest.restoreProperty(AuditHelperTest.ENHANCED_ENABLE, previousEnhanced);
        AuditHelperTest.restoreProperty("zookeeper.extendedTypesEnabled", previousExtendedTypes);
    }

    @AfterClass
    public static void teardown() {
        System.clearProperty(ZKAuditProvider.AUDIT_ENABLE);
    }

    @Test
    public void testCreateAuditLog() throws KeeperException, InterruptedException, IOException {
        final ZooKeeper zk = createClient();
        String path = "/createPath";
        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        List<String> logs = capture.read(1);
        assertEquals(1, logs.size());
        assertTrue(logs.get(0).endsWith("operation=create\tznode=/createPath\tznode_type=persistent\tresult=success"));
    }

    @Test
    public void testEnhancedWriteResultsMatchClientResponses() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        ZooKeeper zk = createClient();
        byte[] data = "audit-secret".getBytes(StandardCharsets.UTF_8);
        zk.create("/enhanced", data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String log = capture.read(1).get(0);
        Map<String, String> created = fields(log);
        assertWrite(created, "create", "/enhanced", "12", "committed", "0");
        assertFalse(log.contains("audit-secret"));
        assertEquals(Long.toString(zk.exists("/enhanced", false).getCzxid()), created.get("zxid"));

        try {
            zk.create("/enhanced", new byte[7], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fail("Duplicate create must fail");
        } catch (KeeperException e) {
            assertEquals(Code.NODEEXISTS, e.code());
        }
        assertWrite(fields(capture.read(1).get(0)), "create", "/enhanced", "7", "failed", "-110");

        byte[] unicode = "\u00e9\ud83d\ude00".getBytes(StandardCharsets.UTF_8);
        Stat changed = zk.setData("/enhanced", unicode, -1);
        Map<String, String> changedLog = fields(capture.read(1).get(0));
        assertWrite(changedLog, "setData", "/enhanced", "6", "committed", "0");
        assertEquals(Long.toString(changed.getMzxid()), changedLog.get("zxid"));
        try {
            zk.setData("/enhanced", new byte[1], -100);
            fail("Invalid version must fail");
        } catch (KeeperException e) {
            assertEquals(Code.BADVERSION, e.code());
        }
        assertWrite(fields(capture.read(1).get(0)), "setData", "/enhanced", "1", "failed", "-103");
        assertArrayEquals(unicode, zk.getData("/enhanced", false, null));
        zk.getChildren("/", false);
        zk.exists("/enhanced", false);
        capture.read(0);
    }

    @Test
    public void testEnhancedCreateVariants() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
        ZooKeeper zk = createClient();
        Stat stat = new Stat();
        zk.create("/create2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
        assertWrite(fields(capture.read(1).get(0)), "create", "/create2", "0", "committed", "0");
        zk.create("/container", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
        Map<String, String> container = fields(capture.read(1).get(0));
        assertWrite(container, "create", "/container", "2", "committed", "0");
        assertEquals("container", container.get("znode_type"));
        String path = zk.create("/ttl-", new byte[3], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, null, 60000);
        Map<String, String> ttl = fields(capture.read(1).get(0));
        assertWrite(ttl, "create", path, "3", "committed", "0");
        assertEquals("persistent_sequential_with_ttl", ttl.get("znode_type"));

        zk.create("/ttl", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_WITH_TTL, null, 60000);
        assertWrite(fields(capture.read(1).get(0)), "create", "/ttl", "0", "committed", "0");
        try {
            zk.create("/ttl", new byte[4], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_WITH_TTL, null, 60000);
            fail("Duplicate TTL create must fail");
        } catch (KeeperException e) {
            assertEquals(Code.NODEEXISTS, e.code());
        }
        Map<String, String> failedTtl = fields(capture.read(1).get(0));
        assertWrite(failedTtl, "create", "/ttl", "4", "failed", "-110");
        assertEquals("persistent_with_ttl", failedTtl.get("znode_type"));
    }

    @Test
    public void testEnhancedMultiUsesReturnedPathsAndCompleteIndexes() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
        ZooKeeper zk = createClient();
        List<OpResult> results = zk.multi(Arrays.asList(
                Op.create("/same", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                Op.delete("/same", -1),
                Op.check("/", -1),
                Op.create("/same", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
                Op.create("/seq-", new byte[3], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, 60000)));
        assertEquals(5, results.size());
        String finalPath = ((OpResult.CreateResult) results.get(4)).getPath();
        assertFalse("/seq-".equals(finalPath));
        List<String> logs = capture.read(4);
        assertWrite(fields(logs.get(0)), "create", "/same", "1", "committed", "0");
        assertEquals("persistent", fields(logs.get(0)).get("znode_type"));
        assertWrite(fields(logs.get(1)), "delete", "/same", null, "committed", "0");
        assertWrite(fields(logs.get(2)), "create", "/same", "2", "committed", "0");
        assertEquals("ephemeral", fields(logs.get(2)).get("znode_type"));
        assertEquals("3", fields(logs.get(2)).get("multi_index"));
        assertWrite(fields(logs.get(3)), "create", finalPath, "3", "committed", "0");
        assertEquals("4", fields(logs.get(3)).get("multi_index"));
        assertEquals("persistent_sequential_with_ttl", fields(logs.get(3)).get("znode_type"));
        assertArrayEquals(new byte[3], zk.getData(finalPath, false, null));
    }

    @Test
    public void testEnhancedFailedMultiMatchesAtomicRollback() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        ZooKeeper zk = createClient();
        try {
            zk.multi(Arrays.asList(
                    Op.create("/rolled", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                    Op.check("/", -1),
                    Op.setData("/missing", new byte[3], -1),
                    Op.create("/later", new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
            fail("Missing node must abort the multi");
        } catch (KeeperException e) {
            assertEquals(Code.NONODE, e.code());
        }
        assertNull(zk.exists("/rolled", false));
        assertNull(zk.exists("/later", false));
        List<String> logs = capture.read(4);
        assertWrite(fields(logs.get(0)), "multiOperation", null, null, "failed", "-101");
        assertWrite(fields(logs.get(1)), "create", "/rolled", "1", "rolled_back", "0");
        assertWrite(fields(logs.get(2)), "setData", "/missing", "3", "failed", "-101");
        assertWrite(fields(logs.get(3)), "create", "/later", "2", "rolled_back", "-2");
        assertEquals("0", fields(logs.get(1)).get("multi_index"));
        assertEquals("2", fields(logs.get(2)).get("multi_index"));
        assertEquals("3", fields(logs.get(3)).get("multi_index"));
    }

    @Test
    public void testFailedCheckRollsBackMutationsWithoutACheckEvent() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        ZooKeeper zk = createClient();
        try {
            zk.multi(Arrays.asList(
                    Op.create("/rolled", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                    Op.check("/absent", -1),
                    Op.setData("/rolled", new byte[2], -1)));
            fail("Missing check target must abort the multi");
        } catch (KeeperException e) {
            assertEquals(Code.NONODE, e.code());
        }
        assertNull(zk.exists("/rolled", false));
        List<String> logs = capture.read(3);
        assertWrite(fields(logs.get(0)), "multiOperation", null, null, "failed", "-101");
        assertWrite(fields(logs.get(1)), "create", "/rolled", "1", "rolled_back", "0");
        assertWrite(fields(logs.get(2)), "setData", "/rolled", "2", "rolled_back", "-2");
        assertEquals("2", fields(logs.get(2)).get("multi_index"));
    }

    @Test
    public void testEnhancedAclRedactionDoesNotChangeStoredAcl() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        ZooKeeper zk = createClient();
        String credentials = "alice:synthetic-password";
        String digest = DigestAuthenticationProvider.generateDigest(credentials);
        zk.addAuthInfo("digest", credentials.getBytes(StandardCharsets.UTF_8));
        zk.create("/acl", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        capture.read(1);
        List<ACL> acls = Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("digest", digest)));
        zk.setACL("/acl", acls, -1);
        String log = capture.read(1).get(0);
        assertWrite(fields(log), "setAcl", "/acl", null, "committed", "0");
        assertEquals("digest:alice:cdrwa", fields(log).get("acl"));
        assertFalse(log.contains(digest));
        assertFalse(log.contains("synthetic-password"));
        assertEquals(acls, zk.getACL("/acl", new Stat()));
        capture.read(0);
    }

    @Test
    public void testRegisteredCustomUserIsRedactedOnlyInEnhancedMode() throws Exception {
        String property = ProviderRegistry.AUTHPROVIDER_PROPERTY_PREFIX + "c1-audit-user";
        String previous = System.getProperty(property);
        System.setProperty(property, CredentialAuthenticationProvider.class.getName());
        ProviderRegistry.initialize();
        try {
            System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
            ZooKeeper zk = createClient();
            zk.addAuthInfo("audit-test-custom", "alice:synthetic-password".getBytes(StandardCharsets.UTF_8));
            zk.create("/custom-user", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            String enhanced = capture.read(1).get(0);
            assertWrite(fields(enhanced), "create", "/custom-user", "1", "committed", "0");
            assertFalse(enhanced.contains("synthetic-password"));
            List<String> enhancedUsers = Arrays.asList(fields(enhanced).get("user").split(","));
            Collections.sort(enhancedUsers);
            assertEquals(Arrays.asList("127.0.0.1", "[redacted]"), enhancedUsers);

            System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "false");
            zk.setData("/custom-user", new byte[2], -1);
            Map<String, String> legacy = fields(capture.read(1).get(0));
            assertNull(legacy.get("schema_version"));
            List<String> legacyUsers = Arrays.asList(legacy.get("user").split(","));
            Collections.sort(legacyUsers);
            assertEquals(Arrays.asList("127.0.0.1", "alice:synthetic-password"), legacyUsers);
            assertArrayEquals(new byte[2], zk.getData("/custom-user", false, null));
        } finally {
            ProviderRegistry.removeProvider("audit-test-custom");
            AuditHelperTest.restoreProperty(property, previous);
        }
    }

    @Test
    public void testAuditFailureDoesNotRejectValidWrite() throws Exception {
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        ZooKeeper zk = createClient();
        AuditLogger failingLogger = event -> {
            throw new IllegalStateException("synthetic audit sink failure");
        };
        Object previous = AuditHelperTest.replaceProviderField("auditLogger", failingLogger);
        long before = AuditHelperTest.auditErrors();
        try {
            assertEquals("/valid", zk.create("/valid", new byte[2],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            assertArrayEquals(new byte[2], zk.getData("/valid", false, null));
            assertEquals(before + 1, AuditHelperTest.auditErrors());
        } finally {
            AuditHelperTest.replaceProviderField("auditLogger", previous);
        }
    }

    @Test
    public void testAuditReporterFailureDoesNotRejectAppliedWrite() throws Exception {
        ZooKeeper zk = createClient();
        FailingCounter counter = new FailingCounter();
        Counter previousCounter = AuditHelperTest.replaceAuditErrorCounter(counter);
        Object previousLogger = AuditHelperTest.replaceProviderField("auditLogger", (AuditLogger) event -> {
            throw new IllegalStateException("synthetic audit sink failure");
        });
        try {
            for (String enhanced : Arrays.asList("false", "true")) {
                System.setProperty(AuditHelperTest.ENHANCED_ENABLE, enhanced);
                String path = "/reporter-" + enhanced;
                Code replyError = null;
                String created = null;
                long before = counter.get();
                try {
                    created = zk.create(path, new byte[2], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException e) {
                    replyError = e.code();
                }
                assertArrayEquals(new byte[2], zk.getData(path, false, null));
                assertNull("Audit reporting changed the reply after the write was applied", replyError);
                assertEquals(path, created);
                assertEquals("Do not retry a failing error reporter", before + 1, counter.get());
            }
        } finally {
            AuditHelperTest.replaceProviderField("auditLogger", previousLogger);
            AuditHelperTest.replaceAuditErrorCounter(previousCounter);
        }
    }
}
