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

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;
import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.audit.AuditEvent.Result;
import org.apache.zookeeper.audit.AuditHelperTest.AuditCapture;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.quorum.QuorumPeerTestBase;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Slf4JAuditLoggerTest extends QuorumPeerTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(Slf4JAuditLoggerTest.class);
    private static int SERVER_COUNT = 3;
    private static MainThread[] mt;
    private static ZooKeeper zk;
    private static AuditCapture os;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.setProperty(ZKAuditProvider.AUDIT_ENABLE, "true");
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
        // setup the logger to capture all logs
        os = new AuditCapture();
        mt = startQuorum();
        zk = ClientBase.createZKClient("127.0.0.1:" + mt[0].getQuorumPeer().getClientPort());
        //Verify start audit log here itself
        String expectedAuditLog = getStartLog();
        List<String> logs = readAuditLog(os, SERVER_COUNT);
        verifyLogs(expectedAuditLog, logs);
    }

    @Before
    public void setUp() {
        os.clear();
    }

    @Test
    public void testCreateAuditLogs()
            throws KeeperException, InterruptedException, IOException {
        String path = "/createPath";
        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        // success log
        String createMode = CreateMode.PERSISTENT.toString().toLowerCase();
        verifyLog(
                getAuditLog(AuditConstants.OP_CREATE, path, Result.SUCCESS,
                        null, createMode), readAuditLog(os));
        try {
            zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException exception) {
            Code code = exception.code();
            assertEquals(Code.NODEEXISTS, code);
        }
        // Verify create operation log
        verifyLog(
                getAuditLog(AuditConstants.OP_CREATE, path, Result.FAILURE,
                        null, createMode), readAuditLog(os));
    }

    @Test
    public void testCreateWithTtlAuditLogs() throws Exception {
        String path = zk.create("/createTtlPath", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_WITH_TTL, null, 60000);
        verifyLog(getAuditLog(AuditConstants.OP_CREATE, path, Result.SUCCESS,
                null, "persistent_with_ttl"), readAuditLog(os));
    }

    @Test
    public void testCreateSequentialWithTtlAuditLogs() throws Exception {
        String path = zk.create("/createTtlSeqPath", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, null, 60000);
        verifyLog(getAuditLog(AuditConstants.OP_CREATE, path, Result.SUCCESS,
                null, "persistent_sequential_with_ttl"), readAuditLog(os));
    }

    @Test
    public void testEnhancedEscapingThroughSlf4j() {
        String previous = System.getProperty(AuditHelperTest.ENHANCED_ENABLE);
        try (AuditHelperTest.AuditCapture capture = new AuditHelperTest.AuditCapture()) {
            System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
            ZKAuditProvider.log("team\tname\r\n\\t", "create", "/name=value\\child",
                    null, "persistent", "0x123", "127.0.0.1", Result.SUCCESS);
            String log = capture.read(1).get(0);
            assertEquals("2", AuditHelperTest.fields(log).get("schema_version"));
            assertEquals("team\\tname\\r\\n\\\\t", AuditHelperTest.fields(log).get("user"));
            assertEquals("/name=value\\\\child", AuditHelperTest.fields(log).get("znode"));
            Assert.assertFalse(log.contains("\n"));
            Assert.assertFalse(log.contains("\r"));
        } finally {
            AuditHelperTest.restoreProperty(AuditHelperTest.ENHANCED_ENABLE, previous);
        }
    }

    @Test
    public void testDeleteAuditLogs()
            throws InterruptedException, IOException, KeeperException {
        String path = "/deletePath";
        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        os.clear();
        try {
            zk.delete(path, -100);
        } catch (KeeperException exception) {
            Code code = exception.code();
            assertEquals(Code.BADVERSION, code);
        }
        verifyLog(getAuditLog(AuditConstants.OP_DELETE, path,
                Result.FAILURE),
                readAuditLog(os));
        zk.delete(path, -1);
        verifyLog(getAuditLog(AuditConstants.OP_DELETE, path),
                readAuditLog(os));
    }

    @Test
    public void testSetDataAuditLogs()
            throws InterruptedException, IOException, KeeperException {
        String path = "/setDataPath";
        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        os.clear();
        try {
            zk.setData(path, "newData".getBytes(), -100);
        } catch (KeeperException exception) {
            Code code = exception.code();
            assertEquals(Code.BADVERSION, code);
        }
        verifyLog(getAuditLog(AuditConstants.OP_SETDATA, path,
                Result.FAILURE),
                readAuditLog(os));
        zk.setData(path, "newdata".getBytes(), -1);
        verifyLog(getAuditLog(AuditConstants.OP_SETDATA, path),
                readAuditLog(os));
    }

    @Test
    public void testSetACLAuditLogs()
            throws InterruptedException, IOException, KeeperException {
        ArrayList<ACL> openAclUnsafe = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        String path = "/aclPath";
        zk.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        os.clear();
        try {
            zk.setACL(path, openAclUnsafe, -100);
        } catch (KeeperException exception) {
            Code code = exception.code();
            assertEquals(Code.BADVERSION, code);
        }
        verifyLog(
                getAuditLog(AuditConstants.OP_SETACL, path, Result.FAILURE,
                        ZKUtil.aclToString(openAclUnsafe), null), readAuditLog(os));
        zk.setACL(path, openAclUnsafe, -1);
        verifyLog(
                getAuditLog(AuditConstants.OP_SETACL, path, Result.SUCCESS,
                        ZKUtil.aclToString(openAclUnsafe), null), readAuditLog(os));
    }

    @Test
    public void testMultiOperationAuditLogs()
            throws InterruptedException, KeeperException, IOException {
        List<Op> ops = new ArrayList<>();

        String multiop = "/b";
        Op create = Op.create(multiop, "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        Op setData = Op.setData(multiop, "newData".getBytes(), -1);
        // check does nothing so it is audit logged
        Op check = Op.check(multiop, -1);
        Op delete = Op.delete(multiop, -1);

        String createMode = CreateMode.PERSISTENT.toString().toLowerCase();

        ops.add(create);
        ops.add(setData);
        ops.add(check);
        ops.add(delete);

        zk.multi(ops);
        List<String> multiOpLogs = readAuditLog(os, 3);
        // verify that each multi operation success is logged
        verifyLog(getAuditLog(AuditConstants.OP_CREATE, multiop,
                Result.SUCCESS, null, createMode),
                multiOpLogs.get(0));
        verifyLog(getAuditLog(AuditConstants.OP_SETDATA, multiop),
                multiOpLogs.get(1));
        verifyLog(getAuditLog(AuditConstants.OP_DELETE, multiop),
                multiOpLogs.get(2));

        ops = new ArrayList<>();
        ops.add(create);
        ops.add(create);
        try {
            zk.multi(ops);
        } catch (KeeperException exception) {
            Code code = exception.code();
            assertEquals(Code.NODEEXISTS, code);
        }

        // Verify that multi operation failure is logged, and there is no path
        // mentioned in the audit log
        verifyLog(getAuditLog(AuditConstants.OP_MULTI_OP, null,
                Result.FAILURE),
                readAuditLog(os));
    }

    @Test
    public void testEphemralZNodeAuditLogs()
            throws Exception {
        String ephemralPath = "/ephemral";
        CountdownWatcher watcher2 = new CountdownWatcher();
        ZooKeeper zk2 = new ZooKeeper(
                "127.0.0.1:" + mt[0].getQuorumPeer().getClientPort(),
                ClientBase.CONNECTION_TIMEOUT, watcher2);
        watcher2.waitForConnected(ClientBase.CONNECTION_TIMEOUT);
        zk2.create(ephemralPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        String session2 = "0x" + Long.toHexString(zk2.getSessionId());
        verifyLog(getAuditLog(AuditConstants.OP_CREATE, ephemralPath,
                Result.SUCCESS, null,
                CreateMode.EPHEMERAL.toString().toLowerCase(),
                session2), readAuditLog(os));
        zk2.close();
        waitForDeletion(zk, ephemralPath);
        // verify that ephemeral node deletion on session close are captured
        // in audit log
        // Because these operations are done by ZooKeeper server itself,
        // there are no IP user is zkServer user, not any client user
        verifyLogs(getAuditLog(AuditConstants.OP_DEL_EZNODE_EXP, ephemralPath,
                Result.SUCCESS, null, null, session2,
                ZKAuditProvider.getZKUser(), null), readAuditLog(os, SERVER_COUNT));
    }

    @Test
    public void testEnhancedSystemDeletionIdentityAcrossReplicas() throws Exception {
        String previous = System.getProperty(AuditHelperTest.ENHANCED_ENABLE);
        System.setProperty(AuditHelperTest.ENHANCED_ENABLE, "true");
        try (ZooKeeper client = ClientBase.createZKClient("127.0.0.1:" + mt[0].getQuorumPeer().getClientPort())) {
            client.create("/enhanced-ephemeral", new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            String session = "0x" + Long.toHexString(client.getSessionId());
            os.read(1);
            client.close();
            List<String> logs = os.await(SERVER_COUNT, CONNECTION_TIMEOUT);
            String zxid = Long.toString(zk.exists("/", false).getPzxid());
            for (String log : logs) {
                Map<String, String> fields = AuditHelperTest.fields(log);
                AuditHelperTest.assertWrite(fields, AuditConstants.OP_DEL_EZNODE_EXP,
                        "/enhanced-ephemeral", null, "committed", "0");
                assertEquals(zxid, fields.get("zxid"));
                assertEquals(session, fields.get("session"));
                assertEquals(ZKAuditProvider.getZKUser(), fields.get("user"));
                Assert.assertNull(fields.get("cxid"));
                Assert.assertNull(fields.get("ip"));
            }
        } finally {
            AuditHelperTest.restoreProperty(AuditHelperTest.ENHANCED_ENABLE, previous);
        }
    }


    private static String getStartLog() {
        // user=userName operation=ZooKeeperServer start  result=success
        AuditEvent logEvent = ZKAuditProvider.createLogEvent(ZKAuditProvider.getZKUser(),
                AuditConstants.OP_START, Result.SUCCESS);
        return logEvent.toString();
    }

    private String getAuditLog(String operation, String znode) {
        return getAuditLog(operation, znode, Result.SUCCESS);
    }

    private String getAuditLog(String operation, String znode, Result result) {
        return getAuditLog(operation, znode, result, null, null);
    }

    private String getAuditLog(String operation, String znode, Result result,
                               String acl, String createMode) {
        String session = getSession();
        return getAuditLog(operation, znode, result, acl, createMode, session);
    }

    private String getAuditLog(String operation, String znode, Result result,
                               String acl, String createMode, String session) {
        String user = getUser();
        String ip = getIp();
        return getAuditLog(operation, znode, result, acl, createMode, session,
                user, ip);
    }

    private String getAuditLog(String operation, String znode, Result result,
                               String acl, String createMode, String session, String user, String ip) {
        AuditEvent logEvent = ZKAuditProvider.createLogEvent(user, operation, znode, acl, createMode, session, ip,
                result);
        String auditLog = logEvent.toString();
        LOG.info("expected audit log for operation '" + operation + "' is '"
                + auditLog + "'");
        return auditLog;
    }

    private String getSession() {
        return "0x" + Long.toHexString(zk.getSessionId());
    }

    private String getUser() {
        ServerCnxn next = getServerCnxn();
        Request request = new Request(next, -1, -1, -1, null,
                next.getAuthInfo());
        return request.getUsers();
    }

    private String getIp() {
        ServerCnxn next = getServerCnxn();
        InetSocketAddress remoteSocketAddress = next.getRemoteSocketAddress();
        InetAddress address = remoteSocketAddress.getAddress();
        return address.getHostAddress();
    }

    private ServerCnxn getServerCnxn() {
        Iterable<ServerCnxn> connections = mt[0].getQuorumPeer()
                .getActiveServer()
                .getServerCnxnFactory().getConnections();
        return connections.iterator().next();
    }

    private static void verifyLog(String expectedLog, String log) {
        Assert.assertTrue(log, log.endsWith(expectedLog));
    }

    private static void verifyLogs(String expectedLog, List<String> logs) {
        for (String log : logs) {
            verifyLog(expectedLog, log);
        }
    }

    private String readAuditLog(AuditCapture os) throws IOException {
        return readAuditLog(os, 1).get(0);
    }

    private static List<String> readAuditLog(AuditCapture os,
                                             int numberOfLogEntry)
            throws IOException {
        return os.read(numberOfLogEntry);
    }

    private static MainThread[] startQuorum() throws IOException {
        final int[] clientPorts = new int[SERVER_COUNT];
        StringBuilder sb = new StringBuilder();
        sb.append("4lw.commands.whitelist=*");
        sb.append("\n");
        String server;

        for (int i = 0; i < SERVER_COUNT; i++) {
            clientPorts[i] = PortAssignment.unique();
            server = "server." + i + "=127.0.0.1:" + PortAssignment.unique()
                    + ":"
                    + PortAssignment.unique() + ":participant;127.0.0.1:"
                    + clientPorts[i];
            sb.append(server);
            sb.append("\n");
        }
        String currentQuorumCfgSection = sb.toString();
        MainThread[] mt = new MainThread[SERVER_COUNT];

        // start all the servers
        for (int i = 0; i < SERVER_COUNT; i++) {
            mt[i] = new MainThread(i, clientPorts[i], currentQuorumCfgSection,
                    false);
            mt[i].start();
        }

        // ensure all servers started
        for (int i = 0; i < SERVER_COUNT; i++) {
            Assert.assertTrue("waiting for server " + i + " being up",
                    ClientBase.waitForServerUp("127.0.0.1:" + clientPorts[i],
                            CONNECTION_TIMEOUT));
        }
        return mt;
    }

    private void waitForDeletion(ZooKeeper zooKeeper, String path)
            throws Exception {
        long elapsedTime = 0;
        long waitInterval = 10;
        int timeout = 100;
        Stat exists = zooKeeper.exists(path, false);
        while (exists != null && elapsedTime < timeout) {
            try {
                Thread.sleep(waitInterval);
            } catch (InterruptedException e) {
                Assert.fail("CurrentEpoch update failed");
            }
            elapsedTime = elapsedTime + waitInterval;
            exists = zooKeeper.exists(path, false);
        }
        Assert.assertNull("Node " + path + " not deleted in " + timeout + " ms",
                exists);
    }

    @AfterClass
    public static void tearDownAfterClass() {
        System.clearProperty(ZKAuditProvider.AUDIT_ENABLE);
        System.clearProperty("zookeeper.extendedTypesEnabled");
        for (int i = 0; i < SERVER_COUNT; i++) {
            try {
                if (mt[i] != null) {
                    mt[i].shutdown();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        os.close();
    }
}
