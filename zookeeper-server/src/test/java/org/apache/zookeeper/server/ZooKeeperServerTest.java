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

package org.apache.zookeeper.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.SnapStream;
import org.apache.zookeeper.server.persistence.Util;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.util.QuotaMetricsUtils;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.LoggerTestTool;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@Execution(ExecutionMode.SAME_THREAD)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
public class ZooKeeperServerTest extends ZKTestCase {

    @Test
    public void testDirSize() throws Exception {
        ZooKeeperServer zks = null;
        ServerCnxnFactory cnxnFactory = null;

        try {
            final File dataDir = ClientBase.createTmpDir();
            final File logDir = ClientBase.createTmpDir();

            zks = new ZooKeeperServer(dataDir, logDir, 3000);

            // validate dir size before server starts
            assertEquals(0, zks.getDataDirSize());
            assertEquals(0, zks.getLogDirSize());

            // start server
            final String hostPort = "127.0.0.1:" + PortAssignment.unique();
            final int port = Integer.parseInt(hostPort.split(":")[1]);
            cnxnFactory = ServerCnxnFactory.createFactory(port, -1);
            cnxnFactory.startup(zks);
            assertTrue(ClientBase.waitForServerUp(hostPort, 120000));

            // validate data size is greater than 0 as snapshot has been taken when server starts
            assertTrue(zks.getDataDirSize() > 0);

            // validate log size is 0 as no txn yet
            assertEquals(0, zks.getLogDirSize());
        } finally {
            if (cnxnFactory != null) {
                cnxnFactory.shutdown();
            }

            if (zks != null) {
                zks.shutdown();
            }
        }
    }


    @Test
    public void testSortDataDirAscending() {
        File[] files = new File[5];

        files[0] = new File("foo.10027c6de");
        files[1] = new File("foo.10027c6df");
        files[2] = new File("bar.10027c6dd");
        files[3] = new File("foo.10027c6dc");
        files[4] = new File("foo.20027c6dc");

        File[] orig = files.clone();

        List<File> filelist = Util.sortDataDir(files, "foo", true);

        assertEquals(orig[2], filelist.get(0));
        assertEquals(orig[3], filelist.get(1));
        assertEquals(orig[0], filelist.get(2));
        assertEquals(orig[1], filelist.get(3));
        assertEquals(orig[4], filelist.get(4));
    }

    @Test
    public void testSortDataDirDescending() {
        File[] files = new File[5];

        files[0] = new File("foo.10027c6de");
        files[1] = new File("foo.10027c6df");
        files[2] = new File("bar.10027c6dd");
        files[3] = new File("foo.10027c6dc");
        files[4] = new File("foo.20027c6dc");

        File[] orig = files.clone();

        List<File> filelist = Util.sortDataDir(files, "foo", false);

        assertEquals(orig[4], filelist.get(0));
        assertEquals(orig[1], filelist.get(1));
        assertEquals(orig[0], filelist.get(2));
        assertEquals(orig[3], filelist.get(3));
        assertEquals(orig[2], filelist.get(4));
    }

    @Test
    public void testGetLogFiles() {
        File[] files = new File[5];

        files[0] = new File("log.10027c6de");
        files[1] = new File("log.10027c6df");
        files[2] = new File("snapshot.10027c6dd");
        files[3] = new File("log.10027c6dc");
        files[4] = new File("log.20027c6dc");

        File[] orig = files.clone();

        File[] filelist = FileTxnLog.getLogFiles(files, Long.parseLong("10027c6de", 16));

        assertEquals(3, filelist.length);
        assertEquals(orig[0], filelist[0]);
        assertEquals(orig[1], filelist[1]);
        assertEquals(orig[4], filelist[2]);
    }

    @Test
    public void testForceSyncDefaultEnabled() {
        File file = new File("foo.10027c6de");
        FileTxnLog log = new FileTxnLog(file);
        assertTrue(log.isForceSync());
    }

    @Test
    public void testForceSyncDefaultDisabled() {
        try {
            File file = new File("foo.10027c6de");
            System.setProperty("zookeeper.forceSync", "no");
            FileTxnLog log = new FileTxnLog(file);
            assertFalse(log.isForceSync());
        } finally {
            //Reset back to default.
            System.setProperty("zookeeper.forceSync", "yes");
        }
    }

    @Test
    public void testInvalidSnapshot() {
        File f = null;
        File tmpFileDir = null;
        try {
            tmpFileDir = ClientBase.createTmpDir();
            f = new File(tmpFileDir, "snapshot.0");
            if (!f.exists()) {
                f.createNewFile();
            }
            assertFalse(SnapStream.isValidSnapshot(f), "Snapshot file size is greater than 9 bytes");
            assertTrue(f.delete(), "Can't delete file");
        } catch (IOException e) {
        } finally {
            if (null != tmpFileDir) {
                ClientBase.recursiveDelete(tmpFileDir);
            }
        }
    }

    @Test
    public void testClientZxidAhead() {
        ZooKeeperServer zooKeeperServer = new ZooKeeperServer();
        final ZKDatabase zkDatabase = new ZKDatabase(mock(FileTxnSnapLog.class));
        zooKeeperServer.setZKDatabase(zkDatabase);

        final ByteBuffer output = ByteBuffer.allocate(30);
        // serialize a connReq
        output.putInt(1);
        // lastZxid
        output.putLong(99L);
        output.putInt(500);
        output.putLong(123L);
        output.putInt(1);
        output.put((byte) 1);
        output.put((byte) 1);
        output.flip();

        ServerCnxn.CloseRequestException e = assertThrows(ServerCnxn.CloseRequestException.class, () -> {
            final NIOServerCnxn nioServerCnxn = mock(NIOServerCnxn.class);
            zooKeeperServer.processConnectRequest(nioServerCnxn, output);
        });
        assertEquals(e.getReason(), ServerCnxn.DisconnectReason.CLIENT_ZXID_AHEAD);
    }

    @Test
    public void testUpdateQuotaExceededMetrics() {
        final String name = QuotaMetricsUtils.QUOTA_EXCEEDED_ERROR_PER_NAMESPACE;
        final String namespace = UUID.randomUUID().toString();
        final long count = 3L;

        for (int i = 0; i < count; i++) {
            ZooKeeperServer.updateQuotaExceededMetrics(namespace);
        }

        final Map<String, Object> values = MetricsUtils.currentServerMetrics();
        assertEquals(1, values.keySet().stream().filter(
                key -> key.contains(String.format("%s_%s", namespace, name))).count());

        assertEquals(count, values.get(String.format("%s_%s", namespace, name)));
    }

    @Test
    public void testUpdateQuotaExceededMetrics_nullNamespace() {
        assertDoesNotThrow(() -> ZooKeeperServer.updateQuotaExceededMetrics(null));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"false", "true"})
    public void testLoadDataStartupSnapshotPolicy(String skipSnapshot, @TempDir File directory) throws Exception {
        FileTxnSnapLog snapLog = new FileTxnSnapLog(directory, directory);
        String prior = System.getProperty(QuorumPeer.SKIP_LEADER_STARTUP_SNAPSHOT);
        try {
            System.setProperty(QuorumPeer.SKIP_LEADER_STARTUP_SNAPSHOT, "true");
            ZooKeeperServer server = new ZooKeeperServer(snapLog, 2000, null);
            ZKDatabase database = server.getZKDatabase();
            assertEquals(0, database.loadDataBase());
            byte[] data = {1, 2, 3};
            TxnHeader header = new TxnHeader(1, 1, 1, 1, ZooDefs.OpCode.create);
            CreateTxn transaction = new CreateTxn("/policy", data, ZooDefs.Ids.READ_ACL_UNSAFE, false, 1);
            assertTrue(snapLog.append(new Request(1, 1, ZooDefs.OpCode.create, header, transaction, 1)));
            snapLog.commit();
            assertEquals(0, database.processTxn(header, transaction, null).err);
            File snapshot = new File(snapLog.getSnapDir(), Util.makeSnapshotName(1));
            assertFalse(snapshot.exists());

            if (skipSnapshot == null) {
                database.clear();
                server.startdata();
            } else {
                server.loadData(Boolean.parseBoolean(skipSnapshot));
            }

            assertEquals(!Boolean.parseBoolean(skipSnapshot), snapshot.isFile());
            assertEquals(1, server.getZxid());
            assertArrayEquals(data, database.getData("/policy", new Stat(), null));
            assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE, database.getACL("/policy", new Stat()));

            // An explicit snapshot is never suppressed by the startup policy.
            server.takeSnapshot(true);
            assertTrue(snapshot.isFile());
        } finally {
            if (prior == null) {
                System.clearProperty(QuorumPeer.SKIP_LEADER_STARTUP_SNAPSHOT);
            } else {
                System.setProperty(QuorumPeer.SKIP_LEADER_STARTUP_SNAPSHOT, prior);
            }
            snapLog.close();
        }
    }

    @Test
    public void testSkippedStartupSnapshotRetainsInitialSnapshot(@TempDir File directory) throws Exception {
        FileTxnSnapLog snapLog = new FileTxnSnapLog(directory, directory);
        try {
            ZooKeeperServer server = new ZooKeeperServer(snapLog, 2000, null);
            assertFalse(server.getZKDatabase().isInitialized());
            server.loadData(true);
            assertTrue(server.getZKDatabase().isInitialized());
            assertEquals(0, server.getZxid());
            assertTrue(new File(snapLog.getSnapDir(), Util.makeSnapshotName(0)).isFile(),
                       "Database initialization must still write the initial snapshot");
        } finally {
            snapLog.close();
        }
    }

    @Test
    public void testSkippedStartupSnapshotRecoversDataAndSessionsOnRestart(@TempDir File directory) throws Exception {
        long deadSession = 11;
        long liveSession = 22;
        byte[] data = {4, 5, 6};
        FileTxnSnapLog snapLog = new FileTxnSnapLog(directory, directory);
        try {
            ZKDatabase database = new ZKDatabase(snapLog);
            database.loadDataBase();
            assertEquals(0, database.processTxn(
                new TxnHeader(deadSession, 1, 1, 1, ZooDefs.OpCode.create),
                new CreateTxn("/dead", data, ZooDefs.Ids.READ_ACL_UNSAFE, true, 1), null).err);
            assertEquals(0, database.processTxn(
                new TxnHeader(liveSession, 2, 2, 2, ZooDefs.OpCode.create),
                new CreateTxn("/live", data, ZooDefs.Ids.READ_ACL_UNSAFE, true, 2), null).err);
            database.getSessionWithTimeOuts().put(liveSession, 30000);
            snapLog.save(database.getDataTree(), database.getSessionWithTimeOuts(), true);
            TxnHeader header = new TxnHeader(liveSession, 3, 3, 3, ZooDefs.OpCode.create);
            CreateTxn transaction = new CreateTxn("/durable", data, ZooDefs.Ids.READ_ACL_UNSAFE, false, 3);
            assertTrue(snapLog.append(new Request(liveSession, 3, ZooDefs.OpCode.create, header, transaction, 3)));
            snapLog.commit();
        } finally {
            snapLog.close();
        }

        for (int restart = 0; restart < 2; restart++) {
            snapLog = new FileTxnSnapLog(directory, directory);
            try {
                ZooKeeperServer server = new ZooKeeperServer(snapLog, 2000, null);
                ZKDatabase database = server.getZKDatabase();
                if (restart == 1) {
                    database.loadDataBase();
                    assertNotNull(database.getNode("/dead"), "The previous startup did not rewrite the snapshot");
                }
                server.loadData(true);
                assertEquals(3, server.getZxid());
                assertNull(database.getNode("/dead"));
                assertFalse(database.getSessions().contains(deadSession));
                assertArrayEquals(data, database.getData("/live", new Stat(), null));
                assertEquals(Integer.valueOf(30000), database.getSessionWithTimeOuts().get(liveSession));
                assertArrayEquals(data, database.getData("/durable", new Stat(), null));
                assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE, database.getACL("/durable", new Stat()));
                assertEquals(ZooDefs.Ids.READ_ACL_UNSAFE, database.getACL("/live", new Stat()));
                assertFalse(new File(snapLog.getSnapDir(), Util.makeSnapshotName(3)).exists());
            } finally {
                snapLog.close();
            }
        }

    }

    @ParameterizedTest
    @ValueSource(ints = {0, 2})
    public void testSkippedStartupSnapshotLogsDeadSessionCount(int deadSessionCount, @TempDir File directory) throws Exception {
        FileTxnSnapLog snapLog = new FileTxnSnapLog(directory, directory);
        try (LoggerTestTool logger = new LoggerTestTool(ZooKeeperServer.class)) {
            ZooKeeperServer server = new ZooKeeperServer(snapLog, 2000, null);
            ZKDatabase database = server.getZKDatabase();
            database.loadDataBase();
            for (int i = 0; i <= deadSessionCount; i++) {
                long session = i + 1;
                assertEquals(0, database.processTxn(
                    new TxnHeader(session, 1, session, 1, ZooDefs.OpCode.create),
                    new CreateTxn("/session-" + session, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, true, i + 1), null).err);
            }
            database.getSessionWithTimeOuts().put(1L, 30000);

            server.loadData(true);
            String log = logger.readLogLine("Skipping startup snapshot");
            assertNotNull(log);
            assertTrue(log.endsWith(", dead sessions cleaned: " + deadSessionCount), log);
            assertEquals(Collections.singleton(1L), database.getSessions());

            server.loadData(true);
            log = logger.readLogLine("Skipping startup snapshot");
            assertNotNull(log);
            assertTrue(log.endsWith(", dead sessions cleaned: 0"), log);
        } finally {
            snapLog.close();
        }
    }
}
