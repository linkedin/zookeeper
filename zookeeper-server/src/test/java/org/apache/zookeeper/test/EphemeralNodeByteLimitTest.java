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

package org.apache.zookeeper.test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;
import java.util.List;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Exercises the per-session ephemeral path-byte budget configured through
 * {@code zookeeper.ephemeralNodes.total.byte.limit}. Each ephemeral node costs
 * the serialized size of its full path (4-byte length prefix plus the UTF-8
 * bytes of the path). The budget is checked against committed state, so these
 * tests issue requests serially unless they explicitly characterize a batch.
 */
public class EphemeralNodeByteLimitTest extends ClientBase {

    private static final int LIMIT = 60;
    private static final String VIOLATION_METRIC = "ephemeral_node_limit_violation";
    private static final String EXTENDED_TYPES = "zookeeper.extendedTypesEnabled";

    @BeforeAll
    public static void setUpLimit() {
        System.setProperty(ZooKeeperServer.EPHEMERAL_NODES_TOTAL_BYTE_LIMIT_KEY, Integer.toString(LIMIT));
        System.setProperty(EXTENDED_TYPES, "true");
    }

    @AfterAll
    public static void clearLimit() {
        System.clearProperty(ZooKeeperServer.EPHEMERAL_NODES_TOTAL_BYTE_LIMIT_KEY);
        System.clearProperty(EXTENDED_TYPES);
    }

    // 15 bytes each: 11 ASCII bytes plus the 4-byte length prefix.
    private static String asciiPath(int i) {
        return String.format("/node-%05d", i);
    }

    private static int cost(String path) {
        return path.getBytes(UTF_8).length + 4;
    }

    private DataTree dataTree() {
        return serverFactory.getZooKeeperServer().getZKDatabase().getDataTree();
    }

    private int accounted(ZooKeeper zk) {
        return dataTree().getTotalEphemeralsByteSize(zk.getSessionId());
    }

    private static long violations() {
        Object value = MetricsUtils.currentServerMetrics().get(VIOLATION_METRIC);
        assertNotNull(value, VIOLATION_METRIC + " is not registered");
        return (Long) value;
    }

    private static void assertBudgetExceeded(KeeperException e) {
        assertTrue(e instanceof KeeperException.TotalEphemeralLimitExceeded, e.toString());
        assertEquals(Code.TOTALEPHEMERALLIMITEXCEEDED, e.code());
        assertEquals(-128, e.code().intValue());
    }

    private static void createEphemeralRejected(ZooKeeper zk, String path) {
        KeeperException e = assertThrows(KeeperException.class,
                () -> zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL));
        assertBudgetExceeded(e);
    }

    private static void createEphemeral(ZooKeeper zk, String path) throws Exception {
        zk.create(path, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    @Test
    public void testExactBoundaryAndRejection() throws Exception {
        try (ZooKeeper zk = createClient()) {
            for (int i = 0; i < 3; i++) {
                createEphemeral(zk, asciiPath(i));
            }
            assertEquals(45, accounted(zk));

            long before = violations();
            String oneByteOver = "/node-00003x";
            assertEquals(16, cost(oneByteOver));
            try (LoggerTestTool log = new LoggerTestTool(PrepRequestProcessor.class)) {
                createEphemeralRejected(zk, oneByteOver);
                String logged = log.readLogLine("Rejecting ephemeral node creation");
                assertNotNull(logged);
                assertTrue(logged.contains("Rejecting ephemeral node creation for session " + zk.getSessionId() + ", zxid "), logged);
                assertTrue(logged.contains("path " + oneByteOver + "."), logged);
            }
            assertEquals(before + 1, violations());
            assertNull(zk.exists(oneByteOver, false));
            assertEquals(45, accounted(zk));

            // 45 + 15 = 60 == limit is accepted
            createEphemeral(zk, asciiPath(3));
            assertEquals(LIMIT, accounted(zk));

            createEphemeralRejected(zk, "/a");
            assertEquals(before + 2, violations());
            assertEquals(LIMIT, accounted(zk));
            assertEquals(4, zk.getEphemerals().size());
        }
    }

    @Test
    public void testSerializedSizeUsesUtf8Bytes() throws Exception {
        String threeCjk = "/\u4e2d\u4e2d\u4e2d";
        String fourCjk = "/\u4e2d\u4e2d\u4e2d\u4e2d";
        String twoByte = "/\u00e9\u00e9";
        assertEquals(14, cost(threeCjk));
        assertEquals(17, cost(fourCjk));
        assertEquals(9, cost(twoByte));
        assertEquals(cost(threeCjk), BinaryOutputArchive.getSerializedStringByteSize(threeCjk));
        assertEquals(cost(twoByte), BinaryOutputArchive.getSerializedStringByteSize(twoByte));

        try (ZooKeeper zk = createClient()) {
            for (int i = 0; i < 3; i++) {
                createEphemeral(zk, asciiPath(i));
            }
            // 15 bytes remain: fourCjk has only 5 chars but costs 17 bytes.
            createEphemeralRejected(zk, fourCjk);
            createEphemeral(zk, threeCjk);
            assertEquals(59, accounted(zk));
            assertEquals(threeCjk, zk.getEphemerals("/\u4e2d").get(0));

            zk.delete(threeCjk, -1);
            createEphemeral(zk, twoByte);
            assertEquals(54, accounted(zk));
        }
    }

    @Test
    public void testSequentialNodesAreChargedForFinalPath() throws Exception {
        try (ZooKeeper zk = createClient()) {
            String first = zk.create("/seq-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            assertEquals("/seq-0000000000", first);
            assertEquals(19, accounted(zk));
            zk.create("/seq-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            zk.create("/seq-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            assertEquals(57, accounted(zk));

            // The 9-byte prefix would fit, the 19-byte final path does not.
            KeeperException e = assertThrows(KeeperException.class,
                    () -> zk.create("/seq-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL));
            assertBudgetExceeded(e);
            assertEquals(57, accounted(zk));
        }
    }

    @Test
    public void testOnlySessionEphemeralsAreCharged() throws Exception {
        try (ZooKeeper zk = createClient()) {
            for (int i = 0; i < 4; i++) {
                createEphemeral(zk, asciiPath(i));
            }
            assertEquals(LIMIT, accounted(zk));
            createEphemeralRejected(zk, "/a");

            zk.create("/persistent", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create("/persistent/seq-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            zk.create("/container", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER);
            zk.create("/ttl", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_WITH_TTL, null, 60000);
            assertEquals(LIMIT, accounted(zk));
            assertEquals(4, zk.getEphemerals().size());
        }
    }

    @Test
    public void testSessionsHaveIndependentBudgets() throws Exception {
        try (ZooKeeper first = createClient(); ZooKeeper second = createClient()) {
            for (int i = 0; i < 4; i++) {
                createEphemeral(first, asciiPath(i));
            }
            createEphemeralRejected(first, "/a");

            for (int i = 4; i < 8; i++) {
                createEphemeral(second, asciiPath(i));
            }
            createEphemeralRejected(second, "/b");
            assertEquals(LIMIT, accounted(first));
            assertEquals(LIMIT, accounted(second));
        }
    }

    @Test
    public void testDeleteReleasesBudget() throws Exception {
        try (ZooKeeper zk = createClient()) {
            for (int i = 0; i < 4; i++) {
                createEphemeral(zk, asciiPath(i));
            }
            createEphemeralRejected(zk, asciiPath(4));

            zk.delete(asciiPath(0), -1);
            assertEquals(45, accounted(zk));
            createEphemeral(zk, asciiPath(4));
            assertEquals(LIMIT, accounted(zk));

            // A failed delete does not change the accounting.
            assertThrows(KeeperException.BadVersionException.class, () -> zk.delete(asciiPath(1), 100));
            assertEquals(LIMIT, accounted(zk));
        }
    }

    @Test
    public void testCloseAndExpiryClearAccounting() throws Exception {
        ZooKeeper closed = createClient();
        long closedId = closed.getSessionId();
        createEphemeral(closed, asciiPath(0));
        assertEquals(15, dataTree().getTotalEphemeralsByteSize(closedId));
        closed.close();
        assertEquals(0, dataTree().getTotalEphemeralsByteSize(closedId));

        try (ZooKeeper observer = createClient()) {
            ZooKeeper expired = createClient();
            long expiredId = expired.getSessionId();
            createEphemeral(expired, asciiPath(1));
            createEphemeral(expired, asciiPath(2));
            assertEquals(30, dataTree().getTotalEphemeralsByteSize(expiredId));

            serverFactory.getZooKeeperServer().expire(expiredId);
            waitForNodeRemoval(observer, asciiPath(2));
            assertEquals(0, dataTree().getTotalEphemeralsByteSize(expiredId));
            expired.close();

            // The budget of a new session is not affected by the previous ones.
            for (int i = 3; i < 7; i++) {
                createEphemeral(observer, asciiPath(i));
            }
            assertEquals(LIMIT, accounted(observer));
        }
    }

    private static void waitForNodeRemoval(ZooKeeper zk, String path) throws Exception {
        long deadline = System.currentTimeMillis() + CONNECTION_TIMEOUT;
        while (zk.exists(path, false) != null) {
            assertTrue(System.currentTimeMillis() < deadline, path + " still exists");
            Thread.sleep(50);
        }
    }

    @Test
    public void testReconnectKeepsSessionBudget() throws Exception {
        CountdownWatcher watcher = new CountdownWatcher();
        try (TestableZooKeeper zk = createClient(watcher)) {
            long sessionId = zk.getSessionId();
            for (int i = 0; i < 4; i++) {
                createEphemeral(zk, asciiPath(i));
            }

            watcher.reset();
            zk.testableConnloss();
            watcher.waitForConnected(CONNECTION_TIMEOUT);
            assertEquals(sessionId, zk.getSessionId());

            createEphemeralRejected(zk, "/a");
            assertEquals(LIMIT, accounted(zk));
            zk.delete(asciiPath(3), -1);
            createEphemeral(zk, "/a");
            assertEquals(51, accounted(zk));
        }
    }

    @Test
    public void testRejectedOrFailedMultiDoesNotChangeAccounting() throws Exception {
        try (ZooKeeper zk = createClient()) {
            for (int i = 0; i < 3; i++) {
                createEphemeral(zk, asciiPath(i));
            }
            long before = violations();

            List<Op> rejected = Arrays.asList(
                    Op.create("/persistent", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                    Op.create("/node-00003x", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL));
            KeeperException e = assertThrows(KeeperException.class, () -> zk.multi(rejected));
            assertBudgetExceeded(e);
            List<OpResult> results = e.getResults();
            assertEquals(Code.TOTALEPHEMERALLIMITEXCEEDED.intValue(), ((OpResult.ErrorResult) results.get(1)).getErr());
            assertEquals(before + 1, violations());
            assertNull(zk.exists("/persistent", false));
            assertEquals(45, accounted(zk));

            List<Op> failed = Arrays.asList(
                    Op.create(asciiPath(3), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
                    Op.check(asciiPath(0), 100));
            assertThrows(KeeperException.BadVersionException.class, () -> zk.multi(failed));
            assertNull(zk.exists(asciiPath(3), false));
            assertEquals(45, accounted(zk));
            assertEquals(before + 1, violations());
        }
    }

    /**
     * The budget is best effort: each create is checked against the committed
     * per-session total only, so creates in the same multi (or otherwise in
     * flight together) are not counted against each other and the committed
     * total can end up above the configured value.
     */
    @Test
    public void testCreatesInOneMultiAreCheckedAgainstCommittedTotal() throws Exception {
        try (ZooKeeper zk = createClient()) {
            for (int i = 0; i < 3; i++) {
                createEphemeral(zk, asciiPath(i));
            }
            zk.multi(Arrays.asList(
                    Op.create(asciiPath(3), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
                    Op.create(asciiPath(4), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)));
            assertEquals(75, accounted(zk));
            assertFalse(accounted(zk) <= LIMIT);

            createEphemeralRejected(zk, "/a");
            zk.delete(asciiPath(0), -1);
            zk.delete(asciiPath(1), -1);
            createEphemeral(zk, "/a");
            assertEquals(51, accounted(zk));
        }
    }
}
