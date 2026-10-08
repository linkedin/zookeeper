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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.PaginationNextPage;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.PathTrie;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTreeTest extends ZKTestCase {

    protected static final Logger LOG = LoggerFactory.getLogger(DataTreeTest.class);

    /**
     * For ZOOKEEPER-1755 - Test race condition when taking dumpEphemerals and
     * removing the session related ephemerals from DataTree structure
     */
    @Test(timeout = 60000)
    public void testDumpEphemerals() throws Exception {
        int count = 1000;
        long session = 1000;
        long zxid = 2000;
        final DataTree dataTree = new DataTree();
        LOG.info("Create {} zkclient sessions and its ephemeral nodes", count);
        createEphemeralNode(session, dataTree, count);
        final AtomicBoolean exceptionDuringDumpEphemerals = new AtomicBoolean(false);
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread thread = new Thread() {
            public void run() {
                PrintWriter pwriter = new PrintWriter(new StringWriter());
                try {
                    while (running.get()) {
                        dataTree.dumpEphemerals(pwriter);
                    }
                } catch (Exception e) {
                    LOG.error("Received exception while dumpEphemerals!", e);
                    exceptionDuringDumpEphemerals.set(true);
                }
            }
        };
        thread.start();
        LOG.debug("Killing {} zkclient sessions and its ephemeral nodes", count);
        killZkClientSession(session, zxid, dataTree, count);
        running.set(false);
        thread.join();
        assertFalse("Should have got exception while dumpEphemerals!", exceptionDuringDumpEphemerals.get());
    }

    private void killZkClientSession(long session, long zxid, final DataTree dataTree, int count) {
        for (int i = 0; i < count; i++) {
            dataTree.killSession(session + i, zxid);
        }
    }

    private void createEphemeralNode(long session, final DataTree dataTree, int count) throws NoNodeException, NodeExistsException {
        for (int i = 0; i < count; i++) {
            dataTree.createNode("/test" + i, new byte[0], null, session + i, dataTree.getNode("/").stat.getCversion()
                                                                                     + 1, 1, 1);
        }
    }

    @Test(timeout = 60000)
    public void testRootWatchTriggered() throws Exception {
        DataTree dt = new DataTree();

        CompletableFuture<Void> fire = new CompletableFuture<>();
        // set a watch on the root node
        dt.getChildren("/", new Stat(), event -> {
            if (event.getPath().equals("/")) {
                fire.complete(null);
            }
        });

        // add a new node, should trigger a watch
        dt.createNode("/xyz", new byte[0], null, 0, dt.getNode("/").stat.getCversion() + 1, 1, 1);

        assertTrue("Root node watch not triggered", fire.isDone());
    }

    /**
     * For ZOOKEEPER-1046 test if cversion is getting incremented correctly.
     */
    @Test(timeout = 60000)
    public void testIncrementCversion() throws Exception {
        try {
            // digestCalculator gets initialized for the new DataTree constructor based on the system property
            ZooKeeperServer.setDigestEnabled(true);
            DataTree dt = new DataTree();
            dt.createNode("/test", new byte[0], null, 0, dt.getNode("/").stat.getCversion() + 1, 1, 1);
            DataNode zk = dt.getNode("/test");
            int prevCversion = zk.stat.getCversion();
            long prevPzxid = zk.stat.getPzxid();
            long digestBefore = dt.getTreeDigest();
            dt.setCversionPzxid("/test/", prevCversion + 1, prevPzxid + 1);
            int newCversion = zk.stat.getCversion();
            long newPzxid = zk.stat.getPzxid();
            assertTrue("<cversion, pzxid> verification failed. Expected: <"
                                      + (prevCversion + 1)
                                      + ", "
                                      + (prevPzxid + 1)
                                      + ">, found: <"
                                      + newCversion
                                      + ", "
                                      + newPzxid
                                      + ">", (newCversion == prevCversion + 1 && newPzxid == prevPzxid + 1));
            assertNotEquals(digestBefore, dt.getTreeDigest());
        } finally {
            ZooKeeperServer.setDigestEnabled(false);
        }
    }

    @Test
    public void testNoCversionRevert() throws Exception {
        DataTree dt = new DataTree();
        DataNode parent = dt.getNode("/");
        dt.createNode("/test", new byte[0], null, 0, parent.stat.getCversion() + 1, 1, 1);
        int currentCversion = parent.stat.getCversion();
        long currentPzxid = parent.stat.getPzxid();
        dt.createNode("/test1", new byte[0], null, 0, currentCversion - 1, 1, 1);
        parent = dt.getNode("/");
        int newCversion = parent.stat.getCversion();
        long newPzxid = parent.stat.getPzxid();
        assertTrue("<cversion, pzxid> verification failed. Expected: <"
                                  + currentCversion
                                  + ", "
                                  + currentPzxid
                                  + ">, found: <"
                                  + newCversion
                                  + ", "
                                  + newPzxid
                                  + ">", (newCversion >= currentCversion && newPzxid >= currentPzxid));
    }

    @Test
    public void testPzxidUpdatedWhenDeletingNonExistNode() throws Exception {
        DataTree dt = new DataTree();
        DataNode root = dt.getNode("/");
        long currentPzxid = root.stat.getPzxid();

        // pzxid updated with deleteNode on higher zxid
        long zxid = currentPzxid + 1;
        try {
            dt.deleteNode("/testPzxidUpdatedWhenDeletingNonExistNode", zxid);
        } catch (NoNodeException e) { /* expected */ }
        root = dt.getNode("/");
        currentPzxid = root.stat.getPzxid();
        assertEquals(currentPzxid, zxid);

        // pzxid not updated with smaller zxid
        long prevPzxid = currentPzxid;
        zxid = prevPzxid - 1;
        try {
            dt.deleteNode("/testPzxidUpdatedWhenDeletingNonExistNode", zxid);
        } catch (NoNodeException e) { /* expected */ }
        root = dt.getNode("/");
        currentPzxid = root.stat.getPzxid();
        assertEquals(currentPzxid, prevPzxid);
    }

    @Test
    public void testDigestUpdatedWhenReplayCreateTxnForExistNode() {
        try {
            // digestCalculator gets initialized for the new DataTree constructor based on the system property
            ZooKeeperServer.setDigestEnabled(true);
            DataTree dt = new DataTree();

            dt.processTxn(new TxnHeader(13, 1000, 1, 30, ZooDefs.OpCode.create), new CreateTxn("/foo", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 1));

            // create the same node with a higher cversion to simulate the
            // scenario when replaying a create txn for an existing node due
            // to fuzzy snapshot
            dt.processTxn(new TxnHeader(13, 1000, 1, 30, ZooDefs.OpCode.create), new CreateTxn("/foo", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 2));

            // check the current digest value
            assertEquals(dt.getTreeDigest(), dt.getLastProcessedZxidDigest().getDigest());
        } finally {
            ZooKeeperServer.setDigestEnabled(false);
        }
    }

    @Test(timeout = 60000)
    public void testPathTrieClearOnDeserialize() throws Exception {

        //Create a DataTree with quota nodes so PathTrie get updated
        DataTree dserTree = new DataTree();

        dserTree.createNode("/bug", new byte[20], null, -1, 1, 1, 1);
        dserTree.createNode(Quotas.quotaZookeeper + "/bug", null, null, -1, 1, 1, 1);
        dserTree.createNode(Quotas.quotaPath("/bug"), new byte[20], null, -1, 1, 1, 1);
        dserTree.createNode(Quotas.statPath("/bug"), new byte[20], null, -1, 1, 1, 1);

        //deserialize a DataTree; this should clear the old /bug nodes and pathTrie
        DataTree tree = new DataTree();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive oa = BinaryOutputArchive.getArchive(baos);
        tree.serialize(oa, "test");
        baos.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BinaryInputArchive ia = BinaryInputArchive.getArchive(bais);
        dserTree.deserialize(ia, "test");

        Field pfield = DataTree.class.getDeclaredField("pTrie");
        pfield.setAccessible(true);
        PathTrie pTrie = (PathTrie) pfield.get(dserTree);

        //Check that the node path is removed from pTrie
        assertEquals("/bug is still in pTrie", "/", pTrie.findMaxPrefix("/bug"));
    }

    @Test
    public void testQuotaStatsRejectsMalformedUsage() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        String[] values = {
            "other=1,bytes=3", "count=1,other=3", "bytes=3,count=1", "count=1",
            "count=1,bytes=3,", "count=1,bytes=3,extra=4", "count=1=2,bytes=3",
            "count=+1,bytes=3", "count=1,bytes=+3", "count=1,bytes=3\n",
            " count=1,bytes=3", "count=1,bytes=3.0", "count=\u0661,bytes=3", "sensitive-token"
        };
        for (String value : values) {
            setQuotaData(tree, Quotas.statPath("/quota-test"), value);
            assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_stats");
        }
        tree.setData(Quotas.statPath("/quota-test"), new byte[]{(byte) 0xc3, (byte) 0x28}, 1, 2, 2);
        assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_stats");
    }

    @Test
    public void testQuotaStatsRejectsMalformedLimits() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        for (String value : Arrays.asList("other=10,bytes=100", "bytes=100,count=10",
                                          "count=10,bytes=", "count=10,bytes=100,",
                                          "count=10,bytes=100=sensitive-token", "sensitive-token")) {
            setQuotaData(tree, Quotas.quotaPath("/quota-test"), value);
            assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_limits");
        }
    }

    @Test
    public void testQuotaStatsRejectsNullOrEmptyMetadata() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        for (String value : Arrays.asList(null, "")) {
            setQuotaData(tree, Quotas.statPath("/quota-test"), value);
            assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_stats");
        }
        setQuotaData(tree, Quotas.statPath("/quota-test"), "count=1,bytes=3");
        for (String value : Arrays.asList(null, "")) {
            setQuotaData(tree, Quotas.quotaPath("/quota-test"), value);
            assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_limits");
        }
    }

    @Test
    public void testQuotaStatsRejectsOutOfRangeUsage() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        for (String value : Arrays.asList("count=-1,bytes=3", "count=1,bytes=-1",
                                          "count=-2,bytes=3", "count=2147483648,bytes=3",
                                          "count=1,bytes=9223372036854775808")) {
            setQuotaData(tree, Quotas.statPath("/quota-test"), value);
            assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_stats");
        }
    }

    @Test
    public void testQuotaStatsRejectsOutOfRangeLimits() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        for (String value : Arrays.asList("count=-2,bytes=100", "count=10,bytes=-2",
                                          "count=2147483648,bytes=100",
                                          "count=10,bytes=9223372036854775808")) {
            setQuotaData(tree, Quotas.quotaPath("/quota-test"), value);
            assertQuotaUnavailable(sampleQuota(tree), "invalid_quota_limits");
        }
    }

    @Test
    public void testQuotaStatsIncompleteMetadata() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        tree.deleteNode(Quotas.statPath("/quota-test"), 2);
        assertQuotaUnavailable(sampleQuota(tree), "quota_incomplete");
        tree.createNode(Quotas.statPath("/quota-test"), new byte[0], null, 0, -1, 3, 3);
        tree.deleteNode(Quotas.quotaPath("/quota-test"), 4);
        assertQuotaUnavailable(sampleQuota(tree), "quota_incomplete");
        tree.deleteNode(Quotas.statPath("/quota-test"), 5);
        assertQuotaUnavailable(sampleQuota(tree), "quota_missing");
    }

    @Test
    public void testQuotaStatsNamespaceRemoval() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        tree.deleteNode("/quota-test", 2);
        assertNotNull(tree.getNode(Quotas.statPath("/quota-test")));
        assertNotNull(tree.getNode(Quotas.quotaPath("/quota-test")));
        assertQuotaUnavailable(sampleQuota(tree), "namespace_missing");
        tree.createNode("/quota-test", new byte[5], null, 0, -1, 3, 3);
        DataTree.QuotaStats sample = sampleQuota(tree);
        assertTrue(sample.isAvailable());
        assertEquals(Integer.valueOf(1), sample.getCountUsed());
        assertEquals(Long.valueOf(5), sample.getBytesUsed());
    }

    @Test
    public void testQuotaStatsDetectsRemovalDuringSample() throws Exception {
        for (String path : Arrays.asList("/quota-test", Quotas.statPath("/quota-test"), Quotas.quotaPath("/quota-test"))) {
            ChangingQuotaTree tree = new ChangingQuotaTree();
            createQuotaTree(tree);
            tree.changePath = path;
            assertQuotaUnavailable(sampleQuota(tree), "quota_changed");
        }
    }

    @Test
    public void testQuotaStatsDetectsReplacementDuringSample() throws Exception {
        for (String path : Arrays.asList("/quota-test", Quotas.statPath("/quota-test"), Quotas.quotaPath("/quota-test"))) {
            ChangingQuotaTree tree = new ChangingQuotaTree();
            createQuotaTree(tree);
            tree.changePath = path;
            tree.replace = true;
            assertQuotaUnavailable(sampleQuota(tree), "quota_changed");
        }
    }

    @Test
    public void testQuotaStatsDoesNotTraverseMissingQuota() throws Exception {
        BoundedQuotaTree tree = new BoundedQuotaTree();
        tree.createNode("/quota-test", new byte[3], null, 0, -1, 1, 1);
        for (int i = 0; i < 100; i++) {
            tree.createNode("/quota-test/child" + i, new byte[4], null, 0, -1, 1, 1);
        }
        tree.sampling = true;
        assertQuotaUnavailable(sampleQuota(tree), "quota_missing");
        assertTrue(tree.lookups <= 6);
    }

    @Test
    public void testQuotaStatsSamplesMetadataWithoutRecounting() throws Exception {
        BoundedQuotaTree tree = new BoundedQuotaTree();
        createQuotaTree(tree);
        setQuotaData(tree, Quotas.statPath("/quota-test"), "count=19,bytes=999");
        byte[] data = tree.getNode(Quotas.statPath("/quota-test")).getData().clone();
        long digest = tree.getTreeDigest();
        long size = tree.cachedApproximateDataSize();
        int watches = tree.getWatchCount();
        Map<String, Object> metrics = MetricsUtils.currentServerMetrics();
        tree.sampling = true;
        DataTree.QuotaStats sample = sampleQuota(tree);
        tree.sampling = false;

        assertTrue(sample.isAvailable());
        assertEquals(Integer.valueOf(19), sample.getCountUsed());
        assertEquals(Long.valueOf(999), sample.getBytesUsed());
        assertTrue(tree.lookups <= 6);
        assertArrayEquals(data, tree.getNode(Quotas.statPath("/quota-test")).getData());
        assertEquals(digest, tree.getTreeDigest());
        assertEquals(size, tree.cachedApproximateDataSize());
        assertEquals(watches, tree.getWatchCount());
        assertEquals(metrics, MetricsUtils.currentServerMetrics());
        setQuotaData(tree, Quotas.statPath("/quota-test"), "count=2,bytes=7");
        assertEquals(Integer.valueOf(19), sample.getCountUsed());
        assertEquals(Long.valueOf(999), sample.getBytesUsed());
    }

    @Test
    public void testQuotaStatsNumericBoundaries() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        setQuotaData(tree, Quotas.statPath("/quota-test"), "count=2147483647,bytes=9223372036854775807");
        setQuotaData(tree, Quotas.quotaPath("/quota-test"), "count=2147483647,bytes=9223372036854775807");
        DataTree.QuotaStats sample = sampleQuota(tree);
        assertTrue(sample.isAvailable());
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), sample.getCountUsed());
        assertEquals(Long.valueOf(Long.MAX_VALUE), sample.getBytesUsed());
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), sample.getCountLimit());
        assertEquals(Long.valueOf(Long.MAX_VALUE), sample.getBytesLimit());
        setQuotaData(tree, Quotas.statPath("/quota-test"), "count=000,bytes=000");
        sample = sampleQuota(tree);
        assertTrue(sample.isAvailable());
        assertEquals(Integer.valueOf(0), sample.getCountUsed());
        assertEquals(Long.valueOf(0), sample.getBytesUsed());
    }

    @Test(timeout = 30000)
    public void testQuotaStatsConcurrentMetadataUpdates() throws Exception {
        DataTree tree = new DataTree();
        createQuotaTree(tree);
        setQuotaData(tree, Quotas.statPath("/quota-test"), "count=2,bytes=20");
        setQuotaData(tree, Quotas.quotaPath("/quota-test"), "count=5,bytes=50");
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService writer = Executors.newSingleThreadExecutor();
        try {
            Future<?> updates = writer.submit(() -> {
                start.await();
                for (int i = 0; i < 1000; i++) {
                    setQuotaData(tree, Quotas.statPath("/quota-test"), i % 2 == 0 ? "count=3,bytes=30" : "count=2,bytes=20");
                    setQuotaData(tree, Quotas.quotaPath("/quota-test"), i % 2 == 0 ? "count=7,bytes=70" : "count=5,bytes=50");
                }
                return null;
            });
            start.countDown();
            for (int i = 0; i < 1000; i++) {
                DataTree.QuotaStats sample = sampleQuota(tree);
                assertTrue(sample.isAvailable());
                assertTrue((sample.getCountUsed() == 2 && sample.getBytesUsed() == 20)
                           || (sample.getCountUsed() == 3 && sample.getBytesUsed() == 30));
                assertTrue((sample.getCountLimit() == 5 && sample.getBytesLimit() == 50)
                           || (sample.getCountLimit() == 7 && sample.getBytesLimit() == 70));
            }
            updates.get(10, TimeUnit.SECONDS);
        } finally {
            writer.shutdownNow();
            assertTrue(writer.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private void createQuotaTree(DataTree tree) throws Exception {
        tree.createNode("/quota-test", new byte[3], null, 0, -1, 1, 1);
        tree.createNode(Quotas.quotaZookeeper + "/quota-test", null, null, 0, -1, 1, 1);
        tree.createNode(Quotas.quotaPath("/quota-test"), "count=10,bytes=100".getBytes(StandardCharsets.UTF_8),
                        null, 0, -1, 1, 1);
        tree.createNode(Quotas.statPath("/quota-test"), new byte[0], null, 0, -1, 1, 1);
    }

    private void setQuotaData(DataTree tree, String path, String value) throws NoNodeException {
        tree.setData(path, value == null ? null : value.getBytes(StandardCharsets.UTF_8), 1, 2, 2);
    }

    private DataTree.QuotaStats sampleQuota(DataTree tree) {
        try {
            return tree.getQuotaStats("/quota-test");
        } catch (RuntimeException e) {
            throw new AssertionError("Quota telemetry must report unavailable, not throw " + e.getClass().getSimpleName());
        }
    }

    private void assertQuotaUnavailable(DataTree.QuotaStats sample, String reason) {
        assertFalse(sample.isAvailable());
        assertEquals(reason, sample.getReason());
        assertNull(sample.getCountUsed());
        assertNull(sample.getBytesUsed());
        assertNull(sample.getCountLimit());
        assertNull(sample.getBytesLimit());
    }

    private static class BoundedQuotaTree extends DataTree {

        private boolean sampling;
        private int lookups;

        @Override
        public DataNode getNode(String path) {
            if (sampling) {
                assertTrue("Unexpected subtree lookup", "/quota-test".equals(path)
                           || Quotas.statPath("/quota-test").equals(path)
                           || Quotas.quotaPath("/quota-test").equals(path));
                assertTrue("Quota telemetry must perform bounded lookups", ++lookups <= 6);
            }
            return super.getNode(path);
        }

        @Override
        public String getMaxPrefixWithQuota(String path) {
            assertFalse("Quota telemetry must not search ancestor quotas", sampling);
            return super.getMaxPrefixWithQuota(path);
        }

    }

    private static class ChangingQuotaTree extends DataTree {

        private String changePath;
        private boolean replace;

        @Override
        public DataNode getNode(String path) {
            DataNode node = super.getNode(path);
            if (changePath != null && Quotas.quotaPath("/quota-test").equals(path)) {
                String changed = changePath;
                changePath = null;
                byte[] data = super.getNode(changed).getData();
                try {
                    super.deleteNode(changed, 2);
                    if (replace) {
                        super.createNode(changed, data, null, 0, -1, 3, 3);
                    }
                } catch (NoNodeException | NodeExistsException e) {
                    throw new AssertionError(e);
                }
            }
            return node;
        }

    }


    /* ZOOKEEPER-3531 - org.apache.zookeeper.server.DataTree#serialize calls the aclCache.serialize when doing
     * dataree serialization, however, org.apache.zookeeper.server.ReferenceCountedACLCache#serialize
     * could get stuck at OutputArchieve.writeInt due to potential network/disk issues.
     * This can cause the system experiences hanging issues similar to ZooKeeper-2201.
     * This test verifies the fix that we should not hold ACL cache during dumping aclcache to snapshots
    */
    @Test(timeout = 60000)
    public void testSerializeDoesntLockACLCacheWhileWriting() throws Exception {
        DataTree tree = new DataTree();
        tree.createNode("/marker", new byte[]{42}, null, -1, 1, 1, 1);
        final AtomicBoolean ranTestCase = new AtomicBoolean();
        DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
        BinaryOutputArchive oa = new BinaryOutputArchive(out) {
            @Override
            public void writeInt(int size, String tag) throws IOException {
                final Semaphore semaphore = new Semaphore(0);

                new Thread(new Runnable() {
                    @Override
                    public void run() {

                        synchronized (tree.getReferenceCountedAclCache()) {
                            //When we lock ACLCache, allow writeRecord to continue
                            semaphore.release();
                        }
                    }
                }).start();

                try {
                    boolean acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
                    //This is the real assertion - could another thread lock
                    //the ACLCache
                    assertTrue("Couldn't acquire a lock on the ACLCache while we were calling tree.serialize", acquired);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                ranTestCase.set(true);

                super.writeInt(size, tag);
            }
        };

        tree.serialize(oa, "test");

        //Let's make sure that we hit the code that ran the real assertion above
        assertTrue("Didn't find the expected node", ranTestCase.get());
    }

    @Test(timeout = 60000)
    public void testGetChildrenPaginated() throws NodeExistsException, NoNodeException {
        final String rootPath = "/children";
        final int firstCzxId = 1000;
        final int countNodes = 10;

        //  Create the parent node
        DataTree dt = new DataTree();
        dt.createNode(rootPath, new byte[0], null, 0, dt.getNode("/").stat.getCversion() + 1, 1, 1);

        //  Create 10 child nodes
        List<String> childrenCreated = new ArrayList<>(countNodes);
        for (int i = 0; i < countNodes; ++i) {
            dt.createNode(rootPath + "/test-" + i, new byte[0], null, 0, dt.getNode(rootPath).stat.getCversion() + i + 1, firstCzxId + i, 1);
            childrenCreated.add("test-" + i);
        }

        //  Asking from a negative for 5 nodes should return the 5, and not set the watch
        int curWatchCount = dt.getWatchCount();
        PaginationNextPage nextPage = new PaginationNextPage();
        List<String> result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 5, -1, 0, nextPage);
        assertEquals(5, result.size());
        assertEquals(firstCzxId + 5, nextPage.getMinCzxid());
        assertEquals(0, nextPage.getMinCzxidOffset());
        assertEquals("The watch not should have been set", curWatchCount, dt.getWatchCount());
        //  Verify that the list is sorted
        String before = "";
        for (final String path : result) {
            assertTrue(String.format("The next path (%s) should be > previous (%s)", path, before),
                    path.compareTo(before) > 0);
            before = path;
        }

        //  Asking from a negative would give me all children, and set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), countNodes, -1, 0, nextPage);
        assertEquals(countNodes, result.size());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());
        //  Verify that the list is sorted
        before = "";
        for (final String path : result) {
            assertTrue(String.format("The next path (%s) should be > previous (%s)", path, before),
                    path.compareTo(before) > 0);
            before = path;
        }

        //  Asking with maxReturned = MAX_INT would give me all children with no sorting, and set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), Integer.MAX_VALUE, 0, 0, nextPage);
        assertEquals(countNodes, result.size());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());
        //  Verify that the list is not sorted
        assertNotEquals("The returned list of children should not be sorted", childrenCreated, result);

        // Passing a null watch when fetching all children should not set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, null, Integer.MAX_VALUE, 0, 0, nextPage);
        assertEquals(countNodes, result.size());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount, dt.getWatchCount());

        // Passing a null watch when fetching all children should not set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, null, countNodes, 0, 0, nextPage);
        // Returned result is ordered
        assertEquals(childrenCreated, result);
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount, dt.getWatchCount());

        //  Asking from the last one should return only one node
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 2, 1000 + countNodes - 1, 0, nextPage);
        assertEquals(1, result.size());
        assertEquals("test-" + (countNodes - 1), result.get(0));
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());

        //  Asking from the last created node+1 should return an empty list and set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 2, 1000 + countNodes, 0, nextPage);
        assertTrue("The result should be an empty list", result.isEmpty());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());

        //  Asking from -1 for one node should return two, and NOT set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 1, -1, 0, nextPage);
        assertEquals("No watch should be set", curWatchCount, dt.getWatchCount());
        assertEquals("We only return up to ", 1, result.size());
        //  Check that we ordered correctly
        assertEquals("test-0", result.get(0));
        // Check next page info is returned correctly
        assertEquals(firstCzxId + 1, nextPage.getMinCzxid());
        assertEquals(0, nextPage.getMinCzxidOffset());
    }

    @Test(timeout = 60000)
    public void testGetChildrenPaginatedWithOffset() throws NodeExistsException, NoNodeException {
        final String rootPath = "/children";
        final int childrenCzxId = 1000;
        final int countNodes = 9;
        final int allNodes = countNodes + 2;

        //  Create the parent node
        DataTree dt = new DataTree();
        dt.createNode(rootPath, new byte[0], null, 0, dt.getNode("/").stat.getCversion() + 1, 1, 1);

        int parentVersion = dt.getNode(rootPath).stat.getCversion();

        //  Create a children sometimes "before"
        dt.createNode(rootPath + "/test-0", new byte[0], null, 0, parentVersion + 1, childrenCzxId - 100, 1);

        //  Create 10 child nodes, all with the same
        for (int i = 1; i <= countNodes; ++i) {
            dt.createNode(rootPath + "/test-" + i, new byte[0], null, 0, parentVersion + 2, childrenCzxId, 1);
        }

        //  Create a children sometimes "after"
        dt.createNode(rootPath + "/test-999", new byte[0], null, 0, parentVersion + 3, childrenCzxId + 100, 1);

        //  Asking from a negative would give me all children, and set the watch
        int curWatchCount = dt.getWatchCount();
        PaginationNextPage nextPage = new PaginationNextPage();
        List<String> result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 1000, -1, 0, nextPage);
        assertEquals(allNodes, result.size());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());
        //  Verify that the list is sorted
        String before = "";
        for (final String path : result) {
            assertTrue(String.format("The next path (%s) should be > previous (%s)", path, before),
                    path.compareTo(before) > 0);
            before = path;
        }

        // Asking with minCzxid = 0, offset = 0 should not skip anything.
        // maxReturned = 1, the returned nextPage should be: minCzxid = next czxid, offset = 0.
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 1, 0, 0, nextPage);
        assertEquals(1, result.size());
        assertEquals("test-0", result.get(0));
        assertEquals(childrenCzxId, nextPage.getMinCzxid());
        assertEquals(0, nextPage.getMinCzxidOffset());
        assertEquals("The watch should not have been set", curWatchCount, dt.getWatchCount());

        //  Asking with offset minCzxId below childrenCzxId should not skip anything, regardless of offset
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 2, childrenCzxId - 1, 3, nextPage);
        assertEquals(2, result.size());
        assertEquals("test-1", result.get(0));
        assertEquals("test-2", result.get(1));
        assertEquals(childrenCzxId, nextPage.getMinCzxid());
        assertEquals(2, nextPage.getMinCzxidOffset());
        assertEquals("The watch should not have been set", curWatchCount, dt.getWatchCount());

        //  Asking with offset 5 should skip nodes 1, 2, 3, 4, 5
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 2, childrenCzxId, 5, nextPage);
        assertEquals(2, result.size());
        assertEquals("test-6", result.get(0));
        assertEquals("test-7", result.get(1));
        assertEquals(childrenCzxId, nextPage.getMinCzxid());
        assertEquals(7, nextPage.getMinCzxidOffset());
        assertEquals("The watch should not have been set", curWatchCount, dt.getWatchCount());

        //  Asking with offset 5 for more nodes than are there should skip nodes 1, 2, 3, 4, 5 (plus 0 due to zxid)
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 10, childrenCzxId, 5, nextPage);

        assertEquals(5, result.size());
        assertEquals("test-6", result.get(0));
        assertEquals("test-7", result.get(1));
        assertEquals("test-8", result.get(2));
        assertEquals("test-9", result.get(3));
        assertEquals("test-999", result.get(4));
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());

        //  Asking with offset 5 for fewer nodes than are there should skip nodes 1, 2, 3, 4, 5 (plus 0 due to zxid)
        // Returned next page should be: minCzxid = next czxid, offset = 0
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 4, childrenCzxId, 5, nextPage);

        assertEquals(4, result.size());
        assertEquals("test-6", result.get(0));
        assertEquals("test-7", result.get(1));
        assertEquals("test-8", result.get(2));
        assertEquals("test-9", result.get(3));
        assertEquals(childrenCzxId + 100, nextPage.getMinCzxid());
        assertEquals(0, nextPage.getMinCzxidOffset());
        assertEquals("The watch should not have been set", curWatchCount, dt.getWatchCount());

        //  Asking from the last created node+1 should return an empty list and set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 2, 1000 + childrenCzxId, 0, nextPage);
        assertTrue("The result should be an empty list", result.isEmpty());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());
    }

    @Test(timeout = 60000)
    public void testGetChildrenPaginatedEmpty() throws NodeExistsException, NoNodeException {
        final String rootPath = "/children";

        //  Create the parent node
        DataTree dt = new DataTree();
        dt.createNode(rootPath, new byte[0], null, 0, dt.getNode("/").stat.getCversion() + 1, 1, 1);

        // Asking from a negative would give me all children, and set the watch
        // This goes to the pagination branch.
        int curWatchCount = dt.getWatchCount();
        PaginationNextPage nextPage = new PaginationNextPage();
        List<String> result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), 100, -1, 0, nextPage);
        assertTrue("The result should be empty", result.isEmpty());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());

        // Specify max int to fetch all children - trying to return all without sorting, and set the watch
        curWatchCount = dt.getWatchCount();
        result = dt.getPaginatedChildren(rootPath, null, new DummyWatcher(), Integer.MAX_VALUE, 0, 0, nextPage);
        assertTrue("The result should be empty", result.isEmpty());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageMinCzxid, nextPage.getMinCzxid());
        assertEquals(ZooDefs.GetChildrenPaginated.lastPageCzxidOffset, nextPage.getMinCzxidOffset());
        assertEquals("The watch should have been set", curWatchCount + 1, dt.getWatchCount());
    }

    private class DummyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent ignored) {
        }
    }

    /* ZOOKEEPER-3531 - similarly for aclCache.deserialize, we should not hold lock either
    */
    @Test(timeout = 60000)
    public void testDeserializeDoesntLockACLCacheWhileReading() throws Exception {
        DataTree tree = new DataTree();
        tree.createNode("/marker", new byte[]{42}, null, -1, 1, 1, 1);
        final AtomicBoolean ranTestCase = new AtomicBoolean();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        BinaryOutputArchive oa = new BinaryOutputArchive(out);

        tree.serialize(oa, "test");

        DataTree tree2 = new DataTree();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        BinaryInputArchive ia = new BinaryInputArchive(in) {
            @Override
            public long readLong(String tag) throws IOException {
                final Semaphore semaphore = new Semaphore(0);

                new Thread(new Runnable() {
                    @Override
                    public void run() {

                        synchronized (tree2.getReferenceCountedAclCache()) {
                            //When we lock ACLCache, allow readLong to continue
                            semaphore.release();
                        }
                    }
                }).start();

                try {
                    boolean acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
                    //This is the real assertion - could another thread lock
                    //the ACLCache
                    assertTrue("Couldn't acquire a lock on the ACLCache while we were calling tree.deserialize", acquired);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                ranTestCase.set(true);

                return super.readLong(tag);
            }
        };

        tree2.deserialize(ia, "test");

        //Let's make sure that we hit the code that ran the real assertion above
        assertTrue("Didn't find the expected node", ranTestCase.get());
    }

    /*
     * ZOOKEEPER-2201 - OutputArchive.writeRecord can block for long periods of
     * time, we must call it outside of the node lock.
     * We call tree.serialize, which calls our modified writeRecord method that
     * blocks until it can verify that a separate thread can lock the DataNode
     * currently being written, i.e. that DataTree.serializeNode does not hold
     * the DataNode lock while calling OutputArchive.writeRecord.
     */
    @Test(timeout = 60000)
    public void testSerializeDoesntLockDataNodeWhileWriting() throws Exception {
        DataTree tree = new DataTree();
        tree.createNode("/marker", new byte[]{42}, null, -1, 1, 1, 1);
        final DataNode markerNode = tree.getNode("/marker");
        final AtomicBoolean ranTestCase = new AtomicBoolean();
        DataOutputStream out = new DataOutputStream(new ByteArrayOutputStream());
        BinaryOutputArchive oa = new BinaryOutputArchive(out) {
            @Override
            public void writeRecord(Record r, String tag) throws IOException {
                // Need check if the record is a DataNode instance because of changes in ZOOKEEPER-2014
                // which adds default ACL to config node.
                if (r instanceof DataNode) {
                    DataNode node = (DataNode) r;
                    if (node.data.length == 1 && node.data[0] == 42) {
                        final Semaphore semaphore = new Semaphore(0);
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                synchronized (markerNode) {
                                    //When we lock markerNode, allow writeRecord to continue
                                    semaphore.release();
                                }
                            }
                        }).start();

                        try {
                            boolean acquired = semaphore.tryAcquire(30, TimeUnit.SECONDS);
                            //This is the real assertion - could another thread lock
                            //the DataNode we're currently writing
                            assertTrue("Couldn't acquire a lock on the DataNode while we were calling tree.serialize", acquired);
                        } catch (InterruptedException e1) {
                            throw new RuntimeException(e1);
                        }
                        ranTestCase.set(true);
                    }
                }

                super.writeRecord(r, tag);
            }
        };

        tree.serialize(oa, "test");

        //Let's make sure that we hit the code that ran the real assertion above
        assertTrue("Didn't find the expected node", ranTestCase.get());
    }

    @Test(timeout = 60000)
    public void testReconfigACLClearOnDeserialize() throws Exception {

        DataTree tree = new DataTree();
        // simulate the upgrading scenario, where the reconfig znode
        // doesn't exist and the acl cache is empty
        tree.deleteNode(ZooDefs.CONFIG_NODE, 1);
        tree.getReferenceCountedAclCache().aclIndex = 0;

        assertEquals("expected to have 1 acl in acl cache map", 0, tree.aclCacheSize());

        // serialize the data with one znode with acl
        tree.createNode("/bug", new byte[20], ZooDefs.Ids.OPEN_ACL_UNSAFE, -1, 1, 1, 1);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive oa = BinaryOutputArchive.getArchive(baos);
        tree.serialize(oa, "test");
        baos.flush();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        BinaryInputArchive ia = BinaryInputArchive.getArchive(bais);
        tree.deserialize(ia, "test");

        assertEquals("expected to have 1 acl in acl cache map", 1, tree.aclCacheSize());
        assertEquals("expected to have the same acl", ZooDefs.Ids.OPEN_ACL_UNSAFE, tree.getACL("/bug", new Stat()));

        // simulate the upgrading case where the config node will be created
        // again after leader election
        tree.addConfigNode();

        assertEquals("expected to have 2 acl in acl cache map", 2, tree.aclCacheSize());
        assertEquals("expected to have the same acl", ZooDefs.Ids.OPEN_ACL_UNSAFE, tree.getACL("/bug", new Stat()));
    }

    @Test
    public void testCachedApproximateDataSize() throws Exception {
        DataTree dt = new DataTree();
        long initialSize = dt.approximateDataSize();
        assertEquals(dt.cachedApproximateDataSize(), dt.approximateDataSize());

        // create a node
        dt.createNode("/testApproximateDataSize", new byte[20], null, -1, 1, 1, 1);
        dt.createNode("/testApproximateDataSize1", new byte[20], null, -1, 1, 1, 1);
        assertEquals(dt.cachedApproximateDataSize(), dt.approximateDataSize());

        // update data
        dt.setData("/testApproximateDataSize1", new byte[32], -1, 1, 1);
        assertEquals(dt.cachedApproximateDataSize(), dt.approximateDataSize());

        // delete a node
        dt.deleteNode("/testApproximateDataSize", -1);
        assertEquals(dt.cachedApproximateDataSize(), dt.approximateDataSize());
    }

    @Test
    public void testGetAllChildrenNumber() throws Exception {
        DataTree dt = new DataTree();
        // create a node
        dt.createNode("/all_children_test", new byte[20], null, -1, 1, 1, 1);
        dt.createNode("/all_children_test/nodes", new byte[20], null, -1, 1, 1, 1);
        dt.createNode("/all_children_test/nodes/node1", new byte[20], null, -1, 1, 1, 1);
        dt.createNode("/all_children_test/nodes/node2", new byte[20], null, -1, 1, 1, 1);
        dt.createNode("/all_children_test/nodes/node3", new byte[20], null, -1, 1, 1, 1);
        assertEquals(4, dt.getAllChildrenNumber("/all_children_test"));
        assertEquals(3, dt.getAllChildrenNumber("/all_children_test/nodes"));
        assertEquals(0, dt.getAllChildrenNumber("/all_children_test/nodes/node1"));
        //add these three init nodes:/zookeeper,/zookeeper/quota,/zookeeper/config,so the number is 8.
        assertEquals(8, dt.getAllChildrenNumber("/"));
    }

    @Test
    public void testDeserializeZxidDigest() throws Exception {
        try {
            ZooKeeperServer.setDigestEnabled(true);
            DataTree dt = new DataTree();
            dt.processTxn(new TxnHeader(13, 1000, 1, 30, ZooDefs.OpCode.create),
                    new CreateTxn("/foo", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, false, 1), null);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive oa = BinaryOutputArchive.getArchive(baos);
            dt.serializeZxidDigest(oa);
            baos.flush();

            DataTree.ZxidDigest zd = dt.getLastProcessedZxidDigest();
            assertNotNull(zd);

            // deserialize data tree
            InputArchive ia = BinaryInputArchive.getArchive(
                    new ByteArrayInputStream(baos.toByteArray()));
            dt.deserializeZxidDigest(ia, zd.getZxid());
            assertNotNull(dt.getDigestFromLoadedSnapshot());

            ia = BinaryInputArchive.getArchive(new ByteArrayInputStream(baos.toByteArray()));
            dt.deserializeZxidDigest(ia, zd.getZxid() + 1);
            assertNull(dt.getDigestFromLoadedSnapshot());
        } finally {
            ZooKeeperServer.setDigestEnabled(false);
        }
    }

    @Test
    public void testDataTreeMetrics() throws Exception {
        ServerMetrics.getMetrics().resetAll();

        long readBytes1 = 0;
        long readBytes2 = 0;
        long writeBytes1 = 0;
        long writeBytes2 = 0;

        final String TOP1 = "top1";
        final String TOP2 = "ttop2";
        final String TOP1PATH = "/" + TOP1;
        final String TOP2PATH = "/" + TOP2;
        final String CHILD1 = "child1";
        final String CHILD2 = "springishere";
        final String CHILD1PATH = TOP1PATH + "/" + CHILD1;
        final String CHILD2PATH = TOP1PATH + "/" + CHILD2;

        final int TOP2_LEN = 50;
        final int CHILD1_LEN = 100;
        final int CHILD2_LEN = 250;

        DataTree dt = new DataTree();
        dt.createNode(TOP1PATH, null, null, -1, 1, 1, 1);
        writeBytes1 += TOP1PATH.length();
        dt.createNode(TOP2PATH, new byte[TOP2_LEN], null, -1, 1, 1, 1);
        writeBytes2 += TOP2PATH.length() + TOP2_LEN;
        dt.createNode(CHILD1PATH, null, null, -1, 1, 1, 1);
        writeBytes1 += CHILD1PATH.length();
        dt.setData(CHILD1PATH, new byte[CHILD1_LEN], 1, -1, 1);
        writeBytes1 += CHILD1PATH.length() + CHILD1_LEN;
        dt.createNode(CHILD2PATH, new byte[CHILD2_LEN], null, -1, 1, 1, 1);
        writeBytes1 += CHILD2PATH.length() + CHILD2_LEN;
        dt.getData(TOP1PATH, new Stat(), null);
        readBytes1 += TOP1PATH.length() + DataTree.STAT_OVERHEAD_BYTES;
        dt.getData(TOP2PATH, new Stat(), null);
        readBytes2 += TOP2PATH.length() + TOP2_LEN + DataTree.STAT_OVERHEAD_BYTES;
        dt.statNode(CHILD2PATH, null);
        readBytes1 += CHILD2PATH.length() + DataTree.STAT_OVERHEAD_BYTES;
        dt.getChildren(TOP1PATH, new Stat(), null);
        readBytes1 += TOP1PATH.length() + CHILD1.length() + CHILD2.length() + DataTree.STAT_OVERHEAD_BYTES;
        dt.deleteNode(TOP1PATH, 1);
        writeBytes1 += TOP1PATH.length();

        Map<String, Object> values = MetricsUtils.currentServerMetrics();
        System.out.println("values:" + values);
        assertEquals(writeBytes1, values.get("sum_" + TOP1 + "_write_per_namespace"));
        assertEquals(5L, values.get("cnt_" + TOP1 + "_write_per_namespace"));
        assertEquals(writeBytes2, values.get("sum_" + TOP2 + "_write_per_namespace"));
        assertEquals(1L, values.get("cnt_" + TOP2 + "_write_per_namespace"));

        assertEquals(readBytes1, values.get("sum_" + TOP1 + "_read_per_namespace"));
        assertEquals(3L, values.get("cnt_" + TOP1 + "_read_per_namespace"));
        assertEquals(readBytes2, values.get("sum_" + TOP2 + "_read_per_namespace"));
        assertEquals(1L, values.get("cnt_" + TOP2 + "_read_per_namespace"));
    }

    /**
     * Test digest with general ops in DataTree, check that digest are
     * updated when call different ops.
     */
    @Test
    public void testDigest() throws Exception {
        try {
            // enable diegst check
            ZooKeeperServer.setDigestEnabled(true);

            DataTree dt = new DataTree();

            // create a node and check the digest is updated
            long previousDigest = dt.getTreeDigest();
            dt.createNode("/digesttest", new byte[0], null, -1, 1, 1, 1);
            assertNotEquals(dt.getTreeDigest(), previousDigest);

            // create a child and check the digest is updated
            previousDigest = dt.getTreeDigest();
            dt.createNode("/digesttest/1", "1".getBytes(), null, -1, 2, 2, 2);
            assertNotEquals(dt.getTreeDigest(), previousDigest);

            // check the digest is not chhanged when creating the same node
            previousDigest = dt.getTreeDigest();
            try {
                dt.createNode("/digesttest/1", "1".getBytes(), null, -1, 2, 2, 2);
            } catch (NodeExistsException e) { /* ignore */ }
            assertEquals(dt.getTreeDigest(), previousDigest);

            // check digest with updated data
            previousDigest = dt.getTreeDigest();
            dt.setData("/digesttest/1", "2".getBytes(), 3, 3, 3);
            assertNotEquals(dt.getTreeDigest(), previousDigest);

            // check digest with deleted node
            previousDigest = dt.getTreeDigest();
            dt.deleteNode("/digesttest/1", 5);
            assertNotEquals(dt.getTreeDigest(), previousDigest);
        } finally {
            ZooKeeperServer.setDigestEnabled(false);
        }
    }

}
