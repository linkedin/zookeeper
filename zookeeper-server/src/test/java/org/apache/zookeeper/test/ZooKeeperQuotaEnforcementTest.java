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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Quotas;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.DelQuotaCommand;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.apache.zookeeper.server.ServerMetrics;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZooKeeperQuotaEnforcementTest extends ClientBase {

    @BeforeClass
    public static void enableQuotas() {
        System.setProperty("zookeeper.enforceQuota", "true");
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
    }

    @AfterClass
    public static void clearQuotaProperties() {
        System.clearProperty("zookeeper.enforceQuota");
        System.clearProperty("zookeeper.extendedTypesEnabled");
    }

    @Test
    public void testCreateModesRespectCountQuota() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", 2, -1, new byte[0]);
        zk.create("/limited/a", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        for (CreateMode mode : CreateMode.values()) {
            String path = "/limited/" + mode.name();
            if (mode.isTTL()) {
                expectQuotaExceeded(() -> zk.create(path, null, Ids.OPEN_ACL_UNSAFE, mode, null, 60000));
            } else {
                expectQuotaExceeded(() -> zk.create(path, null, Ids.OPEN_ACL_UNSAFE, mode));
            }
        }
        assertEquals(2L, usage(zk, "/limited").getCount());
        zk.delete("/limited/a", -1);
        zk.create("/limited/b", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(2L, usage(zk, "/limited").getCount());
    }

    @Test
    public void testByteQuotaPreservesDataOnRejectedWrite() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", -1, 5, new byte[3]);
        zk.setData("/limited", new byte[5], -1);
        expectQuotaExceeded(() -> zk.setData("/limited", new byte[6], -1));
        expectQuotaExceeded(() -> zk.create("/limited/child", new byte[1],
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        assertArrayEquals(new byte[5], zk.getData("/limited", false, null));
        assertNull(zk.exists("/limited/child", false));
        assertEquals(5L, usage(zk, "/limited").getBytes());
        zk.setData("/limited", null, -1);
        assertNull(zk.getData("/limited", false, null));
        assertEquals(0L, usage(zk, "/limited").getBytes());
    }

    @Test
    public void testAboveLimitUpdatesMatchReleasedBehavior() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", -1, 4, new byte[6]);
        zk.setData("/limited", new byte[6], -1);
        expectQuotaExceeded(() -> zk.setData("/limited", new byte[5], -1));
        zk.setData("/limited", new byte[4], -1);
        assertEquals(4L, usage(zk, "/limited").getBytes());
    }

    @Test
    public void testMultiCreateThenSetDataUsesPendingData() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", -1, 5, new byte[0]);
        zk.multi(Arrays.asList(
            Op.create("/limited/child", new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT.toFlag()),
            Op.setData("/limited/child", new byte[4], -1)));
        assertArrayEquals(new byte[4], zk.getData("/limited/child", false, null));
        assertEquals(4L, usage(zk, "/limited").getBytes());
    }

    @Test
    public void testRejectedMultiDoesNotApplyEarlierOperations() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", -1, 5, new byte[0]);
        expectQuotaExceeded(() -> zk.multi(Arrays.asList(
            Op.create("/limited/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT.toFlag()),
            Op.setData("/limited/child", new byte[6], -1))));
        assertNull(zk.exists("/limited/child", false));
        assertEquals(1L, usage(zk, "/limited").getCount());
        assertEquals(0L, usage(zk, "/limited").getBytes());
        zk.create("/limited/after", new byte[5], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void testAsyncQuotaErrorKeepsConnectionUsable() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", 1, -1, new byte[0]);
        AtomicInteger result = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);
        zk.create("/limited/child", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
            (rc, path, ctx, name) -> {
                result.set(rc);
                done.countDown();
            }, null);
        assertTrue(done.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
        assertEquals(-125, result.get());
        assertNull(zk.exists("/limited/child", false));
        zk.create("/limited-other", new byte[8], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void testSoftQuotasDoNotRejectWrites() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", -1, -1, new byte[0]);
        zk.setData(Quotas.quotaZookeeper + "/limited/" + Quotas.limitNode,
            "count=1,bytes=0".getBytes(UTF_8), -1);
        zk.create("/limited/child", new byte[2], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEquals(2L, usage(zk, "/limited").getCount());
        assertEquals(2L, usage(zk, "/limited").getBytes());
    }

    @Test
    public void testQuotaMetricsTrackLimitsUsageAndRejections() throws Exception {
        ServerMetrics.getMetrics().resetAll();
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", 2, 4, new byte[1]);
        StatsTrack limits = new StatsTrack();
        limits.setCount(1);
        limits.setBytes(0);
        SetQuotaCommand.createQuota(zk, "/limited", limits);
        assertMetric("quota_count_limit_per_namespace", 2);
        assertMetric("quota_bytes_limit_per_namespace", 4);
        assertMetric("quota_count_usage_per_namespace", 1);
        assertMetric("quota_bytes_usage_per_namespace", 1);
        zk.create("/limited/child", new byte[3], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertMetric("quota_count_usage_per_namespace", 2);
        assertMetric("quota_bytes_usage_per_namespace", 4);
        expectQuotaExceeded(() -> zk.create("/limited/third", null,
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        expectQuotaExceeded(() -> zk.setData("/limited", new byte[2], -1));
        assertMetric("quota_exceeded_error_per_namespace", 2);
        zk.delete("/limited/child", -1);
        assertMetric("quota_count_usage_per_namespace", 1);
        assertMetric("quota_bytes_usage_per_namespace", 1);
        limits = new StatsTrack();
        limits.setCountHardLimit(3);
        SetQuotaCommand.createQuota(zk, "/limited", limits);
        assertMetric("quota_count_limit_per_namespace", 3);
        DelQuotaCommand.delQuota(zk, "/limited", null);
        assertFalse(MetricsUtils.currentServerMetrics().containsKey("limited_quota_count_limit_per_namespace"));
        assertFalse(MetricsUtils.currentServerMetrics().containsKey("limited_quota_bytes_usage_per_namespace"));
        assertMetric("quota_exceeded_error_per_namespace", 2);
    }

    @Test
    public void testQuotaAccountingAndEnforcementSurviveRestart() throws Exception {
        ZooKeeper zk = createClient();
        createQuota(zk, "/limited", 2, 8, new byte[4]);
        zk.create("/limited/child", new byte[4], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.close();
        stopServer();
        startServer();
        ZooKeeper restarted = createClient();
        assertEquals(2L, usage(restarted, "/limited").getCount());
        assertEquals(8L, usage(restarted, "/limited").getBytes());
        expectQuotaExceeded(() -> restarted.create("/limited/another", null,
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        expectQuotaExceeded(() -> restarted.setData("/limited", new byte[5], -1));
        restarted.delete("/limited/child", -1);
        restarted.setData("/limited", new byte[8], -1);
        assertEquals(1L, usage(restarted, "/limited").getCount());
        assertEquals(8L, usage(restarted, "/limited").getBytes());
    }

    private static void createQuota(ZooKeeper zk, String path, int count, long bytes, byte[] data) throws Exception {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String quotaPath = Quotas.quotaZookeeper + path;
        zk.create(quotaPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        String limits = "count=-1,bytes=-1=;byteHardLimit=" + bytes + ";countHardLimit=" + count;
        zk.create(quotaPath + "/" + Quotas.limitNode, limits.getBytes(UTF_8),
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(quotaPath + "/" + Quotas.statNode, "count=0,bytes=0".getBytes(UTF_8),
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private static StatsTrack usage(ZooKeeper zk, String path) throws Exception {
        return new StatsTrack(new String(zk.getData(Quotas.statPath(path), false, null), UTF_8));
    }

    private static void assertMetric(String name, long expected) {
        assertEquals(name, Long.valueOf(expected), MetricsUtils.currentServerMetrics().get("limited_" + name));
    }

    private static void expectQuotaExceeded(QuotaOperation operation) throws Exception {
        try {
            operation.run();
            fail("Expected quota rejection");
        } catch (KeeperException.QuotaExceededException expected) {
            assertEquals(-125, expected.code().intValue());
        }
    }

    private interface QuotaOperation {
        void run() throws Exception;
    }
}
