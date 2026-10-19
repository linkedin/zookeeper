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

package org.apache.zookeeper.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Make sure minimal Apache Curator APIs work correctly. As it's a widely used ZooKeeper
 * client library we should not break it.
 */
public class TestApacheCuratorCompatibility {
    private static final int TIMEOUT_MS = 5000;

    @BeforeClass
    public static void enableQuotas() {
        System.setProperty("zookeeper.enforceQuota", "true");
    }

    @AfterClass
    public static void clearQuotaProperty() {
        System.clearProperty("zookeeper.enforceQuota");
    }

    @Test
    public void testQuotaRejectionKeepsCuratorUsable() throws Exception {
        try (TestingServer server = new TestingServer();
             CuratorFramework client = CuratorFrameworkFactory.newClient(
                 server.getConnectString(), new RetryOneTime(1))) {
            client.start();
            assertTrue(client.blockUntilConnected(TIMEOUT_MS, TimeUnit.MILLISECONDS));
            client.create().forPath("/limited", new byte[0]);
            StatsTrack limits = new StatsTrack();
            limits.setCountHardLimit(1);
            SetQuotaCommand.createQuota(client.getZookeeperClient().getZooKeeper(), "/limited", limits);
            try {
                client.create().forPath("/limited/child", new byte[0]);
                fail("Expected a native quota exception from Curator");
            } catch (KeeperException.QuotaExceededException expected) {
                assertEquals(-125, expected.code().intValue());
            }
            assertNull(client.checkExists().forPath("/limited/child"));
            assertEquals("/unlimited", client.create().forPath("/unlimited", new byte[0]));
        }
    }

    @Test
    public void testBasicUsageOfApisAndRecipes() throws Exception {
        try (TestingServer server = new TestingServer()) {
            RetryOneTime retryPolicy = new RetryOneTime(1);
            try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy)) {
                try (CuratorCache cache = CuratorCache.build(client, "/base/path")) {
                    client.start();
                    cache.start();

                    BlockingQueue<String> paths = new LinkedBlockingQueue<>();
                    cache.listenable().addListener((dummy1, dummy2, data) -> paths.add(data.getPath()));

                    client.create().creatingParentsIfNeeded().forPath("/base/path/1");
                    client.create().creatingParentsIfNeeded().forPath("/base/path/2");
                    client.create().creatingParentsIfNeeded().forPath("/base/path/1/a");
                    client.create().creatingParentsIfNeeded().forPath("/base/path/2/a");

                    assertEquals("/base/path", poll(paths));
                    assertEquals("/base/path/1", poll(paths));
                    assertEquals("/base/path/2", poll(paths));
                    assertEquals("/base/path/1/a", poll(paths));
                    assertEquals("/base/path/2/a", poll(paths));
                }
            }
        }
    }

    private static String poll(BlockingQueue<String> queue) {
        try {
            String value = queue.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            assertNotNull("Event poll timed out", value);
            return value;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
