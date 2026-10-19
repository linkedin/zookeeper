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

package org.apache.zookeeper.server.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.Map;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;

public class QuotaMetricsUtilsTest extends ClientBase {

    @Test
    public void testAggregatesQuotaRootsByTopNamespace() throws Exception {
        ZooKeeper zk = createClient();
        zk.create("/apps", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/apps/a", new byte[2], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/apps/a/child", new byte[3], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/apps/b", new byte[7], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/services", new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        StatsTrack first = new StatsTrack();
        first.setCount(1);
        first.setBytes(1);
        first.setCountHardLimit(4);
        first.setByteHardLimit(10);
        SetQuotaCommand.createQuota(zk, "/apps/a", first);
        StatsTrack second = new StatsTrack();
        second.setCount(3);
        second.setBytes(20);
        SetQuotaCommand.createQuota(zk, "/apps/b", second);
        StatsTrack separate = new StatsTrack();
        separate.setCountHardLimit(2);
        separate.setByteHardLimit(5);
        SetQuotaCommand.createQuota(zk, "/services", separate);

        DataTree tree = serverFactory.getZooKeeperServer().getZKDatabase().getDataTree();
        Map<String, Number> countLimits = QuotaMetricsUtils.getQuotaCountLimit(tree);
        assertEquals(2, countLimits.size());
        assertEquals(7L, countLimits.get("apps").longValue());
        assertEquals(2L, countLimits.get("services").longValue());
        assertEquals(30L, QuotaMetricsUtils.getQuotaBytesLimit(tree).get("apps").longValue());
        assertEquals(5L, QuotaMetricsUtils.getQuotaBytesLimit(tree).get("services").longValue());
        assertEquals(3L, QuotaMetricsUtils.getQuotaCountUsage(tree).get("apps").longValue());
        assertEquals(1L, QuotaMetricsUtils.getQuotaCountUsage(tree).get("services").longValue());
        assertEquals(12L, QuotaMetricsUtils.getQuotaBytesUsage(tree).get("apps").longValue());
        assertEquals(1L, QuotaMetricsUtils.getQuotaBytesUsage(tree).get("services").longValue());
    }

    @Test
    public void testMissingQuotaTreeProducesNoNamespaceValues() {
        assertTrue(QuotaMetricsUtils.getQuotaCountLimit(null).isEmpty());
        assertTrue(QuotaMetricsUtils.getQuotaBytesLimit(new DataTree()).isEmpty());
        assertTrue(QuotaMetricsUtils.getQuotaCountUsage(new DataTree()).isEmpty());
        assertTrue(QuotaMetricsUtils.getQuotaBytesUsage(null).isEmpty());
    }
}
