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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.ListQuotaCommand;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnforceQuotaTest extends ClientBase {

    @BeforeClass
    public static void disableQuotas() {
        System.clearProperty("zookeeper.enforceQuota");
    }

    @Test
    public void testHardLimitsAreDisabledByDefaultButUsageIsUpdated() throws Exception {
        ZooKeeper zk = createClient();
        zk.create("/disabled", new byte[1], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack limits = new StatsTrack();
        limits.setCountHardLimit(1);
        limits.setByteHardLimit(1);
        SetQuotaCommand.createQuota(zk, "/disabled", limits);
        zk.create("/disabled/child", new byte[2], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.setData("/disabled", new byte[3], -1);
        StatsTrack usage = ListQuotaCommand.listQuota(zk, "/disabled").get(1);
        assertEquals(2L, usage.getCount());
        assertEquals(5L, usage.getBytes());
        assertEquals(Long.valueOf(1), MetricsUtils.currentServerMetrics().get("disabled_quota_count_limit_per_namespace"));
        assertEquals(Long.valueOf(1), MetricsUtils.currentServerMetrics().get("disabled_quota_bytes_limit_per_namespace"));
        assertEquals(Long.valueOf(2), MetricsUtils.currentServerMetrics().get("disabled_quota_count_usage_per_namespace"));
        assertEquals(Long.valueOf(5), MetricsUtils.currentServerMetrics().get("disabled_quota_bytes_usage_per_namespace"));
        assertNull(MetricsUtils.currentServerMetrics().get("disabled_quota_exceeded_error_per_namespace"));
    }
}
