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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.JMXEnv;
import org.junit.jupiter.api.Test;

public class RequestMetricsJmxTest extends ClientBase {

    private static final String[] COUNTER_ATTRIBUTES = {
        "ReadRequestCount", "WriteRequestCount",
        "MinReadRequestLatency", "AvgReadRequestLatency", "MaxReadRequestLatency",
        "MinWriteRequestLatency", "AvgWriteRequestLatency", "MaxWriteRequestLatency",
        "ConnectionCreateCount", "ConnectionCloseCount", "SessionCreateCount", "SessionCloseCount"};

    @Test
    public void testRequestCountersThroughMBeanServer() throws Exception {
        MBeanServerConnection mbeanServer = JMXEnv.conn();
        ObjectName serverBean = JMXEnv.getServerBean();

        try (ZooKeeper zk = createClient()) {
            mbeanServer.invoke(serverBean, "resetStatistics", null, null);
            for (String attribute : COUNTER_ATTRIBUTES) {
                assertEquals(0L, mbeanServer.getAttribute(serverBean, attribute), attribute);
            }

            // Counted writes: create, setData, setACL, multi with a create, delete.
            zk.create("/metrics", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.setData("/metrics", new byte[1], -1);
            zk.setACL("/metrics", Ids.OPEN_ACL_UNSAFE, -1);
            zk.multi(Arrays.asList(Op.create("/metrics/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)));
            zk.delete("/metrics/child", -1);

            // Counted reads: getData, exists, getChildren, getACL.
            zk.getData("/metrics", false, null);
            zk.exists("/metrics", false);
            zk.getChildren("/metrics", false);
            zk.getACL("/metrics", new Stat());

            // Not counted: check-only multi, sync, create2.
            zk.multi(Arrays.asList(Op.check("/metrics", -1)));
            CountDownLatch synced = new CountDownLatch(1);
            zk.sync("/metrics", (rc, path, ctx) -> synced.countDown(), null);
            assertTrue(synced.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS));
            zk.create("/metrics2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new Stat());

            assertEquals(4L, mbeanServer.getAttribute(serverBean, "ReadRequestCount"));
            assertEquals(5L, mbeanServer.getAttribute(serverBean, "WriteRequestCount"));
            assertLatencyOrder(mbeanServer, serverBean, "Read");
            assertLatencyOrder(mbeanServer, serverBean, "Write");

            mbeanServer.invoke(serverBean, "resetStatistics", null, null);
            for (String attribute : COUNTER_ATTRIBUTES) {
                assertEquals(0L, mbeanServer.getAttribute(serverBean, attribute), attribute);
            }
        }
    }

    private static void assertLatencyOrder(MBeanServerConnection mbeanServer, ObjectName serverBean, String kind) throws Exception {
        long min = (Long) mbeanServer.getAttribute(serverBean, "Min" + kind + "RequestLatency");
        long avg = (Long) mbeanServer.getAttribute(serverBean, "Avg" + kind + "RequestLatency");
        long max = (Long) mbeanServer.getAttribute(serverBean, "Max" + kind + "RequestLatency");
        assertTrue(0 <= min && min <= avg && avg <= max, kind + " latency min/avg/max: " + min + "/" + avg + "/" + max);
    }

}
