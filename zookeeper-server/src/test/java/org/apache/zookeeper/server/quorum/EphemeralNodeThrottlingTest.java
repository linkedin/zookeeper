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

package org.apache.zookeeper.server.quorum;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EphemeralNodeThrottlingTest extends QuorumPeerTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(EphemeralNodeThrottlingTest.class);

    static final int MAX_EPHEMERAL_NODES = 20;
    static final int NUM_SERVERS = 5;
    static final String PATH = "/ephemeral-throttling-test";

    @Test()
    public void limitingEphemeralsTest() throws Exception {
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
        servers = LaunchServers(NUM_SERVERS);
        boolean threwError = false;
        // Create nodes up to the limit
        for (int i = 0; i < MAX_EPHEMERAL_NODES; i++) {
            servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        try {
            servers.zk[0].create(PATH + "-" + MAX_EPHEMERAL_NODES, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            threwError = true;
        }
        assertTrue(threwError);
    }

    @Test()
    public void limitingSequentialEphemeralsTest() throws Exception {
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
        servers = LaunchServers(NUM_SERVERS);
        boolean threwError = false;
        // Create nodes up to the limit
        for (int i = 0; i < MAX_EPHEMERAL_NODES; i++) {
            servers.zk[0].create(PATH, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        try {
            servers.zk[0].create(PATH, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            threwError = true;
        }
        assertTrue(threwError);
    }

    /* Verify that the ephemeral limit enforced correctly when there are delete operations. */
    @Test
    public void limitingEphemeralsWithDeletesTest() throws Exception {
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
        servers = LaunchServers(NUM_SERVERS);
        for (int i = 0; i < MAX_EPHEMERAL_NODES; i++) {
            servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        boolean threwError = false;

        try {
            servers.zk[0].create(PATH + "-foo", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            threwError = true;
        }
        assertTrue(threwError);
        threwError = false;

        for (int i = 0; i < MAX_EPHEMERAL_NODES / 2; i++) {
            servers.zk[0].delete(PATH + "-" + i, -1);
        }
        for (int i = 0; i < MAX_EPHEMERAL_NODES / 2; i++) {
            servers.zk[0].create(PATH, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        try {
            servers.zk[0].create(PATH, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            threwError = true;
        }
        assertTrue(threwError);
    }

    /* Check that our emitted metric around the number of request rejections from too many ephemerals is accurate. */
    @Test
    public void rejectedEphemeralCreatesMetricsTest() throws Exception {
        int ephemeralExcess = MAX_EPHEMERAL_NODES / 2;
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(MAX_EPHEMERAL_NODES));
        servers = LaunchServers(NUM_SERVERS);
        for (int i = 0; i < MAX_EPHEMERAL_NODES + ephemeralExcess; i++) {
            try {
                servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.EphemeralCountExceededException e) {
                LOG.info("Encountered EphemeralCountExceededException as expected, continuing...");
            }
        }

        long actual = (long) MetricsUtils.currentServerMetrics().get("ephemeral_node_max_count_violation");
        assertEquals(ephemeralExcess, actual);
    }
}
