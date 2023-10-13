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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class EphemeralNodeThrottlingTest extends QuorumPeerTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(EphemeralNodeThrottlingTest.class);

    static final int DEFAULT_MAX_EPHEMERAL_NODES = 20;
    static final int DEFAULT_MAX_EPHEMERAL_NODE_BYTES = 2000;
    static final int NUM_SERVERS = 5;
    static final String PATH = "/ephemeral-throttling-test";

    @Test
    public void byteSizeTest() throws Exception {
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(200));
        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        int cumulativeBytes = 0;
        int i = 0;
        while (cumulativeBytes < 200) {
            cumulativeBytes += (PATH+i).getBytes().length;
            try {
                leaderServer.create(PATH + i++, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception e) {
                break;
            }
        }
        long actual = (long) MetricsUtils.currentServerMetrics().get("ephemeral_node_max_count_violation");
        assertEquals(1, actual);

        servers.shutDownAllServers();
    }

    @Test
    public void limitingEphemeralsTest() throws Exception {
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(DEFAULT_MAX_EPHEMERAL_NODE_BYTES));
        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        String leaderSubPath = PATH + "-leader-";
        assertTrue(checkLimitEnforcedForServer(leaderServer, leaderSubPath));

        ZooKeeper followerServer = servers.zk[servers.findAnyFollower()];
        String followerSubPath = PATH + "-follower-";
        assertTrue(checkLimitEnforcedForServer(followerServer, followerSubPath));

        long actual = (long) MetricsUtils.currentServerMetrics().get("ephemeral_node_max_count_violation");
        assertEquals(2, actual);

        servers.shutDownAllServers();
    }


    // TODO: for sequentials we can just check if byte length + 1 would hit limit. The following request will have greater than 2 bytes so don't need to be super precise.
    @Test
    public void limitingSequentialEphemeralsTest() throws Exception {
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(DEFAULT_MAX_EPHEMERAL_NODES));
        servers = LaunchServers(NUM_SERVERS);
        boolean leaderThrewError = false;
        boolean followerThrewError = false;
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        ZooKeeper followerServer = servers.zk[servers.findAnyFollower()];
        // Create nodes up to the limit
        for (int i = 0; i < DEFAULT_MAX_EPHEMERAL_NODES; i++) {
            leaderServer.create(PATH + "-leader-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            followerServer.create(PATH + "-follower-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }

        try {
            leaderServer.create(PATH + "-leader-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            leaderThrewError = true;
        }
        try {
            followerServer.create(PATH + "-follower-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            followerThrewError = true;
        }
        assertTrue(leaderThrewError && followerThrewError);

        long actual = (long) MetricsUtils.currentServerMetrics().get("ephemeral_node_max_count_violation");
        assertEquals(2, actual);

        servers.shutDownAllServers();
    }

    public boolean checkLimitEnforcedForServer(ZooKeeper server, String subPath) throws Exception {
        int leaderCumulativeBytes = 0;
        int i = 0;
        while (leaderCumulativeBytes + (subPath+i).getBytes(StandardCharsets.UTF_8).length
                < Integer.getInteger("zookeeper.ephemeral.count.limit")) {
            server.create(subPath+i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            leaderCumulativeBytes += (subPath+i).getBytes(StandardCharsets.UTF_8).length;
            i++;
        }

        try {
            server.create(subPath + "-follower-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.EphemeralCountExceededException e) {
            return true;
        }
        return false;
    }

    @Test
    public void rejectedEphemeralCreatesMetricsTest() throws Exception {
        int ephemeralExcess = DEFAULT_MAX_EPHEMERAL_NODES / 2;
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(DEFAULT_MAX_EPHEMERAL_NODES));
        servers = LaunchServers(NUM_SERVERS);
        for (int i = 0; i < DEFAULT_MAX_EPHEMERAL_NODES + ephemeralExcess; i++) {
            try {
                servers.zk[0].create(PATH + "-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.EphemeralCountExceededException e) {
                LOG.info("Encountered EphemeralCountExceededException as expected, continuing...");
            }
        }

        long actual = (long) MetricsUtils.currentServerMetrics().get("ephemeral_node_max_count_violation");
        assertEquals(ephemeralExcess, actual);

        servers.shutDownAllServers();
    }

    // Tests multithreaded creates and deletes against the leader and a follower server
    @Test
    public void multithreadedRequestsTest() throws Exception {
        int ephemeralNodeLimit = 7500;
        System.setProperty("zookeeper.ephemeral.count.limit", Integer.toString(ephemeralNodeLimit));

        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        ZooKeeper followerServer = servers.zk[servers.findAnyFollower()];

        runMultithreadedRequests(leaderServer);
        runMultithreadedRequests(followerServer);

        // TODO: What % delta do we want to allow here?
        assertEquals(ephemeralNodeLimit, leaderServer.getEphemerals().size(), ephemeralNodeLimit/20d);
        assertEquals(ephemeralNodeLimit, followerServer.getEphemerals().size(), ephemeralNodeLimit/20d);

        servers.shutDownAllServers();
    }

    private void runMultithreadedRequests(ZooKeeper server) {
        int threadPoolCount = 8;
        int deleteRequestThreads = 2;
        int createRequestThreads = threadPoolCount - deleteRequestThreads;
        // Spin up threads to repeatedly send CREATE requests to server
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolCount);
        for (int i = 0; i < createRequestThreads; i++) {
            final int threadID = i;
            executor.submit(() ->{
                long startTime = System.currentTimeMillis();
                while (System.currentTimeMillis() - startTime < 10000) {
                    try {
                        server.create(PATH+"_"+threadID+"_", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    } catch (KeeperException.EphemeralCountExceededException expectedException) {
                        //  Ignore Ephemeral Count exceeded exception, as this is expected to occur
                    } catch (Exception e) {
                        LOG.error("Thread encountered an exception but ignored it:\n" + e.getMessage());
                    }
                }
            });
        }

        // Spin up threads to repeatedly send DELETE requests to server
        // After a 1-second sleep, this should run concurrently with the create threads
        for (int i = 0; i < deleteRequestThreads; i++) {
            executor.submit(() -> {
                long startTime = System.currentTimeMillis();
                try {
                    // Brief sleep to reduce chance that ephemeral nodes not yet created
                    Thread.sleep(1000);
                    while (System.currentTimeMillis() - startTime < 6000) {
                        for (String ephemeralNode : server.getEphemerals()) {
                            server.delete(ephemeralNode, -1);
                            System.out.println("deleted node: " + ephemeralNode);
                        }
                    }
                } catch (KeeperException.EphemeralCountExceededException expectedException) {
                    //  Ignore Ephemeral Count exceeded exception, as this is expected to occur
                } catch (Exception e) {
                    LOG.error("Thread encountered an exception but ignored it:\n" + e.getMessage());
                }
            });
        }

        executor.shutdown();
        try {
            if(!executor.awaitTermination(12000, TimeUnit.MILLISECONDS)) {
                LOG.warn("Threads did not finish in the given time!");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            executor.shutdownNow();
        }
    }
}
