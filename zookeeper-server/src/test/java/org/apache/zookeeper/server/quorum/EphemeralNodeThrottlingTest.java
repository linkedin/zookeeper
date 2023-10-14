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

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.metrics.MetricsUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class EphemeralNodeThrottlingTest extends QuorumPeerTestBase {

    protected static final Logger LOG = LoggerFactory.getLogger(EphemeralNodeThrottlingTest.class);

    public static final String EPHEMERAL_BYTE_LIMIT_KEY = "zookeeper.ephemeralNodes.total.byte.limit";
    public static final String EPHEMERAL_BYTE_LIMIT_VIOLATION_KEY = "ephemeral_node_limit_violation";
    // static final int DEFAULT_MAX_EPHEMERAL_NODES = 20;
    static final int DEFAULT_EPHEMERALNODES_TOTAL_BYTE_LIMIT = 2000;
    static final int NUM_SERVERS = 5;
    static final String TEST_PATH = "/ephemeral-throttling-test";

    @Test
    public void byteSizeTest() throws Exception {
        int totalEphemeralNodesByteLimit = 200;
        System.setProperty(EPHEMERAL_BYTE_LIMIT_KEY, Integer.toString(totalEphemeralNodesByteLimit));
        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        int cumulativeBytes = 0;
        int i = 0;
        while (cumulativeBytes <= totalEphemeralNodesByteLimit) {
            cumulativeBytes += BinaryOutputArchive.getSerializedStringByteSize(TEST_PATH +i);
            try {
                leaderServer.create(TEST_PATH + i++, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception e) {
                break;
            }
        }
        long actual = (long) MetricsUtils.currentServerMetrics().get(EPHEMERAL_BYTE_LIMIT_VIOLATION_KEY);
        assertEquals(1, actual);

        servers.shutDownAllServers();
    }

    @Test
    public void limitingEphemeralsTest() throws Exception {
        System.setProperty(EPHEMERAL_BYTE_LIMIT_KEY, Integer.toString(DEFAULT_EPHEMERALNODES_TOTAL_BYTE_LIMIT));
        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        String leaderSubPath = TEST_PATH + "-leader-";
        assertTrue(checkLimitEnforcedForServer(leaderServer, leaderSubPath, CreateMode.EPHEMERAL));

        ZooKeeper followerServer = servers.zk[servers.findAnyFollower()];
        String followerSubPath = TEST_PATH + "-follower-";
        assertTrue(checkLimitEnforcedForServer(followerServer, followerSubPath, CreateMode.EPHEMERAL));

        // Assert both servers emitted failure metric
        long actual = (long) MetricsUtils.currentServerMetrics().get(EPHEMERAL_BYTE_LIMIT_VIOLATION_KEY);
        assertEquals(2, actual);

        servers.shutDownAllServers();
    }

    @Test
    public void limitingSequentialEphemeralsTest() throws Exception {
        System.setProperty(EPHEMERAL_BYTE_LIMIT_KEY, Integer.toString(DEFAULT_EPHEMERALNODES_TOTAL_BYTE_LIMIT));
        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        String leaderSubPath = TEST_PATH + "-leader-";
        assertTrue(checkLimitEnforcedForServer(leaderServer, leaderSubPath, CreateMode.EPHEMERAL_SEQUENTIAL));

        ZooKeeper followerServer = servers.zk[servers.findAnyFollower()];
        String followerSubPath = TEST_PATH + "-follower-";
        assertTrue(checkLimitEnforcedForServer(followerServer, followerSubPath, CreateMode.EPHEMERAL_SEQUENTIAL));

        // Assert both servers emitted failure metric
        long actual = (long) MetricsUtils.currentServerMetrics().get(EPHEMERAL_BYTE_LIMIT_VIOLATION_KEY);
        assertEquals(2, actual);

        servers.shutDownAllServers();
    }

    public boolean checkLimitEnforcedForServer(ZooKeeper server, String subPath, CreateMode mode) throws Exception {
        if (!mode.isEphemeral()) {
            return false;
        }

        int limit = Integer.getInteger(EPHEMERAL_BYTE_LIMIT_KEY);
        int cumulativeBytes = 0;

        if (mode.isSequential()) {
            int lastPathBytes = 0;
            while (cumulativeBytes + lastPathBytes <= limit) {
                String path = server.create(TEST_PATH + "-leader-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                lastPathBytes = BinaryOutputArchive.getSerializedStringByteSize(path);
                cumulativeBytes += lastPathBytes;
            }

            try {
                server.create(TEST_PATH + "-leader-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            } catch (KeeperException.TotalEphemeralLimitExceeded e) {
                return true;
            }
            return false;
        } else {
            int i = 0;
            while (cumulativeBytes + BinaryOutputArchive.getSerializedStringByteSize(subPath + i)
                    <= limit) {
                server.create(subPath + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                cumulativeBytes += BinaryOutputArchive.getSerializedStringByteSize(subPath + i);
                i++;
            }
            try {
                server.create(subPath + "-follower-" + i, new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.TotalEphemeralLimitExceeded e) {
                return true;
            }
            return false;
        }
    }

    @Test
    public void rejectedEphemeralMetricsTest() throws Exception {
        System.setProperty(EPHEMERAL_BYTE_LIMIT_KEY, Integer.toString(DEFAULT_EPHEMERALNODES_TOTAL_BYTE_LIMIT));
        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        int expectedLimitExceededAttempts = 10;
        int i = expectedLimitExceededAttempts;
        int limit = Integer.getInteger(EPHEMERAL_BYTE_LIMIT_KEY);
        int cumulativeBytes = 0;
        int lastPathBytes = 0;
        while (i > 0 || cumulativeBytes + lastPathBytes <= limit) {
            try {
                String path = leaderServer.create(TEST_PATH + "-leader-", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                lastPathBytes = BinaryOutputArchive.getSerializedStringByteSize(path);
                cumulativeBytes += lastPathBytes;
            } catch (KeeperException.TotalEphemeralLimitExceeded e) {
                LOG.info("Encountered TotalEphemeralLimitExceeded as expected, continuing...");
                i--;
            }
        }

        long actual = (long) MetricsUtils.currentServerMetrics().get(EPHEMERAL_BYTE_LIMIT_VIOLATION_KEY);
        assertEquals(expectedLimitExceededAttempts, actual);

        servers.shutDownAllServers();
    }

    // Tests multithreaded creates and deletes against the leader and a follower server
    @Test
    public void multithreadedRequestsTest() throws Exception {
        // 50% of 1mb jute max buffer
        int totalEphemeralNodesByteLimit = (int) (Math.pow(2d, 20d) * .5);
        System.setProperty("zookeeper.ephemeralNodes.total.byte.limit", Integer.toString(totalEphemeralNodesByteLimit));

        servers = LaunchServers(NUM_SERVERS);
        ZooKeeper leaderServer = servers.zk[servers.findLeader()];
        ZooKeeper followerServer = servers.zk[servers.findAnyFollower()];

        runMultithreadedRequests(leaderServer);
        runMultithreadedRequests(followerServer);

        // TODO: What % delta do we want to allow here?
        // Expensive calculation of total byte size for session ephemerals
        long time = System.currentTimeMillis();
        int leaderSessionEphemeralsByteSum = 0;
        for (String nodePath : leaderServer.getEphemerals()) {
            leaderSessionEphemeralsByteSum += BinaryOutputArchive.getSerializedStringByteSize(nodePath);
        }
        assertEquals(totalEphemeralNodesByteLimit, leaderSessionEphemeralsByteSum, totalEphemeralNodesByteLimit/20d);

        int followerSessionEphemeralsByteSum = 0;
        for (String nodePath : leaderServer.getEphemerals()) {
            followerSessionEphemeralsByteSum += BinaryOutputArchive.getSerializedStringByteSize(nodePath);
        }
        assertEquals(totalEphemeralNodesByteLimit, followerSessionEphemeralsByteSum, totalEphemeralNodesByteLimit/20d);

        System.out.println("--- total time to calculate sizes was : " + (System.currentTimeMillis() - time) + " ms -----");
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
                        server.create(TEST_PATH +"_"+threadID+"_", new byte[512], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                    } catch (KeeperException.TotalEphemeralLimitExceeded expectedException) {
                        //  Ignore Ephemeral Count exceeded exception, as this is expected to occur
                    }
                }
            });
        }

        // Spin up threads to repeatedly send DELETE requests to server
        // After a 1-second sleep, this should run concurrently with the create threads, but then end before create threads
        // so that we still have time to hit the limit and can then assert that limit was upheld correctly
        for (int i = 0; i < deleteRequestThreads; i++) {
            executor.submit(() -> {
                long startTime = System.currentTimeMillis();
                try {
                    // Brief sleep to reduce chance that ephemeral nodes not yet created
                    Thread.sleep(1000);
                    while (System.currentTimeMillis() - startTime < 6000) {
                        for (String ephemeralNode : server.getEphemerals()) {
                            server.delete(ephemeralNode, -1);
                        }
                    }
                } catch (KeeperException.TotalEphemeralLimitExceeded expectedException) {
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
