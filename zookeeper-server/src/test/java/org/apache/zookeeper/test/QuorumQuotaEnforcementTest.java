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
import static org.junit.Assert.fail;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.StatsTrack;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.cli.ListQuotaCommand;
import org.apache.zookeeper.cli.SetQuotaCommand;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class QuorumQuotaEnforcementTest extends QuorumBase {

    @BeforeClass
    public static void enableQuotas() {
        System.setProperty("zookeeper.enforceQuota", "true");
    }

    @AfterClass
    public static void clearQuotaProperty() {
        System.clearProperty("zookeeper.enforceQuota");
    }

    @Test
    public void testHardQuotasThroughFollowerAndAfterLeaderChange() throws Exception {
        ZooKeeper leaderClient = createClient(new CountdownWatcher(), ServerState.LEADING);
        ZooKeeper followerClient = createClient(new CountdownWatcher(), ServerState.FOLLOWING);
        leaderClient.create("/limited", new byte[2], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        StatsTrack limits = new StatsTrack();
        limits.setCountHardLimit(2);
        limits.setByteHardLimit(4);
        SetQuotaCommand.createQuota(leaderClient, "/limited", limits);
        leaderClient.create("/limited/child", new byte[2], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertCountRejected(followerClient, "/limited/follower");
        try {
            followerClient.setData("/limited", new byte[3], -1);
            fail("Expected byte quota rejection through a follower");
        } catch (KeeperException.QuotaExceededException expected) {
            assertEquals(-125, expected.code().intValue());
        }

        QuorumPeer previousLeader = getPeerList().get(getLeaderIndex());
        shutdown(previousLeader);
        waitFor("replacement leader election", () -> getPeerList().stream()
            .anyMatch(peer -> peer != previousLeader && peer.isAlive() && peer.getPeerState() == ServerState.LEADING),
            CONNECTION_TIMEOUT / 1000);
        ZooKeeper reconnected = createClient();
        assertCountRejected(reconnected, "/limited/after-election");
        reconnected.delete("/limited/child", -1);
        reconnected.setData("/limited", new byte[4], -1);
        StatsTrack usage = ListQuotaCommand.listQuota(reconnected, "/limited").get(1);
        assertEquals(1L, usage.getCount());
        assertEquals(4L, usage.getBytes());
    }

    private static void assertCountRejected(ZooKeeper zk, String path) throws Exception {
        try {
            zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fail("Expected count quota rejection");
        } catch (KeeperException.QuotaExceededException expected) {
            assertNull(zk.exists(path, false));
        }
    }
}
